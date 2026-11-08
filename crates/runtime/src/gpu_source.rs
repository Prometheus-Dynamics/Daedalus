//! Where the executor's GPU context comes from. The executor holds a source, not a device: the
//! device opens on the first call to [`GpuSource::get`], which the executor makes at the first
//! segment that runs on a GPU, so a graph whose GPU segments never run creates no device.

use daedalus_gpu::{GpuContextHandle, GpuError};

/// A GPU context, opened by its first call to [`get`](Self::get); later calls return the same
/// context. A failed open is returned as an error, and the executor applies the node's
/// auto or gpu rules to it.
pub trait GpuSource: Send + Sync {
    /// The context, opened on the first call.
    fn get(&self) -> Result<&GpuContextHandle, GpuError>;

    /// How many times the context has been requested (a test hook; sources that do not count
    /// report 0).
    fn accesses(&self) -> u64 {
        0
    }
}

/// A context that already exists (the handle is given, not opened).
pub struct ReadyGpu(pub GpuContextHandle);

impl GpuSource for ReadyGpu {
    fn get(&self) -> Result<&GpuContextHandle, GpuError> {
        Ok(&self.0)
    }
}

/// The first wait after a failed open; each further failure doubles it, up to [`RETRY_CAP`].
pub const RETRY_BASE: core::time::Duration = core::time::Duration::from_millis(250);

/// The longest wait between two opens after failures: a failed open is retried within this
/// bound, never given up (the failure-handling rule).
pub const RETRY_CAP: core::time::Duration = core::time::Duration::from_secs(5);

#[derive(Default)]
struct Failure {
    attempts: u32,
    retry_at: Option<std::time::Instant>,
    last: Option<String>,
}

/// A GPU source that opens its context with an opener, and on a failed open keeps the error for
/// a backoff that grows up to the cap. A request inside the backoff returns the same error at
/// once, without another open; the first request after it tries the opener again. A request never
/// sleeps or loops, so no call blocks on the backoff.
///
/// A context that is lost (the backend reports it, or [`mark_lost`](Self::mark_lost) is called)
/// is replaced by the next request under the same backoff, the first wait being the base delay.
/// Each opened context is kept until the source is dropped, so a reference handed out for one
/// context stays valid even after the context is replaced; a lost device is therefore held (not
/// freed) until then, which happens once per loss.
pub struct RetryingGpu {
    open: Box<dyn Fn() -> Result<GpuContextHandle, GpuError> + Send + Sync>,
    base: core::time::Duration,
    cap: core::time::Duration,
    /// Every context opened, in order; a `Box`'s contents never move, so `current` stays valid.
    // The box is what keeps each context at a fixed address while the vector grows.
    #[allow(clippy::vec_box)]
    opened: crate::sync::Mutex<Vec<Box<GpuContextHandle>>>,
    /// The current context: a pointer into `opened`, or null while none is open.
    current: std::sync::atomic::AtomicPtr<GpuContextHandle>,
    /// Set by [`mark_lost`](Self::mark_lost): the current context is replaced on the next request.
    lost: std::sync::atomic::AtomicBool,
    failure: crate::sync::Mutex<Failure>,
    accesses: std::sync::atomic::AtomicU64,
}

impl RetryingGpu {
    /// A source that opens its context with `open` on the first request.
    pub fn new(
        open: impl Fn() -> Result<GpuContextHandle, GpuError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            open: Box::new(open),
            base: RETRY_BASE,
            cap: RETRY_CAP,
            opened: crate::sync::Mutex::new(Vec::new()),
            current: std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()),
            lost: std::sync::atomic::AtomicBool::new(false),
            failure: crate::sync::Mutex::new(Failure::default()),
            accesses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The first wait after a failure and the cap on it (clamped to [`RETRY_CAP`]).
    pub fn with_backoff(mut self, base: core::time::Duration, cap: core::time::Duration) -> Self {
        self.cap = cap.min(RETRY_CAP);
        self.base = base.min(self.cap);
        self
    }

    /// Whether a context is open (and not lost).
    pub fn created(&self) -> bool {
        !self
            .current
            .load(std::sync::atomic::Ordering::Acquire)
            .is_null()
    }

    /// The current context is lost: the next request replaces it, under the backoff.
    pub fn mark_lost(&self, reason: impl Into<String>) {
        let mut failure = self.failure.lock();
        failure.last = Some(reason.into());
        failure.attempts = 0;
        failure.retry_at = Some(std::time::Instant::now() + self.delay(1));
        self.lost.store(true, std::sync::atomic::Ordering::Release);
    }

    fn delay(&self, attempts: u32) -> core::time::Duration {
        self.base
            .checked_mul(1u32 << attempts.min(16))
            .unwrap_or(self.cap)
            .min(self.cap)
    }

    fn current(&self) -> Option<&GpuContextHandle> {
        let ptr = self.current.load(std::sync::atomic::Ordering::Acquire);
        // Safety: a non-null `current` points into a `Box` in `opened`, which is never removed
        // while `self` lives, so the pointee outlives the borrow of `self`.
        (!ptr.is_null()).then(|| unsafe { &*ptr })
    }

    fn is_lost(&self, handle: &GpuContextHandle) -> bool {
        self.lost.load(std::sync::atomic::Ordering::Acquire) || handle.is_lost()
    }

    /// The slow path: the context is missing or lost, so open one (or report the backoff).
    fn reopen(&self) -> Result<&GpuContextHandle, GpuError> {
        let mut failure = self.failure.lock();
        // Another request may have reopened it while this one waited for the lock.
        if let Some(handle) = self.current() {
            if !self.is_lost(handle) {
                return Ok(handle);
            }
            // The backend's own reason when it has one, else the one `mark_lost` recorded.
            if let Some(reason) = handle.lost_reason() {
                failure.last = Some(reason);
            } else if failure.last.is_none() {
                failure.last = Some("the GPU context was lost".to_string());
            }
            self.current
                .store(std::ptr::null_mut(), std::sync::atomic::Ordering::Release);
            self.lost.store(false, std::sync::atomic::Ordering::Release);
            if failure.retry_at.is_none() {
                failure.attempts = 0;
                failure.retry_at = Some(std::time::Instant::now() + self.delay(1));
            }
        }
        if let Some(retry_at) = failure.retry_at
            && std::time::Instant::now() < retry_at
        {
            return Err(GpuError::Internal(failure.last.clone().unwrap_or_default()));
        }
        match (self.open)() {
            Ok(handle) => {
                let mut opened = self.opened.lock();
                opened.push(Box::new(handle));
                let ptr: *mut GpuContextHandle = &mut **opened.last_mut().expect("just pushed");
                self.current
                    .store(ptr, std::sync::atomic::Ordering::Release);
                self.lost.store(false, std::sync::atomic::Ordering::Release);
                *failure = Failure::default();
                drop(opened);
                // Safety: as in `current`.
                Ok(unsafe { &*ptr })
            }
            Err(err) => {
                failure.attempts = failure.attempts.saturating_add(1);
                failure.retry_at = Some(std::time::Instant::now() + self.delay(failure.attempts));
                failure.last = Some(err.to_string());
                Err(err)
            }
        }
    }
}

impl GpuSource for RetryingGpu {
    fn get(&self) -> Result<&GpuContextHandle, GpuError> {
        self.accesses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(handle) = self.current()
            && !self.is_lost(handle)
        {
            return Ok(handle);
        }
        self.reopen()
    }

    fn accesses(&self) -> u64 {
        self.accesses.load(std::sync::atomic::Ordering::Relaxed)
    }
}
