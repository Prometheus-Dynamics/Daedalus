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
/// a backoff that grows up to [`RETRY_CAP`]. A request inside the backoff returns the same error
/// at once, without another open; the first request after it tries the opener again. A request
/// never sleeps or loops, so no call blocks on the backoff. The context, once opened, is kept.
pub struct RetryingGpu {
    open: Box<dyn Fn() -> Result<GpuContextHandle, GpuError> + Send + Sync>,
    handle: std::sync::OnceLock<GpuContextHandle>,
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
            handle: std::sync::OnceLock::new(),
            failure: crate::sync::Mutex::new(Failure::default()),
            accesses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Whether the context has been opened.
    pub fn created(&self) -> bool {
        self.handle.get().is_some()
    }
}

impl GpuSource for RetryingGpu {
    fn get(&self) -> Result<&GpuContextHandle, GpuError> {
        self.accesses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(handle) = self.handle.get() {
            return Ok(handle);
        }
        let mut failure = self.failure.lock();
        if let Some(handle) = self.handle.get() {
            return Ok(handle);
        }
        if let Some(retry_at) = failure.retry_at
            && std::time::Instant::now() < retry_at
        {
            return Err(GpuError::Internal(failure.last.clone().unwrap_or_default()));
        }
        match (self.open)() {
            Ok(handle) => {
                let _ = self.handle.set(handle);
                *failure = Failure::default();
                Ok(self.handle.get().expect("set above"))
            }
            Err(err) => {
                failure.attempts = failure.attempts.saturating_add(1);
                let wait = RETRY_BASE
                    .checked_mul(1u32 << failure.attempts.min(16))
                    .unwrap_or(RETRY_CAP)
                    .min(RETRY_CAP);
                failure.retry_at = Some(std::time::Instant::now() + wait);
                failure.last = Some(err.to_string());
                Err(err)
            }
        }
    }

    fn accesses(&self) -> u64 {
        self.accesses.load(std::sync::atomic::Ordering::Relaxed)
    }
}
