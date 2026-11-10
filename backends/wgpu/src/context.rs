//! An open device. Nothing outside this crate gets one.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use crate::adapter::{Options, choose};
use crate::caps::{CapsRecord, OPTIONAL_FEATURES};
use crate::copy_limiter::{CopyLimiter, MAX_INFLIGHT_COPIES};
use crate::driver::{LockedInstance, driver_lock, open_device};
use crate::inflight::InFlight;
use crate::pool::TempPool;
use crate::staging::StagingPool;
use crate::wait::{PendingErrors, WaitError};

/// Why no device opened. Opening is tried again the next time one is needed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenError {
    /// No adapter passed the options.
    NoAdapter,
    /// Adapters passed, and none gave a device. The text is the last one's reason.
    Device(String),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::NoAdapter => f.write_str("no adapter matches"),
            OpenError::Device(reason) => write!(f, "no adapter gave a device: {reason}"),
        }
    }
}

impl std::error::Error for OpenError {}

pub(crate) struct Context {
    pub(crate) device: wgpu::Device,
    pub(crate) queue: wgpu::Queue,
    /// Held around every call that can submit on `queue`.
    pub(crate) submit: daedalus_vk_sys::SubmitLock,
    pub(crate) record: CapsRecord,
    pub(crate) limits: wgpu::Limits,
    /// Unique to this device in this process.
    pub(crate) key: u64,
    pub(crate) copies: CopyLimiter,
    staging: Mutex<StagingPool<wgpu::Buffer>>,
    shaders: Mutex<crate::shader::ShaderCache>,
    /// What the device reported outside any call.
    pub(crate) errors: Arc<PendingErrors>,
    lost: AtomicBool,
    wait_timeout: std::time::Duration,
    inflight: Mutex<InFlight>,
    submits: AtomicU64,
    passes: AtomicU64,
    pub(crate) moved: crate::transfer::Counters,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "shader nodes take their scratch here, from E4")
    )]
    pub(crate) temp: Mutex<TempPool>,
}

static NEXT_KEY: AtomicU64 = AtomicU64::new(1);

/// The device the process shares. It closes with its last holder.
static SHARED: Mutex<Weak<Context>> = Mutex::new(Weak::new());

/// The device a backend opened most recently. An upload's step is made by a plain function that
/// can't see its backend, so this is where it finds the device to upload to.
static LATEST: Mutex<Weak<Context>> = Mutex::new(Weak::new());

impl Context {
    /// The shared device, opened if nobody holds it. A named adapter gets a device of its own.
    pub(crate) fn open(options: &Options) -> Result<Arc<Context>, OpenError> {
        if options.adapter.is_some() {
            return Context::open_private(options).map(Arc::new);
        }
        let mut shared = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(open) = shared.upgrade() {
            return Ok(open);
        }
        let opened = Arc::new(Context::open_private(options)?);
        *shared = Arc::downgrade(&opened);
        Ok(opened)
    }

    /// Notes `context` as the device uploads go to.
    pub(crate) fn note_latest(context: &Arc<Context>) {
        *LATEST.lock().unwrap_or_else(PoisonError::into_inner) = Arc::downgrade(context);
    }

    /// The device a backend opened most recently, if it is still open.
    pub(crate) fn latest() -> Option<Arc<Context>> {
        LATEST
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade()
    }

    /// The shared device's record, if the device is open. Opens nothing.
    pub(crate) fn shared_record() -> Option<CapsRecord> {
        let shared = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
        shared.upgrade().map(|context| context.record.clone())
    }

    pub(crate) fn adopted(
        device: wgpu::Device,
        queue: wgpu::Queue,
        info: &wgpu::AdapterInfo,
    ) -> Context {
        Context::new(device, queue, info, &Options::default())
    }

    fn new(
        device: wgpu::Device,
        queue: wgpu::Queue,
        info: &wgpu::AdapterInfo,
        options: &Options,
    ) -> Context {
        let limits = device.limits();
        let errors = Arc::new(PendingErrors::default());
        let record = Arc::clone(&errors);
        device.on_uncaptured_error(Arc::new(move |error: wgpu::Error| {
            record.record(error.to_string());
        }));
        Context {
            errors,
            lost: AtomicBool::new(false),
            wait_timeout: options.wait_timeout,
            inflight: Mutex::new(InFlight::new(options.max_in_flight)),
            submits: AtomicU64::new(0),
            passes: AtomicU64::new(0),
            moved: crate::transfer::Counters::default(),
            record: CapsRecord::new(info, &limits, device.features()),
            limits,
            device,
            queue,
            submit: daedalus_vk_sys::SubmitLock::default(),
            key: NEXT_KEY.fetch_add(1, Ordering::Relaxed),
            copies: CopyLimiter::new(MAX_INFLIGHT_COPIES),
            staging: Mutex::new(StagingPool::new(options.staging)),
            shaders: Mutex::default(),
            temp: Mutex::new(TempPool::new(options.temp_pool)),
        }
    }

    pub(crate) fn shaders(&self) -> MutexGuard<'_, crate::shader::ShaderCache> {
        self.shaders.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn count_submit(&self) {
        self.submits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_pass(&self) {
        self.passes.fetch_add(1, Ordering::Relaxed);
    }

    /// The shader cache's counters, with the submits and passes this device has seen.
    pub(crate) fn shader_stats(&self) -> crate::shader::ShaderStats {
        crate::shader::ShaderStats {
            submits: self.submits.load(Ordering::Relaxed),
            passes: self.passes.load(Ordering::Relaxed),
            ..self.shaders().stats()
        }
    }

    pub(crate) fn inflight(&self) -> MutexGuard<'_, InFlight> {
        self.inflight.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Lets the device run the callbacks of what has finished. Never blocks.
    pub(crate) fn poll(&self) {
        let _ = self.device.poll(wgpu::PollType::Poll);
    }

    /// Waits for everything submitted, up to the bound. A timeout marks the device lost.
    pub(crate) fn wait(&self, what: &'static str) -> Result<(), WaitError> {
        if crate::wait::wait_idle(&self.device, self.wait_timeout) {
            Ok(())
        } else {
            Err(self.timed_out(what))
        }
    }

    pub(crate) fn timed_out(&self, what: &'static str) -> WaitError {
        self.lost.store(true, Ordering::Release);
        WaitError::TimedOut {
            what,
            after: self.wait_timeout,
        }
    }

    /// Whether a wait on this device has timed out. Its work can't be trusted to finish.
    pub(crate) fn is_lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }

    /// A number no other buffer or texture in this process has.
    pub(crate) fn next_id() -> u64 {
        NEXT_KEY.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn staging(&self) -> MutexGuard<'_, StagingPool<wgpu::Buffer>> {
        self.staging.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn open_private(options: &Options) -> Result<Context, OpenError> {
        let held = driver_lock();
        let wanted = options.apis.to_wgpu();
        let instance = LockedInstance::new(&held, wanted);
        let mut adapters = instance.adapters(wanted);
        if adapters.is_empty() && wanted != wgpu::Backends::all() {
            adapters = instance.adapters(wgpu::Backends::all());
        }
        let mut last = OpenError::NoAdapter;
        for adapter in choose(adapters, options) {
            let info = adapter.get_info();
            let kind = crate::adapter::AdapterInfo::from_wgpu(&info).kind;
            let desc = wgpu::DeviceDescriptor {
                label: None,
                required_features: adapter.features() & OPTIONAL_FEATURES,
                required_limits: adapter.limits(),
                memory_hints: options.hints.resolve(kind).to_wgpu(),
                ..wgpu::DeviceDescriptor::default()
            };
            match open_device(&held, &adapter, &desc) {
                Ok((device, queue)) => return Ok(Context::new(device, queue, &info, options)),
                Err(error) => last = OpenError::Device(error.to_string()),
            }
        }
        Err(last)
    }
}
