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
    /// The last attempt failed, and the next is due after this long.
    Backoff(std::time::Duration),
    /// The host turned the backend off.
    Off,
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::NoAdapter => f.write_str("no adapter matches"),
            OpenError::Device(reason) => write!(f, "no adapter gave a device: {reason}"),
            OpenError::Backoff(wait) => write!(f, "no device: trying again in {wait:?}"),
            OpenError::Off => f.write_str("the backend is turned off"),
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
    /// Whether the device imports a dma-buf, as far as anything has tried.
    pub(crate) imports: crate::import_state::ImportState,
    pub(crate) limits: wgpu::Limits,
    /// Unique to this device in this process.
    pub(crate) key: u64,
    pub(crate) copies: CopyLimiter,
    staging: Mutex<StagingPool<wgpu::Buffer>>,
    shaders: Mutex<crate::shader::ShaderCache>,
    /// What the device reported outside any call.
    pub(crate) errors: Arc<PendingErrors>,
    lost: Arc<AtomicBool>,
    wait_timeout: std::time::Duration,
    inflight: Mutex<InFlight>,
    /// The host's turn on the GPU, when it set one.
    pub(crate) lease: Arc<crate::lease::LeaseState>,
    submits: AtomicU64,
    passes: AtomicU64,
    dispatches: AtomicU64,
    pub(crate) moved: crate::transfer::Counters,
    pub(crate) temp: Mutex<TempPool>,
    /// Set when bytes were given to the queue to write, which reach the device only with the
    /// queue's next submit.
    unsent: AtomicBool,
    /// Bytes given to the queue to write since its last submit. Their staging memory is only freed
    /// by a submit, so past [`WRITE_BUDGET`] the queue is submitted.
    unsent_bytes: AtomicU64,
    /// Buffers of device memory nothing has cleared, let go by their last holder and kept to be
    /// handed out again: allocating device memory is slow on some devices.
    #[cfg(target_os = "linux")]
    spare: Mutex<Vec<Spare>>,
    /// Regions that record with Vulkan itself, for a wait to cover their work too.
    #[cfg(target_os = "linux")]
    chains: Mutex<Vec<Weak<Mutex<daedalus_vk_sys::chain::Chain>>>>,
}

/// Bytes the queue may hold for writing before it is submitted. A 16 MiB write alone is over the
/// budget, so it is submitted with the write that made it.
const WRITE_BUDGET: u64 = 8 << 20;

/// A buffer kept to hand out again, with its mapping where it has one.
#[cfg(target_os = "linux")]
pub(crate) type Spare = (wgpu::Buffer, Option<daedalus_vk_sys::memory::HostMapping>);

/// The most buffers kept to hand out again, and the most bytes they hold together.
#[cfg(target_os = "linux")]
const SPARE_BUFFERS: usize = 4;
#[cfg(target_os = "linux")]
const SPARE_BYTES: u64 = 64 << 20;

static NEXT_KEY: AtomicU64 = AtomicU64::new(1);

/// The device the process shares. It closes with its last holder.
static SHARED: Mutex<Weak<Context>> = Mutex::new(Weak::new());

/// The device a backend opened most recently. An upload's step is made by a plain function that
/// can't see its backend, so this is where it finds the device to upload to.
static LATEST: Mutex<Weak<Context>> = Mutex::new(Weak::new());

impl Context {
    /// The shared device, opened if nobody holds it. A named adapter gets a device of its own.
    pub(crate) fn open(options: &Options) -> Result<Arc<Context>, OpenError> {
        if options.adapter.is_some() || options.index.is_some() {
            return Context::open_private(options).map(Arc::new);
        }
        let mut shared = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
        // A lost device is nobody's to share any more.
        if let Some(open) = shared.upgrade().filter(|open| !open.is_lost()) {
            return Ok(open);
        }
        let opened = Arc::new(Context::open_private(options)?);
        *shared = Arc::downgrade(&opened);
        Ok(opened)
    }

    /// A device of the caller's own, whatever the options name.
    pub(crate) fn open_own(options: &Options) -> Result<Arc<Context>, OpenError> {
        Context::open_private(options).map(Arc::new)
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
        let (caps, imports) = with_heap(
            CapsRecord::new(info, &limits, device.features()),
            &device,
            options,
        );
        let errors = Arc::new(PendingErrors::default());
        let record = Arc::clone(&errors);
        device.on_uncaptured_error(Arc::new(move |error: wgpu::Error| {
            record.record(error.to_string());
        }));
        let lost = Arc::new(AtomicBool::new(false));
        let (flag, record) = (Arc::clone(&lost), Arc::clone(&errors));
        device.set_device_lost_callback(move |reason, message| {
            // Dropping the device calls this too, and that isn't a loss.
            if reason != wgpu::DeviceLostReason::Destroyed {
                flag.store(true, Ordering::Release);
                record.record(format!("the device was lost: {message}"));
            }
        });
        Context {
            errors,
            lost,
            wait_timeout: options.wait_timeout,
            inflight: Mutex::new(InFlight::new(options.max_in_flight)),
            lease: Arc::default(),
            submits: AtomicU64::new(0),
            passes: AtomicU64::new(0),
            dispatches: AtomicU64::new(0),
            moved: crate::transfer::Counters::default(),
            record: caps,
            imports,
            limits,
            device,
            queue,
            submit: daedalus_vk_sys::SubmitLock::default(),
            key: NEXT_KEY.fetch_add(1, Ordering::Relaxed),
            copies: CopyLimiter::new(MAX_INFLIGHT_COPIES),
            staging: Mutex::new(StagingPool::new(options.staging)),
            shaders: Mutex::default(),
            temp: Mutex::new(TempPool::new(options.temp_pool)),
            unsent: AtomicBool::new(false),
            unsent_bytes: AtomicU64::new(0),
            #[cfg(target_os = "linux")]
            spare: Mutex::default(),
            #[cfg(target_os = "linux")]
            chains: Mutex::default(),
        }
    }

    pub(crate) fn shaders(&self) -> MutexGuard<'_, crate::shader::ShaderCache> {
        self.shaders.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// This submission's part of the host's lease, asked for before the device is given the
    /// work. `None` when the host set no lease, which costs one load.
    #[inline]
    pub(crate) fn leased(&self) -> Option<crate::lease::LeaseUse> {
        self.lease.begin()
    }

    /// Gives `lease` back once the device has finished everything it has been given so far.
    /// Called right after the submit the lease was asked for.
    #[inline]
    pub(crate) fn until_done(&self, lease: Option<crate::lease::LeaseUse>) {
        if let Some(lease) = lease {
            self.queue.on_submitted_work_done(move || drop(lease));
        }
    }

    pub(crate) fn count_submit(&self) {
        self.submits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_pass(&self) {
        self.passes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_dispatch(&self) {
        self.dispatches.fetch_add(1, Ordering::Relaxed);
    }

    /// The shader cache's counters, with the submits and passes this device has seen.
    pub(crate) fn shader_stats(&self) -> crate::shader::ShaderStats {
        crate::shader::ShaderStats {
            submits: self.submits.load(Ordering::Relaxed),
            passes: self.passes.load(Ordering::Relaxed),
            dispatches: self.dispatches.load(Ordering::Relaxed),
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

    /// Notes that the queue was given `bytes` to write. Past [`WRITE_BUDGET`] the queue is
    /// submitted here: a long run of uploads with no submit would hold every staging copy at once.
    pub(crate) fn note_write(&self, bytes: u64) {
        self.unsent.store(true, Ordering::Release);
        let total = self.unsent_bytes.fetch_add(bytes, Ordering::AcqRel) + bytes;
        if total >= WRITE_BUDGET {
            self.flush_writes();
        }
    }

    /// Sends what the queue was given to write, if anything was. Work recorded with Vulkan
    /// itself doesn't go through the queue's submit, so it calls this first.
    pub(crate) fn flush_writes(&self) {
        if self.unsent.swap(false, Ordering::AcqRel) {
            self.unsent_bytes.store(0, Ordering::Release);
            let lease = self.leased();
            let _held = self.submit.lock();
            self.queue.submit([]);
            self.until_done(lease);
        }
    }

    /// Keeps a buffer of uncleared device memory that its last holder let go.
    #[cfg(target_os = "linux")]
    pub(crate) fn keep_spare(&self, kept: Spare) {
        if self.is_lost() {
            return;
        }
        let mut spare = self.spare.lock().unwrap_or_else(PoisonError::into_inner);
        let held: u64 = spare.iter().map(|(buffer, _)| buffer.size()).sum();
        if spare.len() < SPARE_BUFFERS && held + kept.0.size() <= SPARE_BYTES {
            spare.push(kept);
        }
    }

    /// A kept buffer of exactly `size` bytes, if there is one and the device has finished
    /// everything it was given: then nothing still reads or writes a buffer that was let go.
    #[cfg(target_os = "linux")]
    pub(crate) fn take_spare(&self, size: u64) -> Option<Spare> {
        let mut spare = self.spare.lock().unwrap_or_else(PoisonError::into_inner);
        let at = spare.iter().position(|(buffer, _)| buffer.size() == size)?;
        let idle = self
            .device
            .poll(wgpu::PollType::Poll)
            .is_ok_and(|status| status.is_queue_empty());
        let chains = self.chains.lock().unwrap_or_else(PoisonError::into_inner);
        let chains_idle = chains
            .iter()
            .filter_map(Weak::upgrade)
            .all(|chain| chain.try_lock().is_ok_and(|chain| chain.is_idle()));
        (idle && chains_idle).then(|| spare.swap_remove(at))
    }

    /// Notes a region's chain, so a wait on this device covers it.
    #[cfg(target_os = "linux")]
    pub(crate) fn note_chain(&self, chain: &Arc<Mutex<daedalus_vk_sys::chain::Chain>>) {
        let mut chains = self.chains.lock().unwrap_or_else(PoisonError::into_inner);
        chains.retain(|known| known.strong_count() > 0);
        chains.push(Arc::downgrade(chain));
    }

    /// Whether every chain's last run finished within the bound.
    #[cfg(target_os = "linux")]
    fn chains_idle(&self) -> bool {
        let chains = self.chains.lock().unwrap_or_else(PoisonError::into_inner);
        chains.iter().filter_map(Weak::upgrade).all(|chain| {
            let chain = chain.lock().unwrap_or_else(PoisonError::into_inner);
            chain.wait(self.wait_timeout)
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn chains_idle(&self) -> bool {
        true
    }

    /// Waits for everything submitted, up to the bound. A timeout marks the device lost.
    pub(crate) fn wait(&self, what: &'static str) -> Result<(), WaitError> {
        if crate::wait::wait_idle(&self.device, self.wait_timeout) && self.chains_idle() {
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

    /// Marks the device lost: nothing more runs on it.
    pub(crate) fn mark_lost(&self, why: &str) {
        self.lost.store(true, Ordering::Release);
        self.errors.record(format!("the device was lost: {why}"));
    }

    /// Lets go of everything a lost device kept: its readback buffers, its scratch and its
    /// pipelines. Its key is never used again.
    pub(crate) fn retire(&self) {
        *self.staging() = StagingPool::new(crate::StagingConfig::default());
        *self.temp.lock().unwrap_or_else(PoisonError::into_inner) =
            TempPool::new(crate::TempPoolLimits::default());
        *self.shaders() = crate::shader::ShaderCache::default();
        #[cfg(target_os = "linux")]
        self.spare
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    /// Whether the driver lost the device or a wait on it timed out. Its work can't be trusted
    /// to finish.
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

/// Whether the device really imports a buffer from the system dma-heap: one small import is
/// tried. Having the extensions isn't enough. A driver can list them and still refuse every
/// dma-buf it didn't make itself, and a plan must not count on an import that will be refused.
#[cfg(target_os = "linux")]
fn imports_heap_buffers(device: &wgpu::Device, read_only: bool) -> bool {
    use daedalus_vk_sys::memory::{dma_buf_bytes_needed, import_dma_buf, imports_dma_buf};
    const TRIAL: u64 = 4096;
    if !imports_dma_buf(device) {
        return false;
    }
    let Ok(heap) = daedalus_dma_sys::DmaHeap::open("system") else {
        return false;
    };
    let Ok(need) = dma_buf_bytes_needed(device, TRIAL) else {
        return false;
    };
    let made = match read_only {
        true => heap.allocate_read_only(need.max(TRIAL)),
        false => heap.allocate(need.max(TRIAL)),
    };
    let Ok(buf) = made else {
        return false;
    };
    let Ok(have) = buf.as_ref().len() else {
        return false;
    };
    import_dma_buf(device, buf.into_fd(), TRIAL, have, None).is_ok()
}

/// What is known of the device's dma-buf imports when it opens, and the record with what that
/// means for placing outputs: `dma_heap` is set when the system heap opens and the device
/// imports a buffer from it, which is tried once here. A process that can't open the heap tries
/// nothing, and its answers are unknown.
fn with_heap(
    record: CapsRecord,
    device: &wgpu::Device,
    options: &Options,
) -> (CapsRecord, crate::import_state::ImportState) {
    use crate::import_state::ImportState;
    #[cfg(target_os = "linux")]
    let (writable, read_only) = {
        let can = daedalus_vk_sys::memory::imports_dma_buf(device);
        let heap = !options.no_heap_access && daedalus_dma_sys::DmaHeap::open("system").is_ok();
        if options.no_dma_heap || !can {
            (Some(false), Some(false))
        } else if !heap {
            (None, None)
        } else {
            let writable = imports_heap_buffers(device, false);
            // An fd that only allows reading is a second question: a driver may map what it
            // imports.
            (
                Some(writable),
                Some(writable && imports_heap_buffers(device, true)),
            )
        }
    };
    #[cfg(not(target_os = "linux"))]
    let (writable, read_only) = {
        let _ = (device, options);
        (Some(false), Some(false))
    };
    let record = CapsRecord {
        dma_heap: writable == Some(true),
        dma_read_only: read_only == Some(true),
        ..record
    };
    (record, ImportState::new(writable, read_only))
}
