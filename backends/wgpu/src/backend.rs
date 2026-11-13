//! The backend the planner and the host see.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, Transfer,
};

use crate::adapter::Options;
use crate::caps::{CapsRecord, ComputeCaps, Need};
use crate::context::{Context, OpenError};
use crate::loss::Retry;
use crate::staging::StagingStats;

/// A device someone else set up. Whoever made it kept device creation from racing: see
/// [`driver_lock`](crate::driver_lock).
pub struct Handle {
    pub device: wgpu::Device,
    pub queue: wgpu::Queue,
    pub adapter: wgpu::AdapterInfo,
}

/// The wgpu backend. Making one opens nothing: the device opens the first time it's needed.
pub struct WgpuBackend {
    link: Arc<Link>,
}

/// What a backend and the steps it compiled share: how to get the device, and to get another
/// when that one is lost.
pub(crate) struct Link {
    options: Options,
    /// Set for a device of this backend's own, opened by name.
    named: Option<crate::devices::Named>,
    /// Moves for types of the author's own, added by `with_bytes`.
    typed: Mutex<Vec<Transfer>>,
    /// Moves that import a value's dma-buf, added by `with_dmabuf`. Listed only while the open
    /// device imports dma-bufs, so a plan made without one never asks for an import.
    #[cfg(target_os = "linux")]
    dma: Mutex<Vec<(Transfer, bool)>>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    context: Option<Arc<Context>>,
    retry: Retry,
    /// Turned off by the host. Nothing here ever sets it.
    off: bool,
    /// The device was handed in, so there is nothing to reopen it from.
    adopted: bool,
    /// The host's lease, set on each device this opens.
    lease: Option<Arc<dyn crate::GpuLease>>,
}

impl Link {
    /// The key of the device this has open, if it has one.
    pub(crate) fn open_key(&self) -> Option<u64> {
        self.state().context.as_ref().map(|context| context.key)
    }

    /// Adds moves to what this backend lists. One already listed, by id, is left as it is.
    pub(crate) fn offer(&self, more: impl IntoIterator<Item = Transfer>) {
        let mut typed = self.typed.lock().unwrap_or_else(PoisonError::into_inner);
        for transfer in more {
            if typed.iter().all(|have| have.id != transfer.id) {
                typed.push(transfer);
            }
        }
    }

    fn offered(&self) -> Vec<Transfer> {
        self.typed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Adds moves that import a dma-buf. One already listed, by id, is left as it is.
    #[cfg(target_os = "linux")]
    /// `read_only` says the type's fds only allow reading, which not every device imports.
    pub(crate) fn offer_dma(&self, more: impl IntoIterator<Item = Transfer>, read_only: bool) {
        let mut dma = self.dma.lock().unwrap_or_else(PoisonError::into_inner);
        for transfer in more {
            if dma.iter().all(|(have, _)| have.id != transfer.id) {
                dma.push((transfer, read_only));
            }
        }
    }

    /// The imports a device can run: every one, or without those of read-only fds.
    #[cfg(target_os = "linux")]
    fn dma(&self, takes_read_only: bool) -> Vec<Transfer> {
        self.dma
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(_, read_only)| takes_read_only || !read_only)
            .map(|(transfer, _)| transfer.clone())
            .collect()
    }

    pub(crate) fn named_as(&self) -> Option<&crate::devices::Named> {
        self.named.as_ref()
    }

    pub(crate) fn named(options: Options, named: crate::devices::Named) -> Link {
        Link {
            options,
            named: Some(named),
            typed: Mutex::default(),
            #[cfg(target_os = "linux")]
            dma: Mutex::default(),
            state: Mutex::default(),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The device, opened if there is none. A lost device is let go of and another is opened,
    /// under the failure rule: an attempt that fails is tried again after a wait that doubles
    /// from 250 ms to 8 s, for ever.
    pub(crate) fn context(&self) -> Result<Arc<Context>, OpenError> {
        let mut state = self.state();
        if state.off {
            return Err(OpenError::Off);
        }
        match &state.context {
            Some(context) if !context.is_lost() => return Ok(Arc::clone(context)),
            Some(lost) => {
                lost.retire();
                state.context = None;
            }
            None => {}
        }
        if state.adopted {
            return Err(OpenError::Device(
                "the device this backend was given is lost".into(),
            ));
        }
        let now = Instant::now();
        if let Some(wait) = state.retry.wait(now) {
            return Err(OpenError::Backoff(wait));
        }
        // A named device is this backend's alone, and uploads for the default device don't go
        // to it.
        let opened = match &self.named {
            Some(_) => Context::open_own(&self.options),
            None => Context::open(&self.options),
        };
        match opened {
            Ok(opened) => {
                state.retry.worked();
                if self.named.is_none() {
                    Context::note_latest(&opened);
                }
                // A device several backends share keeps the lease it has unless this one has its own.
                if state.lease.is_some() {
                    opened.lease.set(state.lease.clone());
                }
                state.context = Some(Arc::clone(&opened));
                Ok(opened)
            }
            Err(error) => {
                // The wait counts from the failure, not from the start of the attempt: an attempt
                // that takes longer than the wait would otherwise have used it all up.
                state.retry.failed(Instant::now());
                Err(error)
            }
        }
    }
}

impl WgpuBackend {
    /// A backend that finds and opens its own device, on first use.
    pub fn open(options: Options) -> WgpuBackend {
        WgpuBackend {
            link: Arc::new(Link {
                options,
                named: None,
                typed: Mutex::default(),
                #[cfg(target_os = "linux")]
                dma: Mutex::default(),
                state: Mutex::default(),
            }),
        }
    }

    pub(crate) fn of(link: Arc<Link>) -> WgpuBackend {
        WgpuBackend { link }
    }

    pub(crate) fn open_key(&self) -> Option<u64> {
        self.open_context().map(|context| context.key)
    }

    /// A backend on a device that already exists.
    pub fn adopt(handle: Handle) -> WgpuBackend {
        let context = Arc::new(Context::adopted(
            handle.device,
            handle.queue,
            &handle.adapter,
        ));
        Context::note_latest(&context);
        WgpuBackend {
            link: Arc::new(Link {
                options: Options::default(),
                named: None,
                typed: Mutex::default(),
                #[cfg(target_os = "linux")]
                dma: Mutex::default(),
                state: Mutex::new(State {
                    context: Some(context),
                    adopted: true,
                    ..State::default()
                }),
            }),
        }
    }

    /// Opens the device now if it isn't open, and says what it can do. A failure is tried again
    /// on a later call, after the wait the failure rule gives.
    pub fn start(&self) -> Result<CapsRecord, OpenError> {
        self.context().map(|context| context.record.clone())
    }

    /// Lets go of this backend's hold on its device. The device closes when nothing else holds
    /// it, and opens again on the next use.
    pub fn stop(&self) {
        self.link.state().context = None;
    }

    /// Turns the backend off: no device is opened and nothing runs on it until
    /// [`WgpuBackend::turn_on`]. Only the host does this. A device that keeps failing is never
    /// turned off for it.
    pub fn turn_off(&self) {
        let mut state = self.link.state();
        state.off = true;
        state.context = None;
    }

    pub fn turn_on(&self) {
        self.link.state().off = false;
    }

    /// How long until the next attempt to open a device, after one that failed. `None` when an
    /// attempt is due now, or none has failed.
    pub fn retry_in(&self) -> Option<Duration> {
        self.link.state().retry.wait(Instant::now())
    }

    /// Marks the open device lost, as its driver or a timed-out wait would. For tests.
    #[doc(hidden)]
    pub fn mark_lost(&self) {
        if let Some(context) = self.link.state().context.as_ref() {
            context.mark_lost("marked lost by a test");
        }
    }

    fn open_context(&self) -> Option<Arc<Context>> {
        self.link.state().context.clone()
    }

    /// What is known about the device without opening one: this backend's own device, then the
    /// one the process shares, then what the options carried from an earlier run.
    pub fn caps(&self) -> Option<CapsRecord> {
        if let Some(context) = self.open_context() {
            return Some(context.record.clone());
        }
        if self.link.options.adapter.is_none()
            && let Some(record) = Context::shared_record()
        {
            return Some(record);
        }
        self.link.options.known.clone()
    }

    /// Counters of the device's pool of readback buffers. `None` while no device is open.
    pub fn staging_stats(&self) -> Option<StagingStats> {
        self.open_context().map(|context| context.staging().stats())
    }

    /// How much the device's shader cache has built. `None` while no device is open.
    pub fn shader_stats(&self) -> Option<crate::shader::ShaderStats> {
        self.open_context().map(|context| context.shader_stats())
    }

    /// How much has moved between the host and the device. `None` while no device is open.
    pub fn transfer_stats(&self) -> Option<crate::TransferStats> {
        self.open_context().map(|context| context.moved.stats())
    }

    pub(crate) fn context(&self) -> Result<Arc<Context>, OpenError> {
        self.link.context()
    }

    /// Sets the host's lease on the GPU, or takes it away with `None`. From then on the backend
    /// asks for the lease before it gives the device work and gives it back when the last of
    /// that work has finished: see [`GpuLease`](crate::GpuLease). It applies to the device this
    /// backend has open and to any it opens later, and opens nothing itself. Backends that share
    /// one device share its lease: the last one set is the one asked.
    pub fn set_lease(&self, lease: Option<Arc<dyn crate::GpuLease>>) {
        let mut state = self.link.state();
        if let Some(context) = &state.context {
            context.lease.set(lease.clone());
        }
        state.lease = lease;
    }

    pub(crate) fn link(&self) -> &Arc<Link> {
        &self.link
    }
}

impl Backend for WgpuBackend {
    fn id(&self) -> &str {
        self.link.named.as_ref().map_or("wgpu", |named| &named.id)
    }

    /// Zero until something is known about the device, so a plan made on the default limits is
    /// made again once the real ones are known.
    fn version(&self) -> u64 {
        self.caps().map_or(0, |record| {
            let mut hasher = DefaultHasher::new();
            record.hash(&mut hasher);
            hasher.finish() | 1
        })
    }

    fn memory(&self) -> Vec<MemorySpace> {
        crate::handoff::spaces(
            self.link.named.as_ref(),
            !self.link.options.host_ports_on_device,
        )
    }

    /// A shader's output can be written straight into memory the host reads: dma-heap memory
    /// where an open device imports it, which the host reads through its cache, and host-mapped
    /// memory anywhere. The planner places an output there when the host is who reads it, and
    /// then no readback is planned.
    fn placements(&self) -> Vec<std::borrow::Cow<'static, str>> {
        use crate::memory::{SPACE_DMA_HEAP, SPACE_HOST_MAPPED};
        let device = self.device_name();
        let heap = self
            .open_context()
            .is_some_and(|context| context.record.dma_heap);
        let mut spaces = Vec::new();
        if heap {
            spaces.push(crate::handoff::on_device(device, SPACE_DMA_HEAP));
        }
        spaces.push(crate::handoff::on_device(device, SPACE_HOST_MAPPED));
        spaces
    }

    fn transfers(&self) -> Vec<Transfer> {
        // Only a device that is open is asked: a plan made before it opens offers the host-mapped
        // readback alone, and the heap route is then not planned for.
        let heap = self
            .open_context()
            .is_some_and(|context| context.record.dma_heap);
        let mut table = crate::handoff::transfers(self.link.named.as_ref(), heap);
        #[cfg(target_os = "linux")]
        if heap {
            let takes_read_only = self
                .open_context()
                .is_some_and(|context| context.record.dma_read_only);
            table.extend(self.link.dma(takes_read_only));
        }
        #[cfg(not(target_os = "linux"))]
        let _ = heap;
        table.extend(self.link.offered());
        table
    }

    /// Answers from the caps record and opens nothing. With no record, a node is accepted when
    /// its needs fit the WebGPU default limits, and `compile` decides on the real device.
    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        let compute = self
            .caps()
            .map_or(ComputeCaps::WEBGPU_DEFAULTS, |record| record.compute);
        offer
            .nodes
            .iter()
            .filter(|node| {
                node.needs
                    .iter()
                    .all(|text| Need::parse(text).is_some_and(|need| compute.meets(&need)))
            })
            .map(|node| NodeRef {
                node: node.node,
                body: node.body,
            })
            .collect()
    }

    /// One step for the whole region: every node's pass in one encoder, and one submit.
    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        crate::region::compile(self, region, cx).map(|unit| Compiled::Steps(vec![unit]))
    }
}
