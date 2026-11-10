//! The backend the planner and the host see.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError};

use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, Transfer,
};

use crate::adapter::Options;
use crate::caps::{CapsRecord, ComputeCaps, Need};
use crate::context::{Context, OpenError};
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
    options: Options,
    context: Mutex<Option<Arc<Context>>>,
}

impl WgpuBackend {
    /// A backend that finds and opens its own device, on first use.
    pub fn open(options: Options) -> WgpuBackend {
        WgpuBackend {
            options,
            context: Mutex::new(None),
        }
    }

    /// A backend on a device that already exists.
    pub fn adopt(handle: Handle) -> WgpuBackend {
        let context = Context::adopted(handle.device, handle.queue, &handle.adapter);
        WgpuBackend {
            options: Options::default(),
            context: Mutex::new(Some(Arc::new(context))),
        }
    }

    /// Opens the device now if it isn't open, and says what it can do. A failure isn't kept: the
    /// next call tries again.
    pub fn start(&self) -> Result<CapsRecord, OpenError> {
        self.context().map(|context| context.record.clone())
    }

    /// Lets go of this backend's hold on its device. The device closes when nothing else holds
    /// it, and opens again on the next use.
    pub fn stop(&self) {
        *self.context.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// What is known about the device without opening one: this backend's own device, then the
    /// one the process shares, then what the options carried from an earlier run.
    pub fn caps(&self) -> Option<CapsRecord> {
        let own = self.context.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(context) = own.as_ref() {
            return Some(context.record.clone());
        }
        drop(own);
        if self.options.adapter.is_none()
            && let Some(record) = Context::shared_record()
        {
            return Some(record);
        }
        self.options.known.clone()
    }

    /// Counters of the device's pool of readback buffers. `None` while no device is open.
    pub fn staging_stats(&self) -> Option<StagingStats> {
        let own = self.context.lock().unwrap_or_else(PoisonError::into_inner);
        own.as_ref().map(|context| context.staging().stats())
    }

    pub(crate) fn context(&self) -> Result<Arc<Context>, OpenError> {
        let mut own = self.context.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(context) = own.as_ref() {
            return Ok(Arc::clone(context));
        }
        let opened = Context::open(&self.options)?;
        *own = Some(Arc::clone(&opened));
        Ok(opened)
    }
}

impl Backend for WgpuBackend {
    fn id(&self) -> &str {
        "wgpu"
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
        crate::memory::spaces()
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
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

    fn compile(&self, _: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        Err(Refusal::whole_region(
            "the wgpu backend compiles nothing yet",
        ))
    }
}
