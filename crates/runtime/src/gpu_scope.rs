//! The executor's GPU source, visible to transport adapters while they run on an edge.

use core::cell::Cell;
use core::marker::PhantomData;

use daedalus_gpu::{GpuContextHandle, GpuError};

use crate::gpu_source::GpuSource;
use crate::portable::Arc;

std::thread_local! {
    static CURRENT: Cell<*const Arc<dyn GpuSource>> = const { Cell::new(core::ptr::null()) };
}

/// Makes a GPU source current on this thread until dropped (scopes nest).
pub(crate) struct GpuScope<'a> {
    prev: *const Arc<dyn GpuSource>,
    _gpu: PhantomData<&'a Arc<dyn GpuSource>>,
}

/// Make `source` current on this thread for as long as the returned scope lives.
pub(crate) fn enter(source: Option<&Arc<dyn GpuSource>>) -> GpuScope<'_> {
    let ptr = source.map_or(core::ptr::null(), |source| source as *const _);
    let prev = CURRENT.with(|current| current.replace(ptr));
    GpuScope {
        prev,
        _gpu: PhantomData,
    }
}

impl Drop for GpuScope<'_> {
    fn drop(&mut self) {
        CURRENT.with(|current| current.set(self.prev));
    }
}

/// Run `f` with the GPU context of the executor that is running an edge adapter on this thread.
/// The context is opened here, on the adapter's first use of it: `Ok(None)` outside an edge or
/// on an engine without a GPU backend, and `Err` when the device cannot be opened (its cause in
/// the error). Device upload/download functions registered with
/// [`register_typed_device_transport_with_gpu`](crate::plugins::PluginRegistry::register_typed_device_transport_with_gpu)
/// (`#[device]` functions taking `&GpuContextHandle`) get it this way.
pub fn with_current_gpu<R>(f: impl FnOnce(Result<Option<&GpuContextHandle>, GpuError>) -> R) -> R {
    let ptr = CURRENT.with(Cell::get);
    // SAFETY: a non-null pointer was set by an `enter` whose scope borrows the source and is
    // still alive on this thread (scopes restore the previous pointer on drop, in LIFO order);
    // the reference cannot outlive `f`.
    let source = unsafe { ptr.as_ref() };
    let gpu = match source {
        None => Ok(None),
        Some(source) => source.get().map(Some),
    };
    f(gpu)
}
