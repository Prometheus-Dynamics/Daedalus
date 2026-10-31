//! The executor's GPU context, visible to transport adapters while they run on an edge.

use core::cell::Cell;
use core::marker::PhantomData;

use daedalus_gpu::GpuContextHandle;

std::thread_local! {
    static CURRENT: Cell<*const GpuContextHandle> = const { Cell::new(core::ptr::null()) };
}

/// Makes a GPU context current on this thread until dropped (scopes nest).
pub(crate) struct GpuScope<'a> {
    prev: *const GpuContextHandle,
    _gpu: PhantomData<&'a GpuContextHandle>,
}

/// Make `gpu` current on this thread for as long as the returned scope lives.
pub(crate) fn enter(gpu: Option<&GpuContextHandle>) -> GpuScope<'_> {
    let ptr = gpu.map_or(core::ptr::null(), |gpu| gpu as *const _);
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

/// Run `f` with the GPU context of the executor that is running an edge adapter on this thread:
/// `None` outside one, or when the engine has no GPU backend. Device upload/download functions
/// registered with
/// [`register_typed_device_transport_with_gpu`](crate::plugins::PluginRegistry::register_typed_device_transport_with_gpu)
/// (`#[device]` functions taking `&GpuContextHandle`) get it this way.
pub fn with_current_gpu<R>(f: impl FnOnce(Option<&GpuContextHandle>) -> R) -> R {
    let ptr = CURRENT.with(Cell::get);
    // SAFETY: a non-null pointer was set by an `enter` whose scope borrows the handle and is
    // still alive on this thread (scopes restore the previous pointer on drop, in LIFO order);
    // the reference cannot outlive `f`.
    f(unsafe { ptr.as_ref() })
}
