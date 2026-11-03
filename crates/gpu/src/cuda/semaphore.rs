//! External timeline semaphores: a Vulkan timeline semaphore exported as an opaque fd and imported
//! into CUDA, which waits on and signals it on the device's stream. The handoff between the two
//! APIs then runs on the GPU: no host round trip, and no host fence, is needed at the boundary.
//!
//! Nothing is copied. The semaphore only orders work; the memory it orders is shared through
//! [`crate::interop`].

use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd};
use std::ptr::null_mut;
use std::sync::Arc;

use super::device::CudaDevice;
use super::ffi::{
    CuExternalSemaphore, EXTERNAL_SEMAPHORE_TIMELINE_FD, ExternalMemoryHandle,
    ExternalSemaphoreHandleDesc, ExternalSemaphoreParams, SemaphoreFns,
};
use crate::GpuError;

/// A timeline semaphore imported into one CUDA device. Dropping it destroys the import; the
/// Vulkan semaphore it came from must outlive the import's use, not its drop.
pub struct CudaSemaphore {
    dev: Arc<CudaDevice>,
    sem: CuExternalSemaphore,
    fns: SemaphoreFns,
}

impl std::fmt::Debug for CudaSemaphore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CudaSemaphore")
            .field("device", &self.dev.ordinal)
            .finish_non_exhaustive()
    }
}

fn params(value: u64) -> ExternalSemaphoreParams {
    ExternalSemaphoreParams {
        value,
        nv_sci_fence: 0,
        keyed_mutex_key: 0,
        params_reserved: [0; 12],
        flags: 0,
        reserved: [0; 16],
    }
}

impl CudaDevice {
    /// Import `fd`, an opaque fd of a timeline semaphore (`vkGetSemaphoreFdKHR` on the same GPU).
    /// The driver consumes the fd either way.
    pub(crate) fn import_timeline_semaphore(
        self: &Arc<Self>,
        fd: OwnedFd,
    ) -> Result<CudaSemaphore, GpuError> {
        let fns = self.drv.semaphores.ok_or_else(|| {
            GpuError::Internal("the CUDA driver lacks the external-semaphore entry points".into())
        })?;
        self.make_current()?;
        let desc = ExternalSemaphoreHandleDesc {
            handle_type: EXTERNAL_SEMAPHORE_TIMELINE_FD,
            handle: ExternalMemoryHandle { fd: fd.as_raw_fd() },
            flags: 0,
            reserved: [0; 16],
        };
        let mut sem: CuExternalSemaphore = null_mut();
        // SAFETY: `desc` is a fully initialized `CUDA_EXTERNAL_SEMAPHORE_HANDLE_DESC`; `sem` is
        // written on success only.
        let code = unsafe { (fns.import)(&mut sem, &desc) };
        // The driver takes the fd whether or not it imports, so it is never closed again here.
        let _ = fd.into_raw_fd();
        self.drv
            .check("cuImportExternalSemaphore", code)
            .map_err(GpuError::Internal)?;
        Ok(CudaSemaphore {
            dev: Arc::clone(self),
            sem,
            fns,
        })
    }
}

impl CudaSemaphore {
    /// Queue on the device's stream a wait until the semaphore reaches `value`: work queued after
    /// it runs once that point is signaled. Does not block the host.
    pub fn wait(&self, value: u64) -> Result<(), GpuError> {
        self.dev.make_current()?;
        let p = params(value);
        // SAFETY: one live semaphore and one parameter block, both valid for the call.
        let code = unsafe { (self.fns.wait)(&self.sem, &p, 1, self.dev.stream) };
        self.dev
            .drv
            .check("cuWaitExternalSemaphoresAsync", code)
            .map_err(GpuError::Internal)
    }

    /// Queue on the device's stream a signal that sets the semaphore to `value`, after the work
    /// queued before it. Does not block the host.
    pub fn signal(&self, value: u64) -> Result<(), GpuError> {
        self.dev.make_current()?;
        let p = params(value);
        // SAFETY: as in `wait`.
        let code = unsafe { (self.fns.signal)(&self.sem, &p, 1, self.dev.stream) };
        self.dev
            .drv
            .check("cuSignalExternalSemaphoresAsync", code)
            .map_err(GpuError::Internal)
    }
}

impl Drop for CudaSemaphore {
    fn drop(&mut self) {
        let _ = self.dev.make_current();
        // SAFETY: `sem` came from `cuImportExternalSemaphore` on this context and is destroyed once.
        unsafe {
            (self.fns.destroy)(self.sem);
        }
    }
}
