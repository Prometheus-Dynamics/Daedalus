//! External memory imported into CUDA: memory exported as an opaque fd (`vkGetMemoryFdKHR` on the
//! Vulkan device of the same NVIDIA GPU) becomes a device buffer of CUDA, mapped over the same
//! allocation. Nothing is copied: a kernel on the import and a Vulkan read see one memory.

use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd};
use std::ptr::null_mut;
use std::sync::Arc;

use super::device::CudaDevice;
use super::ffi::{
    CuDeviceptr, CuExternalMemory, CuUuid, EXTERNAL_MEMORY_OPAQUE_FD, ExternalMemoryBufferDesc,
    ExternalMemoryHandle, ExternalMemoryHandleDesc,
};
use crate::GpuError;

/// Memory imported from an fd, as a device buffer of one CUDA device. Dropping it unmaps the
/// buffer and releases the import (`cuDestroyExternalMemory`).
///
/// The memory the fd names belongs to the exporter (the Vulkan allocation), which must outlive
/// the import: drop the import before the memory it came from.
pub struct CudaImport {
    dev: Arc<CudaDevice>,
    memory: CuExternalMemory,
    ptr: CuDeviceptr,
    bytes: u64,
}

impl std::fmt::Debug for CudaImport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CudaImport")
            .field("bytes", &self.bytes)
            .field("ptr", &format_args!("{:#x}", self.ptr))
            .finish_non_exhaustive()
    }
}

// SAFETY: the import is a driver object of its device's context; every method makes that context
// current on the calling thread first, so it may be moved between threads.
unsafe impl Send for CudaImport {}

impl CudaDevice {
    /// Import `fd` (an opaque fd of `bytes` of memory) as a device buffer of this device. The
    /// driver consumes the fd either way; on failure the error names the driver's code.
    pub(crate) fn import_opaque_fd(
        self: &Arc<Self>,
        fd: OwnedFd,
        bytes: u64,
    ) -> Result<CudaImport, GpuError> {
        self.make_current()?;
        let desc = ExternalMemoryHandleDesc {
            handle_type: EXTERNAL_MEMORY_OPAQUE_FD,
            handle: ExternalMemoryHandle { fd: fd.as_raw_fd() },
            size: bytes,
            flags: 0,
            reserved: [0; 16],
        };
        let mut memory: CuExternalMemory = null_mut();
        // SAFETY: `desc` is a fully initialized `CUDA_EXTERNAL_MEMORY_HANDLE_DESC`; `memory` is
        // written on success only.
        let code = unsafe { (self.drv.import_external_memory)(&mut memory, &desc) };
        // The driver takes the fd whether or not it imports: on 595.104.02 a refused dma-buf fd is
        // already closed when the call returns, so it is never closed again from here.
        let _ = fd.into_raw_fd();
        self.drv
            .check("cuImportExternalMemory", code)
            .map_err(GpuError::Internal)?;

        let range = ExternalMemoryBufferDesc {
            offset: 0,
            size: bytes,
            flags: 0,
            reserved: [0; 16],
        };
        let mut ptr: CuDeviceptr = 0;
        // SAFETY: `memory` is a live import of this context; `range` is initialized and `ptr` is
        // written on success only.
        let code =
            unsafe { (self.drv.external_memory_get_mapped_buffer)(&mut ptr, memory, &range) };
        if let Err(reason) = self.drv.check("cuExternalMemoryGetMappedBuffer", code) {
            // SAFETY: `memory` was imported above and is destroyed once, here.
            unsafe { (self.drv.destroy_external_memory)(memory) };
            return Err(GpuError::Internal(reason));
        }
        Ok(CudaImport {
            dev: Arc::clone(self),
            memory,
            ptr,
            bytes,
        })
    }

    /// The device's UUID, which matches a Vulkan physical device's `deviceUUID` for the same GPU.
    pub(crate) fn uuid(&self) -> Result<[u8; 16], GpuError> {
        let mut uuid = CuUuid { bytes: [0; 16] };
        // SAFETY: `uuid` is valid for writes of one `CUuuid`.
        let code = unsafe { (self.drv.device_get_uuid)(&mut uuid, self.dev) };
        self.drv
            .check("cuDeviceGetUuid", code)
            .map_err(GpuError::Internal)?;
        Ok(uuid.bytes)
    }
}

impl CudaImport {
    /// The size of the imported memory in bytes.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Queue `data[i] *= factor` over the imported buffer's `f32` values (its length over 4), in
    /// place in the exporter's memory. The fence finishes when the kernel has.
    pub fn scale_f32(&self, factor: f32) -> Result<super::CudaFence, GpuError> {
        let count = u32::try_from(self.bytes / 4)
            .map_err(|_| GpuError::InvalidInput("import too large for scale_f32".into()))?;
        self.dev.launch_scale(self.ptr, count, factor)
    }
}

impl Drop for CudaImport {
    fn drop(&mut self) {
        let _ = self.dev.make_current();
        // SAFETY: `memory` came from `cuImportExternalMemory` on this context and is destroyed
        // once; destroying it unmaps the buffer, so `ptr` is not used after this.
        unsafe {
            (self.dev.drv.destroy_external_memory)(self.memory);
        }
    }
}
