//! One CUDA device: its retained primary context, a stream, the loaded kernels, device memory and
//! the events that fence the stream.
//!
//! Every operation makes the context current on the calling thread first, since the driver keeps
//! the current context per thread. All copies and launches go through one non-blocking stream,
//! so a copy after a launch waits for the kernel without a full-device synchronize.

use std::ffi::{CString, c_void};
use std::fmt;
use std::sync::Arc;

use super::ffi::{
    ATTR_COMPUTE_MAJOR, ATTR_COMPUTE_MINOR, CUDA_SUCCESS, CuContext, CuDevice, CuDeviceptr,
    CuEvent, CuFunction, CuModule, CuResult, CuStream, Driver, EVENT_DISABLE_TIMING,
    STREAM_NON_BLOCKING, driver,
};
use super::ptx::{SCALE_F32_ENTRY, SCALE_F32_PTX};
use crate::GpuError;
use crate::handles::GpuDropToken;

/// `CUDA_ERROR_OUT_OF_MEMORY`.
const OUT_OF_MEMORY: CuResult = 2;
/// Threads per block of the elementwise kernels.
const BLOCK_THREADS: u32 = 256;

/// An open device. Its primary context is retained until the last `Arc` drops.
pub(crate) struct CudaDevice {
    pub(crate) drv: &'static Driver,
    pub(crate) ordinal: i32,
    pub(crate) name: String,
    pub(crate) compute: (i32, i32),
    pub(crate) total_mem: u64,
    dev: CuDevice,
    // Null until created; `Drop` releases only what was created, so a failed `open` cleans up.
    ctx: CuContext,
    stream: CuStream,
    module: CuModule,
    scale: CuFunction,
}

// SAFETY: the raw handles are driver objects, usable from any thread while the context is made
// current there, which every method does first.
unsafe impl Send for CudaDevice {}
unsafe impl Sync for CudaDevice {}

impl fmt::Debug for CudaDevice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CudaDevice")
            .field("ordinal", &self.ordinal)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

fn internal(message: String) -> GpuError {
    GpuError::Internal(message)
}

impl CudaDevice {
    /// Open device `ordinal`: retain its primary context, create the stream and load the kernels.
    pub(crate) fn open(ordinal: i32) -> Result<Self, String> {
        let drv = driver()?;
        drv.check("cuInit", unsafe { (drv.init)(0) })?;
        let mut count = 0;
        drv.check("cuDeviceGetCount", unsafe {
            (drv.device_get_count)(&mut count)
        })?;
        if ordinal < 0 || ordinal >= count {
            return Err(format!(
                "no CUDA device {ordinal} (the driver reports {count})"
            ));
        }
        let mut dev: CuDevice = 0;
        drv.check("cuDeviceGet", unsafe {
            (drv.device_get)(&mut dev, ordinal)
        })?;
        let mut name: [std::ffi::c_char; 256] = [0; 256];
        drv.check("cuDeviceGetName", unsafe {
            (drv.device_get_name)(name.as_mut_ptr(), name.len() as i32, dev)
        })?;
        // SAFETY: the driver wrote a NUL-terminated name into the buffer.
        let name = unsafe { std::ffi::CStr::from_ptr(name.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        let mut major = 0;
        let mut minor = 0;
        drv.check("cuDeviceGetAttribute", unsafe {
            (drv.device_get_attribute)(&mut major, ATTR_COMPUTE_MAJOR, dev)
        })?;
        drv.check("cuDeviceGetAttribute", unsafe {
            (drv.device_get_attribute)(&mut minor, ATTR_COMPUTE_MINOR, dev)
        })?;
        let mut total = 0usize;
        drv.check("cuDeviceTotalMem", unsafe {
            (drv.device_total_mem)(&mut total, dev)
        })?;

        let mut this = Self {
            drv,
            ordinal,
            name,
            compute: (major, minor),
            total_mem: total as u64,
            dev,
            ctx: std::ptr::null_mut(),
            stream: std::ptr::null_mut(),
            module: std::ptr::null_mut(),
            scale: std::ptr::null_mut(),
        };
        let mut ctx: CuContext = std::ptr::null_mut();
        drv.check("cuDevicePrimaryCtxRetain", unsafe {
            (drv.primary_ctx_retain)(&mut ctx, dev)
        })?;
        this.ctx = ctx;
        this.make_current().map_err(|e| e.to_string())?;
        drv.check("cuStreamCreate", unsafe {
            (drv.stream_create)(&mut this.stream, STREAM_NON_BLOCKING)
        })?;
        let ptx = CString::new(SCALE_F32_PTX).map_err(|e| e.to_string())?;
        drv.check("cuModuleLoadData", unsafe {
            (drv.module_load_data)(&mut this.module, ptx.as_ptr().cast::<c_void>())
        })?;
        let entry = CString::new(SCALE_F32_ENTRY).map_err(|e| e.to_string())?;
        drv.check("cuModuleGetFunction", unsafe {
            (drv.module_get_function)(&mut this.scale, this.module, entry.as_ptr())
        })?;
        Ok(this)
    }

    /// Make this device's context current on the calling thread.
    pub(crate) fn make_current(&self) -> Result<(), GpuError> {
        let code = unsafe { (self.drv.ctx_set_current)(self.ctx) };
        if code == CUDA_SUCCESS {
            Ok(())
        } else {
            Err(internal(self.drv.describe("cuCtxSetCurrent", code)))
        }
    }

    /// Check a driver call that made `what`, after the context is current.
    fn check(&self, what: &str, code: CuResult) -> Result<(), GpuError> {
        self.drv.check(what, code).map_err(internal)
    }

    /// Allocate `bytes` of device memory.
    pub(crate) fn alloc(self: &Arc<Self>, bytes: u64) -> Result<DeviceMemory, GpuError> {
        self.make_current()?;
        let mut ptr: CuDeviceptr = 0;
        let code = unsafe { (self.drv.mem_alloc)(&mut ptr, bytes as usize) };
        if code == OUT_OF_MEMORY {
            return Err(GpuError::AllocationFailed);
        }
        self.check("cuMemAlloc", code)?;
        Ok(DeviceMemory {
            dev: Arc::clone(self),
            ptr,
            bytes,
        })
    }

    /// Copy `src` into `mem` at its start, and wait for the copy.
    pub(crate) fn upload(&self, mem: &DeviceMemory, src: &[u8]) -> Result<(), GpuError> {
        if src.len() as u64 > mem.bytes {
            return Err(GpuError::InvalidInput(format!(
                "{} bytes do not fit a {}-byte CUDA buffer",
                src.len(),
                mem.bytes
            )));
        }
        self.make_current()?;
        let code = unsafe {
            (self.drv.memcpy_htod_async)(
                mem.ptr,
                src.as_ptr().cast::<c_void>(),
                src.len(),
                self.stream,
            )
        };
        self.check("cuMemcpyHtoDAsync", code)?;
        self.synchronize()
    }

    /// Copy `mem` into `dst` from its start, once the stream's earlier work (launches) finished.
    pub(crate) fn download(&self, mem: &DeviceMemory, dst: &mut [u8]) -> Result<(), GpuError> {
        if dst.len() as u64 > mem.bytes {
            return Err(GpuError::InvalidInput(format!(
                "a {}-byte read from a {}-byte CUDA buffer",
                dst.len(),
                mem.bytes
            )));
        }
        self.make_current()?;
        let code = unsafe {
            (self.drv.memcpy_dtoh_async)(
                dst.as_mut_ptr().cast::<c_void>(),
                mem.ptr,
                dst.len(),
                self.stream,
            )
        };
        self.check("cuMemcpyDtoHAsync", code)?;
        self.synchronize()
    }

    /// Wait until everything queued on the stream so far has finished.
    pub(crate) fn synchronize(&self) -> Result<(), GpuError> {
        self.make_current()?;
        self.check("cuStreamSynchronize", unsafe {
            (self.drv.stream_synchronize)(self.stream)
        })
    }

    /// Queue `data[i] *= factor` over the first `count` f32 of `mem`, and record a fence after it.
    pub(crate) fn scale_f32(
        self: &Arc<Self>,
        mem: &DeviceMemory,
        count: u32,
        factor: f32,
    ) -> Result<CudaFence, GpuError> {
        if u64::from(count) * 4 > mem.bytes {
            return Err(GpuError::InvalidInput(format!(
                "{count} f32 do not fit a {}-byte CUDA buffer",
                mem.bytes
            )));
        }
        self.make_current()?;
        let mut data = mem.ptr;
        let mut n = count;
        let mut k = factor;
        let mut params: [*mut c_void; 3] = [
            (&mut data as *mut CuDeviceptr).cast(),
            (&mut n as *mut u32).cast(),
            (&mut k as *mut f32).cast(),
        ];
        let blocks = count.div_ceil(BLOCK_THREADS).max(1);
        self.check("cuLaunchKernel", unsafe {
            (self.drv.launch_kernel)(
                self.scale,
                blocks,
                1,
                1,
                BLOCK_THREADS,
                1,
                1,
                0,
                self.stream,
                params.as_mut_ptr(),
                std::ptr::null_mut(),
            )
        })?;
        let mut event: CuEvent = std::ptr::null_mut();
        self.check("cuEventCreate", unsafe {
            (self.drv.event_create)(&mut event, EVENT_DISABLE_TIMING)
        })?;
        let fence = CudaFence {
            dev: Arc::clone(self),
            event,
        };
        self.check("cuEventRecord", unsafe {
            (self.drv.event_record)(event, self.stream)
        })?;
        Ok(fence)
    }
}

impl Drop for CudaDevice {
    fn drop(&mut self) {
        if self.ctx.is_null() {
            return;
        }
        let _ = self.make_current();
        // SAFETY: each handle is non-null only once created, and is destroyed once here. The
        // memory and fences that used the context hold an `Arc<CudaDevice>`, so none is left.
        unsafe {
            if !self.module.is_null() {
                (self.drv.module_unload)(self.module);
            }
            if !self.stream.is_null() {
                (self.drv.stream_destroy)(self.stream);
            }
            (self.drv.primary_ctx_release)(self.dev);
        }
    }
}

/// CudaDevice memory of one buffer, freed with its last handle.
pub(crate) struct DeviceMemory {
    dev: Arc<CudaDevice>,
    ptr: CuDeviceptr,
    pub(crate) bytes: u64,
}

impl fmt::Debug for DeviceMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceMemory")
            .field("ptr", &format_args!("{:#x}", self.ptr))
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Drop for DeviceMemory {
    fn drop(&mut self) {
        let _ = self.dev.make_current();
        // SAFETY: `ptr` came from `cuMemAlloc` on this context and is freed once.
        unsafe {
            (self.dev.drv.mem_free)(self.ptr);
        }
    }
}

impl GpuDropToken for DeviceMemory {}

/// A point in a stream: [`CudaFence::wait`] returns once the work queued before it has finished.
pub struct CudaFence {
    dev: Arc<CudaDevice>,
    event: CuEvent,
}

impl CudaFence {
    /// Block until the work queued before this fence has finished.
    pub fn wait(&self) -> Result<(), GpuError> {
        self.dev.make_current()?;
        self.dev.check("cuEventSynchronize", unsafe {
            (self.dev.drv.event_synchronize)(self.event)
        })
    }
}

impl fmt::Debug for CudaFence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CudaFence").finish_non_exhaustive()
    }
}

impl Drop for CudaFence {
    fn drop(&mut self) {
        let _ = self.dev.make_current();
        // SAFETY: the event was created by `cuEventCreate` and is destroyed once.
        unsafe {
            (self.dev.drv.event_destroy)(self.event);
        }
    }
}
