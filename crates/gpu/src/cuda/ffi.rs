//! The CUDA driver API, loaded at runtime.
//!
//! Nothing here links against CUDA. The driver library is opened when the first [`CudaBackend`]
//! is made, so a build needs no toolkit, and a machine without the driver or an NVIDIA GPU runs
//! the rest of the crate and reports why CUDA is unavailable.
//!
//! [`CudaBackend`]: crate::CudaBackend

use std::ffi::{CStr, c_char, c_int, c_uint, c_void};
use std::sync::OnceLock;

use libloading::Library;

pub(crate) type CuResult = c_int;
pub(crate) type CuDevice = c_int;
pub(crate) type CuDeviceptr = u64;
pub(crate) type CuContext = *mut c_void;
pub(crate) type CuModule = *mut c_void;
pub(crate) type CuFunction = *mut c_void;
pub(crate) type CuStream = *mut c_void;
pub(crate) type CuEvent = *mut c_void;
pub(crate) type CuExternalMemory = *mut c_void;
pub(crate) type CuExternalSemaphore = *mut c_void;
pub(crate) type CuArray = *mut c_void;

pub(crate) const CUDA_SUCCESS: CuResult = 0;
/// `CU_DEVICE_ATTRIBUTE_COMPUTE_CAPABILITY_MAJOR` and `_MINOR`.
pub(crate) const ATTR_COMPUTE_MAJOR: c_int = 75;
pub(crate) const ATTR_COMPUTE_MINOR: c_int = 76;
/// `CU_STREAM_NON_BLOCKING`: the stream does not synchronize with the legacy default stream.
pub(crate) const STREAM_NON_BLOCKING: c_uint = 0x1;
/// `CU_EVENT_DISABLE_TIMING`: fences need no timestamps.
pub(crate) const EVENT_DISABLE_TIMING: c_uint = 0x2;
/// `CU_AD_FORMAT_UNSIGNED_INT8` and `CU_AD_FORMAT_HALF`: the texel formats of the CUDA images.
pub(crate) const AD_FORMAT_UNSIGNED_INT8: c_uint = 0x01;
pub(crate) const AD_FORMAT_HALF: c_uint = 0x10;
/// `CU_MEMORYTYPE_HOST` and `CU_MEMORYTYPE_ARRAY`: the ends of a 2D copy.
pub(crate) const MEMORYTYPE_HOST: c_uint = 0x01;
pub(crate) const MEMORYTYPE_ARRAY: c_uint = 0x03;
/// `CU_EXTERNAL_MEMORY_HANDLE_TYPE_OPAQUE_FD`: a memory fd from `vkGetMemoryFdKHR` (Linux).
pub(crate) const EXTERNAL_MEMORY_OPAQUE_FD: c_uint = 1;
/// `CU_EXTERNAL_SEMAPHORE_HANDLE_TYPE_TIMELINE_SEMAPHORE_FD`: a fd of a Vulkan timeline semaphore
/// (`vkGetSemaphoreFdKHR` with the opaque fd handle type). The binary `OPAQUE_FD` type (1) imports
/// the same payload, but its signals do not change the timeline's value.
pub(crate) const EXTERNAL_SEMAPHORE_TIMELINE_FD: c_uint = 9;

/// `CUuuid`: a device's 16-byte UUID, as `VkPhysicalDeviceIDProperties::deviceUUID` spells it.
#[repr(C)]
pub(crate) struct CuUuid {
    pub(crate) bytes: [u8; 16],
}

/// The handle of a `CUDA_EXTERNAL_MEMORY_HANDLE_DESC`: an fd for `OPAQUE_FD` (the other members
/// are the Windows and NvSci forms, here only to give the union its size).
#[repr(C)]
pub(crate) union ExternalMemoryHandle {
    pub(crate) fd: c_int,
    pub(crate) win32: [*mut c_void; 2],
    pub(crate) nv_sci_buf: *const c_void,
}

/// `CUDA_EXTERNAL_MEMORY_HANDLE_DESC` of cuda.h (104 bytes): the handle type, the handle, the size
/// of the memory, the flags, and the reserved words cuda.h keeps zeroed.
#[repr(C)]
pub(crate) struct ExternalMemoryHandleDesc {
    pub(crate) handle_type: c_uint,
    pub(crate) handle: ExternalMemoryHandle,
    pub(crate) size: u64,
    pub(crate) flags: c_uint,
    pub(crate) reserved: [c_uint; 16],
}

/// `CUDA_EXTERNAL_SEMAPHORE_HANDLE_DESC` of cuda.h: the handle type, the handle (an fd for
/// `OPAQUE_FD`; the union has the same shape as the memory one), the flags and the reserved words.
#[repr(C)]
pub(crate) struct ExternalSemaphoreHandleDesc {
    pub(crate) handle_type: c_uint,
    pub(crate) handle: ExternalMemoryHandle,
    pub(crate) flags: c_uint,
    pub(crate) reserved: [c_uint; 16],
}

/// The wait and signal parameters of cuda.h (`CUDA_EXTERNAL_SEMAPHORE_{WAIT,SIGNAL}_PARAMS`, one
/// layout, 144 bytes): `params` holds the timeline value (`fence.value`), the NvSci fence and the
/// keyed-mutex key (both unused, zero) and 12 reserved words; then the flags and 16 more words.
#[repr(C)]
pub(crate) struct ExternalSemaphoreParams {
    pub(crate) value: u64,
    pub(crate) nv_sci_fence: u64,
    pub(crate) keyed_mutex_key: u64,
    pub(crate) params_reserved: [c_uint; 12],
    pub(crate) flags: c_uint,
    pub(crate) reserved: [c_uint; 16],
}

/// `CUDA_ARRAY3D_DESCRIPTOR` of cuda.h: a 2D array has depth 0.
#[repr(C)]
pub(crate) struct ArrayDesc {
    pub(crate) width: usize,
    pub(crate) height: usize,
    pub(crate) depth: usize,
    pub(crate) format: c_uint,
    pub(crate) num_channels: c_uint,
    pub(crate) flags: c_uint,
}

/// `CUDA_MEMCPY2D` of cuda.h: a copy of rows between host memory, device memory or an array.
#[repr(C)]
pub(crate) struct Memcpy2d {
    pub(crate) src_x_in_bytes: usize,
    pub(crate) src_y: usize,
    pub(crate) src_memory_type: c_uint,
    pub(crate) src_host: *const c_void,
    pub(crate) src_device: CuDeviceptr,
    pub(crate) src_array: CuArray,
    pub(crate) src_pitch: usize,
    pub(crate) dst_x_in_bytes: usize,
    pub(crate) dst_y: usize,
    pub(crate) dst_memory_type: c_uint,
    pub(crate) dst_host: *mut c_void,
    pub(crate) dst_device: CuDeviceptr,
    pub(crate) dst_array: CuArray,
    pub(crate) dst_pitch: usize,
    pub(crate) width_in_bytes: usize,
    pub(crate) height: usize,
}

/// `CUDA_EXTERNAL_MEMORY_BUFFER_DESC` of cuda.h: the range of the memory mapped as a device buffer.
#[repr(C)]
pub(crate) struct ExternalMemoryBufferDesc {
    pub(crate) offset: u64,
    pub(crate) size: u64,
    pub(crate) flags: c_uint,
    pub(crate) reserved: [c_uint; 16],
}

type FnInit = unsafe extern "C" fn(c_uint) -> CuResult;
type FnDeviceGetCount = unsafe extern "C" fn(*mut c_int) -> CuResult;
type FnDeviceGet = unsafe extern "C" fn(*mut CuDevice, c_int) -> CuResult;
type FnDeviceGetName = unsafe extern "C" fn(*mut c_char, c_int, CuDevice) -> CuResult;
type FnDeviceTotalMem = unsafe extern "C" fn(*mut usize, CuDevice) -> CuResult;
type FnDeviceGetAttribute = unsafe extern "C" fn(*mut c_int, c_int, CuDevice) -> CuResult;
type FnPrimaryCtxRetain = unsafe extern "C" fn(*mut CuContext, CuDevice) -> CuResult;
type FnPrimaryCtxRelease = unsafe extern "C" fn(CuDevice) -> CuResult;
type FnCtxSetCurrent = unsafe extern "C" fn(CuContext) -> CuResult;
type FnMemAlloc = unsafe extern "C" fn(*mut CuDeviceptr, usize) -> CuResult;
type FnMemFree = unsafe extern "C" fn(CuDeviceptr) -> CuResult;
type FnMemcpyHtoDAsync =
    unsafe extern "C" fn(CuDeviceptr, *const c_void, usize, CuStream) -> CuResult;
type FnMemcpyDtoHAsync =
    unsafe extern "C" fn(*mut c_void, CuDeviceptr, usize, CuStream) -> CuResult;
type FnModuleLoadData = unsafe extern "C" fn(*mut CuModule, *const c_void) -> CuResult;
type FnModuleGetFunction =
    unsafe extern "C" fn(*mut CuFunction, CuModule, *const c_char) -> CuResult;
type FnModuleUnload = unsafe extern "C" fn(CuModule) -> CuResult;
type FnLaunchKernel = unsafe extern "C" fn(
    CuFunction,
    c_uint,
    c_uint,
    c_uint,
    c_uint,
    c_uint,
    c_uint,
    c_uint,
    CuStream,
    *mut *mut c_void,
    *mut *mut c_void,
) -> CuResult;
type FnStreamCreate = unsafe extern "C" fn(*mut CuStream, c_uint) -> CuResult;
type FnStreamSynchronize = unsafe extern "C" fn(CuStream) -> CuResult;
type FnStreamDestroy = unsafe extern "C" fn(CuStream) -> CuResult;
type FnEventCreate = unsafe extern "C" fn(*mut CuEvent, c_uint) -> CuResult;
type FnEventRecord = unsafe extern "C" fn(CuEvent, CuStream) -> CuResult;
type FnEventSynchronize = unsafe extern "C" fn(CuEvent) -> CuResult;
type FnEventDestroy = unsafe extern "C" fn(CuEvent) -> CuResult;
type FnImportExternalMemory =
    unsafe extern "C" fn(*mut CuExternalMemory, *const ExternalMemoryHandleDesc) -> CuResult;
type FnExternalMemoryGetMappedBuffer = unsafe extern "C" fn(
    *mut CuDeviceptr,
    CuExternalMemory,
    *const ExternalMemoryBufferDesc,
) -> CuResult;
type FnDestroyExternalMemory = unsafe extern "C" fn(CuExternalMemory) -> CuResult;
type FnImportExternalSemaphore =
    unsafe extern "C" fn(*mut CuExternalSemaphore, *const ExternalSemaphoreHandleDesc) -> CuResult;
type FnExternalSemaphoresAsync = unsafe extern "C" fn(
    *const CuExternalSemaphore,
    *const ExternalSemaphoreParams,
    c_uint,
    CuStream,
) -> CuResult;
type FnDestroyExternalSemaphore = unsafe extern "C" fn(CuExternalSemaphore) -> CuResult;

/// The external-semaphore entry points. Optional in [`Driver`]: a driver without them still runs
/// the rest of the backend, and [`crate::CudaBackend::import_timeline_semaphore`] says so.
#[derive(Clone, Copy)]
pub(crate) struct SemaphoreFns {
    pub(crate) import: FnImportExternalSemaphore,
    pub(crate) signal: FnExternalSemaphoresAsync,
    pub(crate) wait: FnExternalSemaphoresAsync,
    pub(crate) destroy: FnDestroyExternalSemaphore,
}
type FnDeviceGetUuid = unsafe extern "C" fn(*mut CuUuid, CuDevice) -> CuResult;
type FnArray3DCreate = unsafe extern "C" fn(*mut CuArray, *const ArrayDesc) -> CuResult;
type FnArrayDestroy = unsafe extern "C" fn(CuArray) -> CuResult;
type FnMemcpy2DAsync = unsafe extern "C" fn(*const Memcpy2d, CuStream) -> CuResult;
type FnGetErrorName = unsafe extern "C" fn(CuResult, *mut *const c_char) -> CuResult;
type FnGetErrorString = unsafe extern "C" fn(CuResult, *mut *const c_char) -> CuResult;

/// The driver's entry points. Holds the library open for as long as the table lives.
pub(crate) struct Driver {
    _lib: Library,
    pub(crate) init: FnInit,
    pub(crate) device_get_count: FnDeviceGetCount,
    pub(crate) device_get: FnDeviceGet,
    pub(crate) device_get_name: FnDeviceGetName,
    pub(crate) device_total_mem: FnDeviceTotalMem,
    pub(crate) device_get_attribute: FnDeviceGetAttribute,
    pub(crate) primary_ctx_retain: FnPrimaryCtxRetain,
    pub(crate) primary_ctx_release: FnPrimaryCtxRelease,
    pub(crate) ctx_set_current: FnCtxSetCurrent,
    pub(crate) mem_alloc: FnMemAlloc,
    pub(crate) mem_free: FnMemFree,
    pub(crate) memcpy_htod_async: FnMemcpyHtoDAsync,
    pub(crate) memcpy_dtoh_async: FnMemcpyDtoHAsync,
    pub(crate) module_load_data: FnModuleLoadData,
    pub(crate) module_get_function: FnModuleGetFunction,
    pub(crate) module_unload: FnModuleUnload,
    pub(crate) launch_kernel: FnLaunchKernel,
    pub(crate) stream_create: FnStreamCreate,
    pub(crate) stream_synchronize: FnStreamSynchronize,
    pub(crate) stream_destroy: FnStreamDestroy,
    pub(crate) event_create: FnEventCreate,
    pub(crate) event_record: FnEventRecord,
    pub(crate) event_synchronize: FnEventSynchronize,
    pub(crate) event_destroy: FnEventDestroy,
    pub(crate) import_external_memory: FnImportExternalMemory,
    pub(crate) external_memory_get_mapped_buffer: FnExternalMemoryGetMappedBuffer,
    pub(crate) destroy_external_memory: FnDestroyExternalMemory,
    pub(crate) device_get_uuid: FnDeviceGetUuid,
    /// `None` when the driver lacks the external-semaphore entry points.
    pub(crate) semaphores: Option<SemaphoreFns>,
    pub(crate) array_create: FnArray3DCreate,
    pub(crate) array_destroy: FnArrayDestroy,
    pub(crate) memcpy_2d_async: FnMemcpy2DAsync,
    get_error_name: FnGetErrorName,
    get_error_string: FnGetErrorString,
}

// SAFETY: the function pointers are plain C entry points, and the driver is documented as callable
// from any thread once a context is current on that thread (every call site makes it current).
unsafe impl Send for Driver {}
unsafe impl Sync for Driver {}

/// The driver library file for this platform.
#[cfg(windows)]
const DRIVER_LIBRARY: &str = "nvcuda.dll";
#[cfg(not(windows))]
const DRIVER_LIBRARY: &str = "libcuda.so.1";

static DRIVER: OnceLock<Result<Driver, String>> = OnceLock::new();

/// The process's driver table, or why it could not be loaded (no driver library, or a library
/// missing an entry point). Loaded once; the result is cached.
pub(crate) fn driver() -> Result<&'static Driver, String> {
    DRIVER
        .get_or_init(load)
        .as_ref()
        .map_err(|reason| reason.clone())
}

fn load() -> Result<Driver, String> {
    // SAFETY: opening the driver runs its library constructors; the driver is the one the NVIDIA
    // userspace installs for this platform and is not otherwise used by this crate.
    let lib = unsafe { Library::new(DRIVER_LIBRARY) }
        .map_err(|e| format!("{DRIVER_LIBRARY} not loadable: {e}"))?;
    // SAFETY: each symbol is looked up by its driver-API name and the table types match the
    // `cuda.h` prototypes of the driver version that exports them (the `_v2` entry points where
    // cuda.h renames them).
    unsafe {
        Ok(Driver {
            init: sym(&lib, b"cuInit\0")?,
            device_get_count: sym(&lib, b"cuDeviceGetCount\0")?,
            device_get: sym(&lib, b"cuDeviceGet\0")?,
            device_get_name: sym(&lib, b"cuDeviceGetName\0")?,
            device_total_mem: sym(&lib, b"cuDeviceTotalMem_v2\0")?,
            device_get_attribute: sym(&lib, b"cuDeviceGetAttribute\0")?,
            primary_ctx_retain: sym(&lib, b"cuDevicePrimaryCtxRetain\0")?,
            primary_ctx_release: sym(&lib, b"cuDevicePrimaryCtxRelease_v2\0")?,
            ctx_set_current: sym(&lib, b"cuCtxSetCurrent\0")?,
            mem_alloc: sym(&lib, b"cuMemAlloc_v2\0")?,
            mem_free: sym(&lib, b"cuMemFree_v2\0")?,
            memcpy_htod_async: sym(&lib, b"cuMemcpyHtoDAsync_v2\0")?,
            memcpy_dtoh_async: sym(&lib, b"cuMemcpyDtoHAsync_v2\0")?,
            module_load_data: sym(&lib, b"cuModuleLoadData\0")?,
            module_get_function: sym(&lib, b"cuModuleGetFunction\0")?,
            module_unload: sym(&lib, b"cuModuleUnload\0")?,
            launch_kernel: sym(&lib, b"cuLaunchKernel\0")?,
            stream_create: sym(&lib, b"cuStreamCreate\0")?,
            stream_synchronize: sym(&lib, b"cuStreamSynchronize\0")?,
            stream_destroy: sym(&lib, b"cuStreamDestroy_v2\0")?,
            event_create: sym(&lib, b"cuEventCreate\0")?,
            event_record: sym(&lib, b"cuEventRecord\0")?,
            event_synchronize: sym(&lib, b"cuEventSynchronize\0")?,
            event_destroy: sym(&lib, b"cuEventDestroy_v2\0")?,
            import_external_memory: sym(&lib, b"cuImportExternalMemory\0")?,
            external_memory_get_mapped_buffer: sym(&lib, b"cuExternalMemoryGetMappedBuffer\0")?,
            destroy_external_memory: sym(&lib, b"cuDestroyExternalMemory\0")?,
            device_get_uuid: sym(&lib, b"cuDeviceGetUuid_v2\0")?,
            semaphores: load_semaphores(&lib).ok(),
            array_create: sym(&lib, b"cuArray3DCreate_v2\0")?,
            array_destroy: sym(&lib, b"cuArrayDestroy\0")?,
            memcpy_2d_async: sym(&lib, b"cuMemcpy2DAsync_v2\0")?,
            get_error_name: sym(&lib, b"cuGetErrorName\0")?,
            get_error_string: sym(&lib, b"cuGetErrorString\0")?,
            _lib: lib,
        })
    }
}

/// The external-semaphore entry points of `lib`, or why one is missing.
///
/// # Safety
/// `lib` is a driver library; each prototype matches the cuda.h function of that name.
unsafe fn load_semaphores(lib: &Library) -> Result<SemaphoreFns, String> {
    // SAFETY: the prototypes are those of cuda.h (see the type aliases above).
    unsafe {
        Ok(SemaphoreFns {
            import: sym(lib, b"cuImportExternalSemaphore\0")?,
            signal: sym(lib, b"cuSignalExternalSemaphoresAsync\0")?,
            wait: sym(lib, b"cuWaitExternalSemaphoresAsync\0")?,
            destroy: sym(lib, b"cuDestroyExternalSemaphore\0")?,
        })
    }
}

/// Looks up one entry point; `name` is NUL-terminated.
///
/// # Safety
/// `T` must be the prototype of the function `name` names.
unsafe fn sym<T: Copy>(lib: &Library, name: &[u8]) -> Result<T, String> {
    // SAFETY: the caller guarantees the prototype; the copied pointer is only used while `lib`
    // (owned by the same `Driver`) is open.
    let symbol = unsafe { lib.get::<T>(name) }.map_err(|e| {
        let shown = name.strip_suffix(b"\0").unwrap_or(name);
        format!("{}: {e}", String::from_utf8_lossy(shown))
    })?;
    Ok(*symbol)
}

impl Driver {
    /// `what` failed with `code`: the driver's name and text for it.
    pub(crate) fn describe(&self, what: &str, code: CuResult) -> String {
        let mut name: *const c_char = std::ptr::null();
        let mut text: *const c_char = std::ptr::null();
        // SAFETY: both out pointers are valid for writes; the driver returns pointers to static
        // strings, read below only when it reported success.
        unsafe {
            (self.get_error_name)(code, &mut name);
            (self.get_error_string)(code, &mut text);
        }
        let read = |p: *const c_char| {
            if p.is_null() {
                "?".to_string()
            } else {
                // SAFETY: a non-null pointer from the driver is a NUL-terminated static string.
                unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
            }
        };
        format!(
            "CUDA {what} failed: {} ({code}): {}",
            read(name),
            read(text)
        )
    }

    /// `Ok` for `CUDA_SUCCESS`, else the driver's description of `what`.
    pub(crate) fn check(&self, what: &str, code: CuResult) -> Result<(), String> {
        if code == CUDA_SUCCESS {
            Ok(())
        } else {
            Err(self.describe(what, code))
        }
    }
}
