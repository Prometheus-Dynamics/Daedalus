//! CUDA images: a 2D CUDA array per image, filled and read with 2D copies from host memory. The
//! texel layouts are the byte layouts of the formats they carry (one 8-bit channel per byte, or
//! half floats), so a row of a host image is a row of the array.

use std::sync::Arc;

use super::device::CudaDevice;
use super::ffi::{
    AD_FORMAT_HALF, AD_FORMAT_UNSIGNED_INT8, ArrayDesc, CuArray, MEMORYTYPE_ARRAY, MEMORYTYPE_HOST,
    Memcpy2d,
};
use crate::handles::GpuDropToken;
use crate::{GpuError, GpuFormat};

/// The CUDA array format, channel count and bytes per texel of a [`GpuFormat`], for the formats a
/// CUDA image holds (one sample per texel, no depth or planar formats).
fn texel(format: GpuFormat) -> Option<(u32, u32, usize)> {
    match format {
        GpuFormat::R8Unorm => Some((AD_FORMAT_UNSIGNED_INT8, 1, 1)),
        GpuFormat::Rg8Unorm => Some((AD_FORMAT_UNSIGNED_INT8, 2, 2)),
        GpuFormat::Rgba8Unorm => Some((AD_FORMAT_UNSIGNED_INT8, 4, 4)),
        GpuFormat::Rgba16Float => Some((AD_FORMAT_HALF, 4, 8)),
        _ => None,
    }
}

/// A 2D CUDA array, destroyed with its last handle.
pub(crate) struct DeviceImage {
    dev: Arc<CudaDevice>,
    array: CuArray,
    width: usize,
    height: usize,
    /// Bytes per texel row of the host image (`width * bytes per texel`).
    row_bytes: usize,
}

impl std::fmt::Debug for DeviceImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceImage")
            .field("width", &self.width)
            .field("height", &self.height)
            .finish()
    }
}

// SAFETY: as for `CudaDevice`: the array is a driver object, usable from any thread while its
// context is current there, which every method makes so first.
unsafe impl Send for DeviceImage {}
unsafe impl Sync for DeviceImage {}

impl GpuDropToken for DeviceImage {}

impl Drop for DeviceImage {
    fn drop(&mut self) {
        let _ = self.dev.make_current();
        // SAFETY: `array` came from `cuArray3DCreate` on this context and is destroyed once.
        unsafe { (self.dev.drv.array_destroy)(self.array) };
    }
}

impl CudaDevice {
    /// A `width` by `height` array of `format`, on this device.
    pub(crate) fn alloc_image(
        self: &Arc<Self>,
        format: GpuFormat,
        width: u32,
        height: u32,
    ) -> Result<Arc<DeviceImage>, GpuError> {
        let (cu_format, channels, bytes) = texel(format).ok_or(GpuError::Unsupported)?;
        if width == 0 || height == 0 {
            return Err(GpuError::InvalidInput("zero-sized CUDA image".into()));
        }
        self.make_current()?;
        let desc = ArrayDesc {
            width: width as usize,
            height: height as usize,
            depth: 0,
            format: cu_format,
            num_channels: channels,
            flags: 0,
        };
        let mut array: CuArray = std::ptr::null_mut();
        // SAFETY: `desc` is a fully initialized `CUDA_ARRAY3D_DESCRIPTOR`; `array` is written on
        // success only.
        let code = unsafe { (self.drv.array_create)(&mut array, &desc) };
        self.drv
            .check("cuArray3DCreate", code)
            .map_err(GpuError::Internal)?;
        Ok(Arc::new(DeviceImage {
            dev: Arc::clone(self),
            array,
            width: width as usize,
            height: height as usize,
            row_bytes: width as usize * bytes,
        }))
    }

    /// Copy `data` (rows of `row_bytes`, tightly packed) into the image, and wait for the copy.
    pub(crate) fn image_upload(&self, img: &DeviceImage, data: &[u8]) -> Result<(), GpuError> {
        if data.len() != img.row_bytes * img.height {
            return Err(GpuError::InvalidInput(format!(
                "{} bytes do not fill a {}x{} CUDA image",
                data.len(),
                img.width,
                img.height
            )));
        }
        let copy = Memcpy2d {
            src_x_in_bytes: 0,
            src_y: 0,
            src_memory_type: MEMORYTYPE_HOST,
            src_host: data.as_ptr().cast(),
            src_device: 0,
            src_array: std::ptr::null_mut(),
            src_pitch: img.row_bytes,
            dst_x_in_bytes: 0,
            dst_y: 0,
            dst_memory_type: MEMORYTYPE_ARRAY,
            dst_host: std::ptr::null_mut(),
            dst_device: 0,
            dst_array: img.array,
            dst_pitch: 0,
            width_in_bytes: img.row_bytes,
            height: img.height,
        };
        self.copy_2d(&copy)
    }

    /// The image's bytes, copied out (rows of `row_bytes`, tightly packed), after the work queued
    /// before the copy.
    pub(crate) fn image_download(&self, img: &DeviceImage) -> Result<Vec<u8>, GpuError> {
        let mut out = vec![0u8; img.row_bytes * img.height];
        let copy = Memcpy2d {
            src_x_in_bytes: 0,
            src_y: 0,
            src_memory_type: MEMORYTYPE_ARRAY,
            src_host: std::ptr::null(),
            src_device: 0,
            src_array: img.array,
            src_pitch: 0,
            dst_x_in_bytes: 0,
            dst_y: 0,
            dst_memory_type: MEMORYTYPE_HOST,
            dst_host: out.as_mut_ptr().cast(),
            dst_device: 0,
            dst_array: std::ptr::null_mut(),
            dst_pitch: img.row_bytes,
            width_in_bytes: img.row_bytes,
            height: img.height,
        };
        self.copy_2d(&copy)?;
        Ok(out)
    }

    /// Queue a 2D copy on the stream and wait for it.
    fn copy_2d(&self, copy: &Memcpy2d) -> Result<(), GpuError> {
        self.make_current()?;
        // SAFETY: `copy` describes live host memory (borrowed by the caller for this call) and a
        // live array; the stream is synchronized below before the caller reads or drops either.
        let code = unsafe { (self.drv.memcpy_2d_async)(copy, self.stream) };
        self.drv
            .check("cuMemcpy2DAsync", code)
            .map_err(GpuError::Internal)?;
        self.synchronize()
    }
}
