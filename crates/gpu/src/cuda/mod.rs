//! The NVIDIA CUDA backend (`gpu-cuda`): the driver API loaded at runtime, device memory with
//! its own residency, and one kernel (`scale_f32`) that proves a node runs on the device.
//!
//! A CUDA buffer is a [`GpuBufferHandle`] like any other; the difference is the port type. Values
//! in CUDA device memory are keyed `…@cuda` (see [`GpuBackendKind::device_type_suffix`]), so a
//! wgpu device value is never handed to a CUDA node, and the other way round.
//!
//! Not implemented: images and textures, dmabuf import, and CUDA↔Vulkan interop
//! (`cudaImportExternalMemory`), which would need both backends on one device.

mod device;
mod external;
mod ffi;
mod image;
mod ptx;

use std::collections::HashMap;
use std::os::fd::OwnedFd;
use std::sync::{Arc, Mutex, Weak};

use crate::handles::GpuDropToken;
use crate::{
    GpuAdapterInfo, GpuBackend, GpuBackendKind, GpuBufferHandle, GpuBufferId, GpuCapabilities,
    GpuComputeCaps, GpuContextHandle, GpuError, GpuImageHandle, GpuImageId, GpuImageRequest,
    GpuMemoryLocation, GpuOptions, GpuRequest, GpuUsage, TransferStats,
};
use device::{CudaDevice, DeviceMemory};
use image::DeviceImage;

pub use device::CudaFence;
pub use external::CudaImport;

/// The NVIDIA vendor's PCI id.
const NVIDIA_VENDOR_ID: u32 = 0x10DE;

/// A CUDA device, and the device memory of the buffers made on it.
///
/// Opened by [`CudaBackend::new`] (or by selection with `preferred_backend = Cuda`): device 0 of
/// the driver. Buffers made by one `CudaBackend` are usable only through that backend.
pub struct CudaBackend {
    dev: Arc<CudaDevice>,
    adapter: GpuAdapterInfo,
    caps: GpuCapabilities,
    /// Live device buffers by id; weak, so a buffer's memory goes with its last handle.
    live: Mutex<HashMap<GpuBufferId, Weak<DeviceMemory>>>,
    /// Live CUDA images by id, weak like `live`.
    live_images: Mutex<HashMap<GpuImageId, Weak<DeviceImage>>>,
    stats: Mutex<TransferStats>,
}

impl std::fmt::Debug for CudaBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CudaBackend")
            .field("adapter", &self.adapter.name)
            .finish_non_exhaustive()
    }
}

impl CudaBackend {
    /// Open CUDA device 0. Fails with the reason when the driver library, the driver's entry
    /// points, or a device is missing, so a caller can report why CUDA was skipped.
    pub fn new() -> Result<Self, GpuError> {
        Self::open(0).map_err(GpuError::Internal)
    }

    fn open(ordinal: i32) -> Result<Self, String> {
        let dev = CudaDevice::open(ordinal).map(Arc::new)?;
        let adapter = GpuAdapterInfo {
            name: dev.name.clone(),
            backend: GpuBackendKind::Cuda,
            device_id: Some(format!("cuda:{}", dev.ordinal)),
            vendor_id: Some(format!("{NVIDIA_VENDOR_ID:04x}")),
        };
        let caps = capabilities(&dev);
        Ok(Self {
            dev,
            adapter,
            caps,
            live: Mutex::new(HashMap::new()),
            live_images: Mutex::new(HashMap::new()),
            stats: Mutex::new(TransferStats::default()),
        })
    }

    /// The device's compute capability, `(major, minor)`.
    pub fn compute_capability(&self) -> (i32, i32) {
        self.dev.compute
    }

    /// Allocate device memory for `data` and copy it in, waiting for the copy.
    ///
    /// The handle's memory is a CUDA device buffer of this backend.
    pub fn upload(&self, data: &[u8]) -> Result<GpuBufferHandle, GpuError> {
        if data.is_empty() {
            return Err(GpuError::InvalidInput("empty CUDA upload".into()));
        }
        let handle = self.alloc(data.len() as u64, GpuUsage::UPLOAD | GpuUsage::STORAGE)?;
        let mem = self.memory(&handle)?;
        self.dev.upload(&mem, data)?;
        self.with_stats(|s| s.record_upload(data.len() as u64));
        Ok(handle)
    }

    /// Copy the whole of a device buffer back to the host, after the work queued before it.
    pub fn download(&self, buffer: &GpuBufferHandle) -> Result<Vec<u8>, GpuError> {
        let mem = self.memory(buffer)?;
        let mut out = vec![0u8; mem.bytes as usize];
        self.dev.download(&mem, &mut out)?;
        self.with_stats(|s| s.record_download(out.len() as u64));
        Ok(out)
    }

    /// Queue `data[i] *= factor` over the buffer's `f32` values (its length in bytes over 4). The
    /// returned fence finishes when the kernel has; a later [`download`](Self::download) waits
    /// for it on its own.
    pub fn scale_f32(&self, buffer: &GpuBufferHandle, factor: f32) -> Result<CudaFence, GpuError> {
        let mem = self.memory(buffer)?;
        let count = u32::try_from(mem.bytes / 4)
            .map_err(|_| GpuError::InvalidInput("buffer too large for scale_f32".into()))?;
        self.dev.scale_f32(&mem, count, factor)
    }

    /// Import memory exported as an opaque fd of `bytes` (a Vulkan `vkGetMemoryFdKHR` of an
    /// allocation on the same GPU, see [`CudaBackend::device_uuid`]) as a device buffer of this
    /// backend, without a copy. The driver consumes the fd; on failure the error carries the
    /// driver's code. Imports are not counted in [`TransferStats`]: nothing moves.
    pub fn import_opaque_fd(&self, fd: OwnedFd, bytes: u64) -> Result<CudaImport, GpuError> {
        self.dev.import_opaque_fd(fd, bytes)
    }

    /// This device's UUID: the Vulkan `deviceUUID` of the same GPU, which is how a caller finds
    /// the wgpu device that can share memory with this backend.
    pub fn device_uuid(&self) -> Result<[u8; 16], GpuError> {
        self.dev.uuid()
    }

    /// The image of `handle`, which must be a live CUDA image of this backend.
    fn image(&self, handle: &GpuImageHandle) -> Result<Arc<DeviceImage>, GpuError> {
        self.live_images
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&handle.id)
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                GpuError::InvalidInput(format!(
                    "image {} is not a live CUDA image of this backend",
                    handle.id
                ))
            })
    }

    /// A handle for `img`, which lives as long as the handle does.
    fn register_image(&self, req: &GpuImageRequest, img: Arc<DeviceImage>) -> GpuImageHandle {
        let mut handle = GpuImageHandle::new(
            req.format,
            req.width,
            req.height,
            GpuMemoryLocation::Gpu,
            req.usage,
        );
        let token: Arc<dyn GpuDropToken> = Arc::clone(&img) as Arc<dyn GpuDropToken>;
        handle.drop_token = Some(token);
        let mut live = self.live_images.lock().unwrap_or_else(|e| e.into_inner());
        live.retain(|_, weak| weak.strong_count() > 0);
        live.insert(handle.id, Arc::downgrade(&img));
        handle
    }

    fn alloc(&self, bytes: u64, usage: GpuUsage) -> Result<GpuBufferHandle, GpuError> {
        let mem = Arc::new(self.dev.alloc(bytes)?);
        let mut handle = GpuBufferHandle::new(bytes, GpuMemoryLocation::Gpu, usage);
        let token: Arc<dyn GpuDropToken> = Arc::clone(&mem) as Arc<dyn GpuDropToken>;
        handle.drop_token = Some(token);
        let mut live = self.live.lock().unwrap_or_else(|e| e.into_inner());
        live.retain(|_, weak| weak.strong_count() > 0);
        live.insert(handle.id, Arc::downgrade(&mem));
        Ok(handle)
    }

    /// The device memory of `buffer`, which must be a live buffer of this backend.
    fn memory(&self, buffer: &GpuBufferHandle) -> Result<Arc<DeviceMemory>, GpuError> {
        self.live
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&buffer.id)
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                GpuError::InvalidInput(format!(
                    "buffer {} is not a live CUDA device buffer of this backend",
                    buffer.id
                ))
            })
    }

    fn with_stats(&self, f: impl FnOnce(&mut TransferStats)) {
        f(&mut self.stats.lock().unwrap_or_else(|e| e.into_inner()));
    }
}

fn capabilities(dev: &CudaDevice) -> GpuCapabilities {
    GpuCapabilities {
        supported_formats: Vec::new(),
        format_features: Vec::new(),
        format_blocks: Vec::new(),
        max_buffer_size: dev.total_mem,
        max_texture_dimension: 0,
        max_texture_samples: 1,
        staging_alignment: 1,
        max_inflight_copies: 1,
        queue_count: 1,
        min_buffer_copy_offset_alignment: 1,
        bytes_per_row_alignment: 1,
        rows_per_image_alignment: 1,
        has_transfer_queue: false,
        compute: GpuComputeCaps {
            max_invocations_per_workgroup: 1024,
            max_workgroup_size: [1024, 1024, 64],
            ..GpuComputeCaps::default()
        },
    }
}

impl GpuBackend for CudaBackend {
    fn kind(&self) -> GpuBackendKind {
        GpuBackendKind::Cuda
    }

    fn adapter_info(&self) -> GpuAdapterInfo {
        self.adapter.clone()
    }

    fn capabilities(&self) -> GpuCapabilities {
        self.caps.clone()
    }

    fn select_adapter(&self, opts: &GpuOptions) -> Result<GpuAdapterInfo, GpuError> {
        if let Some(pref) = opts.preferred_backend
            && pref != GpuBackendKind::Cuda
        {
            return Err(GpuError::AdapterUnavailable);
        }
        if let Some(label) = &opts.adapter_label
            && label != &self.adapter.name
        {
            return Err(GpuError::AdapterUnavailable);
        }
        Ok(self.adapter.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn create_buffer(&self, req: &GpuRequest) -> Result<GpuBufferHandle, GpuError> {
        if req.usage.is_empty() {
            return Err(GpuError::Unsupported);
        }
        if req.size_bytes == 0 {
            return Err(GpuError::InvalidInput("zero-sized CUDA buffer".into()));
        }
        self.alloc(req.size_bytes, req.usage)
    }

    fn create_image(&self, req: &GpuImageRequest) -> Result<GpuImageHandle, GpuError> {
        if req.samples > 1 {
            return Err(GpuError::Unsupported);
        }
        let img = self.dev.alloc_image(req.format, req.width, req.height)?;
        Ok(self.register_image(req, img))
    }

    fn upload_texture(
        &self,
        req: &GpuImageRequest,
        data: &[u8],
    ) -> Result<GpuImageHandle, GpuError> {
        if req.samples > 1 {
            return Err(GpuError::Unsupported);
        }
        let img = self.dev.alloc_image(req.format, req.width, req.height)?;
        self.dev.image_upload(&img, data)?;
        self.with_stats(|s| s.record_upload(data.len() as u64));
        Ok(self.register_image(req, img))
    }

    fn read_texture(&self, handle: &GpuImageHandle) -> Result<Vec<u8>, GpuError> {
        let img = self.image(handle)?;
        let out = self.dev.image_download(&img)?;
        self.with_stats(|s| s.record_download(out.len() as u64));
        Ok(out)
    }

    fn stats(&self) -> TransferStats {
        *self.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn take_stats(&self) -> TransferStats {
        std::mem::take(&mut *self.stats.lock().unwrap_or_else(|e| e.into_inner()))
    }

    fn record_download(&self, bytes: u64) {
        self.with_stats(|s| s.record_download(bytes));
    }
}

impl GpuContextHandle {
    /// The CUDA backend of this handle, if it is one.
    pub fn cuda(&self) -> Option<&CudaBackend> {
        self.backend_ref().as_any().downcast_ref::<CudaBackend>()
    }
}
