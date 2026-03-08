use crate::{GpuContextHandle, GpuError, GpuImageHandle, upload_rgba8_texture};
use image::{DynamicImage, GenericImageView, GrayImage, RgbImage, RgbaImage};
use std::any::Any;
use std::sync::Arc;
use std::sync::OnceLock;

/// Opt-in bridge to allow CPU types to participate in GPU segments.
/// Users implement this for their own types to describe how to upload/download.
pub trait GpuSendable {
    type GpuRepr;

    /// Upload CPU data to a GPU representation.
    fn upload(self, _ctx: &GpuContextHandle) -> Result<Self::GpuRepr, GpuError>
    where
        Self: Sized,
    {
        Err(GpuError::Unsupported)
    }

    /// Download a GPU representation back to CPU data.
    fn download(_gpu: &Self::GpuRepr, _ctx: &GpuContextHandle) -> Result<Self, GpuError>
    where
        Self: Sized,
    {
        Err(GpuError::Unsupported)
    }
}

/// Generic payload that can carry either CPU data or a GPU representation.
#[derive(Debug, Clone)]
pub enum Payload<T: GpuSendable> {
    Cpu(T),
    Gpu(T::GpuRepr),
}

impl<T: GpuSendable> Payload<T> {
    pub fn is_gpu(&self) -> bool {
        matches!(self, Payload::Gpu(_))
    }

    pub fn as_cpu(&self) -> Option<&T> {
        match self {
            Payload::Cpu(t) => Some(t),
            _ => None,
        }
    }

    pub fn as_gpu(&self) -> Option<&T::GpuRepr> {
        match self {
            Payload::Gpu(g) => Some(g),
            _ => None,
        }
    }
}

impl Payload<DynamicImage> {
    /// Return image dimensions without forcing the caller to pattern match.
    pub fn dimensions(&self) -> (u32, u32) {
        match self {
            Payload::Cpu(img) => img.dimensions(),
            Payload::Gpu(handle) => (handle.width, handle.height),
        }
    }

    /// Ensure this payload is resident on GPU; uploads if necessary and returns the handle and dimensions.
    pub fn into_gpu(self, ctx: &GpuContextHandle) -> Result<(GpuImageHandle, u32, u32), GpuError> {
        match self {
            Payload::Gpu(handle) => Ok((handle.clone(), handle.width, handle.height)),
            Payload::Cpu(img) => {
                let rgba = img.to_rgba8();
                let (w, h) = rgba.dimensions();
                let handle = upload_rgba8_texture(ctx, w, h, rgba.as_raw())?;
                Ok((handle, w, h))
            }
        }
    }
}

#[derive(Clone)]
enum ErasedPayloadInner {
    // Shared storage so CPU<->GPU transfers can be memoized across clones/fanout.
    Cached(Arc<ErasedPayloadCache>),
}

struct ErasedPayloadCache {
    cpu: OnceLock<Arc<dyn Any + Send + Sync>>,
    gpu: OnceLock<Arc<dyn Any + Send + Sync>>,
}

impl ErasedPayloadCache {
    fn new() -> Self {
        Self {
            cpu: OnceLock::new(),
            gpu: OnceLock::new(),
        }
    }
}

/// Type-erased payload wrapper so runtimes can carry GPU-capable data without monomorphizing.
#[derive(Clone)]
pub struct ErasedPayload {
    is_gpu: bool,
    inner: ErasedPayloadInner,
    cpu_type_name: &'static str,
    upload: fn(&ErasedPayloadInner, &GpuContextHandle) -> Result<ErasedPayload, GpuError>,
    download: fn(&ErasedPayloadInner, &GpuContextHandle) -> Result<ErasedPayload, GpuError>,
}

impl ErasedPayload {
    #[inline]
    fn cached_has_gpu(&self) -> bool {
        match &self.inner {
            ErasedPayloadInner::Cached(cell) => cell.gpu.get().is_some(),
        }
    }

    #[inline]
    fn cached_has_cpu(&self) -> bool {
        match &self.inner {
            ErasedPayloadInner::Cached(cell) => cell.cpu.get().is_some(),
        }
    }

    /// Upload to GPU only if this payload is not already GPU-resident.
    ///
    /// Returns `(payload, did_transfer)`. `did_transfer` is `true` only when this call performed
    /// a real CPU->GPU materialization (i.e. the cache did not already contain a GPU repr).
    ///
    /// This is intended for runtimes that want to dedupe transfer accounting across fanout.
    pub fn upload_if_needed(
        &self,
        ctx: &GpuContextHandle,
    ) -> Result<(ErasedPayload, bool), GpuError> {
        if self.is_gpu {
            return Ok((self.clone(), false));
        }
        // If another edge already uploaded this cached payload, avoid doing the work again.
        if self.cached_has_gpu() {
            let uploaded = (self.upload)(&self.inner, ctx)?;
            return Ok((uploaded, false));
        }
        let uploaded = (self.upload)(&self.inner, ctx)?;
        Ok((uploaded, true))
    }

    /// Download to CPU only if this payload is not already CPU-resident.
    ///
    /// Returns `(payload, did_transfer)`. `did_transfer` is `true` only when this call performed
    /// a real GPU->CPU materialization (i.e. the cache did not already contain a CPU repr).
    pub fn download_if_needed(
        &self,
        ctx: &GpuContextHandle,
    ) -> Result<(ErasedPayload, bool), GpuError> {
        if !self.is_gpu {
            return Ok((self.clone(), false));
        }
        if self.cached_has_cpu() {
            let downloaded = (self.download)(&self.inner, ctx)?;
            return Ok((downloaded, false));
        }
        let downloaded = (self.download)(&self.inner, ctx)?;
        Ok((downloaded, true))
    }
}

impl ErasedPayload {
    fn cross_dylib_ref<'a, T: 'static>(any: &'a dyn Any, expected: &str) -> Option<&'a T> {
        let actual = std::any::type_name::<T>();
        if expected != actual && !expected.ends_with(actual) && !actual.ends_with(expected) {
            return None;
        }
        let size_ok = std::mem::size_of_val(any) == std::mem::size_of::<T>();
        let align_ok = std::mem::align_of_val(any) == std::mem::align_of::<T>();
        if !(size_ok && align_ok) {
            if std::env::var_os("DAEDALUS_TRACE_PAYLOAD_CROSS_DYLIB").is_some() {
                eprintln!(
                    "daedalus-gpu: cross_dylib_ref size/align mismatch expected={} actual={} size_any={} size_t={} align_any={} align_t={}",
                    expected,
                    actual,
                    std::mem::size_of_val(any),
                    std::mem::size_of::<T>(),
                    std::mem::align_of_val(any),
                    std::mem::align_of::<T>(),
                );
            }
            return None;
        }
        let (data_ptr, _): (*const (), *const ()) = unsafe { std::mem::transmute(any) };
        Some(unsafe { &*(data_ptr as *const T) })
    }

    pub fn from_cpu<T>(val: T) -> Self
    where
        T: GpuSendable + Clone + Send + Sync + 'static,
        T::GpuRepr: Clone + Send + Sync + 'static,
    {
        fn upload<T>(
            inner: &ErasedPayloadInner,
            ctx: &GpuContextHandle,
        ) -> Result<ErasedPayload, GpuError>
        where
            T: GpuSendable + Clone + Send + Sync + 'static,
            T::GpuRepr: Clone + Send + Sync + 'static,
        {
            let ErasedPayloadInner::Cached(cell) = inner;
            if cell.gpu.get().is_some() {
                return Ok(ErasedPayload {
                    is_gpu: true,
                    inner: inner.clone(),
                    cpu_type_name: std::any::type_name::<T>(),
                    upload: upload::<T>,
                    download: download::<T>,
                });
            }
            let cpu_arc = cell.cpu.get().ok_or(GpuError::Unsupported)?;
            let cpu = cpu_arc
                .downcast_ref::<T>()
                .cloned()
                .or_else(|| {
                    ErasedPayload::cross_dylib_ref::<T>(
                        cpu_arc.as_ref(),
                        std::any::type_name::<T>(),
                    )
                    .cloned()
                })
                .ok_or(GpuError::Unsupported)?;
            let handle = cpu.upload(ctx)?;
            let _ = cell.gpu.set(Arc::new(handle.clone()));
            Ok(ErasedPayload {
                is_gpu: true,
                inner: inner.clone(),
                cpu_type_name: std::any::type_name::<T>(),
                upload: upload::<T>,
                download: download::<T>,
            })
        }

        fn download<T>(
            inner: &ErasedPayloadInner,
            _ctx: &GpuContextHandle,
        ) -> Result<ErasedPayload, GpuError>
        where
            T: GpuSendable + Clone + Send + Sync + 'static,
            T::GpuRepr: Clone + Send + Sync + 'static,
        {
            // CPU input: downloading is a no-op.
            let ErasedPayloadInner::Cached(cell) = inner;
            let cpu_arc = cell.cpu.get().ok_or(GpuError::Unsupported)?;
            if cpu_arc.downcast_ref::<T>().is_none()
                && ErasedPayload::cross_dylib_ref::<T>(cpu_arc.as_ref(), std::any::type_name::<T>())
                    .is_none()
            {
                return Err(GpuError::Unsupported);
            }
            Ok(ErasedPayload {
                is_gpu: false,
                inner: inner.clone(),
                cpu_type_name: std::any::type_name::<T>(),
                upload: upload::<T>,
                download: download::<T>,
            })
        }

        Self {
            is_gpu: false,
            inner: ErasedPayloadInner::Cached({
                let cache = ErasedPayloadCache::new();
                let _ = cache.cpu.set(Arc::new(val));
                Arc::new(cache)
            }),
            cpu_type_name: std::any::type_name::<T>(),
            upload: upload::<T>,
            download: download::<T>,
        }
    }

    pub fn from_gpu<T>(val: T::GpuRepr) -> Self
    where
        T: GpuSendable + Clone + Send + Sync + 'static,
        T::GpuRepr: Clone + Send + Sync + 'static,
    {
        fn upload<T>(
            inner: &ErasedPayloadInner,
            _ctx: &GpuContextHandle,
        ) -> Result<ErasedPayload, GpuError>
        where
            T: GpuSendable + Clone + Send + Sync + 'static,
            T::GpuRepr: Clone + Send + Sync + 'static,
        {
            // GPU input: uploading is a no-op.
            let ErasedPayloadInner::Cached(cell) = inner;
            let gpu_arc = cell.gpu.get().ok_or(GpuError::Unsupported)?;
            if gpu_arc.downcast_ref::<T::GpuRepr>().is_none()
                && ErasedPayload::cross_dylib_ref::<T::GpuRepr>(
                    gpu_arc.as_ref(),
                    std::any::type_name::<T::GpuRepr>(),
                )
                .is_none()
            {
                return Err(GpuError::Unsupported);
            }
            Ok(ErasedPayload {
                is_gpu: true,
                inner: inner.clone(),
                cpu_type_name: std::any::type_name::<T>(),
                upload: upload::<T>,
                download: download::<T>,
            })
        }

        fn download<T>(
            inner: &ErasedPayloadInner,
            ctx: &GpuContextHandle,
        ) -> Result<ErasedPayload, GpuError>
        where
            T: GpuSendable + Clone + Send + Sync + 'static,
            T::GpuRepr: Clone + Send + Sync + 'static,
        {
            let ErasedPayloadInner::Cached(cell) = inner;
            // Fast path: already downloaded by another clone.
            if let Some(cpu) = cell.cpu.get()
                && (cpu.downcast_ref::<T>().is_some()
                    || ErasedPayload::cross_dylib_ref::<T>(
                        cpu.as_ref(),
                        std::any::type_name::<T>(),
                    )
                    .is_some())
            {
                return Ok(ErasedPayload {
                    is_gpu: false,
                    inner: inner.clone(),
                    cpu_type_name: std::any::type_name::<T>(),
                    upload: upload::<T>,
                    download: download::<T>,
                });
            }

            let gpu_arc = cell.gpu.get().ok_or(GpuError::Unsupported)?;
            let g = gpu_arc
                .downcast_ref::<T::GpuRepr>()
                .or_else(|| {
                    ErasedPayload::cross_dylib_ref::<T::GpuRepr>(
                        gpu_arc.as_ref(),
                        std::any::type_name::<T::GpuRepr>(),
                    )
                })
                .ok_or(GpuError::Unsupported)?;
            let cpu = T::download(g, ctx)?;
            let _ = cell.cpu.set(Arc::new(cpu));
            Ok(ErasedPayload {
                is_gpu: false,
                inner: inner.clone(),
                cpu_type_name: std::any::type_name::<T>(),
                upload: upload::<T>,
                download: download::<T>,
            })
        }

        Self {
            is_gpu: true,
            inner: ErasedPayloadInner::Cached({
                let cache = ErasedPayloadCache::new();
                let _ = cache.gpu.set(Arc::new(val));
                Arc::new(cache)
            }),
            cpu_type_name: std::any::type_name::<T>(),
            upload: upload::<T>,
            download: download::<T>,
        }
    }

    pub fn is_gpu(&self) -> bool {
        self.is_gpu
    }

    pub fn upload(&self, ctx: &GpuContextHandle) -> Result<ErasedPayload, GpuError> {
        (self.upload)(&self.inner, ctx)
    }

    pub fn download(&self, ctx: &GpuContextHandle) -> Result<ErasedPayload, GpuError> {
        (self.download)(&self.inner, ctx)
    }

    pub fn as_cpu<T>(&self) -> Option<&T>
    where
        T: GpuSendable + 'static,
    {
        if self.is_gpu {
            None
        } else {
            match &self.inner {
                ErasedPayloadInner::Cached(cell) => {
                    let inner = cell.cpu.get()?;
                    inner
                        .downcast_ref::<T>()
                        .or_else(|| Self::cross_dylib_ref::<T>(inner.as_ref(), self.cpu_type_name))
                }
            }
        }
    }

    /// Borrow the CPU-side backing allocation as an `Arc<T>` when possible.
    ///
    /// This is a fast path for same-crate / same-TypeId consumers. Across dynamic plugin
    /// boundaries, `TypeId` may not match; use `as_cpu` to borrow without cloning.
    pub fn arc_cpu_any<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        if self.is_gpu {
            return None;
        }
        let ErasedPayloadInner::Cached(cell) = &self.inner;
        let cpu = cell.cpu.get()?.clone();
        Arc::downcast::<T>(cpu).ok()
    }

    /// Borrow the GPU-side backing allocation as an `Arc<T>` when possible.
    pub fn arc_gpu_any<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        if !self.is_gpu {
            return None;
        }
        let ErasedPayloadInner::Cached(cell) = &self.inner;
        let gpu = cell.gpu.get()?.clone();
        Arc::downcast::<T>(gpu).ok()
    }

    pub fn as_gpu<T>(&self) -> Option<&T::GpuRepr>
    where
        T: GpuSendable + 'static,
        T::GpuRepr: 'static,
    {
        if self.is_gpu {
            match &self.inner {
                ErasedPayloadInner::Cached(cell) => {
                    let inner = cell.gpu.get()?;
                    inner.downcast_ref::<T::GpuRepr>()
                }
            }
        } else {
            None
        }
    }

    pub fn try_downcast_cpu_any<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        if self.is_gpu {
            return None;
        }
        let ErasedPayloadInner::Cached(cell) = &self.inner;
        let inner = cell.cpu.get()?;
        if let Some(v) = inner.downcast_ref::<T>() {
            return Some(v.clone());
        }
        Self::cross_dylib_ref::<T>(inner.as_ref(), self.cpu_type_name).cloned()
    }

    pub fn clone_cpu<T>(&self) -> Option<T>
    where
        T: GpuSendable + Clone + 'static,
    {
        if self.is_gpu {
            return None;
        }
        let ErasedPayloadInner::Cached(cell) = &self.inner;
        let inner = cell.cpu.get()?;
        if let Some(v) = inner.downcast_ref::<T>().cloned() {
            return Some(v);
        }
        if self.cpu_type_name != std::any::type_name::<T>() {
            return None;
        }
        Self::cross_dylib_ref::<T>(inner.as_ref(), self.cpu_type_name).cloned()
    }

    pub fn take_cpu<T>(self) -> Result<T, Self>
    where
        T: GpuSendable + Clone + Send + Sync + 'static,
    {
        if self.is_gpu {
            return Err(self);
        }

        let ErasedPayload {
            is_gpu,
            inner,
            cpu_type_name,
            upload,
            download,
        } = self;
        let restore = |inner| ErasedPayload {
            is_gpu,
            inner,
            cpu_type_name,
            upload,
            download,
        };

        match inner {
            ErasedPayloadInner::Cached(cell) => {
                // Conservatively clone; `take_cpu` is an optimization path and should remain correct
                // under sharing/fanout.
                let cpu = cell
                    .cpu
                    .get()
                    .ok_or_else(|| restore(ErasedPayloadInner::Cached(cell.clone())))?;
                if let Some(v) = cpu.downcast_ref::<T>().cloned() {
                    return Ok(v);
                }
                if cpu_type_name == std::any::type_name::<T>()
                    && let Some(v) = Self::cross_dylib_ref::<T>(cpu.as_ref(), cpu_type_name)
                {
                    return Ok(v.clone());
                }
                Err(restore(ErasedPayloadInner::Cached(cell.clone())))
            }
        }
    }

    pub fn take_cpu_any<T>(self) -> Result<T, Self>
    where
        T: Clone + Send + Sync + 'static,
    {
        if self.is_gpu {
            return Err(self);
        }

        let ErasedPayload {
            is_gpu,
            inner,
            cpu_type_name,
            upload,
            download,
        } = self;
        let restore = |inner| ErasedPayload {
            is_gpu,
            inner,
            cpu_type_name,
            upload,
            download,
        };

        match inner {
            ErasedPayloadInner::Cached(cell) => {
                let cpu = cell
                    .cpu
                    .get()
                    .ok_or_else(|| restore(ErasedPayloadInner::Cached(cell.clone())))?;
                if let Some(v) = cpu.downcast_ref::<T>().cloned() {
                    return Ok(v);
                }
                if let Some(v) = Self::cross_dylib_ref::<T>(cpu.as_ref(), cpu_type_name) {
                    return Ok(v.clone());
                }
                Err(restore(ErasedPayloadInner::Cached(cell.clone())))
            }
        }
    }

    pub fn clone_gpu<T>(&self) -> Option<T::GpuRepr>
    where
        T: GpuSendable + 'static,
        T::GpuRepr: Clone + 'static,
    {
        if !self.is_gpu {
            return None;
        }
        let ErasedPayloadInner::Cached(cell) = &self.inner;
        let inner = cell.gpu.get()?;
        if let Some(v) = inner.downcast_ref::<T::GpuRepr>().cloned() {
            return Some(v);
        }
        if let Some(v) =
            Self::cross_dylib_ref::<T::GpuRepr>(inner.as_ref(), std::any::type_name::<T::GpuRepr>())
        {
            return Some(v.clone());
        }
        if self.cpu_type_name != std::any::type_name::<T>() {
            return None;
        }
        None
    }
}

impl std::fmt::Debug for ErasedPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasedPayload")
            .field("is_gpu", &self.is_gpu)
            .field("cpu_type_name", &self.cpu_type_name)
            .finish()
    }
}

impl GpuSendable for DynamicImage {
    type GpuRepr = GpuImageHandle;

    fn upload(self, ctx: &GpuContextHandle) -> Result<Self::GpuRepr, GpuError> {
        let rgba = self.to_rgba8();
        let (width, height) = rgba.dimensions();
        upload_rgba8_texture(ctx, width, height, rgba.as_raw())
    }

    fn download(gpu: &Self::GpuRepr, ctx: &GpuContextHandle) -> Result<Self, GpuError> {
        let bytes = ctx.read_texture(gpu)?;
        let buf = image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(gpu.width, gpu.height, bytes)
            .ok_or(GpuError::AllocationFailed)?;
        Ok(DynamicImage::ImageRgba8(buf))
    }
}

impl GpuSendable for RgbaImage {
    type GpuRepr = GpuImageHandle;

    fn upload(self, ctx: &GpuContextHandle) -> Result<Self::GpuRepr, GpuError> {
        let (width, height) = self.dimensions();
        upload_rgba8_texture(ctx, width, height, self.as_raw())
    }

    fn download(gpu: &Self::GpuRepr, ctx: &GpuContextHandle) -> Result<Self, GpuError> {
        let bytes = ctx.read_texture(gpu)?;
        image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(gpu.width, gpu.height, bytes)
            .ok_or(GpuError::AllocationFailed)
    }
}

impl GpuSendable for RgbImage {
    type GpuRepr = GpuImageHandle;

    fn upload(self, ctx: &GpuContextHandle) -> Result<Self::GpuRepr, GpuError> {
        let (width, height) = self.dimensions();
        let rgba = image::ImageBuffer::from_fn(width, height, |x, y| {
            let p = self.get_pixel(x, y);
            image::Rgba([p[0], p[1], p[2], 255])
        });
        upload_rgba8_texture(ctx, width, height, rgba.as_raw())
    }

    fn download(gpu: &Self::GpuRepr, ctx: &GpuContextHandle) -> Result<Self, GpuError> {
        let bytes = ctx.read_texture(gpu)?;
        let rgba = image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(gpu.width, gpu.height, bytes)
            .ok_or(GpuError::AllocationFailed)?;
        Ok(image::ImageBuffer::from_fn(
            gpu.width,
            gpu.height,
            |x, y| {
                let p = rgba.get_pixel(x, y);
                image::Rgb([p[0], p[1], p[2]])
            },
        ))
    }
}

impl GpuSendable for GrayImage {
    type GpuRepr = GpuImageHandle;

    fn upload(self, ctx: &GpuContextHandle) -> Result<Self::GpuRepr, GpuError> {
        let (width, height) = self.dimensions();
        // Prefer an R8 upload when supported (reduces memory and readback bandwidth).
        if ctx
            .capabilities()
            .supported_formats
            .iter()
            .any(|f| matches!(f, crate::GpuFormat::R8Unorm))
        {
            return crate::upload_r8_texture(ctx, width, height, self.as_raw());
        }

        // Fallback: expand to RGBA8.
        let mut rgba = Vec::with_capacity(
            (width as usize)
                .saturating_mul(height as usize)
                .saturating_mul(4),
        );
        for &v in self.as_raw() {
            rgba.extend_from_slice(&[v, v, v, 255]);
        }
        upload_rgba8_texture(ctx, width, height, &rgba)
    }

    fn download(gpu: &Self::GpuRepr, ctx: &GpuContextHandle) -> Result<Self, GpuError> {
        let bytes = ctx.read_texture(gpu)?;
        match gpu.format {
            crate::GpuFormat::R8Unorm => image::ImageBuffer::from_raw(gpu.width, gpu.height, bytes)
                .ok_or(GpuError::AllocationFailed),
            _ => {
                let mut gray =
                    Vec::with_capacity((gpu.width as usize).saturating_mul(gpu.height as usize));
                for rgba in bytes.chunks_exact(4) {
                    gray.push(rgba[0]);
                }
                image::ImageBuffer::from_raw(gpu.width, gpu.height, gray)
                    .ok_or(GpuError::AllocationFailed)
            }
        }
    }
}

impl Payload<DynamicImage> {
    /// Get RGBA8 bytes + dimensions, downloading from GPU if needed.
    pub fn to_rgba_bytes(
        &self,
        ctx: Option<&GpuContextHandle>,
    ) -> Result<(Vec<u8>, u32, u32), GpuError> {
        match self {
            Payload::Cpu(cpu) => {
                let rgba = cpu.to_rgba8();
                let (w, h) = rgba.dimensions();
                Ok((rgba.into_raw(), w, h))
            }
            Payload::Gpu(handle) => {
                let ctx = ctx.ok_or(GpuError::Unsupported)?;
                let bytes = ctx.read_texture(handle)?;
                Ok((bytes, handle.width, handle.height))
            }
        }
    }

    /// Construct a payload from RGBA8 bytes, uploading if a GPU context is available.
    pub fn from_rgba_bytes(
        ctx: Option<&GpuContextHandle>,
        bytes: Vec<u8>,
        w: u32,
        h: u32,
    ) -> Result<Self, GpuError> {
        if let Some(ctx) = ctx {
            upload_rgba8_texture(ctx, w, h, &bytes).map(Payload::Gpu)
        } else {
            image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(w, h, bytes)
                .map(DynamicImage::ImageRgba8)
                .map(Payload::Cpu)
                .ok_or(GpuError::AllocationFailed)
        }
    }
}
