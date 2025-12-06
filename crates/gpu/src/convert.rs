use crate::{GpuContextHandle, GpuError, GpuImageHandle, upload_rgba8_texture};
use image::{DynamicImage, GenericImageView, GrayImage, RgbImage, RgbaImage};
use std::any::Any;
use std::sync::Arc;

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
    Any(Arc<dyn Any + Send + Sync>),
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
    fn cross_dylib_ref<'a, T: 'static>(any: &'a dyn Any, expected: &str) -> Option<&'a T> {
        let actual = std::any::type_name::<T>();
        if expected != actual && !expected.ends_with(actual) && !actual.ends_with(expected) {
            return None;
        }
        if std::mem::size_of_val(any) != std::mem::size_of::<T>()
            || std::mem::align_of_val(any) != std::mem::align_of::<T>()
        {
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
        fn upload<T>(inner: &ErasedPayloadInner, ctx: &GpuContextHandle) -> Result<ErasedPayload, GpuError>
        where
            T: GpuSendable + Clone + Send + Sync + 'static,
            T::GpuRepr: Clone + Send + Sync + 'static,
        {
            let ErasedPayloadInner::Any(inner) = inner;
            let cpu = inner
                .downcast_ref::<T>()
                .ok_or(GpuError::Unsupported)?
                .clone();
            let handle = cpu.upload(ctx)?;
            Ok(ErasedPayload::from_gpu::<T>(handle))
        }

        fn download<T>(inner: &ErasedPayloadInner, _ctx: &GpuContextHandle) -> Result<ErasedPayload, GpuError>
        where
            T: GpuSendable + Clone + Send + Sync + 'static,
            T::GpuRepr: Clone + Send + Sync + 'static,
        {
            let ErasedPayloadInner::Any(inner) = inner;
            let cpu = inner
                .downcast_ref::<T>()
                .ok_or(GpuError::Unsupported)?
                .clone();
            Ok(ErasedPayload::from_cpu::<T>(cpu))
        }

        Self {
            is_gpu: false,
            inner: ErasedPayloadInner::Any(Arc::new(val)),
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
        fn upload<T>(inner: &ErasedPayloadInner, _ctx: &GpuContextHandle) -> Result<ErasedPayload, GpuError>
        where
            T: GpuSendable + Clone + Send + Sync + 'static,
            T::GpuRepr: Clone + Send + Sync + 'static,
        {
            let ErasedPayloadInner::Any(inner) = inner;
            let g = inner
                .downcast_ref::<T::GpuRepr>()
                .ok_or(GpuError::Unsupported)?
                .clone();
            Ok(ErasedPayload::from_gpu::<T>(g))
        }

        fn download<T>(inner: &ErasedPayloadInner, ctx: &GpuContextHandle) -> Result<ErasedPayload, GpuError>
        where
            T: GpuSendable + Clone + Send + Sync + 'static,
            T::GpuRepr: Clone + Send + Sync + 'static,
        {
            let ErasedPayloadInner::Any(inner) = inner;
            let g = inner
                .downcast_ref::<T::GpuRepr>()
                .ok_or(GpuError::Unsupported)?;
            let cpu = T::download(g, ctx)?;
            Ok(ErasedPayload::from_cpu::<T>(cpu))
        }

        Self {
            is_gpu: true,
            inner: ErasedPayloadInner::Any(Arc::new(val)),
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
                ErasedPayloadInner::Any(inner) => {
                    inner
                        .downcast_ref::<T>()
                        .or_else(|| Self::cross_dylib_ref::<T>(inner.as_ref(), self.cpu_type_name))
                }
            }
        }
    }

    pub fn as_gpu<T>(&self) -> Option<&T::GpuRepr>
    where
        T: GpuSendable + 'static,
        T::GpuRepr: 'static,
    {
        if self.is_gpu {
            match &self.inner {
                ErasedPayloadInner::Any(inner) => inner.downcast_ref::<T::GpuRepr>(),
            }
        } else {
            None
        }
    }

    pub fn clone_cpu<T>(&self) -> Option<T>
    where
        T: GpuSendable + Clone + 'static,
    {
        if self.is_gpu {
            return None;
        }
        let ErasedPayloadInner::Any(inner) = &self.inner;
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
            ErasedPayloadInner::Any(inner) => match Arc::downcast::<T>(inner) {
                Ok(arc) => match Arc::try_unwrap(arc) {
                    Ok(v) => Ok(v),
                    Err(arc) => Err(restore(ErasedPayloadInner::Any(arc))),
                },
                Err(arc) => {
                    if let Some(v) = Self::cross_dylib_ref::<T>(arc.as_ref(), cpu_type_name) {
                        return Ok(v.clone());
                    }
                    Err(restore(ErasedPayloadInner::Any(arc)))
                }
            },
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
        let ErasedPayloadInner::Any(inner) = &self.inner;
        if let Some(v) = inner.downcast_ref::<T::GpuRepr>().cloned() {
            return Some(v);
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
