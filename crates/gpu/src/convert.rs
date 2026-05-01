use crate::{GpuContextHandle, GpuError};
#[cfg(feature = "image")]
use crate::{GpuImageHandle, upload_rgba8_texture};
#[cfg(feature = "image")]
use image::{DynamicImage, GenericImageView, GrayImage, RgbImage, RgbaImage};
use std::ops::Deref;
use std::sync::Arc;

/// Opt-in bridge to allow CPU types to participate in GPU segments.
/// Users implement this for their own types to describe how to upload/download.
pub trait DeviceBridge {
    type Device;

    /// Upload CPU data to a GPU representation.
    fn upload(self, _ctx: &GpuContextHandle) -> Result<Self::Device, GpuError>
    where
        Self: Sized,
    {
        Err(GpuError::Unsupported)
    }

    /// Download a GPU representation back to CPU data.
    fn download(_gpu: &Self::Device, _ctx: &GpuContextHandle) -> Result<Self, GpuError>
    where
        Self: Sized,
    {
        Err(GpuError::Unsupported)
    }
}

type RecycleFn<T> = Arc<dyn Fn(T) + Send + Sync>;

enum BackingInner<T> {
    Owned(T),
    Shared(Arc<T>),
    Recycled(RecycledCell<T>),
}

struct RecycledCell<T> {
    value: Option<T>,
    recycler: RecycleFn<T>,
}

impl<T> Drop for RecycledCell<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            (self.recycler)(value);
        }
    }
}

/// Shared host-side backing for runtime-managed CPU values.
///
/// This is the carrier the transport layer can retain, share across fanout, and recycle on the
/// final drop, while ordinary nodes still decode plain `T` / `&T`.
#[derive(Clone)]
pub struct Backing<T> {
    inner: Arc<BackingInner<T>>,
}

impl<T> Backing<T> {
    pub fn owned(value: T) -> Self {
        Self {
            inner: Arc::new(BackingInner::Owned(value)),
        }
    }

    pub fn shared(value: Arc<T>) -> Self {
        Self {
            inner: Arc::new(BackingInner::Shared(value)),
        }
    }

    pub fn recycled<F>(value: T, recycler: F) -> Self
    where
        F: Fn(T) + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(BackingInner::Recycled(RecycledCell {
                value: Some(value),
                recycler: Arc::new(recycler),
            })),
        }
    }

    fn value_ref(&self) -> &T {
        match self.inner.as_ref() {
            BackingInner::Owned(value) => value,
            BackingInner::Shared(value) => value.as_ref(),
            BackingInner::Recycled(cell) => cell
                .value
                .as_ref()
                .expect("recycled backing missing value during live access"),
        }
    }

    pub fn into_owned(self) -> T
    where
        T: Clone,
    {
        match Arc::try_unwrap(self.inner) {
            Ok(BackingInner::Owned(value)) => value,
            Ok(BackingInner::Shared(value)) => match Arc::try_unwrap(value) {
                Ok(value) => value,
                Err(value) => value.as_ref().clone(),
            },
            Ok(BackingInner::Recycled(mut cell)) => cell
                .value
                .take()
                .expect("recycled backing missing owned value"),
            Err(shared) => match shared.as_ref() {
                BackingInner::Owned(value) => value.clone(),
                BackingInner::Shared(value) => value.as_ref().clone(),
                BackingInner::Recycled(cell) => cell
                    .value
                    .as_ref()
                    .expect("recycled backing missing shared value")
                    .clone(),
            },
        }
    }

    pub fn into_arc(self) -> Arc<T>
    where
        T: Clone,
    {
        match Arc::try_unwrap(self.inner) {
            Ok(BackingInner::Owned(value)) => Arc::new(value),
            Ok(BackingInner::Shared(value)) => value,
            Ok(BackingInner::Recycled(mut cell)) => Arc::new(
                cell.value
                    .take()
                    .expect("recycled backing missing owned value"),
            ),
            Err(shared) => match shared.as_ref() {
                BackingInner::Owned(value) => Arc::new(value.clone()),
                BackingInner::Shared(value) => value.clone(),
                BackingInner::Recycled(cell) => Arc::new(
                    cell.value
                        .as_ref()
                        .expect("recycled backing missing shared value")
                        .clone(),
                ),
            },
        }
    }

    pub fn shared_arc(&self) -> Option<Arc<T>> {
        match self.inner.as_ref() {
            BackingInner::Shared(value) => Some(value.clone()),
            _ => None,
        }
    }
}

impl<T> Deref for Backing<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value_ref()
    }
}

impl<T> AsRef<T> for Backing<T> {
    fn as_ref(&self) -> &T {
        self.value_ref()
    }
}

impl<T> From<T> for Backing<T> {
    fn from(value: T) -> Self {
        Self::owned(value)
    }
}

impl<T> From<Arc<T>> for Backing<T> {
    fn from(value: Arc<T>) -> Self {
        Self::shared(value)
    }
}

impl<T> std::fmt::Debug for Backing<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl<T> DeviceBridge for Backing<T>
where
    T: DeviceBridge + Clone + Send + Sync + 'static,
    T::Device: Clone + Send + Sync + 'static,
{
    type Device = T::Device;

    fn upload(self, ctx: &GpuContextHandle) -> Result<Self::Device, GpuError> {
        self.into_owned().upload(ctx)
    }

    fn download(gpu: &Self::Device, ctx: &GpuContextHandle) -> Result<Self, GpuError> {
        T::download(gpu, ctx).map(Backing::owned)
    }
}

/// Generic payload that can carry either CPU data or a GPU representation.
#[derive(Debug, Clone)]
pub enum Compute<T: DeviceBridge> {
    Cpu(T),
    Gpu(T::Device),
}

impl<T: DeviceBridge> Compute<T> {
    pub fn is_gpu(&self) -> bool {
        matches!(self, Compute::Gpu(_))
    }

    pub fn as_cpu(&self) -> Option<&T> {
        match self {
            Compute::Cpu(t) => Some(t),
            _ => None,
        }
    }

    pub fn as_gpu(&self) -> Option<&T::Device> {
        match self {
            Compute::Gpu(g) => Some(g),
            _ => None,
        }
    }
}

#[cfg(feature = "image")]
impl Compute<DynamicImage> {
    /// Return image dimensions without forcing the caller to pattern match.
    pub fn dimensions(&self) -> (u32, u32) {
        match self {
            Compute::Cpu(img) => img.dimensions(),
            Compute::Gpu(handle) => (handle.width, handle.height),
        }
    }

    /// Ensure this payload is resident on GPU; uploads if necessary and returns the handle and dimensions.
    pub fn into_gpu(self, ctx: &GpuContextHandle) -> Result<(GpuImageHandle, u32, u32), GpuError> {
        match self {
            Compute::Gpu(handle) => Ok((handle.clone(), handle.width, handle.height)),
            Compute::Cpu(img) => {
                let rgba = img.to_rgba8();
                let (w, h) = rgba.dimensions();
                let handle = upload_rgba8_texture(ctx, w, h, rgba.as_raw())?;
                Ok((handle, w, h))
            }
        }
    }
}

#[cfg(feature = "image")]
impl DeviceBridge for DynamicImage {
    type Device = GpuImageHandle;

    fn upload(self, ctx: &GpuContextHandle) -> Result<Self::Device, GpuError> {
        let rgba = self.to_rgba8();
        let (width, height) = rgba.dimensions();
        upload_rgba8_texture(ctx, width, height, rgba.as_raw())
    }

    fn download(gpu: &Self::Device, ctx: &GpuContextHandle) -> Result<Self, GpuError> {
        let bytes = ctx.read_texture(gpu)?;
        let buf = image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(gpu.width, gpu.height, bytes)
            .ok_or(GpuError::AllocationFailed)?;
        Ok(DynamicImage::ImageRgba8(buf))
    }
}

#[cfg(feature = "image")]
impl DeviceBridge for RgbaImage {
    type Device = GpuImageHandle;

    fn upload(self, ctx: &GpuContextHandle) -> Result<Self::Device, GpuError> {
        let (width, height) = self.dimensions();
        upload_rgba8_texture(ctx, width, height, self.as_raw())
    }

    fn download(gpu: &Self::Device, ctx: &GpuContextHandle) -> Result<Self, GpuError> {
        let bytes = ctx.read_texture(gpu)?;
        image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(gpu.width, gpu.height, bytes)
            .ok_or(GpuError::AllocationFailed)
    }
}

#[cfg(feature = "image")]
impl DeviceBridge for RgbImage {
    type Device = GpuImageHandle;

    fn upload(self, ctx: &GpuContextHandle) -> Result<Self::Device, GpuError> {
        let (width, height) = self.dimensions();
        let rgba = image::ImageBuffer::from_fn(width, height, |x, y| {
            let p = self.get_pixel(x, y);
            image::Rgba([p[0], p[1], p[2], 255])
        });
        upload_rgba8_texture(ctx, width, height, rgba.as_raw())
    }

    fn download(gpu: &Self::Device, ctx: &GpuContextHandle) -> Result<Self, GpuError> {
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

#[cfg(feature = "image")]
impl DeviceBridge for GrayImage {
    type Device = GpuImageHandle;

    fn upload(self, ctx: &GpuContextHandle) -> Result<Self::Device, GpuError> {
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

    fn download(gpu: &Self::Device, ctx: &GpuContextHandle) -> Result<Self, GpuError> {
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

#[cfg(feature = "image")]
impl Compute<DynamicImage> {
    /// Get RGBA8 bytes + dimensions, downloading from GPU if needed.
    pub fn to_rgba_bytes(
        &self,
        ctx: Option<&GpuContextHandle>,
    ) -> Result<(Vec<u8>, u32, u32), GpuError> {
        match self {
            Compute::Cpu(cpu) => {
                let rgba = cpu.to_rgba8();
                let (w, h) = rgba.dimensions();
                Ok((rgba.into_raw(), w, h))
            }
            Compute::Gpu(handle) => {
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
            upload_rgba8_texture(ctx, w, h, &bytes).map(Compute::Gpu)
        } else {
            image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(w, h, bytes)
                .map(DynamicImage::ImageRgba8)
                .map(Compute::Cpu)
                .ok_or(GpuError::AllocationFailed)
        }
    }
}
