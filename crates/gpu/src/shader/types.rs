use crate::{Compute, GpuContextHandle};
use image::DynamicImage;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BindingKind {
    Storage,
    Uniform,
    Texture2D,
    StorageTexture2D,
    Sampler,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Access {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SamplerKind {
    Filtering,
    Comparison,
}

#[derive(Clone, Copy)]
pub struct SamplerDesc {
    pub address_u: wgpu::AddressMode,
    pub address_v: wgpu::AddressMode,
    pub address_w: wgpu::AddressMode,
    pub mag_filter: wgpu::FilterMode,
    pub min_filter: wgpu::FilterMode,
    pub mipmap_filter: wgpu::MipmapFilterMode,
}

impl Default for SamplerDesc {
    fn default() -> Self {
        Self {
            address_u: wgpu::AddressMode::ClampToEdge,
            address_v: wgpu::AddressMode::ClampToEdge,
            address_w: wgpu::AddressMode::ClampToEdge,
            mag_filter: wgpu::FilterMode::Linear,
            min_filter: wgpu::FilterMode::Linear,
            mipmap_filter: wgpu::MipmapFilterMode::Nearest,
        }
    }
}

impl SamplerDesc {
    pub fn with_filters(mut self, min: wgpu::FilterMode, mag: wgpu::FilterMode) -> Self {
        self.min_filter = min;
        self.mag_filter = mag;
        self
    }

    pub fn with_address_mode(mut self, mode: wgpu::AddressMode) -> Self {
        self.address_u = mode;
        self.address_v = mode;
        self.address_w = mode;
        self
    }

    pub fn with_mipmap_filter(mut self, mip: wgpu::MipmapFilterMode) -> Self {
        self.mipmap_filter = mip;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BindingSpec {
    pub binding: u32,
    pub kind: BindingKind,
    pub access: Access,
    /// Optional stride (bytes per invocation) for storage buffers to aid workgroup inference.
    /// If `None`, caller should supply `invocation_hint`/`workgroups` explicitly.
    pub invocation_stride: Option<u32>,
    pub texture_format: Option<wgpu::TextureFormat>,
    pub sample_type: Option<wgpu::TextureSampleType>,
    pub view_dimension: Option<wgpu::TextureViewDimension>,
    pub sampler_kind: Option<SamplerKind>,
}

impl Default for BindingSpec {
    fn default() -> Self {
        Self {
            binding: 0,
            kind: BindingKind::Storage,
            access: Access::ReadOnly,
            invocation_stride: None,
            texture_format: None,
            sample_type: None,
            view_dimension: None,
            sampler_kind: None,
        }
    }
}

impl BindingSpec {
    pub fn storage_read(binding: u32, invocation_stride: Option<u32>) -> Self {
        Self {
            binding,
            kind: BindingKind::Storage,
            access: Access::ReadOnly,
            invocation_stride,
            ..Default::default()
        }
    }

    pub fn storage_write(binding: u32, invocation_stride: Option<u32>) -> Self {
        Self {
            binding,
            kind: BindingKind::Storage,
            access: Access::WriteOnly,
            invocation_stride,
            ..Default::default()
        }
    }

    pub fn storage_read_write(binding: u32, invocation_stride: Option<u32>) -> Self {
        Self {
            binding,
            kind: BindingKind::Storage,
            access: Access::ReadWrite,
            invocation_stride,
            ..Default::default()
        }
    }

    pub fn uniform(binding: u32) -> Self {
        Self {
            binding,
            kind: BindingKind::Uniform,
            access: Access::ReadOnly,
            ..Default::default()
        }
    }

    pub fn texture(
        binding: u32,
        sample_type: Option<wgpu::TextureSampleType>,
        view_dimension: Option<wgpu::TextureViewDimension>,
    ) -> Self {
        Self {
            binding,
            kind: BindingKind::Texture2D,
            access: Access::ReadOnly,
            sample_type,
            view_dimension,
            ..Default::default()
        }
    }

    pub fn storage_texture(
        binding: u32,
        format: wgpu::TextureFormat,
        access: Access,
        view_dimension: Option<wgpu::TextureViewDimension>,
    ) -> Self {
        Self {
            binding,
            kind: BindingKind::StorageTexture2D,
            access,
            texture_format: Some(format),
            view_dimension,
            ..Default::default()
        }
    }

    pub fn sampler(binding: u32, kind: Option<SamplerKind>) -> Self {
        Self {
            binding,
            kind: BindingKind::Sampler,
            access: Access::ReadOnly,
            sampler_kind: kind,
            ..Default::default()
        }
    }
}

/// Static shader specification.
pub struct ShaderSpec {
    pub name: &'static str,
    pub src: &'static str,
    pub entry: &'static str,
    pub workgroup_size: Option<[u32; 3]>,
    /// Empty bindings will be inferred from WGSL; otherwise use the provided layout.
    pub bindings: &'static [BindingSpec],
}

/// Optional hints/overrides for a dispatch.
#[derive(Clone, Copy, Default)]
pub struct DispatchOptions {
    pub workgroups: Option<[u32; 3]>,
    pub invocations: Option<[u32; 3]>,
}

/// Wrapper for uniform data passed as raw bytes.
pub struct UniformBytes<'a>(pub &'a [u8]);

/// Wrapper for typed uniform data; pads to 16-byte multiples for WGSL alignment.
pub struct Uniform<T: bytemuck::Pod> {
    pub(crate) bytes: std::borrow::Cow<'static, [u8]>,
    pub(crate) _marker: std::marker::PhantomData<T>,
}

impl<T: bytemuck::Pod> Uniform<T> {
    pub fn new(value: T) -> Self {
        let mut v = bytemuck::bytes_of(&value).to_vec();
        let pad = (16 - (v.len() as u64 % 16)) % 16;
        if pad > 0 {
            v.extend(std::iter::repeat_n(0u8, pad as usize));
        }
        Uniform {
            bytes: std::borrow::Cow::Owned(v),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: bytemuck::Pod> From<T> for Uniform<T> {
    fn from(value: T) -> Self {
        Uniform::new(value)
    }
}

/// Wrapper for storage outputs with optional readback.
pub struct BufferOut {
    pub len: u64,
    pub readback: bool,
}

impl BufferOut {
    pub fn write_bytes(len: u64) -> Self {
        Self {
            len,
            readback: true,
        }
    }

    pub fn write_bytes_no_readback(len: u64) -> Self {
        Self {
            len,
            readback: false,
        }
    }
}

/// Storage texture output description.
pub struct TextureOut {
    pub width: u32,
    pub height: u32,
    pub readback: bool,
}

impl TextureOut {
    pub fn write(width: u32, height: u32) -> Self {
        Self {
            width,
            height,
            readback: false,
        }
    }

    pub fn write_with_readback(width: u32, height: u32) -> Self {
        Self {
            width,
            height,
            readback: true,
        }
    }

    /// Create a storage texture description using the dimensions of the input payload.
    /// If no GPU context is present, readback is enabled so the caller can pull bytes.
    pub fn from_input(img: &Compute<DynamicImage>, gpu: Option<&GpuContextHandle>) -> Self {
        let (width, height) = img.dimensions();
        Self {
            width,
            height,
            readback: gpu.is_none(),
        }
    }
}
