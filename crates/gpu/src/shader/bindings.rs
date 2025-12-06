use crate::{GpuContextHandle, GpuError};

use super::{Access, BindingKind, BufferOut, SamplerDesc, ShaderSpec, Uniform, UniformBytes};

/// Runtime-provided buffer contents for a binding.
pub enum BufferInit<'a> {
    /// Allocate a buffer of this size, zero-filled.
    Empty(u64),
    /// Allocate a buffer of this size, explicitly zero-filled.
    Zeroed(u64),
    /// Initialize the buffer with the provided bytes.
    Bytes(&'a [u8]),
}

/// Binding payload for buffers/textures/samplers.
pub enum BindingData<'a> {
    Buffer(BufferInit<'a>),
    BufferDevice {
        buffer: std::sync::Arc<wgpu::Buffer>,
        size: u64,
        device_key: usize,
    },
    TextureRgba8 {
        width: u32,
        height: u32,
        bytes: std::borrow::Cow<'a, [u8]>,
    },
    TextureAlloc {
        width: u32,
        height: u32,
    },
    TextureHandle {
        handle: crate::GpuImageHandle,
    },
    Sampler(SamplerDesc),
}

/// Full binding description for a single dispatch, including data and readback request.
pub struct ShaderBinding<'a> {
    pub binding: u32,
    pub kind: BindingKind,
    pub access: Access,
    pub data: BindingData<'a>,
    /// If true, the buffer will be copied back to CPU and returned.
    pub readback: bool,
}

/// Trait for derive-generated binding packs.
pub trait GpuBindings<'a> {
    fn spec() -> &'static ShaderSpec;
    fn bindings(
        &'a self,
        gpu: Option<&GpuContextHandle>,
    ) -> Result<Vec<ShaderBinding<'a>>, GpuError>;
    /// Optional hint for dispatch invocation count (elements to cover). If provided, this
    /// is used when explicit invocations/workgroups are not passed.
    fn invocation_hint(&'a self) -> Option<[u32; 3]> {
        None
    }
}

/// Trait for turning various data holders into binding bytes.
pub trait BindingBytes {
    fn as_bytes(&self) -> &[u8];
}

impl BindingBytes for &[u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl BindingBytes for &[f32] {
    fn as_bytes(&self) -> &[u8] {
        bytemuck::cast_slice(self)
    }
}

impl<'a> BindingBytes for UniformBytes<'a> {
    fn as_bytes(&self) -> &[u8] {
        self.0
    }
}

impl<T: bytemuck::Pod> BindingBytes for Uniform<T> {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Helper to view binding-friendly types as bytes.
pub fn as_bytes<T: BindingBytes + ?Sized>(value: &T) -> &[u8] {
    value.as_bytes()
}

/// Helper to compute buffer len for BufferOut.
pub fn buffer_len(value: &BufferOut) -> u64 {
    value.len
}
