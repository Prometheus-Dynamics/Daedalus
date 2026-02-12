//! Lightweight shader dispatch helper for compute pipelines.
//! The heavy lifting lives in submodules; this file stitches them together and
//! keeps a couple of lightweight helpers for WGSL inference.
use crate::Payload;
use daedalus_wgsl_infer as wgsl_infer;
use image::DynamicImage;
use wgsl_infer::InferredAccess;

mod bindings;
mod dispatch;
#[cfg(feature = "gpu-async")]
mod dispatch_async;
mod fallback;
mod gpu_state;
mod pipeline;
mod pool;
mod prepare;
mod readback;
#[cfg(feature = "gpu-async")]
mod readback_async;
mod run_output;
mod types;
mod workgroups;

pub use bindings::*;
pub use dispatch::*;
#[cfg(feature = "gpu-async")]
pub use dispatch_async::*;
pub use gpu_state::*;
pub use gpu_state::{gpu_state_pool_limit, set_gpu_state_pool_limit};
pub use pipeline::{
    bind_group_cache_limit, pipeline_cache_limit, set_bind_group_cache_limit,
    set_pipeline_cache_limit,
};
pub(crate) use pool::{clear_temp_pool, temp_pool};
pub use pool::{
    set_temp_pool_buffer_limit, set_temp_pool_texture_limit, temp_pool_buffer_limit,
    temp_pool_texture_limit,
};
pub use run_output::*;
pub use types::*;

// Needed by gpu_state and inference helpers.
pub(crate) use fallback::ctx;

impl TextureOut {
    /// Create a storage texture description using the dimensions of the input payload,
    /// pulling GPU availability from the shader context.
    pub fn from_input_ctx(img: &Payload<DynamicImage>, ctx: &ShaderContext) -> Self {
        Self::from_input(img, ctx.gpu.as_ref())
    }
}

/// Extremely lightweight parser to extract @workgroup_size(x[,...]) from WGSL.
pub(crate) fn infer_workgroup_size(src: &str) -> Option<[u32; 3]> {
    wgsl_infer::infer_workgroup_size(src)
}

/// Extremely simple parser to pull bindings from WGSL declarations:
/// looks for `@binding(<n>)` and `var<storage, read>` / `var<storage, read_write>` / `var<uniform>`.
fn parse_texture_format(s: &str) -> Option<wgpu::TextureFormat> {
    match s {
        "rgba8unorm" => Some(wgpu::TextureFormat::Rgba8Unorm),
        "rgba8unorm_srgb" => Some(wgpu::TextureFormat::Rgba8UnormSrgb),
        "rgba8snorm" => Some(wgpu::TextureFormat::Rgba8Snorm),
        "rgba16float" => Some(wgpu::TextureFormat::Rgba16Float),
        "r32float" => Some(wgpu::TextureFormat::R32Float),
        "rgba32float" => Some(wgpu::TextureFormat::Rgba32Float),
        _ => None,
    }
}

fn parse_sample_type(s: &str) -> Option<wgpu::TextureSampleType> {
    let ty = s.trim();
    if ty.contains("f32") || ty.contains("vec4<f32>") {
        Some(wgpu::TextureSampleType::Float { filterable: true })
    } else if ty.contains("i32") {
        Some(wgpu::TextureSampleType::Sint)
    } else if ty.contains("u32") {
        Some(wgpu::TextureSampleType::Uint)
    } else {
        None
    }
}

fn parse_view_dimension(s: Option<String>) -> Option<wgpu::TextureViewDimension> {
    match s.as_deref() {
        Some("2d_array") => Some(wgpu::TextureViewDimension::D2Array),
        _ => Some(wgpu::TextureViewDimension::D2),
    }
}

pub(crate) fn infer_bindings(src: &str) -> Option<Vec<BindingSpec>> {
    let inferred = wgsl_infer::infer_bindings(src);
    if inferred.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(inferred.len());
    for b in inferred {
        let mut texture_format = None;
        let mut sample_type = None;
        let mut view_dimension = Some(wgpu::TextureViewDimension::D2);
        let mut sampler_kind = None;
        let (kind, access) = match b.access {
            InferredAccess::StorageRead => (BindingKind::Storage, Access::ReadOnly),
            InferredAccess::StorageReadWrite => (BindingKind::Storage, Access::ReadWrite),
            InferredAccess::StorageWrite => (BindingKind::Storage, Access::WriteOnly),
            InferredAccess::Uniform => (BindingKind::Uniform, Access::ReadOnly),
            InferredAccess::StorageTexture { format, view } => {
                texture_format = format.as_deref().and_then(parse_texture_format);
                view_dimension = parse_view_dimension(view);
                (BindingKind::StorageTexture2D, Access::WriteOnly)
            }
            InferredAccess::Texture {
                format: _,
                sample_type: st,
                view,
            } => {
                sample_type = st.as_deref().and_then(parse_sample_type);
                view_dimension = parse_view_dimension(view);
                (BindingKind::Texture2D, Access::ReadOnly)
            }
            InferredAccess::Sampler(kind_str) => {
                sampler_kind = match kind_str.as_deref() {
                    Some("comparison") => Some(SamplerKind::Comparison),
                    _ => Some(SamplerKind::Filtering),
                };
                (BindingKind::Sampler, Access::ReadOnly)
            }
        };
        out.push(BindingSpec {
            binding: b.binding,
            kind,
            access,
            invocation_stride: None,
            texture_format,
            sample_type,
            view_dimension,
            sampler_kind,
        });
    }
    Some(out)
}
