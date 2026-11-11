//! Pipelines and bind groups, built once and kept by the device they belong to.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use super::spec::{BindingKind, SampleKind, ShaderSpec, TextureAccess};

/// Bind groups a device keeps. Past this, the oldest goes.
const MAX_BIND_GROUPS: usize = 256;
/// The most bindings one shader has that a bind group's identity covers.
pub(crate) const MAX_BINDINGS: usize = 16;

/// How much a device's shader cache has built.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShaderStats {
    pub pipelines: u64,
    pub bind_groups: u64,
    /// Bind groups dropped to stay within the limit.
    pub evicted: u64,
    /// Queue submits of shader work.
    pub submits: u64,
    /// Compute and render passes begun. Shaders recorded one after the other share one.
    pub passes: u64,
    /// Shader dispatches and draws: one for each time a shader ran.
    pub dispatches: u64,
}

pub(crate) struct Pipeline {
    pub(crate) pipeline: wgpu::ComputePipeline,
    pub(crate) layout: wgpu::BindGroupLayout,
}

/// What a bind group was made of: the pipeline's key and the identity of each resource.
pub(crate) type GroupKey = (u64, [u64; MAX_BINDINGS]);

#[derive(Default)]
pub(crate) struct ShaderCache {
    pipelines: HashMap<u64, Arc<Pipeline>>,
    groups: Vec<(GroupKey, wgpu::BindGroup)>,
    stats: ShaderStats,
}

fn layout_entry(index: u32, kind: &BindingKind) -> wgpu::BindGroupLayoutEntry {
    let buffer = |ty, size: u32| wgpu::BindingType::Buffer {
        ty,
        has_dynamic_offset: false,
        min_binding_size: NonZeroU64::new(u64::from(size)),
    };
    let ty = match *kind {
        BindingKind::Storage { read_only, .. } => {
            buffer(wgpu::BufferBindingType::Storage { read_only }, 0)
        }
        BindingKind::Uniform { size } => buffer(wgpu::BufferBindingType::Uniform, size),
        BindingKind::Texture { sample } => wgpu::BindingType::Texture {
            sample_type: match sample {
                SampleKind::Float => wgpu::TextureSampleType::Float { filterable: true },
                SampleKind::Uint => wgpu::TextureSampleType::Uint,
                SampleKind::Sint => wgpu::TextureSampleType::Sint,
                SampleKind::Depth => wgpu::TextureSampleType::Depth,
            },
            view_dimension: wgpu::TextureViewDimension::D2,
            multisampled: false,
        },
        BindingKind::StorageTexture { format, access } => wgpu::BindingType::StorageTexture {
            access: match access {
                TextureAccess::Read => wgpu::StorageTextureAccess::ReadOnly,
                TextureAccess::Write => wgpu::StorageTextureAccess::WriteOnly,
                TextureAccess::ReadWrite => wgpu::StorageTextureAccess::ReadWrite,
            },
            format: format.to_wgpu(),
            view_dimension: wgpu::TextureViewDimension::D2,
        },
        BindingKind::Sampler { comparison } => wgpu::BindingType::Sampler(if comparison {
            wgpu::SamplerBindingType::Comparison
        } else {
            wgpu::SamplerBindingType::Filtering
        }),
    };
    wgpu::BindGroupLayoutEntry {
        binding: index,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty,
        count: None,
    }
}

impl ShaderCache {
    /// The pipeline for `spec`, built the first time its content is seen on this device.
    /// Two specs with the same content share one, wherever they live.
    pub(crate) fn pipeline(
        &mut self,
        device: &wgpu::Device,
        spec: &ShaderSpec,
    ) -> Result<Arc<Pipeline>, String> {
        let key = spec.key();
        if let Some(found) = self.pipelines.get(&key) {
            return Ok(Arc::clone(found));
        }
        let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
        let entries: Vec<wgpu::BindGroupLayoutEntry> = spec
            .bindings
            .iter()
            .map(|binding| layout_entry(binding.index, &binding.kind))
            .collect();
        let layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: None,
            entries: &entries,
        });
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: None,
            source: wgpu::ShaderSource::Wgsl(spec.source.into()),
        });
        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: None,
            bind_group_layouts: &[Some(&layout)],
            immediate_size: 0,
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: None,
            layout: Some(&pipeline_layout),
            module: &module,
            entry_point: Some(spec.entry),
            compilation_options: wgpu::PipelineCompilationOptions::default(),
            cache: None,
        });
        if let Some(error) = crate::wait::scope_error(scope) {
            return Err(error);
        }
        let built = Arc::new(Pipeline { pipeline, layout });
        self.pipelines.insert(key, Arc::clone(&built));
        self.stats.pipelines += 1;
        Ok(built)
    }

    /// The bind group made of exactly these resources, if one was kept.
    pub(crate) fn group(&self, key: &GroupKey) -> Option<wgpu::BindGroup> {
        self.groups
            .iter()
            .find(|(have, _)| have == key)
            .map(|(_, group)| group.clone())
    }

    pub(crate) fn keep_group(&mut self, key: GroupKey, group: wgpu::BindGroup) {
        if self.groups.len() >= MAX_BIND_GROUPS {
            self.groups.remove(0);
            self.stats.evicted += 1;
        }
        self.groups.push((key, group));
        self.stats.bind_groups += 1;
    }

    pub(crate) fn stats(&self) -> ShaderStats {
        self.stats
    }
}
