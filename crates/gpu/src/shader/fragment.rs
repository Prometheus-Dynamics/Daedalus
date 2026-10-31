//! Fragment-pass nodes: a full-screen draw that reads textures and writes render targets.
//!
//! On tile-based GPUs this is the fast path: on the CM5's V3D a fragment pass runs 10-13x faster
//! than the same kernel as a compute shader (`gpu_bench` `[image-paths]`), and its ALU throughput
//! matches the four CPU cores. The same WGSL runs on Mali and desktop GPUs.

use crate::{GpuContextHandle, GpuError};

use super::{GpuStorage, GpuTexture, GpuTicket};

/// The vertex stage every pass shares: one triangle covering the target. `@builtin(position)`
/// in the fragment stage is the texel centre (`x + 0.5`, `y + 0.5`) of the pixel it writes.
const FULL_SCREEN_VERTEX: &str = "
@vertex fn daedalus_full_screen(@builtin(vertex_index) i: u32) -> @builtin(position) vec4<f32> {
    let p = vec2<f32>(f32((i << 1u) & 2u), f32(i & 2u));
    return vec4<f32>(p * 2.0 - 1.0, 0.0, 1.0);
}
";

/// A compiled full-screen fragment pass. Build it once (in the node's state) from the WGSL of a
/// `@fragment fn fs(@builtin(position) pos: vec4<f32>) -> ...` entry point and its targets'
/// formats; then [`FragmentPass::run`] it every tick on persistent [`GpuTexture`]s.
///
/// Bindings, all in `@group(0)`: the input textures at bindings `0..n` (`texture_2d<f32>`, read
/// with `textureLoad(tex, vec2<i32>(pos.xy), 0)`), then the storage buffers at `n..n+m`
/// (`var<storage, read>`, parameters or lookup tables). Several targets are written through
/// `@location(i)` in an output struct (multiple render targets). Every declared binding must be
/// used by the shader, and every input and buffer passed to `run` must be declared.
pub struct FragmentPass {
    pipeline: wgpu::RenderPipeline,
    targets: Vec<wgpu::TextureFormat>,
    device: wgpu::Device,
    queue: wgpu::Queue,
}

impl std::fmt::Debug for FragmentPass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentPass")
            .field("targets", &self.targets)
            .finish_non_exhaustive()
    }
}

impl FragmentPass {
    /// Compile `fragment_wgsl` (entry point `fs`) for render targets of `targets` formats.
    /// `InvalidInput` with the compiler's message when the shader or the target list is
    /// rejected; `Unsupported` on a non-wgpu backend.
    pub fn new(
        gpu: &GpuContextHandle,
        fragment_wgsl: &str,
        targets: &[wgpu::TextureFormat],
    ) -> Result<Self, GpuError> {
        let (device, queue) = gpu.wgpu().ok_or(GpuError::Unsupported)?;
        if targets.is_empty() {
            return Err(GpuError::InvalidInput(
                "a fragment pass needs at least one target".into(),
            ));
        }
        let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("daedalus-fragment-pass"),
            source: wgpu::ShaderSource::Wgsl(
                format!("{FULL_SCREEN_VERTEX}\n{fragment_wgsl}").into(),
            ),
        });
        let color_targets: Vec<Option<wgpu::ColorTargetState>> =
            targets.iter().map(|&format| Some(format.into())).collect();
        let pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("daedalus-fragment-pass"),
            layout: None,
            vertex: wgpu::VertexState {
                module: &module,
                entry_point: Some("daedalus_full_screen"),
                compilation_options: Default::default(),
                buffers: &[],
            },
            fragment: Some(wgpu::FragmentState {
                module: &module,
                entry_point: Some("fs"),
                compilation_options: Default::default(),
                targets: &color_targets,
            }),
            primitive: Default::default(),
            depth_stencil: None,
            multisample: Default::default(),
            multiview_mask: None,
            cache: None,
        });
        if let Some(err) = pollster::block_on(scope.pop()) {
            return Err(GpuError::InvalidInput(format!(
                "fragment pass rejected: {err}"
            )));
        }
        Ok(Self {
            pipeline,
            targets: targets.to_vec(),
            device: device.clone(),
            queue: queue.clone(),
        })
    }

    /// The target formats, in `@location` order.
    pub fn targets(&self) -> &[wgpu::TextureFormat] {
        &self.targets
    }

    /// Record the pass into `encoder`: `inputs` and `buffers` bound in order, every target
    /// cleared to zero and then written. Targets must match the pass's formats and share one
    /// size.
    pub fn encode(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        inputs: &[&GpuTexture],
        buffers: &[&GpuStorage],
        targets: &[&GpuTexture],
    ) -> Result<(), GpuError> {
        let formats: Vec<_> = targets.iter().map(|t| t.format()).collect();
        if formats != self.targets {
            return Err(GpuError::InvalidInput(format!(
                "targets {formats:?} do not match the pass's {:?}",
                self.targets
            )));
        }
        let size = targets[0].size();
        if targets.iter().any(|t| t.size() != size) {
            return Err(GpuError::InvalidInput(
                "fragment pass targets must share one size".into(),
            ));
        }
        let bind_group = if inputs.is_empty() && buffers.is_empty() {
            None
        } else {
            let entries: Vec<wgpu::BindGroupEntry<'_>> = inputs
                .iter()
                .map(|t| wgpu::BindingResource::TextureView(t.view()))
                .chain(buffers.iter().map(|b| b.buffer().as_entire_binding()))
                .enumerate()
                .map(|(i, resource)| wgpu::BindGroupEntry {
                    binding: i as u32,
                    resource,
                })
                .collect();
            let scope = self.device.push_error_scope(wgpu::ErrorFilter::Validation);
            let group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("daedalus-fragment-pass"),
                layout: &self.pipeline.get_bind_group_layout(0),
                entries: &entries,
            });
            if let Some(err) = pollster::block_on(scope.pop()) {
                return Err(GpuError::InvalidInput(format!(
                    "fragment pass bindings rejected: {err}"
                )));
            }
            Some(group)
        };
        let attachments: Vec<Option<wgpu::RenderPassColorAttachment<'_>>> = targets
            .iter()
            .map(|t| {
                Some(wgpu::RenderPassColorAttachment {
                    view: t.view(),
                    depth_slice: None,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color::TRANSPARENT),
                        store: wgpu::StoreOp::Store,
                    },
                })
            })
            .collect();
        let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
            label: Some("daedalus-fragment-pass"),
            color_attachments: &attachments,
            depth_stencil_attachment: None,
            timestamp_writes: None,
            occlusion_query_set: None,
            multiview_mask: None,
        });
        pass.set_pipeline(&self.pipeline);
        if let Some(group) = &bind_group {
            pass.set_bind_group(0, group, &[]);
        }
        pass.draw(0..3, 0..1);
        Ok(())
    }

    /// Encode and submit the pass on its own; poll the returned ticket (or start a target's
    /// readback, which lands after the pass) instead of waiting.
    pub fn run(
        &self,
        inputs: &[&GpuTexture],
        buffers: &[&GpuStorage],
        targets: &[&GpuTexture],
    ) -> Result<GpuTicket, GpuError> {
        let mut encoder = self.device.create_command_encoder(&Default::default());
        self.encode(&mut encoder, inputs, buffers, targets)?;
        Ok(GpuTicket::submit_on(
            &self.device,
            &self.queue,
            [encoder.finish()],
        ))
    }
}
