//! Fragment-pass nodes: a full-screen (or per-rectangle) draw that reads textures and writes
//! render targets.
//!
//! On tile-based GPUs this is the fast path: on the CM5's V3D a fragment pass runs 10-13x faster
//! than the same kernel as a compute shader (`gpu_bench` `[image-paths]`), and its ALU throughput
//! matches the four CPU cores. The same WGSL runs on Mali and desktop GPUs.

use std::collections::HashMap;

use parking_lot::Mutex;
use wgpu::util::DeviceExt;

use crate::{GpuContextHandle, GpuError};

use super::{GpuStorage, GpuTexture, GpuTicket, GpuTimed, GpuTimer};

/// The vertex stage every pass shares: one quad per instance, covering the instance's rectangle
/// (`[x, y, w, h]` in target pixels, with the target's size). `@builtin(position)` in the fragment
/// stage is the texel centre (`x + 0.5`, `y + 0.5`) of the pixel it writes.
const RECT_VERTEX: &str = "
@vertex fn daedalus_rect(
    @builtin(vertex_index) v: u32,
    @location(0) rect: vec4<u32>,
    @location(1) size: vec2<u32>,
) -> @builtin(position) vec4<f32> {
    var corners = array<vec2<u32>, 6>(
        vec2<u32>(0u, 0u), vec2<u32>(1u, 0u), vec2<u32>(0u, 1u),
        vec2<u32>(0u, 1u), vec2<u32>(1u, 0u), vec2<u32>(1u, 1u),
    );
    let px = vec2<f32>(rect.xy + corners[v] * rect.zw);
    let extent = vec2<f32>(size);
    return vec4<f32>(px.x / extent.x * 2.0 - 1.0, 1.0 - px.y / extent.y * 2.0, 0.0, 1.0);
}
";

/// One instance: the rectangle and the target size, as six `u32`s.
const INSTANCE_BYTES: u64 = 24;

const INSTANCE_LAYOUT: wgpu::VertexBufferLayout<'static> = wgpu::VertexBufferLayout {
    array_stride: INSTANCE_BYTES,
    step_mode: wgpu::VertexStepMode::Instance,
    attributes: &[
        wgpu::VertexAttribute {
            format: wgpu::VertexFormat::Uint32x4,
            offset: 0,
            shader_location: 0,
        },
        wgpu::VertexAttribute {
            format: wgpu::VertexFormat::Uint32x2,
            offset: 16,
            shader_location: 1,
        },
    ],
};

fn instance(rect: [u32; 4], (width, height): (u32, u32)) -> [u32; 6] {
    [rect[0], rect[1], rect[2], rect[3], width, height]
}

/// A compiled fragment pass. Build it once (in the node's state) from the WGSL of a
/// `@fragment fn fs(@builtin(position) pos: vec4<f32>) -> ...` entry point and its targets'
/// formats; then [`FragmentPass::run`] it every tick on persistent [`GpuTexture`]s.
///
/// Bindings, all in `@group(0)`: the input textures at bindings `0..n` (`texture_2d<f32>`, or
/// `texture_2d<u32>` / `texture_2d<i32>` for integer formats, read with
/// `textureLoad(tex, vec2<i32>(pos.xy), 0)`), then the storage buffers at `n..n+m`
/// (`var<storage, read>`, parameters or lookup tables). Several targets are written through
/// `@location(i)` in an output struct (multiple render targets). Every declared binding must be
/// used by the shader, and every input and buffer passed in must be declared.
///
/// For many encodes per tick, bind once ([`FragmentPass::bind`]) and reuse the
/// [`FragmentBinding`] ([`FragmentPass::encode_bound`]): it skips the validation round trip
/// [`FragmentPass::encode`] makes. [`FragmentPass::encode_rects`] draws only a list of
/// rectangles, optionally keeping the targets' contents.
pub struct FragmentPass {
    pipeline: wgpu::RenderPipeline,
    targets: Vec<wgpu::TextureFormat>,
    /// One full-target instance per target size.
    full: Mutex<HashMap<(u32, u32), wgpu::Buffer>>,
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

/// A pass's inputs and parameter buffers, bound once ([`FragmentPass::bind`]) and reused across
/// encodes and ticks while the same textures and buffers are in use.
#[derive(Debug, Clone)]
pub struct FragmentBinding {
    group: Option<wgpu::BindGroup>,
}

/// Rectangles to draw ([`FragmentPass::encode_rects`]): `[x, y, w, h]` in target pixels, one
/// instance each. Keep it in the node's state and [`write`](Self::write) it each tick. A write
/// lands before the next submission, so a submission sees the last write: use one
/// `FragmentRects` per distinct rectangle list in a submission.
pub struct FragmentRects {
    buffer: wgpu::Buffer,
    capacity: usize,
    count: u32,
    size: (u32, u32),
    queue: wgpu::Queue,
    scratch: Vec<u32>,
}

impl std::fmt::Debug for FragmentRects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentRects")
            .field("count", &self.count)
            .field("capacity", &self.capacity)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl FragmentRects {
    /// Room for `capacity` rectangles. `Unsupported` on a non-wgpu backend.
    pub fn new(gpu: &GpuContextHandle, capacity: usize) -> Result<Self, GpuError> {
        let (device, queue) = gpu.wgpu().ok_or(GpuError::Unsupported)?;
        let capacity = capacity.max(1);
        let buffer = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("daedalus-fragment-rects"),
            size: capacity as u64 * INSTANCE_BYTES,
            usage: wgpu::BufferUsages::VERTEX | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        Ok(Self {
            buffer,
            capacity,
            count: 0,
            size: (0, 0),
            queue: queue.clone(),
            scratch: Vec::with_capacity(capacity * 6),
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Set the rectangles for targets of `target_size`. `InvalidInput` when there are more than
    /// the capacity, or a rectangle leaves the target.
    pub fn write(&mut self, target_size: (u32, u32), rects: &[[u32; 4]]) -> Result<(), GpuError> {
        if rects.len() > self.capacity {
            return Err(GpuError::InvalidInput(format!(
                "{} rectangles for a capacity of {}",
                rects.len(),
                self.capacity
            )));
        }
        let (width, height) = target_size;
        if let Some(rect) = rects.iter().find(|[x, y, w, h]| {
            x.checked_add(*w).is_none_or(|end| end > width)
                || y.checked_add(*h).is_none_or(|end| end > height)
        }) {
            return Err(GpuError::InvalidInput(format!(
                "rectangle {rect:?} leaves the {width}x{height} target"
            )));
        }
        self.scratch.clear();
        for &rect in rects {
            self.scratch.extend(instance(rect, target_size));
        }
        if !self.scratch.is_empty() {
            self.queue
                .write_buffer(&self.buffer, 0, bytemuck::cast_slice(&self.scratch));
        }
        self.count = rects.len() as u32;
        self.size = target_size;
        Ok(())
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
            source: wgpu::ShaderSource::Wgsl(format!("{RECT_VERTEX}\n{fragment_wgsl}").into()),
        });
        let cache = super::pipeline_cache_for(device);
        let color_targets: Vec<Option<wgpu::ColorTargetState>> =
            targets.iter().map(|&format| Some(format.into())).collect();
        let pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("daedalus-fragment-pass"),
            layout: None,
            vertex: wgpu::VertexState {
                module: &module,
                entry_point: Some("daedalus_rect"),
                compilation_options: Default::default(),
                buffers: &[Some(INSTANCE_LAYOUT)],
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
            cache: cache.as_ref(),
        });
        if let Some(err) = pollster::block_on(scope.pop()) {
            return Err(GpuError::InvalidInput(format!(
                "fragment pass rejected: {err}"
            )));
        }
        Ok(Self {
            pipeline,
            targets: targets.to_vec(),
            full: Mutex::new(HashMap::new()),
            device: device.clone(),
            queue: queue.clone(),
        })
    }

    /// The target formats, in `@location` order.
    pub fn targets(&self) -> &[wgpu::TextureFormat] {
        &self.targets
    }

    /// Bind `inputs` then `buffers` in binding order, validated once: reuse the binding for every
    /// encode while those textures and buffers stay the same.
    pub fn bind(
        &self,
        inputs: &[&GpuTexture],
        buffers: &[&GpuStorage],
    ) -> Result<FragmentBinding, GpuError> {
        if inputs.is_empty() && buffers.is_empty() {
            return Ok(FragmentBinding { group: None });
        }
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
        Ok(FragmentBinding { group: Some(group) })
    }

    /// Record the pass into `encoder`: `inputs` and `buffers` bound in order, every target
    /// cleared to zero and then written. Targets must match the pass's formats and share one
    /// size. Binds on every call; prefer [`FragmentPass::bind`] + [`FragmentPass::encode_bound`]
    /// for repeated encodes.
    pub fn encode(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        inputs: &[&GpuTexture],
        buffers: &[&GpuStorage],
        targets: &[&GpuTexture],
    ) -> Result<(), GpuError> {
        let binding = self.bind(inputs, buffers)?;
        self.encode_bound(encoder, &binding, targets)
    }

    /// [`FragmentPass::encode`] with a binding made by [`FragmentPass::bind`]: no validation
    /// round trip.
    pub fn encode_bound(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        binding: &FragmentBinding,
        targets: &[&GpuTexture],
    ) -> Result<(), GpuError> {
        let size = self.check_targets(targets)?;
        let full = self.full_target(size);
        self.draw(encoder, binding, full.slice(..), 1, targets, false, None);
        Ok(())
    }

    /// Draw only `rects`, written for these targets' size. With `load`, the targets keep their
    /// contents outside the rectangles (`LoadOp::Load`); without, they are cleared to zero first.
    pub fn encode_rects(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        binding: &FragmentBinding,
        rects: &FragmentRects,
        targets: &[&GpuTexture],
        load: bool,
    ) -> Result<(), GpuError> {
        let size = self.check_targets(targets)?;
        if rects.count > 0 && rects.size != size {
            return Err(GpuError::InvalidInput(format!(
                "rectangles were written for a {:?} target, the targets are {size:?}",
                rects.size
            )));
        }
        let bytes = u64::from(rects.count) * INSTANCE_BYTES;
        let slice = if bytes == 0 {
            rects.buffer.slice(..INSTANCE_BYTES)
        } else {
            rects.buffer.slice(..bytes)
        };
        self.draw(encoder, binding, slice, rects.count, targets, load, None);
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
        let binding = self.bind(inputs, buffers)?;
        self.run_bound(&binding, targets)
    }

    /// [`FragmentPass::run`] with a binding made by [`FragmentPass::bind`].
    pub fn run_bound(
        &self,
        binding: &FragmentBinding,
        targets: &[&GpuTexture],
    ) -> Result<GpuTicket, GpuError> {
        let mut encoder = self.device.create_command_encoder(&Default::default());
        self.encode_bound(&mut encoder, binding, targets)?;
        Ok(GpuTicket::submit_on(
            &self.device,
            &self.queue,
            [encoder.finish()],
        ))
    }

    /// [`FragmentPass::run`], timed by `timer`: poll the result for the pass's GPU time.
    pub fn run_timed(
        &self,
        timer: &GpuTimer,
        inputs: &[&GpuTexture],
        buffers: &[&GpuStorage],
        targets: &[&GpuTexture],
    ) -> Result<GpuTimed, GpuError> {
        let binding = self.bind(inputs, buffers)?;
        let size = self.check_targets(targets)?;
        let full = self.full_target(size);
        let mut encoder = self.device.create_command_encoder(&Default::default());
        self.draw(
            &mut encoder,
            &binding,
            full.slice(..),
            1,
            targets,
            false,
            timer.render_writes(),
        );
        Ok(timer.submit(encoder))
    }

    /// The targets' shared size, after checking their formats against the pass.
    fn check_targets(&self, targets: &[&GpuTexture]) -> Result<(u32, u32), GpuError> {
        if targets.len() != self.targets.len()
            || targets
                .iter()
                .zip(&self.targets)
                .any(|(t, &format)| t.format() != format)
        {
            let formats: Vec<_> = targets.iter().map(|t| t.format()).collect();
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
        Ok(size)
    }

    /// The one-instance buffer covering a target of `size` (made once per size).
    fn full_target(&self, size: (u32, u32)) -> wgpu::Buffer {
        self.full
            .lock()
            .entry(size)
            .or_insert_with(|| {
                self.device
                    .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                        label: Some("daedalus-fragment-full"),
                        contents: bytemuck::cast_slice(&instance([0, 0, size.0, size.1], size)),
                        usage: wgpu::BufferUsages::VERTEX,
                    })
            })
            .clone()
    }

    #[allow(clippy::too_many_arguments)]
    fn draw(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        binding: &FragmentBinding,
        instances: wgpu::BufferSlice<'_>,
        count: u32,
        targets: &[&GpuTexture],
        load: bool,
        timestamp_writes: Option<wgpu::RenderPassTimestampWrites<'_>>,
    ) {
        let load = if load {
            wgpu::LoadOp::Load
        } else {
            wgpu::LoadOp::Clear(wgpu::Color::TRANSPARENT)
        };
        let attachments: Vec<Option<wgpu::RenderPassColorAttachment<'_>>> = targets
            .iter()
            .map(|t| {
                Some(wgpu::RenderPassColorAttachment {
                    view: t.view(),
                    depth_slice: None,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load,
                        store: wgpu::StoreOp::Store,
                    },
                })
            })
            .collect();
        let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
            label: Some("daedalus-fragment-pass"),
            color_attachments: &attachments,
            depth_stencil_attachment: None,
            timestamp_writes,
            occlusion_query_set: None,
            multiview_mask: None,
        });
        if count == 0 {
            return;
        }
        pass.set_pipeline(&self.pipeline);
        if let Some(group) = &binding.group {
            pass.set_bind_group(0, group, &[]);
        }
        pass.set_vertex_buffer(0, instances);
        pass.draw(0..6, 0..count);
    }
}
