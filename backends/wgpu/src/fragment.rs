//! Fragment passes: a draw over a whole target, or over rectangles of it, that reads textures
//! and buffers and writes render targets.
//!
//! On a tile-based GPU this is the fast way to do per-pixel work. The backend supplies the
//! vertex stage, so a pass is one WGSL fragment function named `fs` with `@location` outputs.
//! Its bindings are all in group 0: input textures first, from binding 0, then storage buffers.
//! The pipeline's layout is taken from the shader, so every binding the shader declares has to
//! be used by it, and every one that is bound has to be declared.

mod rects;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::context::{Context, OpenError};
use crate::fence::{Ticket, TicketState};
use crate::texture::{Format, Texture};

pub use rects::FragmentRects;
pub(crate) use rects::{INSTANCE_BYTES, instance};

/// The vertex stage of every fragment pass: one rectangle per instance, in pixels.
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentError {
    Open(OpenError),
    /// The shader, its bindings or its targets aren't what the pass needs. The text says which.
    Invalid(String),
}

impl fmt::Display for FragmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FragmentError::Open(error) => error.fmt(f),
            FragmentError::Invalid(text) => f.write_str(text),
        }
    }
}

impl std::error::Error for FragmentError {}

fn invalid(text: impl Into<String>) -> FragmentError {
    FragmentError::Invalid(text.into())
}

/// Builds the render pipeline of a fragment pass: the backend's rectangle vertex stage and the
/// fragment function `fs` of `wgsl`, writing `targets`. The one builder, for a pass an author
/// writes and for a merged per-pixel chain.
pub(crate) fn fragment_pipeline(
    device: &wgpu::Device,
    wgsl: &str,
    targets: &[Format],
) -> Result<wgpu::RenderPipeline, String> {
    let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
    let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: None,
        source: wgpu::ShaderSource::Wgsl(format!("{RECT_VERTEX}\n{wgsl}").into()),
    });
    let color_targets: Vec<Option<wgpu::ColorTargetState>> = targets
        .iter()
        .map(|format| Some(format.to_wgpu().into()))
        .collect();
    let pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
        label: None,
        layout: None,
        vertex: wgpu::VertexState {
            module: &module,
            entry_point: Some("daedalus_rect"),
            compilation_options: wgpu::PipelineCompilationOptions::default(),
            buffers: &[Some(INSTANCE_LAYOUT)],
        },
        fragment: Some(wgpu::FragmentState {
            module: &module,
            entry_point: Some("fs"),
            compilation_options: wgpu::PipelineCompilationOptions::default(),
            targets: &color_targets,
        }),
        primitive: wgpu::PrimitiveState::default(),
        depth_stencil: None,
        multisample: wgpu::MultisampleState::default(),
        multiview_mask: None,
        cache: None,
    });
    match crate::wait::scope_error(scope) {
        Some(error) => Err(error),
        None => Ok(pipeline),
    }
}

/// A fragment shader and the targets it writes, ready to draw. Made at setup.
pub struct FragmentPass {
    pipeline: wgpu::RenderPipeline,
    targets: Vec<Format>,
    /// The one instance that covers a whole target, by target size.
    full: Mutex<HashMap<(u32, u32), wgpu::Buffer>>,
    context: Arc<Context>,
}

impl fmt::Debug for FragmentPass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FragmentPass")
            .field("targets", &self.targets)
            .finish_non_exhaustive()
    }
}

/// A pass's inputs, bound once and drawn with as often as wanted.
#[derive(Clone, Debug)]
pub struct FragmentBinding {
    group: Option<wgpu::BindGroup>,
}

impl FragmentPass {
    /// A pass whose fragment function `fs` in `fragment_wgsl` writes one `@location` per format
    /// in `targets`.
    pub fn new(
        backend: &WgpuBackend,
        fragment_wgsl: &str,
        targets: &[Format],
    ) -> Result<FragmentPass, FragmentError> {
        let context = backend.context().map_err(FragmentError::Open)?;
        if targets.is_empty() {
            return Err(invalid("a fragment pass needs at least one target"));
        }
        let pipeline = fragment_pipeline(&context.device, fragment_wgsl, targets)
            .map_err(|error| invalid(format!("the fragment pass was refused: {error}")))?;
        Ok(FragmentPass {
            pipeline,
            targets: targets.to_vec(),
            full: Mutex::new(HashMap::new()),
            context,
        })
    }

    pub fn targets(&self) -> &[Format] {
        &self.targets
    }

    /// Binds the pass's inputs: `inputs` at bindings 0 and up, then `buffers`. The result can
    /// be drawn with any number of times, so a pass that runs every tick binds once.
    pub fn bind(
        &self,
        inputs: &[&Texture],
        buffers: &[&Storage],
    ) -> Result<FragmentBinding, FragmentError> {
        if inputs.is_empty() && buffers.is_empty() {
            return Ok(FragmentBinding { group: None });
        }
        let device = &self.context.device;
        let entries: Vec<wgpu::BindGroupEntry<'_>> = inputs
            .iter()
            .map(|texture| wgpu::BindingResource::TextureView(&texture.view))
            .chain(
                buffers
                    .iter()
                    .map(|storage| storage.buffer.as_entire_binding()),
            )
            .enumerate()
            .map(|(binding, resource)| wgpu::BindGroupEntry {
                binding: binding as u32,
                resource,
            })
            .collect();
        let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
        let group = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: None,
            layout: &self.pipeline.get_bind_group_layout(0),
            entries: &entries,
        });
        match crate::wait::scope_error(scope) {
            Some(error) => Err(invalid(format!(
                "the fragment pass's bindings were refused: {error}"
            ))),
            None => Ok(FragmentBinding { group: Some(group) }),
        }
    }

    fn check_targets(&self, targets: &[&Texture]) -> Result<(u32, u32), FragmentError> {
        let formats: Vec<Format> = targets
            .iter()
            .map(|texture| texture.desc().format)
            .collect();
        if formats != self.targets {
            return Err(invalid(format!(
                "targets of {formats:?}, where the pass writes {:?}",
                self.targets
            )));
        }
        let size = |texture: &&Texture| (texture.desc().width, texture.desc().height);
        let first = targets.first().map(size).unwrap_or((0, 0));
        if targets.iter().any(|texture| size(texture) != first) {
            return Err(invalid("the targets of a fragment pass are all one size"));
        }
        if targets
            .iter()
            .any(|texture| texture.device_key() != self.context.key)
        {
            return Err(invalid("a target is on another device"));
        }
        Ok(first)
    }

    fn full_target(&self, size: (u32, u32)) -> wgpu::Buffer {
        let mut full = self.full.lock().unwrap_or_else(PoisonError::into_inner);
        full.entry(size)
            .or_insert_with(|| {
                let buffer = self.context.device.create_buffer(&wgpu::BufferDescriptor {
                    label: None,
                    size: INSTANCE_BYTES,
                    usage: wgpu::BufferUsages::VERTEX | wgpu::BufferUsages::COPY_DST,
                    mapped_at_creation: false,
                });
                let whole = instance([0, 0, size.0, size.1], size);
                self.context.queue.write_buffer(&buffer, 0, &whole);
                buffer
            })
            .clone()
    }

    fn draw(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        binding: &FragmentBinding,
        instances: wgpu::BufferSlice<'_>,
        count: u32,
        targets: &[&Texture],
        load: bool,
    ) {
        let load = if load {
            wgpu::LoadOp::Load
        } else {
            wgpu::LoadOp::Clear(wgpu::Color::TRANSPARENT)
        };
        let attachments: Vec<Option<wgpu::RenderPassColorAttachment<'_>>> = targets
            .iter()
            .map(|texture| {
                Some(wgpu::RenderPassColorAttachment {
                    view: &texture.view,
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
            label: None,
            color_attachments: &attachments,
            depth_stencil_attachment: None,
            timestamp_writes: None,
            occlusion_query_set: None,
            multiview_mask: None,
        });
        self.context.count_pass();
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

    /// Records the pass over the whole of `targets`, which it clears first.
    pub fn encode_bound(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        binding: &FragmentBinding,
        targets: &[&Texture],
    ) -> Result<(), FragmentError> {
        let size = self.check_targets(targets)?;
        let full = self.full_target(size);
        self.draw(encoder, binding, full.slice(..), 1, targets, false);
        Ok(())
    }

    /// Records the pass over `rects` only, in one instanced draw. With `load` the rest of each
    /// target keeps what it had. Without, the targets are cleared first.
    pub fn encode_rects(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        binding: &FragmentBinding,
        rects: &FragmentRects,
        targets: &[&Texture],
        load: bool,
    ) -> Result<(), FragmentError> {
        let size = self.check_targets(targets)?;
        if !rects.is_empty() && rects.target_size() != size {
            return Err(invalid(format!(
                "the rectangles were written for a {:?} target, and the targets are {size:?}",
                rects.target_size()
            )));
        }
        let count = rects.len() as u32;
        let bytes = u64::from(count.max(1)) * INSTANCE_BYTES;
        self.draw(
            encoder,
            binding,
            rects.buffer().slice(..bytes),
            count,
            targets,
            load,
        );
        Ok(())
    }

    /// Submits `encoder`, which this pass's draws were recorded into, and gives the ticket for
    /// it. For a pass run by hand: in a graph, the region submits.
    pub fn submit(&self, encoder: wgpu::CommandEncoder) -> Ticket {
        let context = &self.context;
        let state = TicketState::new();
        let finished = Arc::clone(&state);
        {
            let _held = context.submit.lock();
            context.queue.submit([encoder.finish()]);
            context.count_submit();
            context
                .queue
                .on_submitted_work_done(move || finished.finish());
        }
        Ticket::new(state, Arc::clone(context), None)
    }

    /// An encoder on this pass's device, to record draws into.
    pub fn encoder(&self) -> wgpu::CommandEncoder {
        self.context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor::default())
    }

    /// Draws the whole of `targets` once and submits.
    pub fn run_bound(
        &self,
        binding: &FragmentBinding,
        targets: &[&Texture],
    ) -> Result<Ticket, FragmentError> {
        let mut encoder = self.encoder();
        self.encode_bound(&mut encoder, binding, targets)?;
        Ok(self.submit(encoder))
    }
}
