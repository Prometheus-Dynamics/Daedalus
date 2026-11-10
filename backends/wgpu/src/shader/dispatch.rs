//! A loaded shader, and one run of it.

use std::fmt;
use std::sync::Arc;

use super::pipeline::{GroupKey, MAX_BINDINGS, Pipeline};
use super::spec::{BindingKind, Dispatch, ShaderSpec};
use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::context::{Context, OpenError};
use crate::fence::{Ticket, TicketState};
use crate::texture::Texture;
use crate::timer::PassTimer;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShaderError {
    Open(OpenError),
    /// The device can't run this shader. The text says which limit.
    Limits(String),
    /// The device refused the shader or its pipeline.
    Build(String),
    /// What was bound doesn't match the shader's binding table. The text says where.
    Binding(String),
    /// The run asks for more workgroups than the device dispatches at once.
    Shape {
        count: u32,
        max: u32,
    },
    /// The device reported an error since the last run, or was lost. Nothing was submitted.
    Device(String),
}

impl fmt::Display for ShaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShaderError::Open(error) => error.fmt(f),
            ShaderError::Limits(text)
            | ShaderError::Build(text)
            | ShaderError::Binding(text)
            | ShaderError::Device(text) => f.write_str(text),
            ShaderError::Shape { count, max } => {
                write!(
                    f,
                    "{count} workgroups in one dimension, where the device allows {max}"
                )
            }
        }
    }
}

impl std::error::Error for ShaderError {}

/// What one binding gets for a run, in the order of the shader's binding table.
#[derive(Clone, Copy, Debug)]
pub enum Bound<'a> {
    Storage(&'a Storage),
    /// The bytes of a uniform, exactly as long as the shader's struct.
    Uniform(&'a [u8]),
    Texture(&'a Texture),
    /// The shader's own sampler, made when it loaded.
    Sampler,
}

/// What a run did.
pub enum Run {
    /// Submitted. The ticket is done when the device has finished.
    Started(Ticket),
    /// Not submitted: the device already has as many submissions unfinished as it takes. The
    /// ticket is the oldest of them. Run again once it is done.
    Busy(Ticket),
}

/// A shader ready to run on one device. Loading builds its pipeline, or finds the one already
/// built for the same content. Load at setup, not while a region runs.
pub struct Shader {
    spec: ShaderSpec,
    key: u64,
    pub(super) pipeline: Arc<Pipeline>,
    /// One per uniform or sampler binding, by position in the binding table.
    owned: Vec<Option<Owned>>,
    /// For timing passes with the device's own timestamps. Empty unless asked for.
    timers: Vec<Arc<PassTimer>>,
    context: Arc<Context>,
}

enum Owned {
    Uniform(wgpu::Buffer),
    Sampler(wgpu::Sampler),
}

impl Shader {
    pub fn load(backend: &WgpuBackend, spec: &ShaderSpec) -> Result<Shader, ShaderError> {
        let context = backend.context().map_err(ShaderError::Open)?;
        let caps = &context.record.compute;
        let (buffers, textures) = spec.counts();
        let limit = |what: &str, have: u32, max: u32| {
            (have > max).then(|| {
                ShaderError::Limits(format!(
                    "`{}` has {have} {what}, and this device allows {max}",
                    spec.name
                ))
            })
        };
        if let Some(error) = limit(
            "storage buffers",
            buffers,
            caps.max_storage_buffers_per_stage,
        )
        .or_else(|| {
            limit(
                "storage textures",
                textures,
                caps.max_storage_textures_per_stage,
            )
        })
        .or_else(|| limit("bindings", spec.bindings.len() as u32, MAX_BINDINGS as u32))
        {
            return Err(error);
        }
        if !caps.fits_workgroup(spec.workgroup) {
            return Err(ShaderError::Limits(format!(
                "`{}` has a workgroup of {:?}, which this device doesn't allow",
                spec.name, spec.workgroup
            )));
        }
        let pipeline = context
            .shaders()
            .pipeline(&context.device, spec)
            .map_err(ShaderError::Build)?;
        let owned = spec
            .bindings
            .iter()
            .map(|binding| match binding.kind {
                BindingKind::Uniform { size } => Some(Owned::Uniform(
                    context.device.create_buffer(&wgpu::BufferDescriptor {
                        label: None,
                        size: u64::from(size).next_multiple_of(4),
                        usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
                        mapped_at_creation: false,
                    }),
                )),
                BindingKind::Sampler { comparison } => Some(Owned::Sampler(
                    context.device.create_sampler(&wgpu::SamplerDescriptor {
                        mag_filter: wgpu::FilterMode::Linear,
                        min_filter: wgpu::FilterMode::Linear,
                        compare: comparison.then_some(wgpu::CompareFunction::LessEqual),
                        ..wgpu::SamplerDescriptor::default()
                    }),
                )),
                _ => None,
            })
            .collect();
        Ok(Shader {
            spec: *spec,
            key: spec.key(),
            pipeline,
            owned,
            timers: Vec::new(),
            context,
        })
    }

    /// Times each pass with timestamps the device writes, where it can. Without this, or on a
    /// device that can't, a ticket's time is from submit to done.
    pub fn with_timestamps(mut self) -> Shader {
        let wanted = self.context.inflight().limit();
        self.timers = (0..wanted)
            .filter_map(|_| PassTimer::new(&self.context).map(Arc::new))
            .collect();
        self
    }

    pub fn spec(&self) -> &ShaderSpec {
        &self.spec
    }

    fn mismatch(&self, at: usize, what: &str) -> ShaderError {
        let name = self
            .spec
            .bindings
            .get(at)
            .map_or("?", |binding| binding.name);
        ShaderError::Binding(format!("`{}` binding `{name}`: {what}", self.spec.name))
    }

    /// Checks `bound` against the binding table, writes the uniforms, and gives each binding's
    /// identity and wgpu resource.
    fn resources<'a>(
        &'a self,
        bound: &[Bound<'a>],
    ) -> Result<[(u64, Option<wgpu::BindingResource<'a>>); MAX_BINDINGS], ShaderError> {
        if bound.len() != self.spec.bindings.len() {
            return Err(ShaderError::Binding(format!(
                "`{}` has {} bindings, and {} were bound",
                self.spec.name,
                self.spec.bindings.len(),
                bound.len()
            )));
        }
        let mut out = [const { (0, None) }; MAX_BINDINGS];
        let rows = self.spec.bindings.iter().zip(bound).zip(&self.owned);
        for (at, ((binding, bound), owned)) in rows.enumerate() {
            let slot = out
                .get_mut(at)
                .ok_or_else(|| self.mismatch(at, "too many"))?;
            *slot = match (&binding.kind, bound, owned) {
                (BindingKind::Storage { .. }, Bound::Storage(storage), _) => {
                    if storage.context.key != self.context.key {
                        return Err(self.mismatch(at, "the buffer is on another device"));
                    }
                    let whole = wgpu::BufferBinding {
                        buffer: &storage.buffer,
                        offset: 0,
                        size: std::num::NonZeroU64::new(storage.size),
                    };
                    (storage.id, Some(wgpu::BindingResource::Buffer(whole)))
                }
                (
                    BindingKind::Uniform { size },
                    Bound::Uniform(bytes),
                    Some(Owned::Uniform(buffer)),
                ) => {
                    if bytes.len() != *size as usize {
                        return Err(
                            self.mismatch(at, "the uniform's bytes aren't the struct's size")
                        );
                    }
                    self.context.queue.write_buffer(buffer, 0, bytes);
                    (u64::MAX, Some(buffer.as_entire_binding()))
                }
                (BindingKind::Texture { .. }, Bound::Texture(texture), _) => (
                    texture.id,
                    Some(wgpu::BindingResource::TextureView(&texture.view)),
                ),
                (BindingKind::StorageTexture { format, .. }, Bound::Texture(texture), _) => {
                    if texture.desc().format != *format {
                        return Err(self.mismatch(at, "the texture has another format"));
                    }
                    (
                        texture.id,
                        Some(wgpu::BindingResource::TextureView(&texture.view)),
                    )
                }
                (BindingKind::Sampler { .. }, Bound::Sampler, Some(Owned::Sampler(sampler))) => {
                    (u64::MAX, Some(wgpu::BindingResource::Sampler(sampler)))
                }
                _ => return Err(self.mismatch(at, "the wrong kind of resource")),
            };
        }
        Ok(out)
    }

    fn counts(&self, bound: &[Bound<'_>]) -> Result<[u32; 3], ShaderError> {
        let counts = match self.spec.dispatch {
            Dispatch::Counts(counts) => counts,
            Dispatch::PerElement { binding, stride } => {
                let at = self
                    .spec
                    .bindings
                    .iter()
                    .position(|have| have.index == binding)
                    .unwrap_or(usize::MAX);
                let Some(Bound::Storage(storage)) = bound.get(at) else {
                    return Err(self.mismatch(at, "the run is sized by this buffer"));
                };
                let elements = storage.size / u64::from(stride.max(1));
                let groups = elements.div_ceil(u64::from(self.spec.workgroup[0].max(1)));
                [u32::try_from(groups).unwrap_or(u32::MAX), 1, 1]
            }
        };
        let max = self.context.record.compute.max_workgroups_per_dimension;
        match counts.iter().find(|&&count| count > max) {
            Some(&count) => Err(ShaderError::Shape { count, max }),
            None => Ok(counts),
        }
    }

    /// Records this shader's pass into `encoder`: its bind group, found or made, and one
    /// dispatch.
    fn pass(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        resources: [(u64, Option<wgpu::BindingResource<'_>>); MAX_BINDINGS],
        [x, y, z]: [u32; 3],
        timestamp_writes: Option<wgpu::ComputePassTimestampWrites<'_>>,
    ) {
        let context = &self.context;
        let mut ids = [0; MAX_BINDINGS];
        for (id, (have, _)) in ids.iter_mut().zip(&resources) {
            *id = *have;
        }
        let key: GroupKey = (self.key, ids);
        let kept = context.shaders().group(&key);
        let group = match kept {
            Some(group) => group,
            None => {
                let entries: Vec<wgpu::BindGroupEntry<'_>> = self
                    .spec
                    .bindings
                    .iter()
                    .zip(resources)
                    .filter_map(|(binding, (_, resource))| {
                        Some(wgpu::BindGroupEntry {
                            binding: binding.index,
                            resource: resource?,
                        })
                    })
                    .collect();
                let group = context
                    .device
                    .create_bind_group(&wgpu::BindGroupDescriptor {
                        label: None,
                        layout: &self.pipeline.layout,
                        entries: &entries,
                    });
                context.shaders().keep_group(key, group.clone());
                group
            }
        };
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: None,
            timestamp_writes,
        });
        pass.set_pipeline(&self.pipeline.pipeline);
        pass.set_bind_group(0, &group, &[]);
        pass.dispatch_workgroups(x, y, z);
        context.count_pass();
    }

    /// Checks `bound` and records one pass of this shader into `encoder`, for a region that
    /// submits several passes together. Nothing is submitted here.
    pub(crate) fn encode(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        bound: &[Bound<'_>],
    ) -> Result<(), ShaderError> {
        let resources = self.resources(bound)?;
        let counts = self.counts(bound)?;
        self.pass(encoder, resources, counts, None);
        Ok(())
    }

    /// Runs the shader once, as one compute pass, and submits it. Never waits: the ticket says
    /// when the work is done, and the work is ordered after everything submitted before it on
    /// this device. When the device already has its limit of submissions unfinished, nothing is
    /// submitted and the oldest of them comes back to wait on.
    pub fn run(&self, bound: &[Bound<'_>]) -> Result<Run, ShaderError> {
        let context = &self.context;
        if let Some(reported) = context.errors.take() {
            return Err(ShaderError::Device(reported));
        }
        if context.is_lost() {
            return Err(ShaderError::Device("the device is lost".into()));
        }
        let resources = self.resources(bound)?;
        let [x, y, z] = self.counts(bound)?;
        context.poll();
        if let Err(oldest) = context.inflight().admit() {
            return Ok(Run::Busy(Ticket::new(oldest, Arc::clone(context), None)));
        }
        let timer = self.timers.iter().find(|timer| timer.claim());
        let mut encoder = context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
        self.pass(
            &mut encoder,
            resources,
            [x, y, z],
            timer.map(|timer| timer.writes()),
        );
        if let Some(timer) = timer {
            timer.resolve(&mut encoder);
        }
        let state = TicketState::new();
        let finished = Arc::clone(&state);
        {
            let _held = context.submit.lock();
            context.queue.submit([encoder.finish()]);
            context.count_submit();
            context
                .queue
                .on_submitted_work_done(move || finished.finish());
            if let Some(timer) = timer {
                timer.map();
            }
        }
        context.inflight().started(Arc::clone(&state));
        Ok(Run::Started(Ticket::new(
            state,
            Arc::clone(context),
            timer.cloned(),
        )))
    }
}
