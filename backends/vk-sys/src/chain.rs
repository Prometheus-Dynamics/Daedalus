//! A fixed list of compute shaders run one after the other in one submit, recorded with Vulkan
//! itself into a command buffer that is used again every run. Once each stage has seen its
//! buffers, a run allocates nothing.
//!
//! - A run is begun, given one dispatch for each stage it wants, and submitted. Nothing waits
//!   for the device but [`Chain::begin`], which waits for the run before it.
//! - wgpu doesn't see these commands. The chain keeps every buffer a run uses until the next
//!   run begins, and each recording is between two full memory barriers, with one between
//!   every two dispatches, so a stage reads what the one before it wrote.
//! - Data given to `Queue::write_buffer` reaches the device only with wgpu's next submit, so a
//!   run doesn't see it until then. Flush such writes before [`Chain::submit`].
//! - A buffer wgpu made and has never written is one wgpu clears before its own first use of
//!   it. Clear a buffer through wgpu before a chain writes it, or wgpu will clear it after.

use std::sync::Arc;
use std::time::Duration;

use ash::vk;

use crate::compute::{Binding, ComputePipeline, MAX_BINDINGS, Sets};
use crate::memory::{HostMapping, Placement, storage_buffer};
use crate::submit::Submitter;
use crate::{Error, SubmitLock};

/// What one binding of a stage gets for a run, in the order of the pipeline's slots.
#[derive(Clone, Copy)]
pub enum Arg<'a> {
    /// A storage buffer of the chain's device, and how many bytes of it to bind from its
    /// start. It has storage usage.
    Storage { buffer: &'a wgpu::Buffer, size: u64 },
    /// The bytes of a uniform.
    Bytes(&'a [u8]),
    /// A uniform whose bytes are on the device, at the start of this buffer, which has copy
    /// source usage. They are copied ahead of the stage's dispatch.
    Copy(&'a wgpu::Buffer),
}

struct Uniform {
    buffer: wgpu::Buffer,
    map: HostMapping,
    size: u64,
}

struct Stage {
    pipeline: Arc<ComputePipeline>,
    sets: Sets,
    /// One for each uniform slot, by slot.
    uniforms: Box<[Option<Uniform>]>,
    /// Whether this run has dispatched the stage: its uniforms hold one run's bytes.
    used: bool,
}

#[derive(Clone, Copy)]
enum Op {
    Dispatch {
        stage: usize,
        set: vk::DescriptorSet,
        counts: [u32; 3],
    },
    Copy {
        from: vk::Buffer,
        to: vk::Buffer,
        bytes: u64,
    },
}

/// See the module.
pub struct Chain {
    /// First, so it is dropped first: dropping it waits for the last run, and only then does
    /// anything that run used go.
    submitter: Submitter,
    stages: Box<[Stage]>,
    ops: Vec<Op>,
    /// Buffers a run copies from, kept until the next run begins.
    sources: Vec<wgpu::Buffer>,
    recording: bool,
    runs: u64,
    device: wgpu::Device,
}

impl Chain {
    /// A chain with one stage for each of `pipelines`, all on `device`. Each stage remembers
    /// `sets` different sets of buffers before it rewrites one. `lock` is the device's submit
    /// lock.
    pub fn new(
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        lock: &SubmitLock,
        pipelines: &[Arc<ComputePipeline>],
        sets: usize,
    ) -> Result<Chain, Error> {
        let mut stages = Vec::with_capacity(pipelines.len());
        let mut copies = 0;
        for pipeline in pipelines {
            if pipeline.keep != *device {
                return Err(Error::Range);
            }
            let mut uniforms = Vec::with_capacity(pipeline.slots().len());
            for slot in pipeline.slots() {
                uniforms.push(match slot.kind {
                    Binding::Storage => None,
                    Binding::Uniform => {
                        copies += 1;
                        let size = slot.min_size.max(4).next_multiple_of(4);
                        let (buffer, map) =
                            storage_buffer(device, size, Placement::HostMapped, None)?;
                        let map = map.ok_or(Error::NoMemoryType)?;
                        Some(Uniform { buffer, map, size })
                    }
                });
            }
            stages.push(Stage {
                sets: Sets::new(pipeline, sets)?,
                pipeline: Arc::clone(pipeline),
                uniforms: uniforms.into(),
                used: false,
            });
        }
        Ok(Chain {
            submitter: Submitter::new(device, queue, lock, 1)?,
            ops: Vec::with_capacity(stages.len() + copies),
            sources: Vec::with_capacity(copies),
            stages: stages.into(),
            recording: false,
            runs: 0,
            device: device.clone(),
        })
    }

    /// Begins a run. Waits for the run before it, whose buffers are let go.
    pub fn begin(&mut self) {
        self.submitter.wait_idle();
        self.ops.clear();
        self.sources.clear();
        for stage in &mut self.stages {
            stage.used = false;
        }
        self.recording = true;
    }

    /// Adds stage `stage` to the run with `args`, one for each of its pipeline's slots, and
    /// `counts` workgroups. A stage runs once in a run, and stages run in the order added.
    /// Every buffer has to be one of this chain's device.
    pub fn dispatch(
        &mut self,
        stage: usize,
        args: &[Arg<'_>],
        counts: [u32; 3],
    ) -> Result<(), Error> {
        if !self.recording {
            return Err(Error::Range);
        }
        let at = stage;
        let stage = self.stages.get_mut(at).ok_or(Error::Range)?;
        let slots = stage.pipeline.slots();
        let within = counts
            .iter()
            .zip(stage.pipeline.max_counts)
            .all(|(count, max)| *count <= max);
        if stage.used || args.len() != slots.len() || !within {
            return Err(Error::Range);
        }
        let copies_before = self.ops.len();
        let mut bound: [Option<(&wgpu::Buffer, u64)>; MAX_BINDINGS] = [None; MAX_BINDINGS];
        let rows = slots.iter().zip(args).zip(&stage.uniforms);
        for (out, ((slot, arg), uniform)) in bound.iter_mut().zip(rows) {
            *out = Some(match (slot.kind, arg, uniform) {
                (Binding::Storage, Arg::Storage { buffer, size }, _) => {
                    let fits = *size >= slot.min_size.max(1) && *size <= buffer.size();
                    if !fits || !buffer.usage().contains(wgpu::BufferUsages::STORAGE) {
                        return Err(Error::Range);
                    }
                    (*buffer, *size)
                }
                (Binding::Uniform, Arg::Bytes(bytes), Some(uniform)) => {
                    if (bytes.len() as u64) < slot.min_size {
                        return Err(Error::Range);
                    }
                    // The run before is done, so nothing on the device reads this now.
                    uniform.map.write(0, bytes)?;
                    (&uniform.buffer, uniform.size)
                }
                (Binding::Uniform, Arg::Copy(from), Some(uniform)) => {
                    let usable = from.size() >= uniform.size
                        && from.usage().contains(wgpu::BufferUsages::COPY_SRC);
                    if !usable {
                        return Err(Error::Range);
                    }
                    let handles = (
                        crate::memory::handle(from),
                        crate::memory::handle(&uniform.buffer),
                    );
                    let (Some(from_raw), Some(to)) = handles else {
                        return Err(Error::NotVulkan);
                    };
                    self.sources.push((*from).clone());
                    self.ops.push(Op::Copy {
                        from: from_raw,
                        to,
                        bytes: uniform.size,
                    });
                    (&uniform.buffer, uniform.size)
                }
                _ => {
                    self.ops.truncate(copies_before);
                    return Err(Error::Range);
                }
            });
        }
        // SAFETY: each buffer was checked for usage and size above, or is the stage's own
        // uniform. The caller gives buffers of this device. `begin` waited for the last run,
        // and a stage is used once a run, so no pending submit uses a set of this stage.
        let set = unsafe { stage.sets.bind(&bound[..slots.len()]) }?;
        stage.used = true;
        self.ops.push(Op::Dispatch {
            stage: at,
            set,
            counts,
        });
        Ok(())
    }

    /// Submits the run, and gives its number for [`Chain::is_done`]. Never waits for the
    /// device. On an error nothing was submitted.
    pub fn submit(&mut self) -> Result<u64, Error> {
        if !self.recording {
            return Err(Error::Range);
        }
        self.recording = false;
        let (ops, stages) = (&self.ops, &self.stages);
        let record = |device: &ash::Device, commands: vk::CommandBuffer| {
            for (at, op) in ops.iter().enumerate() {
                // `commands` is recording on this chain's device, as `submit` promises its
                // callback. Every handle is of an object this chain holds: pipelines and sets
                // in its stages, the buffers of each set in the set, uniforms in their stage,
                // and copy sources in `sources`.
                // SAFETY: a recording command buffer and live objects, as said above.
                unsafe {
                    if at > 0 {
                        between(device, commands);
                    }
                    match *op {
                        Op::Dispatch { stage, set, counts } => {
                            stages[stage].pipeline.record(commands, set, counts);
                        }
                        Op::Copy { from, to, bytes } => {
                            let region = vk::BufferCopy::default().size(bytes);
                            device.cmd_copy_buffer(commands, from, to, &[region]);
                        }
                    }
                }
            }
        };
        // SAFETY: what is recorded is valid for the reasons above, and everything it uses is
        // held by this chain until `begin` or `Drop`, both of which wait for this submit first.
        unsafe { self.submitter.submit(record, &[]) }?;
        self.runs += 1;
        Ok(self.runs)
    }

    /// Whether run `run` has finished: a later run has begun, or nothing is running. Never
    /// blocks.
    pub fn is_done(&self, run: u64) -> bool {
        run < self.runs || self.submitter.is_idle()
    }

    /// Whether the last run has finished. Never blocks.
    pub fn is_idle(&self) -> bool {
        self.submitter.is_idle()
    }

    /// Waits up to `timeout` for the last run, and says whether it has finished.
    pub fn wait(&self, timeout: Duration) -> bool {
        self.submitter.wait_idle_for(timeout)
    }

    /// The device the chain runs on.
    pub fn device(&self) -> &wgpu::Device {
        &self.device
    }
}

/// What the commands before wrote is visible to the ones after.
///
/// # Safety
///
/// `commands` must be recording on `device`.
// SAFETY: the contract is in the doc comment above.
unsafe fn between(device: &ash::Device, commands: vk::CommandBuffer) {
    let everything = vk::AccessFlags::MEMORY_READ | vk::AccessFlags::MEMORY_WRITE;
    let barrier = vk::MemoryBarrier::default()
        .src_access_mask(everything)
        .dst_access_mask(everything);
    // SAFETY: the caller's contract. A memory barrier names no object.
    unsafe {
        device.cmd_pipeline_barrier(
            commands,
            vk::PipelineStageFlags::ALL_COMMANDS,
            vk::PipelineStageFlags::ALL_COMMANDS,
            vk::DependencyFlags::empty(),
            &[barrier],
            &[],
            &[],
        );
    }
}
