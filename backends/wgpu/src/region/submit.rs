//! The step: size and pick the region's buffers, record every pass, submit once.

use std::sync::Arc;

use daedalus_abi::{OwnedStep, Slot, Slots, StepCx, StepEnd};
use daedalus_runner::{UNSTAMPED, put_typed, share_typed, typed_view};

use super::{ElementChain, Node, Owned, Place, Size, Source, Work};
use crate::backend::Link;
use crate::buffer::Storage;
use crate::context::Context;
use crate::fence::{Ticket, TicketState};
use crate::memory::SPACE_HOST_MAPPED;
use crate::record::RecordCx;
use crate::shader::Bound;
use crate::shader::OpenPass;
use crate::shader::pipeline::MAX_BINDINGS;
use crate::texture::Texture;

/// Buffers of one output the region keeps to reuse. A consumer that holds more than this at
/// once makes the region allocate.
const KEPT: usize = 4;

pub(crate) struct RegionStep {
    /// Where another device comes from when this one is lost.
    pub(super) link: Arc<Link>,
    pub(super) context: Arc<Context>,
    pub(super) nodes: Vec<Node>,
    pub(super) owned: Vec<Owned>,
    pub(super) reads: usize,
    /// For uniforms nothing feeds.
    pub(super) zeroes: Vec<u8>,
    /// What the last run read. The device may still be reading it after the step has returned,
    /// so the region keeps its own reference until its next run, which the runner holds back
    /// until this run's fence has resolved. For an imported image that reference is what keeps
    /// the producer from reusing the buffer.
    pub(super) held: Vec<Arc<dyn std::any::Any + Send + Sync>>,
    /// The region as a chain recorded with Vulkan itself, where it can be one.
    #[cfg(target_os = "linux")]
    pub(super) chained: Option<super::chained::Chained>,
}

impl RegionStep {
    pub(crate) fn new(
        link: Arc<Link>,
        context: Arc<Context>,
        nodes: Vec<Node>,
        owned: Vec<Owned>,
        reads: usize,
    ) -> RegionStep {
        let largest = nodes
            .iter()
            .filter_map(|node| match &node.work {
                Work::Shader(shader) => Some(shader.spec().bindings),
                Work::Record(_) => None,
            })
            .flatten()
            .filter_map(|binding| match binding.kind {
                crate::shader::BindingKind::Uniform { size } => Some(size as usize),
                _ => None,
            })
            .max()
            .unwrap_or(0);
        RegionStep {
            #[cfg(target_os = "linux")]
            chained: super::chained::Chained::build(&context, &nodes),
            link,
            context,
            nodes,
            owned,
            reads,
            zeroes: vec![0; largest],
            held: Vec::with_capacity(reads),
        }
    }
}

/// The bytes of the storage buffer a source names.
fn bytes_of(source: Source, inputs: &[Slot], owned: &[Owned]) -> Option<u64> {
    match source {
        Source::Cell(cell) => typed_view::<Storage>(inputs.get(cell)?).map(Storage::size),
        Source::Buffer(buffer) => {
            let owned = owned.get(buffer)?;
            owned.kept.get(owned.current).map(|storage| storage.size)
        }
        _ => None,
    }
}

impl Owned {
    /// Picks a buffer of `bytes` for this run: one nobody else holds, or a new one.
    fn prepare(&mut self, context: &Arc<Context>, bytes: u64) -> Result<(), String> {
        // The region's is the only reference, so its last consumer is done with it.
        let free = self
            .kept
            .iter()
            .position(|storage| storage.size == bytes && Arc::strong_count(storage) == 1);
        self.current = match free {
            Some(at) => at,
            None => {
                let made = match self.place {
                    Place::Device => Storage::zeroed(Arc::clone(context), bytes),
                    Place::Mapped => {
                        Storage::placed_on(Arc::clone(context), bytes, SPACE_HOST_MAPPED)
                    }
                    #[cfg(target_os = "linux")]
                    Place::Heap => Storage::dma_heap_on(Arc::clone(context), bytes),
                    #[cfg(not(target_os = "linux"))]
                    Place::Heap => Err(crate::StorageError::Unsupported(
                        "dma-heaps are a Linux interface",
                    )),
                };
                let storage = made.map_err(|error| error.to_string())?;
                // Drop one that is the wrong size or the oldest idle one, to stay bounded.
                let stale = self.kept.iter().position(|storage| {
                    Arc::strong_count(storage) == 1
                        && (storage.size != bytes || self.kept.len() >= KEPT)
                });
                if let Some(stale) = stale {
                    self.kept.swap_remove(stale);
                }
                self.kept.push(Arc::new(storage));
                self.kept.len() - 1
            }
        };
        Ok(())
    }
}

/// The texture a source names, if it is one this run.
fn texture_of<'a>(source: Source, inputs: &'a [Slot], owned: &'a [Owned]) -> Option<&'a Texture> {
    match source {
        Source::Cell(cell) => typed_view::<Texture>(inputs.get(cell)?),
        Source::Buffer(buffer) => owned.get(buffer)?.texture.as_deref(),
        _ => None,
    }
}

/// Textures a chain keeps to draw to. A consumer that holds more than this at once makes the
/// chain allocate.
const TARGETS: usize = 4;

/// Runs a merged chain as one fragment pass when the chain's input is a texture this run, and
/// says whether it did. With a buffer for an input the chain is a compute pass, as any shader.
fn fragment_chain(
    node: &mut Node,
    inputs: &[Slot],
    owned: &[Owned],
    context: &Arc<Context>,
    open: &mut OpenPass,
    encoder: &mut wgpu::CommandEncoder,
) -> Result<Option<(usize, Arc<Texture>)>, String> {
    let Some(chain) = node.element.as_mut() else {
        return Ok(None);
    };
    let Some(&first) = node.sources.first() else {
        return Ok(None);
    };
    let Some(source) = texture_of(first, inputs, owned) else {
        return Ok(None);
    };
    let desc = source.desc();
    if !chain
        .pass
        .as_ref()
        .is_some_and(|(format, _)| *format == desc.format)
    {
        let specs: Vec<&crate::merge::ElementSpec> = chain.specs.iter().collect();
        let pass = crate::merge::fragment_pass_on(Arc::clone(context), &specs, desc.format)
            .map_err(|error| error.to_string())?;
        chain.pass = Some((desc.format, pass));
    }
    // A texture nobody else still holds, of this size, or a new one.
    let free = chain
        .targets
        .iter()
        .position(|target| target.desc() == desc && Arc::strong_count(target) == 1);
    let target = match free {
        Some(at) => Arc::clone(&chain.targets[at]),
        None => {
            let made =
                Texture::new_on(Arc::clone(context), desc).map_err(|error| error.to_string())?;
            let stale = chain.targets.iter().position(|target| {
                Arc::strong_count(target) == 1
                    && (target.desc() != desc || chain.targets.len() >= TARGETS)
            });
            if let Some(stale) = stale {
                chain.targets.swap_remove(stale);
            }
            chain.targets.push(Arc::new(made));
            Arc::clone(&chain.targets[chain.targets.len() - 1])
        }
    };
    // One small buffer for each config: a fragment pass reads it as storage.
    while chain.config_buffers.len() < chain.configs.len() {
        let size = u64::from(chain.configs[chain.config_buffers.len()].1)
            .max(4)
            .next_multiple_of(4);
        let buffer =
            Storage::zeroed(Arc::clone(context), size).map_err(|error| error.to_string())?;
        chain.config_buffers.push(buffer);
    }
    let mut textures = [source; MAX_BINDINGS];
    let mut count = 1;
    for &side in &chain.sides {
        let texture = texture_of(side, inputs, owned)
            .ok_or("a chain that is given a texture takes textures for its other inputs too")?;
        *textures.get_mut(count).ok_or("too many inputs")? = texture;
        count += 1;
    }
    let Some(first_config) = chain.config_buffers.first() else {
        // No configs: bind the textures alone.
        draw(chain, &textures[..count], &[], &target, open, encoder)?;
        return Ok(Some((chain.output, target)));
    };
    let mut buffers = [first_config; MAX_BINDINGS];
    for (at, ((from, size), own)) in chain.configs.iter().zip(&chain.config_buffers).enumerate() {
        let slot = buffers.get_mut(at).ok_or("too many configs")?;
        *slot = match *from {
            Source::Bytes(cell) => {
                let cell = inputs.get(cell);
                match cell.and_then(Slot::bytes) {
                    Some(bytes) if bytes.len() == *size as usize => {
                        let mut padded = vec![0; own.size as usize];
                        padded[..bytes.len()].copy_from_slice(bytes);
                        own.write(0, &padded).map_err(|error| error.to_string())?;
                        own
                    }
                    Some(_) => return Err("a config's bytes aren't its struct's size".into()),
                    // Bytes an upload already put on the device.
                    None => cell
                        .and_then(typed_view::<Storage>)
                        .ok_or("a config's input has no bytes")?,
                }
            }
            // Nothing feeds it: the buffer is zeroes, as it was made.
            _ => own,
        };
    }
    let buffers = &buffers[..chain.configs.len()];
    draw(chain, &textures[..count], buffers, &target, open, encoder)?;
    Ok(Some((chain.output, target)))
}

/// Binds and records a chain's fragment pass, and notes the texture it drew as the chain's
/// output for this run.
fn draw(
    chain: &ElementChain,
    textures: &[&Texture],
    buffers: &[&Storage],
    target: &Arc<Texture>,
    open: &mut OpenPass,
    encoder: &mut wgpu::CommandEncoder,
) -> Result<(), String> {
    let Some((_, pass)) = &chain.pass else {
        return Err("the chain has no pass".into());
    };
    // A draw is a render pass of its own: the compute pass before it ends here.
    open.close();
    let binding = pass
        .bind(textures, buffers)
        .map_err(|error| error.to_string())?;
    pass.encode_bound(encoder, &binding, &[target])
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Sizes what `node` writes from what it was given, and picks the buffers.
pub(super) fn prepare_makes(
    node: &Node,
    inputs: &[Slot],
    owned: &mut [Owned],
    context: &Arc<Context>,
) -> Result<(), (u32, String)> {
    let at = node.index.0;
    for &(_, buffer) in &node.makes {
        let bytes = match owned[buffer].size {
            Size::Bytes(bytes) => bytes,
            Size::Like {
                position: like,
                of,
                stride,
            } => {
                let source = node.sources[like];
                let given = bytes_of(source, inputs, owned)
                    .ok_or_else(|| (at, "an input buffer is missing".to_string()))?;
                (given / u64::from(of)) * u64::from(stride)
            }
            Size::Recorded { output } => {
                let Work::Record(body) = &node.work else {
                    return Err((at, "only a recording body sizes its own outputs".into()));
                };
                let mut given = [0; MAX_BINDINGS];
                for (bytes, source) in given.iter_mut().zip(&node.sources) {
                    *bytes = bytes_of(*source, inputs, owned)
                        .ok_or_else(|| (at, "an input buffer is missing".to_string()))?;
                }
                (body.output_bytes)(&given[..node.sources.len()], output)
            }
        };
        owned[buffer]
            .prepare(context, bytes.max(4).next_multiple_of(4))
            .map_err(|error| (at, error))?;
    }
    Ok(())
}

/// What each binding of the shader node `node` gets this run, in the binding table's order.
pub(super) fn bound_for<'a>(
    node: &Node,
    shader: &crate::shader::Shader,
    inputs: &'a [Slot],
    owned: &'a [Owned],
    zeroes: &'a [u8],
) -> Result<[Bound<'a>; MAX_BINDINGS], (u32, String)> {
    let at = node.index.0;
    let mut bound = [Bound::Sampler; MAX_BINDINGS];
    for (slot, source) in bound.iter_mut().zip(&node.sources) {
        *slot = match *source {
            Source::Cell(cell) => {
                let cell = inputs.get(cell);
                match cell.and_then(typed_view::<Storage>) {
                    Some(storage) => Bound::Storage(storage),
                    None => match cell.and_then(typed_view::<Texture>) {
                        Some(texture) => Bound::Texture(texture),
                        None => {
                            let text = "an input isn't a buffer or a texture on the device";
                            return Err((at, text.into()));
                        }
                    },
                }
            }
            Source::Bytes(cell) => {
                let cell = inputs.get(cell);
                // Plain bytes, or bytes an upload already put on the device.
                match cell.and_then(Slot::bytes) {
                    Some(bytes) => Bound::Uniform(bytes),
                    None => match cell.and_then(typed_view::<Storage>) {
                        Some(storage) => Bound::UniformFrom(storage),
                        None => return Err((at, "a uniform's input has no bytes".into())),
                    },
                }
            }
            Source::Zeroes => Bound::Uniform(&[]),
            Source::Buffer(buffer) => {
                let owned = &owned[buffer];
                // What the node before made: a texture if it drew one this run.
                match (&owned.texture, owned.kept.get(owned.current)) {
                    (Some(texture), _) => Bound::Texture(texture),
                    (None, Some(storage)) => Bound::Storage(storage),
                    (None, None) => {
                        return Err((at, "an input from this region wasn't made".into()));
                    }
                }
            }
            Source::Sampler => Bound::Sampler,
        };
    }
    // A uniform nothing feeds is zeroes of the size the shader declares.
    for (slot, binding) in bound.iter_mut().zip(shader.spec().bindings) {
        if let (Bound::Uniform([]), crate::shader::BindingKind::Uniform { size }) =
            (&*slot, binding.kind)
        {
            *slot = Bound::Uniform(&zeroes[..size as usize]);
        }
    }
    Ok(bound)
}

impl RegionStep {
    fn record(
        &mut self,
        inputs: &[Slot],
        encoder: &mut wgpu::CommandEncoder,
    ) -> Result<(), (u32, String)> {
        // Shader nodes one after the other share one compute pass. Anything else ends it.
        let mut open = OpenPass::default();
        for node in &mut self.nodes {
            let at = node.index.0;
            // A chain of element bodies that is given a texture draws to one.
            let drawn =
                fragment_chain(node, inputs, &self.owned, &self.context, &mut open, encoder)
                    .map_err(|reason| (at, reason))?;
            if let Some((output, texture)) = drawn {
                self.owned[output].texture = Some(texture);
                continue;
            }
            prepare_makes(node, inputs, &mut self.owned, &self.context)?;
            let shader = match &node.work {
                Work::Shader(shader) => shader,
                Work::Record(body) => {
                    // The author's own recording, with the node's buffers by port.
                    let mut given = [None; MAX_BINDINGS];
                    for (slot, source) in given.iter_mut().zip(&node.sources) {
                        *slot = match *source {
                            Source::Cell(cell) => inputs.get(cell).and_then(typed_view::<Storage>),
                            Source::Buffer(buffer) => {
                                let owned = &self.owned[buffer];
                                owned.kept.get(owned.current).map(|storage| &**storage)
                            }
                            _ => None,
                        };
                        if slot.is_none() {
                            return Err((at, "an input isn't a buffer on the device".into()));
                        }
                    }
                    let mut written = [None; MAX_BINDINGS];
                    for &(port, buffer) in &node.makes {
                        let owned = &self.owned[buffer];
                        if let Some(slot) = written.get_mut(port) {
                            *slot = owned.kept.get(owned.current).map(|storage| &**storage);
                        }
                    }
                    // The body records what it likes into the encoder.
                    open.close();
                    let mut cx = RecordCx {
                        device: &self.context.device,
                        encoder: &mut *encoder,
                        inputs: &given[..node.sources.len()],
                        outputs: &written[..node.makes.len()],
                    };
                    (body.record)(&mut cx).map_err(|reason| (at, reason))?;
                    continue;
                }
            };
            let bound = bound_for(node, shader, inputs, &self.owned, &self.zeroes)?;
            shader
                .encode_in(&mut open, encoder, &bound[..node.sources.len()])
                .map_err(|error| (at, error.to_string()))?;
        }
        Ok(())
    }
}

impl RegionStep {
    /// Loads every shader again on a device that isn't lost, and forgets the old buffers.
    fn rebuild(&mut self) -> Result<(), String> {
        let context = self
            .link
            .context()
            .map_err(|error| format!("the device was lost: {error}"))?;
        for node in &mut self.nodes {
            // A recording body keeps what it made itself, and makes it again on the new device.
            if let Work::Shader(shader) = &mut node.work {
                let spec = *shader.spec();
                *shader = crate::shader::Shader::load_on(Arc::clone(&context), &spec).map_err(
                    |error| format!("the device was lost, and the new one refused: {error}"),
                )?;
            }
        }
        for owned in &mut self.owned {
            owned.kept.clear();
            owned.current = 0;
            owned.texture = None;
        }
        for chain in self
            .nodes
            .iter_mut()
            .filter_map(|node| node.element.as_mut())
        {
            chain.pass = None;
            chain.targets.clear();
            chain.config_buffers.clear();
        }
        #[cfg(target_os = "linux")]
        {
            self.chained = super::chained::Chained::build(&context, &self.nodes);
        }
        self.context = context;
        Ok(())
    }
}

impl OwnedStep for RegionStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let context = Arc::clone(&self.context);
        let reported = context.errors.take();
        if context.is_lost() {
            // The device this region was built on is gone. Ask for another, which the backend
            // opens under the failure rule, and build the region again on it. What this run was
            // given is on the lost device, so this run faults either way.
            let reason = match self.rebuild() {
                Ok(()) => "the device was lost, and the region is rebuilt on a new one".into(),
                Err(reason) => reason,
            };
            cx.fault(None, &reason);
            return StepEnd::Fault;
        }
        if let Some(reported) = reported {
            cx.fault(None, &reported);
            return StepEnd::Fault;
        }
        // The last run's work is done, or this run wouldn't have been called.
        self.held.clear();
        for owned in &mut self.owned {
            owned.texture = None;
        }
        let cells = slots.as_mut_slice();
        let reads = self.reads.min(cells.len());
        let (inputs, outputs) = cells.split_at_mut(reads);
        #[cfg(target_os = "linux")]
        let chained = match self.run_chained(inputs) {
            Ok(fence) => fence,
            Err((node, reason)) => {
                cx.fault(Some(node), &reason);
                return StepEnd::Fault;
            }
        };
        #[cfg(not(target_os = "linux"))]
        let chained = None;
        let fence = match chained {
            Some(fence) => fence,
            None => {
                let mut encoder = context
                    .device
                    .create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
                if let Err((node, reason)) = self.record(inputs, &mut encoder) {
                    // Nothing was submitted: the encoder is dropped with everything recorded
                    // in it.
                    cx.fault(Some(node), &reason);
                    return StepEnd::Fault;
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
                }
                Ticket::new(state, Arc::clone(&context), None).into_fence()
            }
        };
        self.held.extend(inputs.iter().filter_map(share_typed));
        // What leaves the region. The values are on the device, and the fence says when the
        // work that fills them is done.
        for owned in &self.owned {
            let Some(out) = owned.publish.and_then(|cell| outputs.get_mut(cell)) else {
                continue;
            };
            if let Some(texture) = &owned.texture {
                put_typed(out, UNSTAMPED.0, UNSTAMPED.1, Arc::clone(texture));
            } else if let Some(storage) = owned.kept.get(owned.current) {
                put_typed(out, UNSTAMPED.0, UNSTAMPED.1, Arc::clone(storage));
            }
        }
        StepEnd::Pending(fence)
    }
}
