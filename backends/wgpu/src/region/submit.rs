//! The step: size and pick the region's buffers, record every pass, submit once.

use std::sync::Arc;

use daedalus_abi::{OwnedStep, Slot, Slots, StepCx, StepEnd};
use daedalus_runner::{UNSTAMPED, put_typed, typed_view};

use super::{Node, Owned, Size, Source};
use crate::buffer::Storage;
use crate::context::Context;
use crate::fence::{Ticket, TicketState};
use crate::shader::Bound;
use crate::shader::pipeline::MAX_BINDINGS;
use crate::texture::Texture;

/// Buffers of one output the region keeps to reuse. A consumer that holds more than this at
/// once makes the region allocate.
const KEPT: usize = 4;

pub(crate) struct RegionStep {
    context: Arc<Context>,
    nodes: Vec<Node>,
    owned: Vec<Owned>,
    reads: usize,
    /// For uniforms nothing feeds.
    zeroes: Vec<u8>,
}

impl RegionStep {
    pub(crate) fn new(
        context: Arc<Context>,
        nodes: Vec<Node>,
        owned: Vec<Owned>,
        reads: usize,
    ) -> RegionStep {
        let largest = nodes
            .iter()
            .flat_map(|node| node.shader.spec().bindings)
            .filter_map(|binding| match binding.kind {
                crate::shader::BindingKind::Uniform { size } => Some(size as usize),
                _ => None,
            })
            .max()
            .unwrap_or(0);
        RegionStep {
            context,
            nodes,
            owned,
            reads,
            zeroes: vec![0; largest],
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
                let storage = Storage::zeroed(Arc::clone(context), bytes)
                    .map_err(|error| error.to_string())?;
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

impl RegionStep {
    fn record(
        &mut self,
        inputs: &[Slot],
        encoder: &mut wgpu::CommandEncoder,
    ) -> Result<(), (u32, String)> {
        for node in &self.nodes {
            let at = node.index.0;
            // Size what this node writes from what it was given, then pick the buffers.
            for &(position, buffer) in &node.makes {
                let bytes = match self.owned[buffer].size {
                    Size::Bytes(bytes) => bytes,
                    Size::Like {
                        position: like,
                        of,
                        stride,
                    } => {
                        let source = node.sources[like];
                        let given = bytes_of(source, inputs, &self.owned)
                            .ok_or_else(|| (at, "an input buffer is missing".to_string()))?;
                        (given / u64::from(of)) * u64::from(stride)
                    }
                };
                let _ = position;
                self.owned[buffer]
                    .prepare(&self.context, bytes.max(4).next_multiple_of(4))
                    .map_err(|error| (at, error))?;
            }
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
                                    return Err((
                                        at,
                                        "an input isn't a buffer or a texture on the device".into(),
                                    ));
                                }
                            },
                        }
                    }
                    Source::Bytes(cell) => match inputs.get(cell).and_then(Slot::bytes) {
                        Some(bytes) => Bound::Uniform(bytes),
                        None => return Err((at, "a uniform's input has no bytes".into())),
                    },
                    Source::Zeroes => Bound::Uniform(&[]),
                    Source::Buffer(buffer) => {
                        let owned = &self.owned[buffer];
                        Bound::Storage(&owned.kept[owned.current])
                    }
                    Source::Sampler => Bound::Sampler,
                };
            }
            // A uniform nothing feeds is zeroes of the size the shader declares.
            let spec = node.shader.spec();
            for (slot, binding) in bound.iter_mut().zip(spec.bindings) {
                if let (Bound::Uniform([]), crate::shader::BindingKind::Uniform { size }) =
                    (&*slot, binding.kind)
                {
                    *slot = Bound::Uniform(&self.zeroes[..size as usize]);
                }
            }
            node.shader
                .encode(encoder, &bound[..node.sources.len()])
                .map_err(|error| (at, error.to_string()))?;
        }
        Ok(())
    }
}

impl OwnedStep for RegionStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let context = Arc::clone(&self.context);
        if let Some(reported) = context.errors.take() {
            cx.fault(None, &reported);
            return StepEnd::Fault;
        }
        if context.is_lost() {
            cx.fault(None, "the device is lost");
            return StepEnd::Fault;
        }
        let cells = slots.as_mut_slice();
        let reads = self.reads.min(cells.len());
        let (inputs, outputs) = cells.split_at_mut(reads);
        let mut encoder = context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
        if let Err((node, reason)) = self.record(inputs, &mut encoder) {
            // Nothing was submitted: the encoder is dropped with everything recorded in it.
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
        // What leaves the region. The values are on the device, and the fence says when the
        // work that fills them is done.
        for owned in &self.owned {
            if let (Some(cell), Some(storage)) = (owned.publish, owned.kept.get(owned.current))
                && let Some(out) = outputs.get_mut(cell)
            {
                put_typed(out, UNSTAMPED.0, UNSTAMPED.1, Arc::clone(storage));
            }
        }
        StepEnd::Pending(Ticket::new(state, context, None).into_fence())
    }
}
