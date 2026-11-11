//! A region recorded with Vulkan itself: every node's dispatch in one command buffer that is
//! used again each run, and one submit. A run allocates nothing once the region has seen its
//! buffers, where an encoder allocates for every run.
//!
//! A region runs this way when the device is on Vulkan, every node is a shader, and every
//! binding is a storage or uniform buffer. A run of it that is given a texture, as a chain of
//! pixel bodies may be, goes through the encoder instead: both are in order on the queue.

use std::sync::{Arc, Mutex, PoisonError};

use daedalus_abi::{Fence, Slot};
use daedalus_runner::typed_view;
use daedalus_vk_sys::Error;
use daedalus_vk_sys::ash::vk;
use daedalus_vk_sys::chain::{Arg, Chain};

use super::submit::{RegionStep, bound_for, prepare_makes};
use super::{Node, Source, Work};
use crate::buffer::Storage;
use crate::context::Context;
use crate::shader::Bound;
use crate::shader::pipeline::MAX_BINDINGS;

/// How many different sets of buffers a node remembers before it rewrites one. A node's
/// output is one of a few the region keeps, and so is what feeds it.
const SETS: usize = 8;

pub(crate) struct Chained {
    chain: Arc<Mutex<Chain>>,
    /// What the chain's fences name.
    place: crate::fence::ChainPlace,
}

impl Chained {
    /// The chain for `nodes`, one stage a node, or `None` when they can't run as one.
    pub(crate) fn build(context: &Arc<Context>, nodes: &[Node]) -> Option<Chained> {
        let mut pipelines = Vec::with_capacity(nodes.len());
        for node in nodes {
            let Work::Shader(shader) = &node.work else {
                return None;
            };
            pipelines.push(context.shaders().chained(&context.device, shader.spec())?);
        }
        let lock = &context.submit;
        let chain = Chain::new(&context.device, &context.queue, lock, &pipelines, SETS).ok()?;
        let chain = Arc::new(Mutex::new(chain));
        let place = crate::fence::ChainPlace::of(&chain, context)?;
        context.note_chain(&chain);
        Some(Chained { chain, place })
    }
}

impl RegionStep {
    /// Whether every value this run was given is a buffer on this region's device, or bytes.
    fn all_buffers(&self, inputs: &[Slot]) -> bool {
        let here = self.context.key;
        let on_device = |cell: usize| {
            inputs
                .get(cell)
                .and_then(typed_view::<Storage>)
                .is_some_and(|storage| storage.context.key == here)
        };
        self.nodes.iter().all(|node| {
            node.sources.iter().all(|source| match *source {
                Source::Cell(cell) => on_device(cell),
                Source::Bytes(cell) => {
                    inputs.get(cell).is_some_and(|cell| cell.bytes().is_some()) || on_device(cell)
                }
                Source::Zeroes | Source::Buffer(_) => true,
                Source::Sampler => false,
            })
        })
    }

    /// Records and submits the region as a chain, and gives the run's fence. `None` when this
    /// run can't go that way, with nothing changed.
    pub(super) fn run_chained(&mut self, inputs: &[Slot]) -> Result<Option<Fence>, (u32, String)> {
        let Some(chained) = &self.chained else {
            return Ok(None);
        };
        if !self.all_buffers(inputs) {
            return Ok(None);
        }
        {
            let mut chain = chained.chain.lock().unwrap_or_else(PoisonError::into_inner);
            chain.begin();
            for (stage, node) in self.nodes.iter().enumerate() {
                let at = node.index.0;
                let Work::Shader(shader) = &node.work else {
                    return Err((at, "a chained region holds only shaders".into()));
                };
                prepare_makes(node, inputs, &mut self.owned, &self.context)?;
                let bound = bound_for(node, shader, inputs, &self.owned, &self.zeroes)?;
                let bound = &bound[..node.sources.len()];
                let counts = shader
                    .counts(bound)
                    .map_err(|error| (at, error.to_string()))?;
                let mut args = [Arg::Bytes(&[]); MAX_BINDINGS];
                for (arg, bound) in args.iter_mut().zip(bound) {
                    *arg = match *bound {
                        Bound::Storage(storage) => Arg::Storage {
                            buffer: &storage.buffer,
                            size: storage.size,
                        },
                        Bound::Uniform(bytes) => Arg::Bytes(bytes),
                        Bound::UniformFrom(storage) => Arg::Copy(&storage.buffer),
                        Bound::Texture(_) | Bound::Sampler => {
                            return Err((at, "a chained region binds only buffers".into()));
                        }
                    };
                }
                chain
                    .dispatch(stage, &args[..bound.len()], counts)
                    .map_err(|error| (at, format!("`{}`: {error}", shader.spec().name)))?;
                self.context.count_dispatch();
            }
            // What an upload gave the queue to write has to be on the device first.
            self.context.flush_writes();
            if let Err(error) = chain.submit() {
                {
                    if error == Error::Vulkan(vk::Result::ERROR_DEVICE_LOST) {
                        self.context.mark_lost("a submit found it gone");
                    }
                    let at = self.nodes.first().map_or(0, |node| node.index.0);
                    return Err((at, format!("the region's submit failed: {error}")));
                }
            }
        }
        self.context.count_submit();
        self.context.count_pass();
        Ok(Some(chained.place.fence()))
    }
}
