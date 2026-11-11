//! Kernel ops run with Vulkan itself: a batch is a chain with one stage an op, recorded into a
//! command buffer that is used again, so running the same ops again allocates nothing.
//!
//! A chain is for one sequence of ops. The first batch of a sequence builds its chain, and the
//! chain is kept: a few for each sequence, since several batches may be in flight at once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use daedalus_vk_sys::chain::{Arg, Chain};

use super::ops::Kernels;
use super::{Kernel, KernelError};
use crate::buffer::Storage;
use crate::fence::{Ticket, chain_ticket};

/// The most ops one chain holds. A longer batch is several chains, submitted in order.
const MAX_STAGES: usize = 30;
/// Chains kept for one sequence of ops: as many batches of it as run at once without one
/// waiting for another.
const KEPT: usize = 8;
/// Sequences whose chains are kept. Past this, they are all let go.
const SEQUENCES: usize = 64;
/// Sets of buffers a stage remembers before it rewrites one.
const SETS: usize = 4;

/// One op of a batch: what its stage binds, and how many workgroups it runs.
pub(crate) struct Op {
    kernel: Kernel,
    /// The index in `SPECS` of the shader this op runs.
    spec: usize,
    /// The input, the output, the counts, and then the op's extra buffers, in the order its spec
    /// binds them. Each with the bytes of it that are bound.
    buffers: [Option<(wgpu::Buffer, u64)>; MAX_OP_BUFFERS],
    counts: [u32; 3],
}

type Shared = Arc<Mutex<Chain>>;

#[derive(Default)]
pub(crate) struct Chains {
    /// Whether this device runs chains at all: it is on Vulkan.
    able: OnceLock<bool>,
    kept: Mutex<HashMap<u128, Vec<Shared>>>,
    /// Lists of ops, kept for their capacity.
    lists: Mutex<Vec<Vec<Op>>>,
}

/// The most buffers one op binds: the three plain ones and the extras of the largest shape.
const MAX_OP_BUFFERS: usize = 6;

/// What names a sequence of ops: its length, and four bits an op.
fn key(ops: &[Op]) -> u128 {
    let mut key = ops.len() as u128;
    for (at, op) in ops.iter().enumerate() {
        key |= (op.spec as u128 + 1) << (8 + 4 * at);
    }
    key
}

/// Adds one op to a batch's list, checked against its buffers.
pub(crate) fn add(
    kernels: &Kernels,
    ops: &mut Vec<Op>,
    kernel: Kernel,
    [src, dst]: [&Storage; 2],
    n: u64,
    extras: &[&Storage],
) -> Result<(), KernelError> {
    let here = kernels.context.key;
    let elsewhere = [src, dst]
        .into_iter()
        .chain(extras.iter().copied())
        .any(|storage| storage.context.key != here);
    if elsewhere {
        return Err(KernelError::Size("a buffer is on another device".into()));
    }
    let whole = |storage: &Storage| Some((storage.buffer().clone(), storage.size));
    for (params, counts, spec) in kernels
        .plan_parts(kernel, src, dst, n)?
        .into_iter()
        .flatten()
    {
        // Plain bindings first, then the extras: contiguous, so the flattened list is in binding order.
        let mut buffers = [const { None }; MAX_OP_BUFFERS];
        let all = [whole(src), whole(dst), whole(&params)];
        for (slot, buffer) in buffers
            .iter_mut()
            .zip(all.into_iter().chain(extras.iter().map(|e| whole(e))))
        {
            *slot = buffer;
        }
        ops.push(Op {
            kernel,
            spec,
            buffers,
            counts,
        });
    }
    Ok(())
}

impl Chains {
    /// An empty list of ops for a batch, or `None` when this device doesn't run chains.
    pub(crate) fn begin(&self, kernels: &Kernels) -> Option<Vec<Op>> {
        let able = *self.able.get_or_init(|| {
            let context = &kernels.context;
            let copy = kernels.specs[Kernel::Copy.index()];
            context.shaders().chained(&context.device, copy).is_some()
        });
        if !able {
            return None;
        }
        let mut lists = self.lists.lock().unwrap_or_else(PoisonError::into_inner);
        Some(lists.pop().unwrap_or_default())
    }

    /// A chain for this sequence of ops that nothing is running on, or a new one.
    fn chain(&self, kernels: &Kernels, ops: &[Op]) -> Result<Shared, String> {
        let context = &kernels.context;
        let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        let key = key(ops);
        if !kept.contains_key(&key) && kept.len() >= SEQUENCES {
            kept.clear();
        }
        let chains = kept.entry(key).or_default();
        let idle = chains
            .iter()
            .find(|chain| chain.try_lock().is_ok_and(|chain| chain.is_idle()));
        if let Some(idle) = idle {
            return Ok(Arc::clone(idle));
        }
        // Every one is running: one more, or the first, which then waits its turn.
        if let Some(first) = chains.first().filter(|_| chains.len() >= KEPT) {
            return Ok(Arc::clone(first));
        }
        let mut pipelines = Vec::with_capacity(ops.len());
        for op in ops {
            let spec = kernels.specs[op.spec];
            let built = context.shaders().chained(&context.device, spec);
            pipelines.push(built.ok_or_else(|| format!("`{}` can't be built", spec.name))?);
        }
        let lock = &context.submit;
        let chain = Chain::new(&context.device, &context.queue, lock, &pipelines, SETS)
            .map_err(|error| error.to_string())?;
        let chain = Arc::new(Mutex::new(chain));
        context.note_chain(&chain);
        chains.push(Arc::clone(&chain));
        Ok(chain)
    }

    /// Runs one chain's worth of ops, and gives its ticket.
    fn run(&self, kernels: &Kernels, ops: &[Op]) -> Result<Ticket, String> {
        let context = &kernels.context;
        let shared = self.chain(kernels, ops)?;
        let run = {
            let mut chain = shared.lock().unwrap_or_else(PoisonError::into_inner);
            chain.begin();
            for (stage, op) in ops.iter().enumerate() {
                let mut args = [Arg::Bytes(&[]); daedalus_vk_sys::compute::MAX_BINDINGS];
                let mut count = 0;
                for (arg, (buffer, size)) in args.iter_mut().zip(op.buffers.iter().flatten()) {
                    *arg = Arg::Storage {
                        buffer,
                        size: *size,
                    };
                    count += 1;
                }
                chain
                    .dispatch(stage, &args[..count], op.counts)
                    .map_err(|error| format!("{:?}: {error}", op.kernel))?;
                context.count_dispatch();
            }
            // Counts and tables given to the queue to write have to be on the device first.
            context.flush_writes();
            chain.submit().map_err(|error| error.to_string())?
        };
        context.count_submit();
        context.count_pass();
        Ok(chain_ticket(shared, run, Arc::clone(context)))
    }

    /// Submits a batch's ops and gives the ticket for them. Never waits for the device, unless
    /// more batches of one sequence are in flight than chains are kept for it. A failure is
    /// reported as the device's errors are: by the ticket's wait.
    pub(crate) fn submit(&self, kernels: &Kernels, mut ops: Vec<Op>) -> Ticket {
        let context = &kernels.context;
        let mut last = None;
        for part in ops.chunks(MAX_STAGES) {
            match self.run(kernels, part) {
                // Work on a device finishes in the order it was submitted: the last is all.
                Ok(ticket) => last = Some(ticket),
                Err(reason) => {
                    context.errors.record(format!("a kernel batch: {reason}"));
                    break;
                }
            }
        }
        ops.clear();
        self.lists
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(ops);
        last.unwrap_or_else(|| Ticket::done(Arc::clone(context)))
    }
}
