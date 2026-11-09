//! What a full output slot does to its producer. The planner picks a strategy per edge and the
//! runner reads only the flags on each slot.
//!
//! A blocking edge holds the producer back: the step isn't called, its inputs stay queued, and it
//! is tried again next tick. Nothing is lost, and nothing parks a thread.

use daedalus_value::{Overflow, Pressure};

use crate::error::RunError;
use crate::exec::Env;
use crate::step::StepMeta;
use crate::{Runner, spanned};

/// How an edge's queue behaves at capacity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackpressureStrategy {
    /// The edge follows its own pressure policy.
    #[default]
    None,
    /// The incoming value is rejected and the queue keeps what it has. Each rejection is counted
    /// as an event, and nothing blocks.
    BoundedQueues,
    /// A push into a full queue is an [`RunError::Overflow`].
    ErrorOnOverflow,
}

/// Whether any output of the step is a blocking edge with no room. A merged region is one step, so
/// it is held back when any of its boundary outputs is.
pub(crate) fn output_blocked<'t, E: Env<'t>>(env: &mut E, meta: StepMeta) -> bool {
    spanned(env.table(), meta.outputs).iter().any(|&slot| {
        (slot as usize) < env.slot_count()
            && env.with_queue(slot as usize, |queue, _| queue.blocking && queue.is_full())
    })
}

impl Runner<'_> {
    /// Sets how `slot` behaves at capacity.
    pub fn set_backpressure(
        &mut self,
        slot: u32,
        strategy: BackpressureStrategy,
    ) -> Result<(), RunError> {
        let index = self.queue_index(slot)?;
        let queue = &mut self.queues[index];
        let cap = queue.cap;
        let bounded = |overflow| Pressure::Bounded { cap, overflow };
        match strategy {
            BackpressureStrategy::None => queue.counts_rejects = false,
            BackpressureStrategy::BoundedQueues => {
                queue.pressure = bounded(Overflow::DropNewest);
                queue.counts_rejects = true;
            }
            BackpressureStrategy::ErrorOnOverflow => {
                queue.pressure = bounded(Overflow::ErrorOnFull);
                queue.counts_rejects = false;
            }
        }
        Ok(())
    }

    /// Makes `slot` a blocking edge, or stops it being one.
    pub fn set_blocking(&mut self, slot: u32, blocking: bool) -> Result<(), RunError> {
        let index = self.queue_index(slot)?;
        self.queues[index].blocking = blocking;
        self.any_blocking = self.queues.iter().any(|queue| queue.blocking);
        Ok(())
    }

    /// How many incoming values a bounded edge has rejected.
    pub fn rejected(&self, slot: u32) -> Option<u32> {
        let index = self.queue_index(slot).ok()?;
        Some(self.queues[index].rejected)
    }
}
