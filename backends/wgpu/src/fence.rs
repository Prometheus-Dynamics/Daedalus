//! A submission, as something to poll. Polling never blocks.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use daedalus_abi::{Fence, FencePoll, FenceWord};

use crate::context::Context;
use crate::timer::{PassTimer, TimeSource};
use crate::wait::WaitError;

/// What a submission's callback and its pollers share.
pub(crate) struct TicketState {
    done: AtomicBool,
    started: Instant,
    /// Nanoseconds from `started` to the device saying it was done.
    took: AtomicU64,
}

impl TicketState {
    pub(crate) fn new() -> Arc<TicketState> {
        Arc::new(TicketState {
            done: AtomicBool::new(false),
            started: Instant::now(),
            took: AtomicU64::new(0),
        })
    }

    /// Called by the device when the submission has finished.
    pub(crate) fn finish(&self) {
        let took = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.took.store(took, Ordering::Relaxed);
        self.done.store(true, Ordering::Release);
    }

    pub(crate) fn finished(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}

/// One submission on a device. Done when the device has finished its work.
pub struct Ticket {
    state: Arc<TicketState>,
    context: Arc<Context>,
    timer: Option<Arc<PassTimer>>,
}

impl Ticket {
    pub(crate) fn new(
        state: Arc<TicketState>,
        context: Arc<Context>,
        timer: Option<Arc<PassTimer>>,
    ) -> Ticket {
        Ticket {
            state,
            context,
            timer,
        }
    }

    /// Whether the work has finished. Lets the device run its callbacks, and never blocks.
    pub fn is_done(&self) -> bool {
        if self.state.finished() {
            return true;
        }
        self.context.poll();
        self.state.finished()
    }

    /// Waits for the work, up to the device's wait bound. For setup, shutdown and tests: a step
    /// polls. A timeout marks the device lost.
    pub fn wait(&self) -> Result<(), WaitError> {
        if !self.is_done() {
            self.context.wait("a submission")?;
            if !self.state.finished() {
                // The device went idle without calling back, which only a lost device does.
                return Err(self.context.timed_out("a submission"));
            }
        }
        match self.context.errors.take() {
            Some(text) => Err(WaitError::Device(text)),
            None => Ok(()),
        }
    }

    /// How long the device took, once the work is done, and how that was measured. With
    /// timestamps it is the pass alone. Without, it is from submit to done, which includes
    /// waiting behind earlier work. `None` until done, and never blocks.
    pub fn gpu_time(&self) -> Option<(Duration, TimeSource)> {
        if !self.is_done() {
            return None;
        }
        if let Some(measured) = self
            .timer
            .as_ref()
            .and_then(|timer| timer.read(&self.context))
        {
            return Some((measured, TimeSource::Timestamps));
        }
        let took = Duration::from_nanos(self.state.took.load(Ordering::Relaxed));
        Some((took, TimeSource::SubmitToDone))
    }

    /// The fence a step returns in `StepEnd::Pending`, for the runner to poll each tick. It is
    /// ready when the work is done, and failed if the device reported an error meanwhile or was
    /// lost.
    pub fn into_fence(self) -> Fence {
        let word = NEXT_WORD.fetch_add(1, Ordering::Relaxed);
        let waiting = Waiting {
            state: self.state,
            context: Arc::downgrade(&self.context),
        };
        table().insert(word, waiting);
        Fence::of::<Polled>(word)
    }
}

/// A fence carries one word, so what it stands for is kept here until it resolves.
struct Waiting {
    state: Arc<TicketState>,
    context: Weak<Context>,
}

static NEXT_WORD: AtomicUsize = AtomicUsize::new(1);
static TABLE: Mutex<Option<HashMap<usize, Waiting>>> = Mutex::new(None);

struct Table(std::sync::MutexGuard<'static, Option<HashMap<usize, Waiting>>>);

fn table() -> Table {
    Table(TABLE.lock().unwrap_or_else(PoisonError::into_inner))
}

impl Table {
    fn insert(&mut self, word: usize, waiting: Waiting) {
        self.0.get_or_insert_default().insert(word, waiting);
    }
}

pub(crate) struct Polled;

impl FenceWord for Polled {
    fn poll(word: usize) -> FencePoll {
        let mut table = table();
        let Some(map) = table.0.as_mut() else {
            return FencePoll::Failed;
        };
        let Some(waiting) = map.get(&word) else {
            // Already resolved, or never issued.
            return FencePoll::Failed;
        };
        let Some(context) = waiting.context.upgrade() else {
            map.remove(&word);
            return FencePoll::Failed;
        };
        if !waiting.state.finished() {
            context.poll();
        }
        if context.is_lost() || context.errors.pending() {
            map.remove(&word);
            return FencePoll::Failed;
        }
        if waiting.state.finished() {
            map.remove(&word);
            return FencePoll::Ready;
        }
        FencePoll::Pending
    }
}

/// How many fences are still waiting. For tests of the table's bookkeeping.
#[cfg(test)]
pub(crate) fn waiting() -> usize {
    table().0.as_ref().map_or(0, HashMap::len)
}
