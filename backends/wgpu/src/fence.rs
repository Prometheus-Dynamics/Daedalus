//! A submission, as something to poll. Polling never blocks.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use daedalus_abi::{Fence, FencePoll, FenceWord};

use crate::context::Context;
use crate::timer::{PassTimer, TimeSource};
use crate::wait::WaitError;

#[cfg(target_os = "linux")]
pub(crate) type Chain = Mutex<daedalus_vk_sys::chain::Chain>;

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

/// A region's chain, as something its fences name. A fence carries one word and is never told
/// it was dropped, so a chain's fences carry the chain's place here and nothing is kept for
/// each one: making one allocates nothing, and one that is never polled leaves nothing behind.
#[cfg(target_os = "linux")]
pub(crate) struct ChainPlace {
    slot: usize,
    generation: usize,
}

#[cfg(target_os = "linux")]
struct ChainEntry {
    /// Counts the chains that have had this place, so a fence of an earlier one is told apart.
    generation: usize,
    live: Option<(Weak<Chain>, Weak<Context>)>,
}

/// The places a word has room to name.
#[cfg(target_os = "linux")]
const PLACES: usize = 1 << 16;

#[cfg(target_os = "linux")]
static CHAINS: Mutex<Vec<ChainEntry>> = Mutex::new(Vec::new());

#[cfg(target_os = "linux")]
impl ChainPlace {
    /// A place for `chain`, which runs on `context`'s device. `None` when every place is
    /// taken.
    pub(crate) fn of(chain: &Arc<Chain>, context: &Arc<Context>) -> Option<ChainPlace> {
        let mut chains = CHAINS.lock().unwrap_or_else(PoisonError::into_inner);
        let live = Some((Arc::downgrade(chain), Arc::downgrade(context)));
        let slot = match chains.iter().position(|entry| entry.live.is_none()) {
            Some(free) => free,
            None if chains.len() < PLACES => {
                chains.push(ChainEntry {
                    generation: 0,
                    live: None,
                });
                chains.len() - 1
            }
            None => return None,
        };
        chains[slot].live = live;
        Some(ChainPlace {
            slot,
            generation: chains[slot].generation,
        })
    }

    /// The fence for the chain's last run. A region runs again only once its fence is ready,
    /// so the last run is the one the fence is for.
    pub(crate) fn fence(&self) -> Fence {
        Fence::of::<ChainPolled>(self.slot | (self.generation << 16))
    }
}

#[cfg(target_os = "linux")]
impl Drop for ChainPlace {
    fn drop(&mut self) {
        let mut chains = CHAINS.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = chains.get_mut(self.slot) {
            entry.live = None;
            // Wraps where a word has no more room, long after a fence that old is gone.
            entry.generation = (entry.generation + 1) & (usize::MAX >> 16);
        }
    }
}

#[cfg(target_os = "linux")]
struct ChainPolled;

#[cfg(target_os = "linux")]
impl FenceWord for ChainPolled {
    fn poll(word: usize) -> FencePoll {
        let chains = CHAINS.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = chains
            .get(word % PLACES)
            .filter(|entry| entry.generation == word >> 16);
        // A fence of a region that is gone.
        let Some((chain, context)) = entry.and_then(|entry| entry.live.as_ref()) else {
            return FencePoll::Failed;
        };
        let (Some(chain), Some(context)) = (chain.upgrade(), context.upgrade()) else {
            return FencePoll::Failed;
        };
        if context.is_lost() || context.errors.pending() {
            return FencePoll::Failed;
        }
        // A chain that is busy recording isn't asked: its fence is polled again.
        match chain.try_lock() {
            Ok(chain) if chain.is_idle() => FencePoll::Ready,
            _ => FencePoll::Pending,
        }
    }
}

/// How many fences are still waiting. For tests of the table's bookkeeping.
#[cfg(test)]
pub(crate) fn waiting() -> usize {
    table().0.as_ref().map_or(0, HashMap::len)
}
