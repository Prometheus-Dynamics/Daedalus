//! A graph input handle: feeds into a per-port queue that the worker drains. A slow body never
//! blocks a feed.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use daedalus_value::{Decision, DropReason, Value};

use crate::PortPolicy;
use crate::stream::Shared;

/// What a feed did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamFeed {
    /// The value was queued.
    Accepted,
    /// The value was queued and an older one gave way, or this one was given up.
    Dropped(DropReason),
    /// The queue is full and the value was not taken.
    Full,
    /// This input was closed.
    Closed,
}

/// One input's counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InputStats {
    pub accepted: u64,
    pub dropped: u64,
    pub queued: u64,
    pub closed: bool,
}

/// The capacity of a stream input with no bounded policy.
const STREAM_INPUT_CAP: u32 = 256;

#[derive(Debug, Default)]
struct InputQueue {
    items: VecDeque<Value>,
    policy: PortPolicy,
    opened_ns: u64,
    closed: bool,
    accepted: u64,
    dropped: u64,
}

/// The shared half of an input handle.
pub(crate) struct InputPort {
    queue: Mutex<InputQueue>,
}

impl InputPort {
    pub(crate) fn new() -> InputPort {
        InputPort {
            queue: Mutex::new(InputQueue::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, InputQueue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn close(&self) {
        self.lock().closed = true;
    }

    pub(crate) fn stats(&self) -> InputStats {
        let queue = self.lock();
        InputStats {
            accepted: queue.accepted,
            dropped: queue.dropped,
            queued: queue.items.len() as u64,
            closed: queue.closed,
        }
    }

    /// The oldest queued value, if any.
    pub(crate) fn pop(&self) -> Option<Value> {
        self.lock().items.pop_front()
    }

    /// Puts a value the graph couldn't take back at the front.
    pub(crate) fn unpop(&self, value: Value) {
        self.lock().items.push_front(value);
    }

    fn feed(&self, value: Value) -> StreamFeed {
        let mut queue = self.lock();
        if queue.closed {
            return StreamFeed::Closed;
        }
        let policy = queue.policy.pressure;
        let len = queue.items.len() as u32;
        match policy.decide(len, STREAM_INPUT_CAP, queue.opened_ns, 0) {
            Decision::Store => {
                queue.items.push_back(value);
                queue.accepted += 1;
                StreamFeed::Accepted
            }
            Decision::Evict(reason) => {
                queue.items.pop_front();
                queue.items.push_back(value);
                queue.accepted += 1;
                queue.dropped += 1;
                StreamFeed::Dropped(reason)
            }
            Decision::Discard(reason) => {
                queue.dropped += 1;
                StreamFeed::Dropped(reason)
            }
            Decision::Refuse => StreamFeed::Full,
        }
    }
}

/// Feeds one input port of a running graph.
#[derive(Clone)]
pub struct GraphInput {
    port: Arc<InputPort>,
    shared: Arc<Shared>,
}

impl GraphInput {
    pub(crate) fn new(port: Arc<InputPort>, shared: Arc<Shared>) -> GraphInput {
        GraphInput { port, shared }
    }

    /// Sets this port's policy, used from the next feed on.
    pub fn set_policy(&self, policy: PortPolicy) {
        self.port.lock().policy = policy;
    }

    /// Queues `value` for the graph.
    pub fn feed(&self, value: Value) -> StreamFeed {
        let outcome = self.port.feed(value);
        if matches!(outcome, StreamFeed::Accepted | StreamFeed::Dropped(_)) {
            self.shared.kick();
        }
        outcome
    }

    /// Closes this port only: later feeds to it are refused, and other ports keep working.
    pub fn close(&self) {
        self.port.close();
    }

    pub fn stats(&self) -> InputStats {
        self.port.stats()
    }
}
