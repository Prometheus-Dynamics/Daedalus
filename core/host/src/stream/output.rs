//! A graph output handle: try_recv, recv_timeout and subscribe. The worker pushes here after each
//! tick.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use daedalus_value::{Decision, Overflow, Pressure, Value};

/// One output's counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OutputStats {
    pub delivered: u64,
    pub dropped: u64,
    pub queued: u64,
}

/// A stream output's default: a bounded queue that keeps the newest.
const STREAM_OUTPUT_POLICY: Pressure = Pressure::Bounded {
    cap: 256,
    overflow: Overflow::DropOldest,
};

fn copy_of(value: &Value) -> Value {
    Value {
        ty: value.ty,
        space: value.space,
        owner: value.owner.clone(),
    }
}

#[derive(Debug, Default)]
struct OutputQueue {
    items: VecDeque<Value>,
    subscribers: std::vec::Vec<Arc<Mutex<VecDeque<Value>>>>,
    delivered: u64,
    dropped: u64,
}

/// The shared half of an output handle.
pub(crate) struct OutputPort {
    queue: Mutex<OutputQueue>,
    ready: Condvar,
    policy: Mutex<Pressure>,
}

impl OutputPort {
    pub(crate) fn new() -> OutputPort {
        OutputPort {
            queue: Mutex::new(OutputQueue::default()),
            ready: Condvar::new(),
            policy: Mutex::new(STREAM_OUTPUT_POLICY),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, OutputQueue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn stats(&self) -> OutputStats {
        let queue = self.lock();
        OutputStats {
            delivered: queue.delivered,
            dropped: queue.dropped,
            queued: queue.items.len() as u64,
        }
    }

    /// Delivers a value to the receiver and every subscriber.
    pub(crate) fn push(&self, value: Value) {
        let policy = *self.policy.lock().unwrap_or_else(PoisonError::into_inner);
        let mut queue = self.lock();
        for subscriber in &queue.subscribers {
            subscriber
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push_back(copy_of(&value));
        }
        let cap = u32::MAX;
        match policy.decide(queue.items.len() as u32, cap, 0, 0) {
            Decision::Store => queue.items.push_back(value),
            Decision::Evict(_) => {
                queue.items.pop_front();
                queue.dropped += 1;
                queue.items.push_back(value);
            }
            Decision::Discard(_) | Decision::Refuse => queue.dropped += 1,
        }
        queue.delivered += 1;
        drop(queue);
        self.ready.notify_all();
    }
}

/// Reads one output port of a running graph.
#[derive(Clone)]
pub struct GraphOutput {
    port: Arc<OutputPort>,
}

impl GraphOutput {
    pub(crate) fn new(port: Arc<OutputPort>) -> GraphOutput {
        GraphOutput { port }
    }

    /// The oldest value waiting, if any.
    pub fn try_recv(&self) -> Option<Value> {
        self.port.lock().items.pop_front()
    }

    /// Waits up to `timeout` for a value.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<Value> {
        let queue = self.port.lock();
        let (mut queue, _) = self
            .port
            .ready
            .wait_timeout_while(queue, timeout, |queue| queue.items.is_empty())
            .unwrap_or_else(PoisonError::into_inner);
        queue.items.pop_front()
    }

    /// A subscription that gets a handle to every value delivered after this call.
    pub fn subscribe(&self) -> OutputSubscription {
        let inbox = Arc::new(Mutex::new(VecDeque::new()));
        self.port.lock().subscribers.push(Arc::clone(&inbox));
        OutputSubscription { inbox }
    }

    pub fn stats(&self) -> OutputStats {
        self.port.stats()
    }
}

/// A subscriber's own queue.
pub struct OutputSubscription {
    inbox: Arc<Mutex<VecDeque<Value>>>,
}

impl OutputSubscription {
    /// The next value delivered to this subscription, if any.
    pub fn try_recv(&self) -> Option<Value> {
        self.inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }
}
