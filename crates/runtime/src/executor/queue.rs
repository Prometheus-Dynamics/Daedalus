use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

#[cfg(feature = "lockfree-queues")]
use crossbeam_queue::ArrayQueue;

use crate::plan::RuntimeEdgePolicy;
use daedalus_transport::{OverflowPolicy, PressurePolicy};

use super::{CorrelatedPayload, RuntimeDataSizeInspectors};

mod policy;
mod ring;

pub use policy::{ApplyPolicyOwnedArgs, apply_policy_owned};

use ring::RingBuf;

pub(super) fn payload_size_bytes(
    inspectors: &RuntimeDataSizeInspectors,
    payload: &daedalus_transport::Payload,
) -> Option<u64> {
    inspectors.estimate_payload_bytes(payload)
}

fn lock_edge_queue<'a>(
    queue: &'a Mutex<EdgeQueue>,
    edge_idx: usize,
    operation: &'static str,
) -> MutexGuard<'a, EdgeQueue> {
    match queue.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(
                target: "daedalus_runtime::executor::queue",
                edge_idx,
                operation,
                "edge queue lock poisoned; recovering queued payloads"
            );
            poisoned.into_inner()
        }
    }
}

pub enum EdgeQueue {
    Deque(std::collections::VecDeque<CorrelatedPayload>),
    Bounded { ring: RingBuf },
}

impl Default for EdgeQueue {
    fn default() -> Self {
        EdgeQueue::Deque(std::collections::VecDeque::new())
    }
}

impl EdgeQueue {
    pub(crate) fn pop_front(&mut self) -> Option<CorrelatedPayload> {
        match self {
            EdgeQueue::Deque(d) => d.pop_front(),
            EdgeQueue::Bounded { ring } => ring.pop_front(),
        }
    }

    pub fn ensure_policy(&mut self, policy: &RuntimeEdgePolicy) {
        match policy.bounded_capacity() {
            Some(cap) => match self {
                EdgeQueue::Bounded { ring } => {
                    if ring.cap() != cap {
                        *ring = RingBuf::new(cap);
                    }
                }
                _ => {
                    *self = EdgeQueue::Bounded {
                        ring: RingBuf::new(cap),
                    }
                }
            },
            None => {
                if let EdgeQueue::Bounded { .. } = self {
                    *self = EdgeQueue::Deque(std::collections::VecDeque::new());
                }
            }
        }
    }

    pub fn is_full(&self) -> bool {
        match self {
            EdgeQueue::Deque(_) => false,
            EdgeQueue::Bounded { ring } => ring.is_full(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            EdgeQueue::Deque(d) => d.len(),
            EdgeQueue::Bounded { ring } => ring.len(),
        }
    }

    pub fn capacity(&self) -> Option<usize> {
        match self {
            EdgeQueue::Deque(_) => None,
            EdgeQueue::Bounded { ring } => Some(ring.cap()),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            EdgeQueue::Deque(d) => d.is_empty(),
            EdgeQueue::Bounded { ring } => ring.is_empty(),
        }
    }

    pub fn transport_bytes(&self, inspectors: &RuntimeDataSizeInspectors) -> u64 {
        match self {
            EdgeQueue::Deque(d) => d
                .iter()
                .map(|payload| payload_size_bytes(inspectors, &payload.inner).unwrap_or(0))
                .fold(0u64, u64::saturating_add),
            EdgeQueue::Bounded { ring } => ring.transport_bytes(inspectors),
        }
    }

    pub fn clear(&mut self) {
        match self {
            EdgeQueue::Deque(d) => d.clear(),
            EdgeQueue::Bounded { ring } => ring.clear(),
        }
    }

    pub fn push(&mut self, policy: &RuntimeEdgePolicy, payload: CorrelatedPayload) -> bool {
        match &policy.pressure {
            PressurePolicy::LatestOnly | PressurePolicy::Coalesce { .. } => {
                let dropped = !self.is_empty();
                match self {
                    EdgeQueue::Deque(d) => {
                        d.clear();
                        d.push_back(payload);
                    }
                    EdgeQueue::Bounded { .. } => {
                        *self = EdgeQueue::Deque(std::collections::VecDeque::from([payload]));
                    }
                }
                dropped
            }
            PressurePolicy::DropNewest | PressurePolicy::ErrorOnFull if !self.is_empty() => true,
            PressurePolicy::DropOldest => {
                let dropped = !self.is_empty();
                let _ = self.pop_front();
                match self {
                    EdgeQueue::Deque(d) => d.push_back(payload),
                    EdgeQueue::Bounded { .. } => {
                        *self = EdgeQueue::Deque(std::collections::VecDeque::from([payload]));
                    }
                }
                dropped
            }
            PressurePolicy::Bounded { capacity, overflow } => match self {
                EdgeQueue::Bounded { ring } => {
                    if ring.is_full() {
                        match overflow {
                            OverflowPolicy::DropIncoming
                            | OverflowPolicy::Backpressure
                            | OverflowPolicy::Error => return true,
                            OverflowPolicy::DropOldest => {}
                        }
                    }
                    ring.push_back(payload)
                }
                EdgeQueue::Deque(d) => {
                    let mut ring = RingBuf::new(*capacity);
                    for p in d.drain(..) {
                        ring.push_back(p);
                    }
                    let dropped = if ring.is_full() {
                        match overflow {
                            OverflowPolicy::DropIncoming
                            | OverflowPolicy::Backpressure
                            | OverflowPolicy::Error => true,
                            OverflowPolicy::DropOldest => ring.push_back(payload),
                        }
                    } else {
                        ring.push_back(payload)
                    };
                    *self = EdgeQueue::Bounded { ring };
                    dropped
                }
            },
            PressurePolicy::BufferAll
            | PressurePolicy::DropNewest
            | PressurePolicy::ErrorOnFull => {
                match self {
                    EdgeQueue::Deque(d) => d.push_back(payload),
                    EdgeQueue::Bounded { .. } => {
                        *self = EdgeQueue::Deque(std::collections::VecDeque::from([payload]));
                    }
                }
                false
            }
        }
    }
}

#[cfg(test)]
#[path = "queue_tests.rs"]
mod tests;

#[derive(Default)]
pub struct EdgeStorageMetrics {
    current_queue_bytes: AtomicU64,
    peak_queue_bytes: AtomicU64,
}

impl EdgeStorageMetrics {
    pub(crate) fn set_current_bytes(&self, current_bytes: u64) {
        self.current_queue_bytes
            .store(current_bytes, Ordering::Relaxed);
        self.peak_queue_bytes
            .fetch_max(current_bytes, Ordering::Relaxed);
    }

    pub(crate) fn adjust_bytes(&self, added_bytes: u64, removed_bytes: u64) {
        let mut current = self.current_queue_bytes.load(Ordering::Relaxed);
        loop {
            let next = current
                .saturating_add(added_bytes)
                .saturating_sub(removed_bytes);
            match self.current_queue_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.peak_queue_bytes.fetch_max(next, Ordering::Relaxed);
                    break;
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn snapshot(&self) -> (u64, u64) {
        (
            self.current_queue_bytes.load(Ordering::Relaxed),
            self.peak_queue_bytes.load(Ordering::Relaxed),
        )
    }
}

/// Storage wrapper per edge; allows swapping queue implementations.
pub enum EdgeStorage {
    Locked {
        queue: Arc<Mutex<EdgeQueue>>,
        metrics: Arc<EdgeStorageMetrics>,
    },
    #[cfg(feature = "lockfree-queues")]
    BoundedLf {
        queue: Arc<ArrayQueue<CorrelatedPayload>>,
        metrics: Arc<EdgeStorageMetrics>,
    },
}

pub fn build_queues(plan: &crate::plan::RuntimePlan) -> Vec<EdgeStorage> {
    plan.edges
        .iter()
        .map(|edge| {
            let policy = edge.policy();
            match policy.bounded_capacity() {
                Some(cap) => {
                    let metrics = Arc::new(EdgeStorageMetrics::default());
                    #[cfg(feature = "lockfree-queues")]
                    {
                        if should_use_lockfree_queue(policy) {
                            EdgeStorage::BoundedLf {
                                queue: Arc::new(ArrayQueue::new(cap)),
                                metrics,
                            }
                        } else {
                            EdgeStorage::Locked {
                                queue: Arc::new(Mutex::new(EdgeQueue::Bounded {
                                    ring: RingBuf::new(cap),
                                })),
                                metrics,
                            }
                        }
                    }
                    #[cfg(not(feature = "lockfree-queues"))]
                    {
                        EdgeStorage::Locked {
                            queue: Arc::new(Mutex::new(EdgeQueue::Bounded {
                                ring: RingBuf::new(cap),
                            })),
                            metrics,
                        }
                    }
                }
                _ => EdgeStorage::Locked {
                    queue: Arc::new(Mutex::new(EdgeQueue::default())),
                    metrics: Arc::new(EdgeStorageMetrics::default()),
                },
            }
        })
        .collect()
}

#[cfg(feature = "lockfree-queues")]
fn should_use_lockfree_queue(policy: &crate::plan::RuntimeEdgePolicy) -> bool {
    // Automatic policy for now: lock-free only helps bounded hot edges where the runtime can avoid
    // a mutex in parallel/streaming paths. Unbounded/latest/coalesced edges stay on the normal
    // queue because their semantics need replacement/inspection behavior.
    policy.bounded_capacity().is_some()
}

pub fn pop_edge(
    edge_idx: usize,
    queues: &Arc<Vec<EdgeStorage>>,
    inspectors: &RuntimeDataSizeInspectors,
) -> Option<CorrelatedPayload> {
    let storage = queues.get(edge_idx)?;
    match storage {
        EdgeStorage::Locked { queue, metrics } => {
            let mut guard = lock_edge_queue(queue, edge_idx, "pop");
            let payload = guard.pop_front();
            if let Some(payload) = payload.as_ref() {
                let removed = payload_size_bytes(inspectors, &payload.inner).unwrap_or(0);
                metrics.adjust_bytes(0, removed);
            } else {
                metrics.set_current_bytes(guard.transport_bytes(inspectors));
            }
            payload
        }
        #[cfg(feature = "lockfree-queues")]
        EdgeStorage::BoundedLf { queue, metrics } => {
            let payload = queue.pop();
            if let Some(payload) = payload.as_ref() {
                let removed = payload_size_bytes(inspectors, &payload.inner).unwrap_or(0);
                metrics.adjust_bytes(0, removed);
            }
            payload
        }
    }
}
