use crate::sync::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "lockfree-queues")]
use crossbeam_queue::ArrayQueue;

use daedalus_transport::PolicyQueue;

use super::{CorrelatedPayload, RuntimeDataSizeInspectors};

mod policy;

pub use policy::{ApplyPolicyOwnedArgs, apply_policy_owned};

pub(super) fn payload_size_bytes(
    inspectors: &RuntimeDataSizeInspectors,
    payload: &daedalus_transport::Payload,
) -> Option<u64> {
    inspectors.estimate_payload_bytes(payload)
}

/// Per-edge queue guarded by [`EdgeStorage::Locked`].
pub type EdgeQueue = PolicyQueue<CorrelatedPayload>;

/// Total estimated transport bytes held by `queue`.
pub(super) fn queue_transport_bytes(
    queue: &EdgeQueue,
    inspectors: &RuntimeDataSizeInspectors,
) -> u64 {
    queue
        .iter()
        .map(|payload| payload_size_bytes(inspectors, &payload.inner).unwrap_or(0))
        .fold(0u64, u64::saturating_add)
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
            let metrics = Arc::new(EdgeStorageMetrics::default());
            // Lock-free queues only help bounded hot edges where the runtime can avoid a mutex in
            // parallel/streaming paths. Unbounded/latest/coalesced edges stay on the locked queue
            // because their semantics need replacement/inspection behavior.
            #[cfg(feature = "lockfree-queues")]
            if let Some(cap) = policy.bounded_capacity() {
                return EdgeStorage::BoundedLf {
                    queue: Arc::new(ArrayQueue::new(cap)),
                    metrics,
                };
            }
            let mut queue = EdgeQueue::default();
            queue.set_policy(&policy.pressure);
            EdgeStorage::Locked {
                queue: Arc::new(Mutex::new(queue)),
                metrics,
            }
        })
        .collect()
}

/// Whether edge `edge_idx` holds a payload, without popping it.
pub(crate) fn edge_has_payload(edge_idx: usize, queues: &[EdgeStorage]) -> bool {
    match queues.get(edge_idx) {
        Some(EdgeStorage::Locked { queue, .. }) => !queue.lock().is_empty(),
        #[cfg(feature = "lockfree-queues")]
        Some(EdgeStorage::BoundedLf { queue, .. }) => !queue.is_empty(),
        None => false,
    }
}

pub fn pop_edge(
    edge_idx: usize,
    queues: &Arc<Vec<EdgeStorage>>,
    inspectors: &RuntimeDataSizeInspectors,
) -> Option<CorrelatedPayload> {
    let storage = queues.get(edge_idx)?;
    match storage {
        EdgeStorage::Locked { queue, metrics } => {
            let mut guard = queue.lock();
            let payload = guard.pop_front();
            if let Some(payload) = payload.as_ref() {
                let removed = payload_size_bytes(inspectors, &payload.inner).unwrap_or(0);
                metrics.adjust_bytes(0, removed);
            } else {
                metrics.set_current_bytes(queue_transport_bytes(&guard, inspectors));
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
