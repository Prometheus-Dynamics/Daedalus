use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

#[cfg(feature = "lockfree-queues")]
use crossbeam_queue::ArrayQueue;

use crate::plan::{BackpressureStrategy, RuntimeEdgePolicy};
use daedalus_transport::{OverflowPolicy, PressurePolicy};

use super::{
    CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, EdgePressureReason,
    ExecutionTelemetry, NodeError, RuntimeDataSizeInspectors,
};

mod ring;

use ring::RingBuf;

fn trace_edge_enqueue(edge_idx: usize, policy: &RuntimeEdgePolicy, payload: &CorrelatedPayload) {
    tracing::trace!(
        target: "daedalus_runtime::executor::queue",
        edge_idx,
        policy = ?policy.pressure,
        freshness = ?policy.freshness,
        payload_type = %payload.inner.type_key(),
        correlation_id = payload.correlation_id,
        "edge payload enqueued",
    );
}

fn warn_edge_backpressure(
    edge_idx: usize,
    policy: &RuntimeEdgePolicy,
    strategy: &BackpressureStrategy,
    reason: EdgePressureReason,
    payload_type: &daedalus_transport::TypeKey,
    correlation_id: u64,
) {
    tracing::warn!(
        target: "daedalus_runtime::executor::queue",
        edge_idx,
        policy = ?policy.pressure,
        freshness = ?policy.freshness,
        strategy = ?strategy,
        reason = reason.as_str(),
        payload_type = %payload_type,
        correlation_id,
        "edge backpressure",
    );
}

fn pressure_reason_for_policy(
    policy: &RuntimeEdgePolicy,
    strategy: &BackpressureStrategy,
) -> EdgePressureReason {
    match strategy {
        BackpressureStrategy::BoundedQueues => EdgePressureReason::Backpressure,
        BackpressureStrategy::ErrorOnOverflow => EdgePressureReason::ErrorOverflow,
        BackpressureStrategy::None => match &policy.pressure {
            PressurePolicy::LatestOnly => EdgePressureReason::LatestReplace,
            PressurePolicy::Coalesce { .. } => EdgePressureReason::CoalesceReplace,
            PressurePolicy::DropNewest => EdgePressureReason::DropNewest,
            PressurePolicy::DropOldest => EdgePressureReason::DropOldest,
            PressurePolicy::ErrorOnFull => EdgePressureReason::ErrorOverflow,
            PressurePolicy::Bounded { overflow, .. } => match overflow {
                OverflowPolicy::DropIncoming => EdgePressureReason::DropIncoming,
                OverflowPolicy::DropOldest => EdgePressureReason::DropOldest,
                OverflowPolicy::Backpressure => EdgePressureReason::Backpressure,
                OverflowPolicy::Error => EdgePressureReason::ErrorOverflow,
            },
            PressurePolicy::BufferAll => EdgePressureReason::DropIncoming,
        },
    }
}

fn record_pressure_event(
    telem: &mut ExecutionTelemetry,
    edge_idx: usize,
    reason: EdgePressureReason,
    dropped_count: u64,
) {
    telem.record_edge_pressure_event(edge_idx, reason, dropped_count);
}

fn payload_lifecycle_desc(payload: &daedalus_transport::Payload) -> String {
    format!("Payload({})", payload.type_key())
}

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

#[cfg(feature = "lockfree-queues")]
fn push_lockfree_with_policy(
    queue: &ArrayQueue<CorrelatedPayload>,
    policy: &RuntimeEdgePolicy,
    payload: CorrelatedPayload,
    inspectors: &RuntimeDataSizeInspectors,
) -> (bool, u64, Option<CorrelatedPayload>) {
    match queue.push(payload) {
        Ok(()) => (false, 0, None),
        Err(payload) => match &policy.pressure {
            PressurePolicy::Bounded {
                overflow: OverflowPolicy::DropOldest,
                ..
            } => {
                let removed_bytes = queue
                    .pop()
                    .and_then(|removed| payload_size_bytes(inspectors, &removed.inner))
                    .unwrap_or(0);
                match queue.push(payload) {
                    Ok(()) => (true, removed_bytes, None),
                    Err(payload) => (true, removed_bytes, Some(payload)),
                }
            }
            PressurePolicy::Bounded {
                overflow:
                    OverflowPolicy::DropIncoming | OverflowPolicy::Backpressure | OverflowPolicy::Error,
                ..
            } => (true, 0, Some(payload)),
            _ => (true, 0, Some(payload)),
        },
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

pub struct ApplyPolicyOwnedArgs<'a> {
    pub edge_idx: usize,
    pub policy: &'a RuntimeEdgePolicy,
    pub payload: CorrelatedPayload,
    pub queues: &'a Arc<Vec<EdgeStorage>>,
    pub warnings_seen: &'a Arc<Mutex<std::collections::HashSet<String>>>,
    pub telem: &'a mut ExecutionTelemetry,
    pub warning_label: Option<String>,
    pub backpressure: BackpressureStrategy,
    pub data_size_inspectors: &'a RuntimeDataSizeInspectors,
}

pub fn apply_policy_owned(args: ApplyPolicyOwnedArgs<'_>) -> Result<(), NodeError> {
    let ApplyPolicyOwnedArgs {
        edge_idx,
        policy,
        mut payload,
        queues,
        warnings_seen,
        telem,
        warning_label,
        backpressure,
        data_size_inspectors,
    } = args;
    let apply_start = Instant::now();
    if let Some(storage) = queues.get(edge_idx) {
        let transport_bytes = if cfg!(feature = "metrics") && telem.metrics_level.is_detailed() {
            payload_size_bytes(data_size_inspectors, &payload.inner)
        } else {
            None
        };
        let payload_desc = if cfg!(feature = "metrics") && telem.metrics_level.is_profile() {
            Some(payload_lifecycle_desc(&payload.inner))
        } else {
            None
        };
        telem.record_edge_transport(edge_idx, transport_bytes);
        match storage {
            EdgeStorage::Locked { queue, metrics } => {
                let mut q = lock_edge_queue(queue, edge_idx, "apply_policy");
                q.ensure_policy(policy);
                telem.record_edge_capacity(edge_idx, q.capacity());
                let payload_type = payload.inner.type_key().clone();
                let correlation_id = payload.correlation_id;
                let dropped = match (policy.bounded_capacity(), &backpressure) {
                    // Runtime-level bounded pressure is nonblocking: keep the queued payload
                    // and reject the incoming one so graph ticks never park on queue capacity.
                    (Some(_), BackpressureStrategy::BoundedQueues) if q.is_full() => true,
                    (Some(_), BackpressureStrategy::ErrorOnOverflow) if q.is_full() => {
                        let reason = EdgePressureReason::ErrorOverflow;
                        warn_edge_backpressure(
                            edge_idx,
                            policy,
                            &backpressure,
                            reason,
                            &payload_type,
                            correlation_id,
                        );
                        record_pressure_event(telem, edge_idx, reason, 0);
                        telem.backpressure_events += 1;
                        let label = warning_label
                            .clone()
                            .unwrap_or_else(|| format!("bounded_error_edge_{edge_idx}"));
                        record_warning(&label, warnings_seen, telem);
                        telem.record_edge_transport_apply_duration(edge_idx, apply_start.elapsed());
                        return Err(NodeError::BackpressureDrop(format!(
                            "edge {edge_idx} overflowed bounded queue"
                        )));
                    }
                    _ => {
                        payload.enqueued_at = Instant::now();
                        trace_edge_enqueue(edge_idx, policy, &payload);
                        let mut lifecycle = DataLifecycleRecord::new(
                            payload.correlation_id,
                            DataLifecycleStage::EdgeEnqueued,
                        );
                        lifecycle.edge_idx = Some(edge_idx);
                        lifecycle.payload = payload_desc.clone();
                        telem.record_data_lifecycle(lifecycle);
                        q.push(policy, payload)
                    }
                };
                if dropped {
                    metrics.set_current_bytes(q.transport_bytes(data_size_inspectors));
                } else {
                    metrics.adjust_bytes(transport_bytes.unwrap_or(0), 0);
                }
                if dropped {
                    warn_edge_backpressure(
                        edge_idx,
                        policy,
                        &backpressure,
                        pressure_reason_for_policy(policy, &backpressure),
                        &payload_type,
                        correlation_id,
                    );
                    telem.backpressure_events += 1;
                    record_pressure_event(
                        telem,
                        edge_idx,
                        pressure_reason_for_policy(policy, &backpressure),
                        1,
                    );
                    let label = warning_label
                        .clone()
                        .unwrap_or_else(|| format!("bounded_drop_edge_{edge_idx}"));
                    record_warning(&label, warnings_seen, telem);
                }
                telem.record_edge_depth(edge_idx, q.len());
                let (current_queue_bytes, _) = metrics.snapshot();
                telem.record_edge_queue_bytes(edge_idx, current_queue_bytes);
            }
            #[cfg(feature = "lockfree-queues")]
            EdgeStorage::BoundedLf { queue, metrics } => {
                let mut dropped = false;
                let mut pressure_reason = None;
                telem.record_edge_capacity(edge_idx, Some(queue.capacity()));
                let added_bytes = transport_bytes.unwrap_or(0);
                match backpressure {
                    BackpressureStrategy::BoundedQueues => {
                        if queue.is_full() {
                            let reason = EdgePressureReason::Backpressure;
                            warn_edge_backpressure(
                                edge_idx,
                                policy,
                                &backpressure,
                                reason,
                                payload.inner.type_key(),
                                payload.correlation_id,
                            );
                            dropped = true;
                            pressure_reason = Some(reason);
                        } else {
                            payload.enqueued_at = Instant::now();
                            trace_edge_enqueue(edge_idx, policy, &payload);
                            let mut lifecycle = DataLifecycleRecord::new(
                                payload.correlation_id,
                                DataLifecycleStage::EdgeEnqueued,
                            );
                            lifecycle.edge_idx = Some(edge_idx);
                            lifecycle.payload = payload_desc.clone();
                            telem.record_data_lifecycle(lifecycle);
                            match queue.push(payload) {
                                Ok(()) => metrics.adjust_bytes(added_bytes, 0),
                                Err(payload) => {
                                    let reason = EdgePressureReason::Backpressure;
                                    warn_edge_backpressure(
                                        edge_idx,
                                        policy,
                                        &backpressure,
                                        reason,
                                        payload.inner.type_key(),
                                        payload.correlation_id,
                                    );
                                    dropped = true;
                                    pressure_reason = Some(reason);
                                }
                            }
                        }
                    }
                    BackpressureStrategy::ErrorOnOverflow => {
                        if queue.is_full() {
                            let reason = EdgePressureReason::ErrorOverflow;
                            warn_edge_backpressure(
                                edge_idx,
                                policy,
                                &backpressure,
                                reason,
                                payload.inner.type_key(),
                                payload.correlation_id,
                            );
                            record_pressure_event(telem, edge_idx, reason, 0);
                            telem.backpressure_events += 1;
                            let label = warning_label
                                .clone()
                                .unwrap_or_else(|| format!("bounded_error_edge_{edge_idx}"));
                            record_warning(&label, warnings_seen, telem);
                            telem.record_edge_transport_apply_duration(
                                edge_idx,
                                apply_start.elapsed(),
                            );
                            return Err(NodeError::BackpressureDrop(format!(
                                "edge {edge_idx} overflowed bounded lock-free queue"
                            )));
                        } else {
                            payload.enqueued_at = Instant::now();
                            trace_edge_enqueue(edge_idx, policy, &payload);
                            let mut lifecycle = DataLifecycleRecord::new(
                                payload.correlation_id,
                                DataLifecycleStage::EdgeEnqueued,
                            );
                            lifecycle.edge_idx = Some(edge_idx);
                            lifecycle.payload = payload_desc.clone();
                            telem.record_data_lifecycle(lifecycle);
                            match queue.push(payload) {
                                Ok(()) => metrics.adjust_bytes(added_bytes, 0),
                                Err(payload) => {
                                    let reason = EdgePressureReason::ErrorOverflow;
                                    warn_edge_backpressure(
                                        edge_idx,
                                        policy,
                                        &backpressure,
                                        reason,
                                        payload.inner.type_key(),
                                        payload.correlation_id,
                                    );
                                    record_pressure_event(telem, edge_idx, reason, 0);
                                    telem.backpressure_events += 1;
                                    let label = warning_label.clone().unwrap_or_else(|| {
                                        format!("bounded_error_edge_{edge_idx}")
                                    });
                                    record_warning(&label, warnings_seen, telem);
                                    telem.record_edge_transport_apply_duration(
                                        edge_idx,
                                        apply_start.elapsed(),
                                    );
                                    return Err(NodeError::BackpressureDrop(format!(
                                        "edge {edge_idx} overflowed bounded lock-free queue"
                                    )));
                                }
                            }
                        }
                    }
                    BackpressureStrategy::None => {
                        payload.enqueued_at = Instant::now();
                        trace_edge_enqueue(edge_idx, policy, &payload);
                        let payload_type = payload.inner.type_key().clone();
                        let correlation_id = payload.correlation_id;
                        let mut lifecycle = DataLifecycleRecord::new(
                            payload.correlation_id,
                            DataLifecycleStage::EdgeEnqueued,
                        );
                        lifecycle.edge_idx = Some(edge_idx);
                        lifecycle.payload = payload_desc.clone();
                        telem.record_data_lifecycle(lifecycle);
                        let rejected;
                        let removed_bytes;
                        (dropped, removed_bytes, rejected) =
                            push_lockfree_with_policy(queue, policy, payload, data_size_inspectors);
                        if dropped {
                            let reason = pressure_reason_for_policy(policy, &backpressure);
                            warn_edge_backpressure(
                                edge_idx,
                                policy,
                                &backpressure,
                                reason,
                                &payload_type,
                                correlation_id,
                            );
                            pressure_reason = Some(reason);
                            if rejected.is_none() {
                                metrics.adjust_bytes(added_bytes, removed_bytes);
                            }
                        } else {
                            metrics.adjust_bytes(added_bytes, 0);
                        }
                    }
                }
                if dropped {
                    telem.backpressure_events += 1;
                    record_pressure_event(
                        telem,
                        edge_idx,
                        pressure_reason
                            .unwrap_or_else(|| pressure_reason_for_policy(policy, &backpressure)),
                        1,
                    );
                    let label = warning_label
                        .clone()
                        .unwrap_or_else(|| format!("bounded_drop_edge_{edge_idx}"));
                    record_warning(&label, warnings_seen, telem);
                }
                telem.record_edge_depth(edge_idx, queue.len());
                let (current_queue_bytes, _) = metrics.snapshot();
                telem.record_edge_queue_bytes(edge_idx, current_queue_bytes);
            }
        }
        telem.record_edge_transport_apply_duration(edge_idx, apply_start.elapsed());
    }
    Ok(())
}

fn record_warning(
    label: &str,
    seen: &Arc<Mutex<std::collections::HashSet<String>>>,
    telem: &mut ExecutionTelemetry,
) {
    match seen.lock() {
        Ok(mut s) => {
            if s.insert(label.to_string()) {
                telem.warnings.push(label.to_string());
            }
        }
        Err(_) => {
            tracing::warn!(
                target: "daedalus_runtime::executor::queue",
                warning = label,
                "warning de-duplication lock poisoned"
            );
            telem.warnings.push(label.to_string());
        }
    }
}
