//! Edge policy application: backpressure, overflow handling and pressure telemetry.

use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Instant;

#[cfg(feature = "lockfree-queues")]
use crossbeam_queue::ArrayQueue;

use crate::plan::{BackpressureStrategy, RuntimeEdgePolicy};
use daedalus_transport::{OverflowPolicy, PressurePolicy};

use crate::executor::{
    CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, EdgePressureReason,
    ExecutionTelemetry, NodeError, RuntimeDataSizeInspectors,
};

use super::{EdgeStorage, payload_size_bytes, queue_transport_bytes};

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

/// Elapsed time for detailed-metrics timers; zero when the timer was not started.
fn elapsed_since(start: Option<Instant>) -> std::time::Duration {
    start.map(|start| start.elapsed()).unwrap_or_default()
}

fn payload_lifecycle_desc(payload: &daedalus_transport::Payload) -> String {
    format!("Payload({})", payload.type_key())
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
    let collect_basic = cfg!(feature = "metrics") && telem.metrics_level.is_basic();
    let apply_start =
        (cfg!(feature = "metrics") && telem.metrics_level.is_detailed()).then(Instant::now);
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
                let mut q = queue.lock();
                q.set_policy(&policy.pressure);
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
                        telem.record_edge_transport_apply_duration(
                            edge_idx,
                            elapsed_since(apply_start),
                        );
                        return Err(NodeError::BackpressureDrop(format!(
                            "edge {edge_idx} overflowed bounded queue"
                        )));
                    }
                    _ => {
                        payload.enqueued_at = collect_basic.then(Instant::now);
                        trace_edge_enqueue(edge_idx, policy, &payload);
                        let mut lifecycle = DataLifecycleRecord::new(
                            payload.correlation_id,
                            DataLifecycleStage::EdgeEnqueued,
                        );
                        lifecycle.edge_idx = Some(edge_idx);
                        lifecycle.payload = payload_desc.clone();
                        telem.record_data_lifecycle(lifecycle);
                        !q.push(&policy.pressure, payload).is_accepted()
                    }
                };
                if dropped {
                    metrics.set_current_bytes(queue_transport_bytes(&q, data_size_inspectors));
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
                            payload.enqueued_at = collect_basic.then(Instant::now);
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
                                elapsed_since(apply_start),
                            );
                            return Err(NodeError::BackpressureDrop(format!(
                                "edge {edge_idx} overflowed bounded lock-free queue"
                            )));
                        } else {
                            payload.enqueued_at = collect_basic.then(Instant::now);
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
                                        elapsed_since(apply_start),
                                    );
                                    return Err(NodeError::BackpressureDrop(format!(
                                        "edge {edge_idx} overflowed bounded lock-free queue"
                                    )));
                                }
                            }
                        }
                    }
                    BackpressureStrategy::None => {
                        payload.enqueued_at = collect_basic.then(Instant::now);
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
        telem.record_edge_transport_apply_duration(edge_idx, elapsed_since(apply_start));
    }
    Ok(())
}

fn record_warning(
    label: &str,
    seen: &Arc<Mutex<std::collections::HashSet<String>>>,
    telem: &mut ExecutionTelemetry,
) {
    if seen.lock().insert(label.to_string()) {
        telem.warnings.push(label.to_string());
    }
}
