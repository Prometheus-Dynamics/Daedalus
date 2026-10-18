//! Edge policy application: backpressure, overflow handling and pressure telemetry.

use crate::portable::Arc;
use crate::prelude::*;
use crate::sync::Mutex;
use daedalus_core::platform::{Clock, Instant};

#[cfg(all(feature = "lockfree-queues", target_has_atomic = "ptr"))]
use crossbeam_queue::ArrayQueue;

use crate::plan::{BackpressureStrategy, RuntimeEdgePolicy};
use daedalus_transport::{CoalesceStrategy, OverflowPolicy, PressurePolicy, PushOutcome};

use crate::executor::{
    CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, EdgePressureReason,
    ExecutionTelemetry, NodeError, RuntimeDataSizeInspectors,
};

use super::{EdgeStorage, payload_size_bytes, queue_transport_bytes};

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
fn trace_edge_enqueue(edge_idx: usize, policy: &RuntimeEdgePolicy, payload: &CorrelatedPayload) {
    crate::trace::trace!(
        target: "daedalus_runtime::executor::queue",
        edge_idx,
        policy = ?policy.pressure,
        freshness = ?policy.freshness,
        payload_type = %payload.inner.type_key(),
        correlation_id = payload.correlation_id,
        "edge payload enqueued",
    );
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
fn warn_edge_backpressure(
    edge_idx: usize,
    policy: &RuntimeEdgePolicy,
    strategy: &BackpressureStrategy,
    reason: EdgePressureReason,
    payload_type: &daedalus_transport::TypeKey,
    correlation_id: u64,
) {
    crate::trace::warn!(
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

/// Pressure a push caused, as `(reason, dropped items)`; `None` when it queued without displacing
/// anything. Every evicted item counts, and a refused arrival counts once.
fn push_pressure<T>(
    outcome: &PushOutcome<T>,
    policy: &RuntimeEdgePolicy,
    strategy: &BackpressureStrategy,
) -> Option<(EdgePressureReason, u64)> {
    match outcome {
        PushOutcome::Accepted => None,
        PushOutcome::Replaced { dropped, .. } => Some((
            pressure_reason_for_policy(policy, strategy),
            *dropped as u64,
        )),
        PushOutcome::Rejected(..) => Some((rejected_pressure_reason(policy, strategy), 1)),
    }
}

/// Why a push was refused. A `KeepOldest` coalesce edge refuses arrivals inside its window, which
/// is not a replacement, so it has a reason of its own.
fn rejected_pressure_reason(
    policy: &RuntimeEdgePolicy,
    strategy: &BackpressureStrategy,
) -> EdgePressureReason {
    match &policy.pressure {
        PressurePolicy::Coalesce {
            strategy: CoalesceStrategy::KeepOldest,
            ..
        } => EdgePressureReason::CoalesceReject,
        _ => pressure_reason_for_policy(policy, strategy),
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
fn elapsed_since(clock: &Clock, start: Option<Instant>) -> core::time::Duration {
    start.map(|start| clock.elapsed(start)).unwrap_or_default()
}

fn payload_lifecycle_desc(payload: &daedalus_transport::Payload) -> String {
    format!("Payload({})", payload.type_key())
}
#[cfg(all(feature = "lockfree-queues", target_has_atomic = "ptr"))]
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
    pub warnings_seen: &'a Arc<Mutex<HashSet<String>>>,
    pub telem: &'a mut ExecutionTelemetry,
    pub warning_label: Option<Arc<str>>,
    pub backpressure: BackpressureStrategy,
    pub data_size_inspectors: &'a RuntimeDataSizeInspectors,
    /// Stamp the enqueue time for queue-wait timing even below basic metrics (frame probe).
    pub stamp_enqueue: bool,
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
        stamp_enqueue,
    } = args;
    let collect_basic = cfg!(feature = "metrics") && telem.metrics_level.is_basic();
    let stamp = collect_basic || stamp_enqueue;
    let clock = telem.clock().clone();
    let apply_start =
        (cfg!(feature = "metrics") && telem.metrics_level.is_detailed()).then(|| clock.now());
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
                let displaced = match (policy.bounded_capacity(), &backpressure) {
                    // Runtime-level bounded pressure is nonblocking: keep the queued payload
                    // and reject the incoming one so graph ticks never park on queue capacity.
                    (Some(_), BackpressureStrategy::BoundedQueues) if q.is_full() => {
                        Some((pressure_reason_for_policy(policy, &backpressure), 1))
                    }
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
                        record_edge_warning(
                            warning_label.as_ref(),
                            "bounded_error_edge_",
                            edge_idx,
                            warnings_seen,
                            telem,
                        );
                        telem.record_edge_transport_apply_duration(
                            edge_idx,
                            elapsed_since(&clock, apply_start),
                        );
                        return Err(NodeError::BackpressureDrop(format!(
                            "edge {edge_idx} overflowed bounded queue"
                        )));
                    }
                    _ => {
                        payload.enqueued_at = stamp.then(|| clock.now());
                        trace_edge_enqueue(edge_idx, policy, &payload);
                        let mut lifecycle = DataLifecycleRecord::new(
                            payload.correlation_id,
                            DataLifecycleStage::EdgeEnqueued,
                        );
                        lifecycle.edge_idx = Some(edge_idx);
                        lifecycle.payload = payload_desc.clone();
                        telem.record_data_lifecycle(lifecycle);
                        let outcome = q.push(&policy.pressure, payload);
                        push_pressure(&outcome, policy, &backpressure)
                    }
                };
                if displaced.is_some() {
                    metrics.set_current_bytes(queue_transport_bytes(&q, data_size_inspectors));
                } else {
                    metrics.adjust_bytes(transport_bytes.unwrap_or(0), 0);
                }
                if let Some((reason, dropped)) = displaced {
                    warn_edge_backpressure(
                        edge_idx,
                        policy,
                        &backpressure,
                        reason,
                        &payload_type,
                        correlation_id,
                    );
                    telem.backpressure_events += 1;
                    record_pressure_event(telem, edge_idx, reason, dropped);
                    record_edge_warning(
                        warning_label.as_ref(),
                        "bounded_drop_edge_",
                        edge_idx,
                        warnings_seen,
                        telem,
                    );
                }
                telem.record_edge_depth(edge_idx, q.len());
                let (current_queue_bytes, _) = metrics.snapshot();
                telem.record_edge_queue_bytes(edge_idx, current_queue_bytes);
            }
            #[cfg(all(feature = "lockfree-queues", target_has_atomic = "ptr"))]
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
                            payload.enqueued_at = stamp.then(|| clock.now());
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
                            record_edge_warning(
                                warning_label.as_ref(),
                                "bounded_error_edge_",
                                edge_idx,
                                warnings_seen,
                                telem,
                            );
                            telem.record_edge_transport_apply_duration(
                                edge_idx,
                                elapsed_since(&clock, apply_start),
                            );
                            return Err(NodeError::BackpressureDrop(format!(
                                "edge {edge_idx} overflowed bounded lock-free queue"
                            )));
                        } else {
                            payload.enqueued_at = stamp.then(|| clock.now());
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
                                    record_edge_warning(
                                        warning_label.as_ref(),
                                        "bounded_error_edge_",
                                        edge_idx,
                                        warnings_seen,
                                        telem,
                                    );
                                    telem.record_edge_transport_apply_duration(
                                        edge_idx,
                                        elapsed_since(&clock, apply_start),
                                    );
                                    return Err(NodeError::BackpressureDrop(format!(
                                        "edge {edge_idx} overflowed bounded lock-free queue"
                                    )));
                                }
                            }
                        }
                    }
                    BackpressureStrategy::None => {
                        payload.enqueued_at = stamp.then(|| clock.now());
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
                    record_edge_warning(
                        warning_label.as_ref(),
                        "bounded_drop_edge_",
                        edge_idx,
                        warnings_seen,
                        telem,
                    );
                }
                telem.record_edge_depth(edge_idx, queue.len());
                let (current_queue_bytes, _) = metrics.snapshot();
                telem.record_edge_queue_bytes(edge_idx, current_queue_bytes);
            }
        }
        telem.record_edge_transport_apply_duration(edge_idx, elapsed_since(&clock, apply_start));
    }
    Ok(())
}

/// Record an overflow warning once per label. The label is only allocated the first time it is
/// seen: a repeated overflow costs one lookup under the lock.
fn record_warning(label: &str, seen: &Arc<Mutex<HashSet<String>>>, telem: &mut ExecutionTelemetry) {
    let mut seen = seen.lock();
    if !seen.contains(label) {
        seen.insert(label.to_string());
        telem.warnings.push(label.to_string());
    }
}

/// Record an overflow warning under the edge's label, or `<prefix><edge_idx>` when the edge has
/// none. The fallback is built on the stack, so a repeated overflow allocates nothing.
fn record_edge_warning(
    warning_label: Option<&Arc<str>>,
    prefix: &str,
    edge_idx: usize,
    seen: &Arc<Mutex<HashSet<String>>>,
    telem: &mut ExecutionTelemetry,
) {
    match warning_label {
        Some(label) => record_warning(label, seen, telem),
        None => {
            let fallback = StackLabel::new(prefix, edge_idx);
            record_warning(fallback.as_str(), seen, telem);
        }
    }
}

/// A `<prefix><edge_idx>` label kept in a fixed stack buffer. The longest edge label is the
/// 19-byte prefix plus at most 20 digits, so the buffer never overflows.
struct StackLabel {
    buf: [u8; 48],
    len: usize,
}

impl StackLabel {
    fn new(prefix: &str, edge_idx: usize) -> Self {
        let mut label = Self {
            buf: [0; 48],
            len: 0,
        };
        let _ = core::fmt::Write::write_fmt(&mut label, format_args!("{prefix}{edge_idx}"));
        label
    }

    fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

impl core::fmt::Write for StackLabel {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let end = self.len + s.len();
        self.buf
            .get_mut(self.len..end)
            .ok_or(core::fmt::Error)?
            .copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}
