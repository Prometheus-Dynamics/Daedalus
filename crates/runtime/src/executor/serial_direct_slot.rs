use super::ExecutorCore;
use super::direct_slot::DirectSlotAccess;
use super::{CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, EdgePressureReason};
use crate::plan::RuntimeEdge;

// The two direct-slot helpers take the executor's core, edge table and slot access rather than
// the whole `Executor<H>`: they never touch the handler, so one body serves every handler type.

pub(crate) fn push_direct_edge(
    core: &mut ExecutorCore,
    edges: &[RuntimeEdge],
    access: DirectSlotAccess,
    edge_idx: usize,
    mut payload: CorrelatedPayload,
) {
    let collect_basic_metrics =
        cfg!(feature = "metrics") && core.run_config.metrics_level.is_basic();
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && core.run_config.metrics_level.is_detailed();
    let collect_lifecycle = cfg!(feature = "metrics")
        && (core.run_config.metrics_level.is_profile() || core.run_config.metrics_level.is_trace());
    let start = collect_detailed_metrics.then(|| core.clock.now());
    let bytes = collect_detailed_metrics
        .then(|| {
            core.data_size_inspectors
                .estimate_payload_bytes(&payload.inner)
        })
        .flatten();
    if collect_detailed_metrics || core.run_config.frame_probe.is_some() {
        payload.enqueued_at = Some(core.clock.now());
    }
    if collect_lifecycle {
        let mut lifecycle =
            DataLifecycleRecord::new(payload.correlation_id, DataLifecycleStage::EdgeEnqueued);
        lifecycle.edge_idx = Some(edge_idx);
        lifecycle.payload = Some(format!("Payload({})", payload.inner.type_key()));
        core.telemetry.record_data_lifecycle(lifecycle);
    }
    let replaced = core
        .direct_slots
        .get(edge_idx)
        .is_some_and(|slot| slot.access(access).put(payload));
    if replaced && collect_basic_metrics {
        let reason = replace_reason(edges.get(edge_idx));
        core.telemetry
            .record_edge_pressure_event(edge_idx, reason, 1);
    }
    if collect_detailed_metrics {
        core.telemetry.record_edge_transport(edge_idx, bytes);
        core.telemetry.record_edge_capacity(edge_idx, Some(1));
        core.telemetry.record_edge_depth(edge_idx, 1);
        core.telemetry
            .record_edge_queue_bytes(edge_idx, bytes.unwrap_or(0));
        if let Some(start) = start {
            let elapsed = core.clock.elapsed(start);
            core.telemetry
                .record_edge_transport_apply_duration(edge_idx, elapsed);
        }
    }
}

/// Why a slot replaced its payload: it keeps the newest, as the edge's policy does in a queue.
pub(crate) fn replace_reason(edge: Option<&crate::plan::RuntimeEdge>) -> EdgePressureReason {
    match edge.map(|edge| &edge.policy().pressure) {
        Some(daedalus_transport::PressurePolicy::LatestOnly) => EdgePressureReason::LatestReplace,
        Some(daedalus_transport::PressurePolicy::Coalesce { .. }) => {
            EdgePressureReason::CoalesceReplace
        }
        _ => EdgePressureReason::DropOldest,
    }
}

#[inline]
pub(crate) fn pop_direct_edge(
    core: &mut ExecutorCore,
    access: DirectSlotAccess,
    edge_idx: usize,
) -> Option<CorrelatedPayload> {
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && core.run_config.metrics_level.is_detailed();
    let payload = core
        .direct_slots
        .get(edge_idx)
        .and_then(|slot| slot.access(access).take())?;
    if let Some(enqueued_at) = payload.enqueued_at {
        let waited = core.clock.elapsed(enqueued_at);
        // Edge waits are detailed metrics, as for queued edges.
        if collect_detailed_metrics {
            core.telemetry.record_edge_wait(edge_idx, waited);
        }
        if let Some(probe) = &core.run_config.frame_probe {
            probe.record_queue_wait(edge_idx, waited);
        }
    }
    if collect_detailed_metrics {
        core.telemetry.record_edge_depth(edge_idx, 0);
        core.telemetry.record_edge_queue_bytes(edge_idx, 0);
    }
    Some(payload)
}
