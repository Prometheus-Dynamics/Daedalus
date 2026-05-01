use super::{CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, Executor, NodeHandler};
use std::time::Instant;

pub(crate) fn push_direct_edge<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    edge_idx: usize,
    mut payload: CorrelatedPayload,
) {
    let collect_basic_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_basic();
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed();
    let collect_lifecycle = cfg!(feature = "metrics")
        && (exec.core.run_config.metrics_level.is_profile()
            || exec.core.run_config.metrics_level.is_trace());
    let start = collect_detailed_metrics.then(Instant::now);
    let bytes = collect_detailed_metrics
        .then(|| {
            exec.core
                .data_size_inspectors
                .estimate_payload_bytes(&payload.inner)
        })
        .flatten();
    if collect_basic_metrics {
        payload.enqueued_at = Instant::now();
    }
    if collect_lifecycle {
        let mut lifecycle =
            DataLifecycleRecord::new(payload.correlation_id, DataLifecycleStage::EdgeEnqueued);
        lifecycle.edge_idx = Some(edge_idx);
        lifecycle.payload = Some(format!("Payload({})", payload.inner.type_key()));
        exec.core.telemetry.record_data_lifecycle(lifecycle);
    }
    if let Some(slot) = exec.core.direct_slots.get(edge_idx) {
        slot.access(exec.direct_slot_access).put(payload);
    }
    if collect_detailed_metrics {
        exec.core.telemetry.record_edge_transport(edge_idx, bytes);
        exec.core.telemetry.record_edge_capacity(edge_idx, Some(1));
        exec.core.telemetry.record_edge_depth(edge_idx, 1);
        exec.core
            .telemetry
            .record_edge_queue_bytes(edge_idx, bytes.unwrap_or(0));
        if let Some(start) = start {
            exec.core
                .telemetry
                .record_edge_transport_apply_duration(edge_idx, start.elapsed());
        }
    }
}

pub(crate) fn pop_direct_edge<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    edge_idx: usize,
) -> Option<CorrelatedPayload> {
    let collect_basic_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_basic();
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed();
    let payload = exec
        .core
        .direct_slots
        .get(edge_idx)
        .and_then(|slot| slot.access(exec.direct_slot_access).take())?;
    if collect_basic_metrics {
        exec.core.telemetry.record_edge_wait(
            edge_idx,
            Instant::now().saturating_duration_since(payload.enqueued_at),
        );
    }
    if collect_detailed_metrics {
        exec.core.telemetry.record_edge_depth(edge_idx, 0);
        exec.core.telemetry.record_edge_queue_bytes(edge_idx, 0);
    }
    Some(payload)
}
