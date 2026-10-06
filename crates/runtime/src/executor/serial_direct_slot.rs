use super::{CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, Executor, NodeHandler};

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
    let start = collect_detailed_metrics.then(|| exec.core.clock.now());
    let bytes = collect_detailed_metrics
        .then(|| {
            exec.core
                .data_size_inspectors
                .estimate_payload_bytes(&payload.inner)
        })
        .flatten();
    if collect_basic_metrics || exec.core.run_config.frame_probe.is_some() {
        payload.enqueued_at = Some(exec.core.clock.now());
    }
    if collect_lifecycle {
        let mut lifecycle =
            DataLifecycleRecord::new(payload.correlation_id, DataLifecycleStage::EdgeEnqueued);
        lifecycle.edge_idx = Some(edge_idx);
        lifecycle.payload = Some(format!("Payload({})", payload.inner.type_key()));
        exec.core.telemetry.record_data_lifecycle(lifecycle);
    }
    let replaced = exec
        .core
        .direct_slots
        .get(edge_idx)
        .and_then(|slot| slot.access(exec.direct_slot_access).put(payload));
    if replaced.is_some() && collect_basic_metrics {
        // The slot keeps the newest payload, as the edge's policy does in a queue.
        let reason = match exec.edges.get(edge_idx).map(|edge| &edge.policy().pressure) {
            Some(daedalus_transport::PressurePolicy::LatestOnly) => {
                super::EdgePressureReason::LatestReplace
            }
            Some(daedalus_transport::PressurePolicy::Coalesce { .. }) => {
                super::EdgePressureReason::CoalesceReplace
            }
            _ => super::EdgePressureReason::DropOldest,
        };
        exec.core
            .telemetry
            .record_edge_pressure_event(edge_idx, reason, 1);
    }
    drop(replaced);
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
                .record_edge_transport_apply_duration(edge_idx, exec.core.clock.elapsed(start));
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
    if let Some(enqueued_at) = payload.enqueued_at {
        let waited = exec.core.clock.elapsed(enqueued_at);
        if collect_basic_metrics {
            exec.core.telemetry.record_edge_wait(edge_idx, waited);
        }
        if let Some(probe) = &exec.core.run_config.frame_probe {
            probe.record_queue_wait(edge_idx, waited);
        }
    }
    if collect_detailed_metrics {
        exec.core.telemetry.record_edge_depth(edge_idx, 0);
        exec.core.telemetry.record_edge_queue_bytes(edge_idx, 0);
    }
    Some(payload)
}
