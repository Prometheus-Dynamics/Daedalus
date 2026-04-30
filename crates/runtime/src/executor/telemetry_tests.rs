#[cfg(feature = "metrics")]
use super::{ExecutionTelemetry, MetricsLevel};
#[cfg(feature = "metrics")]
use std::time::Duration;

#[test]
#[cfg(feature = "metrics")]
fn record_node_resource_snapshot_tracks_current_and_peak_usage() {
    let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
    telemetry.record_node_resource_snapshot(
        1,
        crate::state::NodeResourceSnapshot {
            frame_scratch: crate::state::ResourceUsage {
                live_bytes: 32,
                retained_bytes: 64,
                touched_bytes: 48,
                allocation_events: 2,
            },
            warm_cache: crate::state::ResourceUsage {
                live_bytes: 10,
                retained_bytes: 20,
                touched_bytes: 14,
                allocation_events: 1,
            },
            persistent_state: crate::state::ResourceUsage::default(),
        },
    );
    telemetry.record_node_resource_snapshot(
        1,
        crate::state::NodeResourceSnapshot {
            frame_scratch: crate::state::ResourceUsage {
                live_bytes: 16,
                retained_bytes: 96,
                touched_bytes: 18,
                allocation_events: 1,
            },
            warm_cache: crate::state::ResourceUsage::default(),
            persistent_state: crate::state::ResourceUsage {
                live_bytes: 5,
                retained_bytes: 5,
                touched_bytes: 5,
                allocation_events: 0,
            },
        },
    );

    let metrics = telemetry.node_metrics.get(&1).unwrap();
    let resources = metrics.resources.as_ref().unwrap();
    assert_eq!(resources.frame_scratch.current_live_bytes, 16);
    assert_eq!(resources.frame_scratch.current_retained_bytes, 96);
    assert_eq!(resources.frame_scratch.current_touched_bytes, 18);
    assert_eq!(resources.frame_scratch.peak_live_bytes, 32);
    assert_eq!(resources.frame_scratch.peak_retained_bytes, 96);
    assert_eq!(resources.frame_scratch.peak_touched_bytes, 48);
    assert_eq!(resources.frame_scratch.current_allocation_events, 1);
    assert_eq!(resources.frame_scratch.peak_allocation_events, 2);
    assert_eq!(resources.persistent_state.current_live_bytes, 5);
    assert_eq!(resources.warm_cache.peak_retained_bytes, 20);
    assert_eq!(resources.warm_cache.peak_touched_bytes, 14);
}

#[test]
#[cfg(feature = "metrics")]
fn record_node_internal_transfers_tracks_materialization_conversion_and_gpu_flow() {
    let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
    telemetry.record_node_materialization(3, 64);
    telemetry.record_node_conversion(3, 24);
    telemetry.record_node_gpu_transfer(3, true, 48);
    telemetry.record_node_gpu_transfer(3, false, 32);

    let metrics = telemetry.node_metrics.get(&3).unwrap();
    let resources = metrics.resources.as_ref().unwrap();
    assert_eq!(resources.materialization.count, 1);
    assert_eq!(resources.materialization.total_bytes, 64);
    assert_eq!(resources.conversion.count, 1);
    assert_eq!(resources.conversion.total_bytes, 24);
    assert_eq!(resources.gpu_upload.count, 1);
    assert_eq!(resources.gpu_upload.total_bytes, 48);
    assert_eq!(resources.gpu_download.count, 1);
    assert_eq!(resources.gpu_download.total_bytes, 32);
}

#[test]
#[cfg(feature = "metrics")]
fn explain_node_allocation_spike_reports_dominant_sources() {
    let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
    telemetry.record_node_resource_snapshot(
        2,
        crate::state::NodeResourceSnapshot {
            frame_scratch: crate::state::ResourceUsage {
                live_bytes: 24,
                retained_bytes: 80,
                touched_bytes: 40,
                allocation_events: 2,
            },
            warm_cache: crate::state::ResourceUsage {
                live_bytes: 12,
                retained_bytes: 48,
                touched_bytes: 18,
                allocation_events: 1,
            },
            persistent_state: crate::state::ResourceUsage::default(),
        },
    );
    telemetry.record_node_materialization(2, 96);
    telemetry.record_node_conversion(2, 24);

    let explanation = telemetry
        .explain_node_allocation_spike(2)
        .expect("allocation explanation");
    assert_eq!(explanation.node_idx, 2);
    assert_eq!(explanation.frame_scratch.peak_retained_bytes, 80);
    assert_eq!(explanation.materialization.total_bytes, 96);
    assert_eq!(explanation.conversion.total_bytes, 24);
    assert_eq!(explanation.dominant_sources[0], "materialization:96");
    assert!(
        explanation
            .dominant_sources
            .iter()
            .any(|entry| entry == "frame_scratch:80")
    );
}

#[test]
#[cfg(feature = "metrics")]
fn reset_for_reuse_clears_accumulated_state() {
    let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
    telemetry.nodes_executed = 3;
    telemetry.cpu_segments = 1;
    telemetry.gpu_segments = 2;
    telemetry.gpu_fallbacks = 1;
    telemetry.backpressure_events = 4;
    telemetry.warnings.push("warn".to_string());
    telemetry.errors.push(super::NodeFailure {
        node_idx: 1,
        node_id: "node".to_string(),
        code: "code".to_string(),
        message: "message".to_string(),
    });
    telemetry.graph_duration = Duration::from_millis(12);
    telemetry.record_node_materialization(3, 64);
    telemetry.record_edge_drop(2, 1);
    telemetry.record_trace_event(1, Duration::default(), Duration::from_nanos(5));

    telemetry.reset_for_reuse(MetricsLevel::Basic);

    assert_eq!(telemetry.nodes_executed, 0);
    assert_eq!(telemetry.cpu_segments, 0);
    assert_eq!(telemetry.gpu_segments, 0);
    assert_eq!(telemetry.gpu_fallbacks, 0);
    assert_eq!(telemetry.backpressure_events, 0);
    assert!(telemetry.warnings.is_empty());
    assert!(telemetry.errors.is_empty());
    assert_eq!(telemetry.graph_duration, Duration::default());
    assert_eq!(telemetry.metrics_level, MetricsLevel::Basic);
    assert!(telemetry.node_metrics.is_empty());
    assert!(telemetry.group_metrics.is_empty());
    assert!(telemetry.edge_metrics.is_empty());
    assert!(telemetry.trace.as_ref().is_none_or(Vec::is_empty));
    assert!(telemetry.in_flight_node_transport_metrics.is_empty());
}

#[test]
#[cfg(feature = "metrics")]
fn report_exposes_release_diagnostic_metrics() {
    let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
    telemetry.backpressure_events = 1;
    telemetry.warnings.push("queue pressure".to_string());
    telemetry.errors.push(super::NodeFailure {
        node_idx: 7,
        node_id: "missing".to_string(),
        code: "missing_handler".to_string(),
        message: "node handler is not registered".to_string(),
    });
    telemetry.record_edge_capacity(3, Some(2));
    telemetry.record_edge_depth(3, 2);
    telemetry.record_edge_queue_bytes(3, 128);
    telemetry.record_edge_pressure_event(3, super::EdgePressureReason::DropOldest, 1);

    let report = telemetry.report();
    let edge = report.edge_timing.get(&3).expect("edge metrics");

    assert_eq!(report.backpressure_events, 1);
    assert_eq!(report.warnings, vec!["queue pressure"]);
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].code, "missing_handler");
    assert_eq!(edge.capacity, Some(2));
    assert_eq!(edge.max_depth, 2);
    assert_eq!(edge.peak_queue_bytes, 128);
    assert_eq!(edge.pressure_events.total, 1);
    assert_eq!(edge.pressure_events.drop_oldest, 1);
    assert_eq!(edge.drops, 1);

    let table = report.to_table();
    assert!(table.contains("edge\t3\tpressure_events\t1"));
    assert!(table.contains("edge\t3\tpeak_queue_bytes\t128"));
    assert!(table.contains("failure\t7\tmissing_handler"));
}

#[test]
#[cfg(feature = "metrics")]
fn report_filter_uses_typed_transport_ids_without_changing_json_shape() {
    let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Trace);
    let mut record = super::DataLifecycleRecord::new(99, super::DataLifecycleStage::AdapterStart);
    record.port = Some("image".to_string());
    record.payload = Some("type=example:image bytes=4".to_string());
    record.adapter_steps = vec!["example.image.upload".to_string()];
    telemetry.record_data_lifecycle(record);

    let report = telemetry.report();
    let filtered = report.clone().filter(&super::TelemetryReportFilter {
        port: Some(crate::handles::PortId::new("image")),
        type_key: Some(daedalus_transport::TypeKey::new("example:image")),
        adapter_id: Some(daedalus_transport::AdapterId::new("example.image.upload")),
        ..Default::default()
    });

    assert_eq!(filtered.lifecycle.len(), 1);
    let json = serde_json::to_string(&super::TelemetryReportFilter {
        port: Some(crate::handles::PortId::new("image")),
        type_key: Some(daedalus_transport::TypeKey::new("example:image")),
        adapter_id: Some(daedalus_transport::AdapterId::new("example.image.upload")),
        ..Default::default()
    })
    .expect("serialize filter");
    assert!(json.contains("\"port\":\"image\""));
    assert!(json.contains("\"type_key\":\"example:image\""));
    assert!(json.contains("\"adapter_id\":\"example.image.upload\""));
}
