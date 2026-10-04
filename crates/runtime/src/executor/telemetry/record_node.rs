//! Per-node recording hooks.

use crate::prelude::*;
use core::time::Duration;

use crate::perf::PerfSample;

use super::{
    CustomMetricValue, ExecutionTelemetry, Histogram, InFlightNodeTransportMetrics,
    NodeAllocationSpikeExplanation, NodeResourceMetrics, TransportMetrics,
};

impl ExecutionTelemetry {
    pub fn record_node_duration(&mut self, node_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_basic() {
            return;
        }
        self.in_flight_node_transport_metrics.remove(&node_idx);
        let entry = self.node_metrics.entry(node_idx);
        entry.record(duration);
        if self.metrics_level.is_detailed() {
            entry
                .duration_histogram
                .get_or_insert_with(Histogram::default)
                .record_duration(duration);
        }
    }

    pub fn record_node_handler_duration(&mut self, node_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        entry.record_handler(duration);
    }

    pub fn record_node_cpu_duration(&mut self, node_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        entry.cpu_duration += duration;
    }

    pub fn record_node_perf(&mut self, node_idx: usize, sample: PerfSample) {
        if !cfg!(feature = "metrics") {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        entry.record_perf(sample);
    }

    pub fn record_node_custom_metric(
        &mut self,
        node_idx: usize,
        name: impl Into<String>,
        value: CustomMetricValue,
    ) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_basic() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        entry.record_custom(name, value);
    }

    pub fn record_node_custom_metrics(
        &mut self,
        node_idx: usize,
        metrics: alloc::collections::BTreeMap<String, CustomMetricValue>,
    ) {
        for (name, value) in metrics {
            self.record_node_custom_metric(node_idx, name, value);
        }
    }

    pub fn start_node_call(&mut self, node_idx: usize) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        self.in_flight_node_transport_metrics
            .insert(node_idx, InFlightNodeTransportMetrics::default());
    }

    pub fn record_node_transport_in(&mut self, node_idx: usize, bytes: Option<u64>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        let payload = entry
            .transport
            .get_or_insert_with(TransportMetrics::default);
        payload.in_count = payload.in_count.saturating_add(1);
        if let Some(bytes) = bytes {
            payload.in_bytes = payload.in_bytes.saturating_add(bytes);
            let in_flight = self
                .in_flight_node_transport_metrics
                .entry(node_idx)
                .or_default();
            in_flight.input_bytes = in_flight.input_bytes.saturating_add(bytes);
            payload.peak_input_bytes = payload.peak_input_bytes.max(in_flight.input_bytes);
            payload.peak_working_set_bytes = payload
                .peak_working_set_bytes
                .max(in_flight.input_bytes.saturating_add(in_flight.output_bytes));
        }
    }

    pub fn record_node_transport_out(&mut self, node_idx: usize, bytes: Option<u64>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        let payload = entry
            .transport
            .get_or_insert_with(TransportMetrics::default);
        payload.out_count = payload.out_count.saturating_add(1);
        if let Some(bytes) = bytes {
            payload.out_bytes = payload.out_bytes.saturating_add(bytes);
            let in_flight = self
                .in_flight_node_transport_metrics
                .entry(node_idx)
                .or_default();
            in_flight.output_bytes = in_flight.output_bytes.saturating_add(bytes);
            payload.peak_output_bytes = payload.peak_output_bytes.max(in_flight.output_bytes);
            payload.peak_working_set_bytes = payload
                .peak_working_set_bytes
                .max(in_flight.input_bytes.saturating_add(in_flight.output_bytes));
        }
    }

    pub fn record_node_resource_snapshot(
        &mut self,
        node_idx: usize,
        snapshot: crate::state::NodeResourceSnapshot,
    ) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        if snapshot == crate::state::NodeResourceSnapshot::default() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        let resources = entry
            .resources
            .get_or_insert_with(NodeResourceMetrics::default);
        resources.observe_snapshot(&snapshot);
    }

    pub fn record_node_materialization(&mut self, node_idx: usize, bytes: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() || bytes == 0 {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        let resources = entry
            .resources
            .get_or_insert_with(NodeResourceMetrics::default);
        resources.materialization.record(bytes);
    }

    pub fn record_node_conversion(&mut self, node_idx: usize, bytes: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() || bytes == 0 {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        let resources = entry
            .resources
            .get_or_insert_with(NodeResourceMetrics::default);
        resources.conversion.record(bytes);
    }

    pub fn record_node_gpu_transfer(&mut self, node_idx: usize, upload: bool, bytes: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() || bytes == 0 {
            return;
        }
        let entry = self.node_metrics.entry(node_idx);
        let resources = entry
            .resources
            .get_or_insert_with(NodeResourceMetrics::default);
        if upload {
            resources.gpu_upload.record(bytes);
        } else {
            resources.gpu_download.record(bytes);
        }
    }

    pub fn explain_node_allocation_spike(
        &self,
        node_idx: usize,
    ) -> Option<NodeAllocationSpikeExplanation> {
        let metrics = self.node_metrics.get(node_idx)?;
        let resources = metrics.resources.as_ref()?;
        let mut dominant_sources = vec![
            ("frame_scratch", resources.frame_scratch.peak_retained_bytes),
            ("warm_cache", resources.warm_cache.peak_retained_bytes),
            (
                "persistent_state",
                resources.persistent_state.peak_retained_bytes,
            ),
            ("materialization", resources.materialization.total_bytes),
            ("conversion", resources.conversion.total_bytes),
            ("gpu_upload", resources.gpu_upload.total_bytes),
            ("gpu_download", resources.gpu_download.total_bytes),
        ];
        dominant_sources.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

        Some(NodeAllocationSpikeExplanation {
            node_idx,
            frame_scratch: resources.frame_scratch.clone(),
            warm_cache: resources.warm_cache.clone(),
            persistent_state: resources.persistent_state.clone(),
            materialization: resources.materialization.clone(),
            conversion: resources.conversion.clone(),
            gpu_upload: resources.gpu_upload.clone(),
            gpu_download: resources.gpu_download.clone(),
            dominant_sources: dominant_sources
                .into_iter()
                .filter(|(_, bytes)| *bytes > 0)
                .map(|(name, bytes)| format!("{name}:{bytes}"))
                .collect(),
        })
    }
}
