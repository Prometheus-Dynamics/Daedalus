//! Per-edge recording hooks.

use std::time::Duration;

use super::{EdgePressureReason, ExecutionTelemetry, Histogram};

impl ExecutionTelemetry {
    pub fn record_edge_wait(&mut self, edge_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_basic() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.total_wait += duration;
        entry.samples += 1;
        if self.metrics_level.is_detailed() {
            entry
                .wait_histogram
                .get_or_insert_with(Histogram::default)
                .record_duration(duration);
        }
    }

    pub fn record_edge_depth(&mut self, edge_idx: usize, depth: usize) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        let depth_u64 = depth as u64;
        entry.current_depth = depth_u64;
        entry.max_depth = entry.max_depth.max(depth_u64);
        entry
            .depth_histogram
            .get_or_insert_with(Histogram::default)
            .record_value(depth_u64);
    }

    pub fn record_edge_capacity(&mut self, edge_idx: usize, capacity: Option<usize>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let Some(capacity) = capacity else {
            return;
        };
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.capacity = Some(entry.capacity.unwrap_or(0).max(capacity as u64));
    }

    pub fn record_edge_queue_bytes(&mut self, edge_idx: usize, current_bytes: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.current_queue_bytes = current_bytes;
        entry.peak_queue_bytes = entry.peak_queue_bytes.max(current_bytes);
    }

    pub fn record_edge_transport(&mut self, edge_idx: usize, bytes: Option<u64>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.transport_count = entry.transport_count.saturating_add(1);
        if let Some(bytes) = bytes {
            entry.transport_bytes = entry.transport_bytes.saturating_add(bytes);
        }
    }

    pub fn record_edge_handoff(
        &mut self,
        edge_idx: usize,
        unique: bool,
        cloned_payload: bool,
        copied_bytes: u64,
    ) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        if unique {
            entry.unique_handoffs = entry.unique_handoffs.saturating_add(1);
        } else {
            entry.shared_handoffs = entry.shared_handoffs.saturating_add(1);
        }
        if cloned_payload {
            entry.payload_clone_count = entry.payload_clone_count.saturating_add(1);
        }
        entry.copied_bytes = entry.copied_bytes.saturating_add(copied_bytes);
    }

    pub fn record_edge_transport_apply_duration(&mut self, edge_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.transport_apply_duration += duration;
        entry.transport_apply_count = entry.transport_apply_count.saturating_add(1);
        if self.metrics_level.is_profile() {
            entry
                .transport_apply_histogram
                .get_or_insert_with(Histogram::default)
                .record_duration(duration);
        }
    }

    pub fn record_edge_adapter_duration(&mut self, edge_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.adapter_duration += duration;
        entry.adapter_count = entry.adapter_count.saturating_add(1);
        if self.metrics_level.is_profile() {
            entry
                .adapter_histogram
                .get_or_insert_with(Histogram::default)
                .record_duration(duration);
        }
    }

    pub fn record_edge_adapter_error(&mut self, edge_idx: usize) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.adapter_errors = entry.adapter_errors.saturating_add(1);
    }

    pub fn record_edge_gpu_transfer(&mut self, edge_idx: usize, upload: bool) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        if upload {
            entry.gpu_uploads = entry.gpu_uploads.saturating_add(1);
        } else {
            entry.gpu_downloads = entry.gpu_downloads.saturating_add(1);
        }
    }

    pub fn record_edge_drop(&mut self, edge_idx: usize, count: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() || count == 0 {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.drops = entry.drops.saturating_add(count);
    }

    pub fn record_edge_pressure_event(
        &mut self,
        edge_idx: usize,
        reason: EdgePressureReason,
        dropped_count: u64,
    ) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.pressure_events.total = entry.pressure_events.total.saturating_add(1);
        match reason {
            EdgePressureReason::DropIncoming => {
                entry.pressure_events.drop_incoming =
                    entry.pressure_events.drop_incoming.saturating_add(1);
            }
            EdgePressureReason::DropOldest => {
                entry.pressure_events.drop_oldest =
                    entry.pressure_events.drop_oldest.saturating_add(1);
            }
            EdgePressureReason::DropNewest => {
                entry.pressure_events.drop_newest =
                    entry.pressure_events.drop_newest.saturating_add(1);
            }
            EdgePressureReason::Backpressure => {
                entry.pressure_events.backpressure =
                    entry.pressure_events.backpressure.saturating_add(1);
            }
            EdgePressureReason::ErrorOverflow => {
                entry.pressure_events.error_overflow =
                    entry.pressure_events.error_overflow.saturating_add(1);
            }
            EdgePressureReason::LatestReplace => {
                entry.pressure_events.latest_replace =
                    entry.pressure_events.latest_replace.saturating_add(1);
            }
            EdgePressureReason::CoalesceReplace => {
                entry.pressure_events.coalesce_replace =
                    entry.pressure_events.coalesce_replace.saturating_add(1);
            }
        }
        entry.drops = entry.drops.saturating_add(dropped_count);
    }
}
