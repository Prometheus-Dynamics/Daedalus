use crate::perf::PerfSample;

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceMetrics {
    #[serde(default)]
    pub current_live_bytes: u64,
    #[serde(default)]
    pub current_retained_bytes: u64,
    #[serde(default)]
    pub current_touched_bytes: u64,
    #[serde(default)]
    pub peak_live_bytes: u64,
    #[serde(default)]
    pub peak_retained_bytes: u64,
    #[serde(default)]
    pub peak_touched_bytes: u64,
    #[serde(default)]
    pub current_allocation_events: u64,
    #[serde(default)]
    pub peak_allocation_events: u64,
}

impl ResourceMetrics {
    pub(super) fn observe(&mut self, usage: crate::state::ResourceUsage) {
        self.current_live_bytes = usage.live_bytes;
        self.current_retained_bytes = usage.retained_bytes;
        self.current_touched_bytes = usage.touched_bytes;
        self.peak_live_bytes = self.peak_live_bytes.max(usage.live_bytes);
        self.peak_retained_bytes = self.peak_retained_bytes.max(usage.retained_bytes);
        self.peak_touched_bytes = self.peak_touched_bytes.max(usage.touched_bytes);
        self.current_allocation_events = usage.allocation_events;
        self.peak_allocation_events = self.peak_allocation_events.max(usage.allocation_events);
    }

    pub(super) fn merge(&mut self, other: ResourceMetrics) {
        self.current_live_bytes = self.current_live_bytes.max(other.current_live_bytes);
        self.current_retained_bytes = self
            .current_retained_bytes
            .max(other.current_retained_bytes);
        self.current_touched_bytes = self.current_touched_bytes.max(other.current_touched_bytes);
        self.peak_live_bytes = self.peak_live_bytes.max(other.peak_live_bytes);
        self.peak_retained_bytes = self.peak_retained_bytes.max(other.peak_retained_bytes);
        self.peak_touched_bytes = self.peak_touched_bytes.max(other.peak_touched_bytes);
        self.current_allocation_events = self
            .current_allocation_events
            .max(other.current_allocation_events);
        self.peak_allocation_events = self
            .peak_allocation_events
            .max(other.peak_allocation_events);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.current_live_bytes == 0
            && self.current_retained_bytes == 0
            && self.current_touched_bytes == 0
            && self.peak_live_bytes == 0
            && self.peak_retained_bytes == 0
            && self.peak_touched_bytes == 0
            && self.current_allocation_events == 0
            && self.peak_allocation_events == 0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InternalTransferMetrics {
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub peak_bytes: u64,
}

impl InternalTransferMetrics {
    pub(super) fn record(&mut self, bytes: u64) {
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.count = self.count.saturating_add(1);
        self.peak_bytes = self.peak_bytes.max(bytes);
    }

    pub(super) fn merge(&mut self, other: InternalTransferMetrics) {
        self.total_bytes = self.total_bytes.saturating_add(other.total_bytes);
        self.count = self.count.saturating_add(other.count);
        self.peak_bytes = self.peak_bytes.max(other.peak_bytes);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.total_bytes == 0 && self.count == 0 && self.peak_bytes == 0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeResourceMetrics {
    pub frame_scratch: ResourceMetrics,
    pub warm_cache: ResourceMetrics,
    pub persistent_state: ResourceMetrics,
    #[serde(default, skip_serializing_if = "InternalTransferMetrics::is_empty")]
    pub materialization: InternalTransferMetrics,
    #[serde(default, skip_serializing_if = "InternalTransferMetrics::is_empty")]
    pub conversion: InternalTransferMetrics,
    #[serde(default, skip_serializing_if = "InternalTransferMetrics::is_empty")]
    pub gpu_upload: InternalTransferMetrics,
    #[serde(default, skip_serializing_if = "InternalTransferMetrics::is_empty")]
    pub gpu_download: InternalTransferMetrics,
}

impl NodeResourceMetrics {
    pub(super) fn observe_snapshot(&mut self, snapshot: &crate::state::NodeResourceSnapshot) {
        self.frame_scratch.observe(snapshot.frame_scratch);
        self.warm_cache.observe(snapshot.warm_cache);
        self.persistent_state.observe(snapshot.persistent_state);
    }

    pub(super) fn merge(&mut self, other: NodeResourceMetrics) {
        self.frame_scratch.merge(other.frame_scratch);
        self.warm_cache.merge(other.warm_cache);
        self.persistent_state.merge(other.persistent_state);
        self.materialization.merge(other.materialization);
        self.conversion.merge(other.conversion);
        self.gpu_upload.merge(other.gpu_upload);
        self.gpu_download.merge(other.gpu_download);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.frame_scratch.is_empty()
            && self.warm_cache.is_empty()
            && self.persistent_state.is_empty()
            && self.materialization.is_empty()
            && self.conversion.is_empty()
            && self.gpu_upload.is_empty()
            && self.gpu_download.is_empty()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeAllocationSpikeExplanation {
    pub node_idx: usize,
    pub frame_scratch: ResourceMetrics,
    pub warm_cache: ResourceMetrics,
    pub persistent_state: ResourceMetrics,
    pub materialization: InternalTransferMetrics,
    pub conversion: InternalTransferMetrics,
    pub gpu_upload: InternalTransferMetrics,
    pub gpu_download: InternalTransferMetrics,
    #[serde(default)]
    pub dominant_sources: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodePerfMetrics {
    pub cpu_cycles: u64,
    pub instructions: u64,
    pub cache_misses: u64,
    pub cache_references: u64,
    pub branch_instructions: u64,
    pub branch_misses: u64,
    pub context_switches: u64,
    pub thread_cpu_time_ns: u64,
}

impl NodePerfMetrics {
    pub(super) fn record(&mut self, sample: PerfSample) {
        self.cpu_cycles = self.cpu_cycles.saturating_add(sample.cpu_cycles);
        self.instructions = self.instructions.saturating_add(sample.instructions);
        self.cache_misses = self.cache_misses.saturating_add(sample.cache_misses);
        self.cache_references = self
            .cache_references
            .saturating_add(sample.cache_references);
        self.branch_instructions = self
            .branch_instructions
            .saturating_add(sample.branch_instructions);
        self.branch_misses = self.branch_misses.saturating_add(sample.branch_misses);
        self.context_switches = self
            .context_switches
            .saturating_add(sample.context_switches);
        self.thread_cpu_time_ns = self
            .thread_cpu_time_ns
            .saturating_add(sample.thread_cpu_time_ns);
    }

    pub(super) fn merge(&mut self, other: NodePerfMetrics) {
        self.cpu_cycles = self.cpu_cycles.saturating_add(other.cpu_cycles);
        self.instructions = self.instructions.saturating_add(other.instructions);
        self.cache_misses = self.cache_misses.saturating_add(other.cache_misses);
        self.cache_references = self.cache_references.saturating_add(other.cache_references);
        self.branch_instructions = self
            .branch_instructions
            .saturating_add(other.branch_instructions);
        self.branch_misses = self.branch_misses.saturating_add(other.branch_misses);
        self.context_switches = self.context_switches.saturating_add(other.context_switches);
        self.thread_cpu_time_ns = self
            .thread_cpu_time_ns
            .saturating_add(other.thread_cpu_time_ns);
    }
}
