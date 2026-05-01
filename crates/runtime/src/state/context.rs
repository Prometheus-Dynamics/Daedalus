use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use super::{
    ManagedByteBuffer, ManagedResource, NodeResourceSnapshot, ResourceClass,
    ResourceLifecycleEvent, StateError, StateStore,
};

/// Execution context passed to nodes.
#[derive(Clone)]
pub struct ExecutionContext {
    pub state: StateStore,
    pub node_id: Arc<str>,
    pub metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
    /// Graph-level metadata (typed values) shared by all nodes in the graph.
    pub graph_metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
    pub capabilities: Arc<crate::capabilities::CapabilityRegistry>,
    #[cfg(feature = "gpu")]
    pub gpu: Option<GpuContextHandle>,
}

#[cfg(feature = "gpu")]
pub type GpuContextHandle = daedalus_gpu::GpuContextHandle;

pub struct RuntimeResources<'a> {
    state: &'a StateStore,
    node_id: &'a str,
}

impl<'a> RuntimeResources<'a> {
    pub fn node_id(&self) -> &str {
        self.node_id
    }

    pub fn before_frame(&self) -> Result<(), StateError> {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::BeforeFrame)
    }

    pub fn after_frame(&self) -> Result<(), StateError> {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::AfterFrame)
    }

    pub fn on_memory_pressure(&self) -> Result<(), StateError> {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::MemoryPressure)
    }

    pub fn on_idle(&self) -> Result<(), StateError> {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::Idle)
    }

    pub fn on_stop(&self) -> Result<(), StateError> {
        self.state.release_node_resources(self.node_id)
    }

    pub fn snapshot(&self) -> Result<NodeResourceSnapshot, StateError> {
        self.state.snapshot_node_resources(self.node_id)
    }

    pub fn record_frame_scratch_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), StateError> {
        self.state.record_node_resource_usage(
            self.node_id,
            name,
            ResourceClass::FrameScratch,
            live_bytes,
            retained_bytes,
        )
    }

    pub fn record_warm_cache_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), StateError> {
        self.state.record_node_resource_usage(
            self.node_id,
            name,
            ResourceClass::WarmCache,
            live_bytes,
            retained_bytes,
        )
    }

    pub fn record_persistent_state_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), StateError> {
        self.state.record_node_resource_usage(
            self.node_id,
            name,
            ResourceClass::PersistentState,
            live_bytes,
            retained_bytes,
        )
    }

    pub fn with_frame_scratch<T, R, Init, F>(
        &self,
        name: &str,
        init: Init,
        f: F,
    ) -> Result<R, StateError>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.state
            .with_node_resource(self.node_id, name, ResourceClass::FrameScratch, init, f)
    }

    pub fn with_warm_cache<T, R, Init, F>(
        &self,
        name: &str,
        init: Init,
        f: F,
    ) -> Result<R, StateError>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.state
            .with_node_resource(self.node_id, name, ResourceClass::WarmCache, init, f)
    }

    pub fn with_persistent_state<T, R, Init, F>(
        &self,
        name: &str,
        init: Init,
        f: F,
    ) -> Result<R, StateError>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.state
            .with_node_resource(self.node_id, name, ResourceClass::PersistentState, init, f)
    }

    pub fn with_frame_scratch_bytes<R, F>(
        &self,
        name: &str,
        len: usize,
        f: F,
    ) -> Result<R, StateError>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.with_frame_scratch(name, ManagedByteBuffer::frame_scratch, |buffer| {
            let bytes = buffer.prepare(len);
            f(bytes)
        })
    }

    pub fn with_warm_cache_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, StateError>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.with_warm_cache(name, ManagedByteBuffer::warm_cache, |buffer| {
            let bytes = buffer.prepare(len);
            f(bytes)
        })
    }

    pub fn with_persistent_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, StateError>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.with_persistent_state(name, ManagedByteBuffer::persistent_state, |buffer| {
            let bytes = buffer.prepare(len);
            f(bytes)
        })
    }
}

impl ExecutionContext {
    pub fn resources(&self) -> RuntimeResources<'_> {
        RuntimeResources {
            state: &self.state,
            node_id: &self.node_id,
        }
    }

    pub fn begin_resource_frame(&self) -> Result<(), StateError> {
        self.resources().before_frame()
    }

    pub fn snapshot_resources(&self) -> Result<NodeResourceSnapshot, StateError> {
        self.resources().snapshot()
    }

    pub fn end_resource_frame(&self) -> Result<(), StateError> {
        self.resources().after_frame()
    }

    pub fn apply_memory_pressure(&self) -> Result<(), StateError> {
        self.resources().on_memory_pressure()
    }

    pub fn notify_idle(&self) -> Result<(), StateError> {
        self.resources().on_idle()
    }

    pub fn release_resources(&self) -> Result<(), StateError> {
        self.resources().on_stop()
    }

    pub fn record_metric(
        &self,
        name: impl Into<String>,
        value: crate::executor::CustomMetricValue,
    ) -> Result<(), StateError> {
        self.state
            .record_node_custom_metric(&self.node_id, name, value)
    }

    pub fn increment_metric(&self, name: impl Into<String>, value: u64) -> Result<(), StateError> {
        self.record_metric(name, crate::executor::CustomMetricValue::Counter(value))
    }

    pub fn gauge_metric(&self, name: impl Into<String>, value: f64) -> Result<(), StateError> {
        self.record_metric(name, crate::executor::CustomMetricValue::Gauge(value))
    }

    pub fn duration_metric(
        &self,
        name: impl Into<String>,
        value: Duration,
    ) -> Result<(), StateError> {
        self.record_metric(name, crate::executor::CustomMetricValue::Duration(value))
    }

    pub fn bytes_metric(&self, name: impl Into<String>, value: u64) -> Result<(), StateError> {
        self.record_metric(name, crate::executor::CustomMetricValue::Bytes(value))
    }

    pub fn text_metric(
        &self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), StateError> {
        self.record_metric(name, crate::executor::CustomMetricValue::Text(value.into()))
    }

    pub fn bool_metric(&self, name: impl Into<String>, value: bool) -> Result<(), StateError> {
        self.record_metric(name, crate::executor::CustomMetricValue::Bool(value))
    }

    pub fn json_metric(
        &self,
        name: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<(), StateError> {
        self.record_metric(name, crate::executor::CustomMetricValue::Json(value))
    }

    pub fn record_frame_scratch_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), StateError> {
        self.resources()
            .record_frame_scratch_bytes(name, live_bytes, retained_bytes)
    }

    pub fn record_warm_cache_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), StateError> {
        self.resources()
            .record_warm_cache_bytes(name, live_bytes, retained_bytes)
    }

    pub fn record_persistent_state_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), StateError> {
        self.resources()
            .record_persistent_state_bytes(name, live_bytes, retained_bytes)
    }

    pub fn with_frame_scratch<T, R, Init, F>(
        &self,
        name: &str,
        init: Init,
        f: F,
    ) -> Result<R, StateError>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.resources().with_frame_scratch(name, init, f)
    }

    pub fn with_warm_cache<T, R, Init, F>(
        &self,
        name: &str,
        init: Init,
        f: F,
    ) -> Result<R, StateError>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.resources().with_warm_cache(name, init, f)
    }

    pub fn with_persistent_state<T, R, Init, F>(
        &self,
        name: &str,
        init: Init,
        f: F,
    ) -> Result<R, StateError>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.resources().with_persistent_state(name, init, f)
    }

    pub fn with_frame_scratch_bytes<R, F>(
        &self,
        name: &str,
        len: usize,
        f: F,
    ) -> Result<R, StateError>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.resources().with_frame_scratch_bytes(name, len, f)
    }

    pub fn with_warm_cache_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, StateError>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.resources().with_warm_cache_bytes(name, len, f)
    }

    pub fn with_persistent_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, StateError>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.resources().with_persistent_bytes(name, len, f)
    }
}
