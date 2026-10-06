use crate::portable::Arc;
use crate::prelude::*;
use alloc::collections::BTreeMap;
use core::time::Duration;

use super::{
    ManagedByteBuffer, ManagedResource, NodeResourceSnapshot, NodeStateSlot, ResourceClass,
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
    /// `node_id`'s slot in `state`, resolved when the context is built.
    node_state: Arc<NodeStateSlot>,
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

    pub fn before_frame(&self) {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::BeforeFrame)
    }

    pub fn after_frame(&self) {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::AfterFrame)
    }

    pub fn on_memory_pressure(&self) {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::MemoryPressure)
    }

    pub fn on_idle(&self) {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::Idle)
    }

    pub fn on_stop(&self) {
        self.state.release_node_resources(self.node_id)
    }

    pub fn snapshot(&self) -> NodeResourceSnapshot {
        self.state.snapshot_node_resources(self.node_id)
    }

    pub fn record_frame_scratch_bytes(&self, name: &str, live_bytes: u64, retained_bytes: u64) {
        self.state.record_node_resource_usage(
            self.node_id,
            name,
            ResourceClass::FrameScratch,
            live_bytes,
            retained_bytes,
        )
    }

    pub fn record_warm_cache_bytes(&self, name: &str, live_bytes: u64, retained_bytes: u64) {
        self.state.record_node_resource_usage(
            self.node_id,
            name,
            ResourceClass::WarmCache,
            live_bytes,
            retained_bytes,
        )
    }

    pub fn record_persistent_state_bytes(&self, name: &str, live_bytes: u64, retained_bytes: u64) {
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
    /// A context for running a handler outside an executor (a dynamic plugin's stable `invoke`
    /// entry point): `state` and `node_id`, no metadata, capabilities or GPU.
    pub fn detached(state: StateStore, node_id: Arc<str>) -> Self {
        Self::new(
            state,
            node_id,
            Arc::default(),
            Arc::default(),
            Arc::new(crate::capabilities::CapabilityRegistry::new()),
        )
    }

    /// A context for node `node_id` (no GPU; set [`Self::gpu`] after), resolving its state slot.
    pub fn new(
        state: StateStore,
        node_id: Arc<str>,
        metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
        graph_metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
        capabilities: Arc<crate::capabilities::CapabilityRegistry>,
    ) -> Self {
        Self {
            node_state: state.node_state_slot(&node_id),
            state,
            node_id,
            metadata,
            graph_metadata,
            capabilities,
            #[cfg(feature = "gpu")]
            gpu: None,
        }
    }

    /// Move this node's state of type `T` out (`StateStore::take_node_state` without the
    /// lookup).
    pub fn take_node_state<T: Send + Sync + 'static>(&self) -> Option<T> {
        self.node_state.take()
    }

    /// Store this node's state of type `T` (`StateStore::set_node_state` without the lookup).
    pub fn set_node_state<T: Send + Sync + 'static>(&self, value: T) {
        self.node_state.set(value)
    }

    pub fn resources(&self) -> RuntimeResources<'_> {
        RuntimeResources {
            state: &self.state,
            node_id: &self.node_id,
        }
    }

    pub fn begin_resource_frame(&self) {
        self.resources().before_frame()
    }

    pub fn snapshot_resources(&self) -> NodeResourceSnapshot {
        self.resources().snapshot()
    }

    pub fn end_resource_frame(&self) {
        self.resources().after_frame()
    }

    pub fn apply_memory_pressure(&self) {
        self.resources().on_memory_pressure()
    }

    pub fn notify_idle(&self) {
        self.resources().on_idle()
    }

    pub fn release_resources(&self) {
        self.resources().on_stop()
    }

    pub fn record_metric(
        &self,
        name: impl Into<String>,
        value: crate::executor::CustomMetricValue,
    ) {
        self.state
            .record_node_custom_metric(&self.node_id, name, value);
    }

    pub fn increment_metric(&self, name: impl Into<String>, value: u64) {
        self.record_metric(name, crate::executor::CustomMetricValue::Counter(value))
    }

    pub fn gauge_metric(&self, name: impl Into<String>, value: f64) {
        self.record_metric(name, crate::executor::CustomMetricValue::Gauge(value))
    }

    pub fn duration_metric(&self, name: impl Into<String>, value: Duration) {
        self.record_metric(name, crate::executor::CustomMetricValue::Duration(value))
    }

    pub fn bytes_metric(&self, name: impl Into<String>, value: u64) {
        self.record_metric(name, crate::executor::CustomMetricValue::Bytes(value))
    }

    pub fn text_metric(&self, name: impl Into<String>, value: impl Into<String>) {
        self.record_metric(name, crate::executor::CustomMetricValue::Text(value.into()))
    }

    pub fn bool_metric(&self, name: impl Into<String>, value: bool) {
        self.record_metric(name, crate::executor::CustomMetricValue::Bool(value))
    }

    pub fn json_metric(&self, name: impl Into<String>, value: serde_json::Value) {
        self.record_metric(name, crate::executor::CustomMetricValue::Json(value))
    }

    pub fn record_frame_scratch_bytes(&self, name: &str, live_bytes: u64, retained_bytes: u64) {
        self.resources()
            .record_frame_scratch_bytes(name, live_bytes, retained_bytes)
    }

    pub fn record_warm_cache_bytes(&self, name: &str, live_bytes: u64, retained_bytes: u64) {
        self.resources()
            .record_warm_cache_bytes(name, live_bytes, retained_bytes)
    }

    pub fn record_persistent_state_bytes(&self, name: &str, live_bytes: u64, retained_bytes: u64) {
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
