use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceClass {
    #[default]
    FrameScratch,
    WarmCache,
    PersistentState,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLifecycleEvent {
    #[default]
    BeforeFrame,
    AfterFrame,
    MemoryPressure,
    Idle,
    Stop,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceUsage {
    pub live_bytes: u64,
    pub retained_bytes: u64,
    #[serde(default)]
    pub touched_bytes: u64,
    #[serde(default)]
    pub allocation_events: u64,
}

impl ResourceUsage {
    fn new(live_bytes: u64, retained_bytes: u64) -> Self {
        Self {
            live_bytes,
            retained_bytes: retained_bytes.max(live_bytes),
            touched_bytes: live_bytes,
            allocation_events: 0,
        }
    }

    fn with_details(
        live_bytes: u64,
        retained_bytes: u64,
        touched_bytes: u64,
        allocation_events: u64,
    ) -> Self {
        Self {
            live_bytes,
            retained_bytes: retained_bytes.max(live_bytes),
            touched_bytes,
            allocation_events,
        }
    }

    fn clear_live(&mut self) {
        self.live_bytes = 0;
        self.touched_bytes = 0;
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeResourceSnapshot {
    pub frame_scratch: ResourceUsage,
    pub warm_cache: ResourceUsage,
    pub persistent_state: ResourceUsage,
}

impl NodeResourceSnapshot {
    fn add_usage(&mut self, class: ResourceClass, usage: ResourceUsage) {
        let target = match class {
            ResourceClass::FrameScratch => &mut self.frame_scratch,
            ResourceClass::WarmCache => &mut self.warm_cache,
            ResourceClass::PersistentState => &mut self.persistent_state,
        };
        target.live_bytes = target.live_bytes.saturating_add(usage.live_bytes);
        target.retained_bytes = target.retained_bytes.saturating_add(usage.retained_bytes);
        target.touched_bytes = target.touched_bytes.saturating_add(usage.touched_bytes);
        target.allocation_events = target
            .allocation_events
            .saturating_add(usage.allocation_events);
    }
}

struct ResourceEntry {
    class: ResourceClass,
    storage: ResourceStorage,
}

impl ResourceEntry {
    fn usage(&self) -> ResourceUsage {
        match &self.storage {
            ResourceStorage::Usage(usage) => *usage,
            ResourceStorage::Managed(managed) => managed.usage(),
        }
    }

    fn apply_lifecycle(&mut self, event: ResourceLifecycleEvent) {
        match &mut self.storage {
            ResourceStorage::Usage(usage) => match event {
                ResourceLifecycleEvent::BeforeFrame => {
                    if self.class == ResourceClass::FrameScratch {
                        usage.clear_live();
                    }
                }
                ResourceLifecycleEvent::AfterFrame => {}
                ResourceLifecycleEvent::MemoryPressure => match self.class {
                    ResourceClass::FrameScratch => {
                        *usage = ResourceUsage::default();
                    }
                    ResourceClass::WarmCache => {
                        usage.retained_bytes = usage.live_bytes;
                    }
                    ResourceClass::PersistentState => {
                        usage.retained_bytes = usage.retained_bytes.max(usage.live_bytes);
                    }
                },
                ResourceLifecycleEvent::Idle => match self.class {
                    ResourceClass::FrameScratch => {
                        *usage = ResourceUsage::default();
                    }
                    ResourceClass::WarmCache => {
                        usage.live_bytes = 0;
                        usage.retained_bytes = 0;
                    }
                    ResourceClass::PersistentState => {
                        usage.live_bytes = 0;
                    }
                },
                ResourceLifecycleEvent::Stop => {}
            },
            ResourceStorage::Managed(managed) => match event {
                ResourceLifecycleEvent::BeforeFrame => managed.before_frame(),
                ResourceLifecycleEvent::AfterFrame => managed.after_frame(),
                ResourceLifecycleEvent::MemoryPressure => managed.on_memory_pressure(),
                ResourceLifecycleEvent::Idle => managed.on_idle(),
                ResourceLifecycleEvent::Stop => managed.on_stop(),
            },
        }
    }
}

enum ResourceStorage {
    Usage(ResourceUsage),
    Managed(Box<dyn ManagedResourceBox>),
}

pub trait ManagedResource: Send + Sync + 'static {
    fn live_bytes(&self) -> u64 {
        0
    }

    fn retained_bytes(&self) -> u64 {
        self.live_bytes()
    }

    fn touched_bytes(&self) -> u64 {
        self.live_bytes()
    }

    fn allocation_events(&self) -> u64 {
        0
    }

    fn before_frame(&mut self) {}

    fn after_frame(&mut self) {}

    fn on_memory_pressure(&mut self) {}

    fn on_idle(&mut self) {}

    fn on_stop(&mut self) {}
}

trait ManagedResourceBox: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn usage(&self) -> ResourceUsage;
    fn before_frame(&mut self);
    fn after_frame(&mut self);
    fn on_memory_pressure(&mut self);
    fn on_idle(&mut self);
    fn on_stop(&mut self);
}

impl<T: ManagedResource> ManagedResourceBox for T {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn usage(&self) -> ResourceUsage {
        ResourceUsage::with_details(
            self.live_bytes(),
            self.retained_bytes(),
            self.touched_bytes(),
            self.allocation_events(),
        )
    }

    fn before_frame(&mut self) {
        ManagedResource::before_frame(self);
    }

    fn after_frame(&mut self) {
        ManagedResource::after_frame(self);
    }

    fn on_memory_pressure(&mut self) {
        ManagedResource::on_memory_pressure(self);
    }

    fn on_idle(&mut self) {
        ManagedResource::on_idle(self);
    }

    fn on_stop(&mut self) {
        ManagedResource::on_stop(self);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ManagedByteBufferPolicy {
    FrameScratch,
    WarmCache,
    PersistentState,
}

impl Default for ManagedByteBufferPolicy {
    fn default() -> Self {
        Self::FrameScratch
    }
}

/// Runtime-managed reusable byte buffer for scratch, cache, or persistent byte state.
#[derive(Clone, Debug, Default)]
pub struct ManagedByteBuffer {
    bytes: Vec<u8>,
    live_len: usize,
    touched_bytes: u64,
    allocation_events: u64,
    policy: ManagedByteBufferPolicy,
}

impl ManagedByteBuffer {
    pub fn frame_scratch() -> Self {
        Self {
            policy: ManagedByteBufferPolicy::FrameScratch,
            ..Default::default()
        }
    }

    pub fn warm_cache() -> Self {
        Self {
            policy: ManagedByteBufferPolicy::WarmCache,
            ..Default::default()
        }
    }

    pub fn persistent_state() -> Self {
        Self {
            policy: ManagedByteBufferPolicy::PersistentState,
            ..Default::default()
        }
    }

    pub fn prepare(&mut self, len: usize) -> &mut [u8] {
        if self.bytes.capacity() < len {
            self.allocation_events = self.allocation_events.saturating_add(1);
        }
        if self.bytes.len() < len {
            self.bytes.resize(len, 0);
        } else {
            self.bytes[..len].fill(0);
            self.bytes.truncate(len);
        }
        self.live_len = len;
        self.touched_bytes = self.touched_bytes.saturating_add(len as u64);
        &mut self.bytes[..len]
    }

    pub fn reserve_exact(&mut self, capacity: usize) {
        if self.bytes.capacity() < capacity {
            self.bytes
                .reserve_exact(capacity.saturating_sub(self.bytes.capacity()));
            self.allocation_events = self.allocation_events.saturating_add(1);
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.live_len]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.touched_bytes = self.touched_bytes.saturating_add(self.live_len as u64);
        &mut self.bytes[..self.live_len]
    }

    pub fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    pub fn len(&self) -> usize {
        self.live_len
    }

    pub fn clear_live(&mut self) {
        self.live_len = 0;
        self.bytes.clear();
    }
}

impl ManagedResource for ManagedByteBuffer {
    fn live_bytes(&self) -> u64 {
        self.live_len as u64
    }

    fn retained_bytes(&self) -> u64 {
        self.bytes.capacity() as u64
    }

    fn touched_bytes(&self) -> u64 {
        self.touched_bytes
    }

    fn allocation_events(&self) -> u64 {
        self.allocation_events
    }

    fn before_frame(&mut self) {
        if self.policy == ManagedByteBufferPolicy::FrameScratch {
            self.clear_live();
            self.touched_bytes = 0;
        }
    }

    fn after_frame(&mut self) {
        if self.policy == ManagedByteBufferPolicy::FrameScratch {
            self.touched_bytes = 0;
        }
    }

    fn on_memory_pressure(&mut self) {
        match self.policy {
            ManagedByteBufferPolicy::FrameScratch => {
                self.clear_live();
                self.bytes.shrink_to_fit();
            }
            ManagedByteBufferPolicy::WarmCache => {
                self.bytes.shrink_to(self.live_len);
            }
            ManagedByteBufferPolicy::PersistentState => {}
        }
        self.touched_bytes = 0;
    }

    fn on_idle(&mut self) {
        match self.policy {
            ManagedByteBufferPolicy::FrameScratch | ManagedByteBufferPolicy::WarmCache => {
                self.clear_live();
                self.bytes.shrink_to_fit();
            }
            ManagedByteBufferPolicy::PersistentState => {
                self.live_len = 0;
            }
        }
        self.touched_bytes = 0;
    }

    fn on_stop(&mut self) {
        self.clear_live();
        self.bytes.shrink_to_fit();
        self.touched_bytes = 0;
    }
}

/// Shared runtime state store keyed by node id.
#[derive(Default, Clone)]
pub struct StateStore {
    inner: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    native: Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>>,
    resources: Arc<RwLock<HashMap<String, HashMap<String, ResourceEntry>>>>,
}

impl StateStore {
    pub fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.inner.read().ok().and_then(|m| m.get(key).cloned())
    }

    /// Fallible getter for raw values.
    pub fn get_result(&self, key: &str) -> Result<Option<serde_json::Value>, String> {
        let guard = self
            .inner
            .read()
            .map_err(|_| "state lock poisoned".to_string())?;
        Ok(guard.get(key).cloned())
    }

    /// Fallible getter with error context.
    pub fn get_checked<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, String> {
        let guard = self
            .inner
            .read()
            .map_err(|_| "state lock poisoned".to_string())?;
        if let Some(val) = guard.get(key) {
            serde_json::from_value(val.clone())
                .map(Some)
                .map_err(|e| format!("serde error: {e}"))
        } else {
            Ok(None)
        }
    }

    pub fn get_typed<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.get(key).and_then(|v| serde_json::from_value(v).ok())
    }

    /// Fallible getter for native typed values stored without JSON serialization.
    pub fn get_native<T: Clone + Send + Sync + 'static>(
        &self,
        key: &str,
    ) -> Result<Option<T>, String> {
        let guard = self
            .native
            .read()
            .map_err(|_| "state native lock poisoned".to_string())?;
        let Some(value) = guard.get(key) else {
            return Ok(None);
        };
        value
            .downcast_ref::<T>()
            .cloned()
            .map(Some)
            .ok_or_else(|| format!("state type mismatch for key '{key}'"))
    }

    /// Fallible getter that moves a native typed value out of the store without cloning it.
    pub fn take_native<T: Send + Sync + 'static>(&self, key: &str) -> Result<Option<T>, String> {
        let mut guard = self
            .native
            .write()
            .map_err(|_| "state native lock poisoned".to_string())?;
        let Some(value) = guard.remove(key) else {
            return Ok(None);
        };
        match value.downcast::<T>() {
            Ok(value) => Ok(Some(*value)),
            Err(value) => {
                guard.insert(key.to_string(), value);
                Err(format!("state type mismatch for key '{key}'"))
            }
        }
    }

    pub fn set(&self, key: &str, value: serde_json::Value) -> Result<(), String> {
        let mut m = self
            .inner
            .write()
            .map_err(|_| "state lock poisoned".to_string())?;
        m.insert(key.to_string(), value);
        drop(m);
        if let Ok(mut native) = self.native.write() {
            native.remove(key);
        }
        Ok(())
    }

    pub fn set_typed<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<(), String> {
        let json = serde_json::to_value(value).map_err(|e| format!("serde error: {e}"))?;
        self.set(key, json)
    }

    /// Store a native typed value without serializing it through `serde_json`.
    pub fn set_native<T: Send + Sync + 'static>(&self, key: &str, value: T) -> Result<(), String> {
        let mut native = self
            .native
            .write()
            .map_err(|_| "state native lock poisoned".to_string())?;
        native.insert(key.to_string(), Box::new(value));
        drop(native);
        if let Ok(mut json) = self.inner.write() {
            json.remove(key);
        }
        Ok(())
    }

    pub fn record_node_resource_usage(
        &self,
        node_id: &str,
        name: &str,
        class: ResourceClass,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), String> {
        let mut resources = self
            .resources
            .write()
            .map_err(|_| "state resource lock poisoned".to_string())?;
        let node_resources = resources.entry(node_id.to_string()).or_default();
        node_resources.insert(
            name.to_string(),
            ResourceEntry {
                class,
                storage: ResourceStorage::Usage(ResourceUsage::new(live_bytes, retained_bytes)),
            },
        );
        Ok(())
    }

    pub fn with_node_resource<T, R, Init, F>(
        &self,
        node_id: &str,
        name: &str,
        class: ResourceClass,
        init: Init,
        f: F,
    ) -> Result<R, String>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        let mut resources = self
            .resources
            .write()
            .map_err(|_| "state resource lock poisoned".to_string())?;
        let node_resources = resources.entry(node_id.to_string()).or_default();
        let mut init = Some(init);
        let entry = node_resources
            .entry(name.to_string())
            .or_insert_with(|| ResourceEntry {
                class,
                storage: ResourceStorage::Managed(Box::new(init
                    .take()
                    .expect("resource initializer should exist for insertion")(
                ))),
            });
        if entry.class != class {
            return Err(format!(
                "resource class mismatch for node '{node_id}' resource '{name}'"
            ));
        }
        let managed = match &mut entry.storage {
            ResourceStorage::Managed(managed) => managed,
            ResourceStorage::Usage(_) => {
                return Err(format!(
                    "resource '{name}' on node '{node_id}' is tracked as usage-only"
                ));
            }
        };
        let typed = managed.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
            format!("resource type mismatch for node '{node_id}' resource '{name}'")
        })?;
        Ok(f(typed))
    }

    pub fn begin_node_resource_frame(&self, node_id: &str) -> Result<(), String> {
        self.apply_node_resource_lifecycle(node_id, ResourceLifecycleEvent::BeforeFrame)
    }

    pub fn release_node_resources(&self, node_id: &str) -> Result<(), String> {
        let mut resources = self
            .resources
            .write()
            .map_err(|_| "state resource lock poisoned".to_string())?;
        if let Some(mut node_resources) = resources.remove(node_id) {
            for entry in node_resources.values_mut() {
                entry.apply_lifecycle(ResourceLifecycleEvent::Stop);
            }
        }
        Ok(())
    }

    pub fn apply_node_resource_lifecycle(
        &self,
        node_id: &str,
        event: ResourceLifecycleEvent,
    ) -> Result<(), String> {
        if matches!(event, ResourceLifecycleEvent::Stop) {
            return self.release_node_resources(node_id);
        }
        let mut resources = self
            .resources
            .write()
            .map_err(|_| "state resource lock poisoned".to_string())?;
        let mut remove_node = false;
        if let Some(node_resources) = resources.get_mut(node_id) {
            for entry in node_resources.values_mut() {
                entry.apply_lifecycle(event);
            }
            if !remove_node {
                node_resources.retain(|_, entry| {
                    entry.usage() != ResourceUsage::default()
                        || matches!(&entry.storage, ResourceStorage::Managed(_))
                });
                remove_node = node_resources.is_empty();
            }
        }
        if remove_node {
            resources.remove(node_id);
        }
        Ok(())
    }

    pub fn apply_resource_lifecycle(&self, event: ResourceLifecycleEvent) -> Result<(), String> {
        if matches!(event, ResourceLifecycleEvent::Stop) {
            let mut resources = self
                .resources
                .write()
                .map_err(|_| "state resource lock poisoned".to_string())?;
            for node_resources in resources.values_mut() {
                for entry in node_resources.values_mut() {
                    entry.apply_lifecycle(ResourceLifecycleEvent::Stop);
                }
            }
            resources.clear();
            return Ok(());
        }
        let mut resources = self
            .resources
            .write()
            .map_err(|_| "state resource lock poisoned".to_string())?;
        let mut empty_nodes = Vec::new();
        for (node_id, node_resources) in resources.iter_mut() {
            for entry in node_resources.values_mut() {
                entry.apply_lifecycle(event);
            }
            node_resources.retain(|_, entry| {
                entry.usage() != ResourceUsage::default()
                    || matches!(&entry.storage, ResourceStorage::Managed(_))
            });
            if node_resources.is_empty() {
                empty_nodes.push(node_id.clone());
            }
        }
        for node_id in empty_nodes {
            resources.remove(&node_id);
        }
        Ok(())
    }

    pub fn snapshot_node_resources(&self, node_id: &str) -> Result<NodeResourceSnapshot, String> {
        let resources = self
            .resources
            .read()
            .map_err(|_| "state resource lock poisoned".to_string())?;
        let mut snapshot = NodeResourceSnapshot::default();
        if let Some(node_resources) = resources.get(node_id) {
            for entry in node_resources.values() {
                snapshot.add_usage(entry.class, entry.usage());
            }
        }
        Ok(snapshot)
    }

    pub fn dump_json(&self) -> Result<String, String> {
        let m = self
            .inner
            .read()
            .map_err(|_| "state lock poisoned".to_string())?;
        serde_json::to_string(&*m).map_err(|e| format!("serde error: {e}"))
    }

    pub fn load_json(&self, json: &str) -> Result<(), String> {
        let map = serde_json::from_str::<HashMap<String, serde_json::Value>>(json)
            .map_err(|e| format!("serde error: {e}"))?;
        let mut guard = self
            .inner
            .write()
            .map_err(|_| "state lock poisoned".to_string())?;
        *guard = map;
        drop(guard);
        if let Ok(mut native) = self.native.write() {
            native.clear();
        }
        if let Ok(mut resources) = self.resources.write() {
            resources.clear();
        }
        Ok(())
    }
}

/// Execution context passed to nodes.
pub struct ExecutionContext {
    pub state: StateStore,
    pub node_id: Arc<str>,
    pub metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
    /// Graph-level metadata (typed values) shared by all nodes in the graph.
    pub graph_metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
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

    pub fn before_frame(&self) -> Result<(), String> {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::BeforeFrame)
    }

    pub fn after_frame(&self) -> Result<(), String> {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::AfterFrame)
    }

    pub fn on_memory_pressure(&self) -> Result<(), String> {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::MemoryPressure)
    }

    pub fn on_idle(&self) -> Result<(), String> {
        self.state
            .apply_node_resource_lifecycle(self.node_id, ResourceLifecycleEvent::Idle)
    }

    pub fn on_stop(&self) -> Result<(), String> {
        self.state.release_node_resources(self.node_id)
    }

    pub fn snapshot(&self) -> Result<NodeResourceSnapshot, String> {
        self.state.snapshot_node_resources(self.node_id)
    }

    pub fn record_frame_scratch_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), String> {
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
    ) -> Result<(), String> {
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
    ) -> Result<(), String> {
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
    ) -> Result<R, String>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.state
            .with_node_resource(self.node_id, name, ResourceClass::FrameScratch, init, f)
    }

    pub fn with_warm_cache<T, R, Init, F>(&self, name: &str, init: Init, f: F) -> Result<R, String>
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
    ) -> Result<R, String>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.state
            .with_node_resource(self.node_id, name, ResourceClass::PersistentState, init, f)
    }

    pub fn with_frame_scratch_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, String>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.with_frame_scratch(name, ManagedByteBuffer::frame_scratch, |buffer| {
            let bytes = buffer.prepare(len);
            f(bytes)
        })
    }

    pub fn with_warm_cache_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, String>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.with_warm_cache(name, ManagedByteBuffer::warm_cache, |buffer| {
            let bytes = buffer.prepare(len);
            f(bytes)
        })
    }

    pub fn with_persistent_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, String>
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

    pub fn begin_resource_frame(&self) -> Result<(), String> {
        self.resources().before_frame()
    }

    pub fn snapshot_resources(&self) -> Result<NodeResourceSnapshot, String> {
        self.resources().snapshot()
    }

    pub fn end_resource_frame(&self) -> Result<(), String> {
        self.resources().after_frame()
    }

    pub fn apply_memory_pressure(&self) -> Result<(), String> {
        self.resources().on_memory_pressure()
    }

    pub fn notify_idle(&self) -> Result<(), String> {
        self.resources().on_idle()
    }

    pub fn release_resources(&self) -> Result<(), String> {
        self.resources().on_stop()
    }

    pub fn record_frame_scratch_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), String> {
        self.resources()
            .record_frame_scratch_bytes(name, live_bytes, retained_bytes)
    }

    pub fn record_warm_cache_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), String> {
        self.resources()
            .record_warm_cache_bytes(name, live_bytes, retained_bytes)
    }

    pub fn record_persistent_state_bytes(
        &self,
        name: &str,
        live_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(), String> {
        self.resources()
            .record_persistent_state_bytes(name, live_bytes, retained_bytes)
    }

    pub fn with_frame_scratch<T, R, Init, F>(
        &self,
        name: &str,
        init: Init,
        f: F,
    ) -> Result<R, String>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.resources().with_frame_scratch(name, init, f)
    }

    pub fn with_warm_cache<T, R, Init, F>(&self, name: &str, init: Init, f: F) -> Result<R, String>
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
    ) -> Result<R, String>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        self.resources().with_persistent_state(name, init, f)
    }

    pub fn with_frame_scratch_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, String>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.resources().with_frame_scratch_bytes(name, len, f)
    }

    pub fn with_warm_cache_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, String>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.resources().with_warm_cache_bytes(name, len, f)
    }

    pub fn with_persistent_bytes<R, F>(&self, name: &str, len: usize, f: F) -> Result<R, String>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.resources().with_persistent_bytes(name, len, f)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{
        ExecutionContext, ManagedResource, NodeResourceSnapshot, ResourceClass,
        ResourceLifecycleEvent, StateStore,
    };

    #[test]
    fn begin_resource_frame_clears_only_frame_scratch_live_bytes() {
        let state = StateStore::default();
        state
            .record_node_resource_usage("node", "scratch", ResourceClass::FrameScratch, 64, 128)
            .unwrap();
        state
            .record_node_resource_usage("node", "cache", ResourceClass::WarmCache, 32, 96)
            .unwrap();

        state.begin_node_resource_frame("node").unwrap();

        let snapshot = state.snapshot_node_resources("node").unwrap();
        assert_eq!(
            snapshot,
            NodeResourceSnapshot {
                frame_scratch: super::ResourceUsage {
                    live_bytes: 0,
                    retained_bytes: 128,
                    touched_bytes: 0,
                    allocation_events: 0,
                },
                warm_cache: super::ResourceUsage {
                    live_bytes: 32,
                    retained_bytes: 96,
                    touched_bytes: 32,
                    allocation_events: 0,
                },
                persistent_state: super::ResourceUsage::default(),
            }
        );
    }

    #[test]
    fn snapshot_node_resources_aggregates_by_class() {
        let state = StateStore::default();
        state
            .record_node_resource_usage("node", "scratch-a", ResourceClass::FrameScratch, 10, 20)
            .unwrap();
        state
            .record_node_resource_usage("node", "scratch-b", ResourceClass::FrameScratch, 5, 12)
            .unwrap();
        state
            .record_node_resource_usage("node", "persistent", ResourceClass::PersistentState, 7, 9)
            .unwrap();

        let snapshot = state.snapshot_node_resources("node").unwrap();
        assert_eq!(snapshot.frame_scratch.live_bytes, 15);
        assert_eq!(snapshot.frame_scratch.retained_bytes, 32);
        assert_eq!(snapshot.persistent_state.live_bytes, 7);
        assert_eq!(snapshot.persistent_state.retained_bytes, 9);
    }

    #[test]
    fn memory_pressure_compacts_caches_and_drops_frame_scratch() {
        let state = StateStore::default();
        state
            .record_node_resource_usage("node", "scratch", ResourceClass::FrameScratch, 10, 20)
            .unwrap();
        state
            .record_node_resource_usage("node", "cache", ResourceClass::WarmCache, 8, 30)
            .unwrap();

        state
            .apply_node_resource_lifecycle("node", ResourceLifecycleEvent::MemoryPressure)
            .unwrap();

        let snapshot = state.snapshot_node_resources("node").unwrap();
        assert_eq!(snapshot.frame_scratch.live_bytes, 0);
        assert_eq!(snapshot.frame_scratch.retained_bytes, 0);
        assert_eq!(snapshot.warm_cache.live_bytes, 8);
        assert_eq!(snapshot.warm_cache.retained_bytes, 8);
    }

    #[test]
    fn stop_lifecycle_removes_node_resources() {
        let state = StateStore::default();
        state
            .record_node_resource_usage("node", "persistent", ResourceClass::PersistentState, 5, 9)
            .unwrap();

        state
            .apply_node_resource_lifecycle("node", ResourceLifecycleEvent::Stop)
            .unwrap();

        assert_eq!(
            state.snapshot_node_resources("node").unwrap(),
            NodeResourceSnapshot::default()
        );
    }

    struct TestManagedResource {
        live: u64,
        retained: u64,
        before_frame_runs: Arc<AtomicUsize>,
        after_frame_runs: Arc<AtomicUsize>,
        memory_pressure_runs: Arc<AtomicUsize>,
        idle_runs: Arc<AtomicUsize>,
        stop_runs: Arc<AtomicUsize>,
    }

    impl ManagedResource for TestManagedResource {
        fn live_bytes(&self) -> u64 {
            self.live
        }

        fn retained_bytes(&self) -> u64 {
            self.retained
        }

        fn before_frame(&mut self) {
            self.live = 0;
            self.before_frame_runs.fetch_add(1, Ordering::SeqCst);
        }

        fn after_frame(&mut self) {
            self.after_frame_runs.fetch_add(1, Ordering::SeqCst);
        }

        fn on_memory_pressure(&mut self) {
            self.retained = self.live;
            self.memory_pressure_runs.fetch_add(1, Ordering::SeqCst);
        }

        fn on_idle(&mut self) {
            self.live = 0;
            self.idle_runs.fetch_add(1, Ordering::SeqCst);
        }

        fn on_stop(&mut self) {
            self.stop_runs.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn test_context(state: StateStore) -> ExecutionContext {
        ExecutionContext {
            state,
            node_id: Arc::<str>::from("node"),
            metadata: Arc::new(BTreeMap::new()),
            graph_metadata: Arc::new(BTreeMap::new()),
            #[cfg(feature = "gpu")]
            gpu: None,
        }
    }

    #[test]
    fn managed_resources_are_reused_and_snapshotted() {
        let state = StateStore::default();
        let ctx = test_context(state);
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let pressure = Arc::new(AtomicUsize::new(0));
        let idle = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicUsize::new(0));

        ctx.with_warm_cache(
            "cache",
            || TestManagedResource {
                live: 10,
                retained: 20,
                before_frame_runs: before.clone(),
                after_frame_runs: after.clone(),
                memory_pressure_runs: pressure.clone(),
                idle_runs: idle.clone(),
                stop_runs: stop.clone(),
            },
            |resource| {
                resource.live = 18;
                resource.retained = 30;
            },
        )
        .unwrap();

        ctx.with_warm_cache::<TestManagedResource, _, _, _>(
            "cache",
            || unreachable!("managed resource should already exist"),
            |resource| {
                resource.live = 22;
            },
        )
        .unwrap();

        let snapshot = ctx.snapshot_resources().unwrap();
        assert_eq!(snapshot.warm_cache.live_bytes, 22);
        assert_eq!(snapshot.warm_cache.retained_bytes, 30);

        ctx.begin_resource_frame().unwrap();
        ctx.end_resource_frame().unwrap();
        ctx.apply_memory_pressure().unwrap();
        ctx.notify_idle().unwrap();
        ctx.release_resources().unwrap();

        assert_eq!(before.load(Ordering::SeqCst), 1);
        assert_eq!(after.load(Ordering::SeqCst), 1);
        assert_eq!(pressure.load(Ordering::SeqCst), 1);
        assert_eq!(idle.load(Ordering::SeqCst), 1);
        assert_eq!(stop.load(Ordering::SeqCst), 1);
        assert_eq!(
            ctx.snapshot_resources().unwrap(),
            NodeResourceSnapshot::default()
        );
    }

    #[test]
    fn managed_byte_buffer_helpers_track_touch_and_reuse_capacity() {
        let state = StateStore::default();
        let ctx = test_context(state);

        ctx.with_frame_scratch_bytes("scratch", 16, |bytes| {
            bytes[0] = 7;
            bytes[15] = 9;
        })
        .unwrap();

        let first = ctx.snapshot_resources().unwrap();
        let first_retained = first.frame_scratch.retained_bytes;
        assert_eq!(first.frame_scratch.live_bytes, 16);
        assert_eq!(first.frame_scratch.touched_bytes, 16);
        assert_eq!(first.frame_scratch.allocation_events, 1);
        assert!(first_retained >= 16);

        ctx.begin_resource_frame().unwrap();

        let reset = ctx.snapshot_resources().unwrap();
        assert_eq!(reset.frame_scratch.live_bytes, 0);
        assert_eq!(reset.frame_scratch.touched_bytes, 0);
        assert_eq!(reset.frame_scratch.retained_bytes, first_retained);
        assert_eq!(reset.frame_scratch.allocation_events, 1);

        ctx.with_frame_scratch_bytes("scratch", 8, |bytes| bytes.fill(0xAB))
            .unwrap();

        let second = ctx.snapshot_resources().unwrap();
        assert_eq!(second.frame_scratch.live_bytes, 8);
        assert_eq!(second.frame_scratch.touched_bytes, 8);
        assert_eq!(second.frame_scratch.retained_bytes, first_retained);
        assert_eq!(second.frame_scratch.allocation_events, 1);
    }
}
