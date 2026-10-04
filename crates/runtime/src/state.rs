mod context;
mod resources;

pub use context::{ExecutionContext, RuntimeResources};
pub use resources::{
    ManagedByteBuffer, ManagedResource, NodeResourceSnapshot, ResourceClass,
    ResourceLifecycleEvent, ResourceUsage,
};

pub use crate::StateError;
use parking_lot::RwLock;
use resources::{ResourceEntry, ResourceStorage, SharedNodeResources};
use std::any::Any;
use std::collections::{BTreeMap, HashMap, hash_map::Entry};
use std::sync::Arc;

/// Shared runtime state store keyed by node id.
#[derive(Default, Clone)]
pub struct StateStore {
    inner: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    native: Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>>,
    resources: Arc<RwLock<HashMap<String, SharedNodeResources>>>,
    custom_metrics:
        Arc<RwLock<HashMap<String, BTreeMap<String, crate::executor::CustomMetricValue>>>>,
}

struct ManagedResourceRestore {
    node_resources: SharedNodeResources,
    node_id: String,
    name: String,
    class: ResourceClass,
    managed: Option<Box<dyn resources::ManagedResourceBox>>,
}

impl ManagedResourceRestore {
    fn new(
        node_resources: SharedNodeResources,
        node_id: String,
        name: String,
        class: ResourceClass,
        managed: Box<dyn resources::ManagedResourceBox>,
    ) -> Self {
        Self {
            node_resources,
            node_id,
            name,
            class,
            managed: Some(managed),
        }
    }

    fn managed_mut(&mut self) -> Option<&mut Box<dyn resources::ManagedResourceBox>> {
        self.managed.as_mut()
    }

    fn restore(mut self) -> Result<(), StateError> {
        self.restore_inner()
    }

    fn restore_inner(&mut self) -> Result<(), StateError> {
        let Some(managed) = self.managed.take() else {
            return Ok(());
        };
        let mut node_resources = self.node_resources.lock();
        let entry = node_resources
            .entry(self.name.clone())
            .or_insert(ResourceEntry {
                class: self.class,
                storage: ResourceStorage::InUse(ResourceUsage::default()),
            });
        entry.storage = ResourceStorage::Managed(managed);
        if entry.class != self.class {
            return Err(StateError::resource_class_mismatch(
                &self.node_id,
                &self.name,
            ));
        }
        Ok(())
    }
}

impl Drop for ManagedResourceRestore {
    fn drop(&mut self) {
        let _ = self.restore_inner();
    }
}

impl StateStore {
    pub fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.inner.read().get(key).cloned()
    }

    /// Fallible getter with error context.
    pub fn get_checked<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StateError> {
        let guard = self.inner.read();
        if let Some(val) = guard.get(key) {
            serde_json::from_value(val.clone())
                .map(Some)
                .map_err(Into::into)
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
    ) -> Result<Option<T>, StateError> {
        let guard = self.native.read();
        let Some(value) = guard.get(key) else {
            return Ok(None);
        };
        value
            .downcast_ref::<Option<T>>()
            .cloned()
            .ok_or_else(|| StateError::state_type_mismatch(key))
    }

    /// Fallible getter that moves a native typed value out of the store without cloning it.
    pub fn take_native<T: Send + Sync + 'static>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StateError> {
        let mut guard = self.native.write();
        let Some(slot) = guard.get_mut(key) else {
            return Ok(None);
        };
        slot.downcast_mut::<Option<T>>()
            .map(Option::take)
            .ok_or_else(|| StateError::state_type_mismatch(key))
    }

    pub fn set(&self, key: &str, value: serde_json::Value) {
        self.inner.write().insert(key.to_string(), value);
        self.native.write().remove(key);
    }

    pub fn set_typed<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<(), StateError> {
        self.set(key, serde_json::to_value(value)?);
        Ok(())
    }

    /// Store a native typed value without serializing it through `serde_json`.
    ///
    /// Values live in per-key `Option<T>` slots: [`Self::take_native`] leaves the slot behind,
    /// so a take/set cycle with the same key and type (per-tick node state) does not allocate.
    pub fn set_native<T: Send + Sync + 'static>(&self, key: &str, value: T) {
        let mut native = self.native.write();
        match native
            .get_mut(key)
            .and_then(|slot| slot.downcast_mut::<Option<T>>())
        {
            Some(slot) => *slot = Some(value),
            None => {
                native.insert(key.to_string(), Box::new(Some(value)));
            }
        }
        drop(native);
        if self.inner.read().contains_key(key) {
            self.inner.write().remove(key);
        }
    }

    pub fn record_node_resource_usage(
        &self,
        node_id: &str,
        name: &str,
        class: ResourceClass,
        live_bytes: u64,
        retained_bytes: u64,
    ) {
        self.node_resources(node_id).lock().insert(
            name.to_string(),
            ResourceEntry {
                class,
                storage: ResourceStorage::Usage(ResourceUsage::new(live_bytes, retained_bytes)),
            },
        );
    }

    pub fn record_node_custom_metric(
        &self,
        node_id: &str,
        name: impl Into<String>,
        value: crate::executor::CustomMetricValue,
    ) {
        self.custom_metrics
            .write()
            .entry(node_id.to_string())
            .or_default()
            .entry(name.into())
            .and_modify(|existing| existing.merge(value.clone()))
            .or_insert(value);
    }

    pub(crate) fn clear_node_custom_metrics(&self, node_id: &str) {
        self.custom_metrics.write().remove(node_id);
    }

    pub(crate) fn drain_node_custom_metrics(
        &self,
        node_id: &str,
    ) -> BTreeMap<String, crate::executor::CustomMetricValue> {
        self.custom_metrics
            .write()
            .remove(node_id)
            .unwrap_or_default()
    }

    pub fn with_node_resource<T, R, Init, F>(
        &self,
        node_id: &str,
        name: &str,
        class: ResourceClass,
        init: Init,
        f: F,
    ) -> Result<R, StateError>
    where
        T: ManagedResource,
        Init: FnOnce() -> T,
        F: FnOnce(&mut T) -> R,
    {
        let node_resources = self.node_resources(node_id);
        let managed = {
            let mut node_resources = node_resources.lock();
            let entry = match node_resources.entry(name.to_string()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(ResourceEntry {
                    class,
                    storage: ResourceStorage::Managed(Box::new(init())),
                }),
            };
            if entry.class != class {
                return Err(StateError::resource_class_mismatch(node_id, name));
            }
            match std::mem::replace(
                &mut entry.storage,
                ResourceStorage::InUse(ResourceUsage::default()),
            ) {
                ResourceStorage::Managed(managed) => {
                    let usage = managed.usage();
                    entry.storage = ResourceStorage::InUse(usage);
                    managed
                }
                ResourceStorage::Usage(usage) => {
                    entry.storage = ResourceStorage::Usage(usage);
                    return Err(StateError::resource_usage_only(node_id, name));
                }
                ResourceStorage::InUse(usage) => {
                    entry.storage = ResourceStorage::InUse(usage);
                    return Err(StateError::resource_already_borrowed(node_id, name));
                }
            }
        };

        let mut restore = ManagedResourceRestore::new(
            Arc::clone(&node_resources),
            node_id.to_string(),
            name.to_string(),
            class,
            managed,
        );

        let result = if let Some(typed) = restore
            .managed_mut()
            .and_then(|managed| managed.as_any_mut().downcast_mut::<T>())
        {
            f(typed)
        } else {
            restore.restore()?;
            return Err(StateError::resource_type_mismatch(node_id, name));
        };

        restore.restore()?;
        Ok(result)
    }

    fn node_resources(&self, node_id: &str) -> SharedNodeResources {
        {
            let resources = self.resources.read();
            if let Some(node_resources) = resources.get(node_id) {
                return Arc::clone(node_resources);
            }
        }

        Arc::clone(
            self.resources
                .write()
                .entry(node_id.to_string())
                .or_insert_with(|| Arc::new(parking_lot::Mutex::new(HashMap::new()))),
        )
    }

    pub fn begin_node_resource_frame(&self, node_id: &str) {
        self.apply_node_resource_lifecycle(node_id, ResourceLifecycleEvent::BeforeFrame)
    }

    pub fn release_node_resources(&self, node_id: &str) {
        let node_resources = self.resources.write().remove(node_id);
        if let Some(node_resources) = node_resources {
            for entry in node_resources.lock().values_mut() {
                entry.apply_lifecycle(ResourceLifecycleEvent::Stop);
            }
        }
    }

    pub fn apply_node_resource_lifecycle(&self, node_id: &str, event: ResourceLifecycleEvent) {
        if matches!(event, ResourceLifecycleEvent::Stop) {
            return self.release_node_resources(node_id);
        }
        let Some(node_resources) = ({
            let resources = self.resources.read();
            resources.get(node_id).cloned()
        }) else {
            return;
        };

        let remove_node = {
            let mut node_resources = node_resources.lock();
            for entry in node_resources.values_mut() {
                entry.apply_lifecycle(event);
            }
            node_resources.retain(|_, entry| {
                entry.usage() != ResourceUsage::default()
                    || matches!(
                        &entry.storage,
                        ResourceStorage::Managed(_) | ResourceStorage::InUse(_)
                    )
            });
            node_resources.is_empty()
        };
        if remove_node {
            self.remove_node_resources_if_current(node_id, &node_resources);
        }
    }

    pub fn apply_resource_lifecycle(&self, event: ResourceLifecycleEvent) {
        if matches!(event, ResourceLifecycleEvent::Stop) {
            let node_resources = self
                .resources
                .write()
                .drain()
                .map(|(_, node_resources)| node_resources)
                .collect::<Vec<_>>();
            for node_resources in node_resources {
                let mut node_resources = node_resources.lock();
                for entry in node_resources.values_mut() {
                    entry.apply_lifecycle(ResourceLifecycleEvent::Stop);
                }
            }
            return;
        }

        let node_resource_sets = {
            let resources = self.resources.read();
            resources
                .iter()
                .map(|(node_id, node_resources)| (node_id.clone(), Arc::clone(node_resources)))
                .collect::<Vec<_>>()
        };
        let mut empty_nodes = Vec::new();
        for (node_id, node_resources) in node_resource_sets {
            let mut node_resources_guard = node_resources.lock();
            for entry in node_resources_guard.values_mut() {
                entry.apply_lifecycle(event);
            }
            node_resources_guard.retain(|_, entry| {
                entry.usage() != ResourceUsage::default()
                    || matches!(
                        &entry.storage,
                        ResourceStorage::Managed(_) | ResourceStorage::InUse(_)
                    )
            });
            if node_resources_guard.is_empty() {
                empty_nodes.push((node_id, Arc::clone(&node_resources)));
            }
        }
        for (node_id, node_resources) in empty_nodes {
            self.remove_node_resources_if_current(&node_id, &node_resources);
        }
    }

    pub fn snapshot_node_resources(&self, node_id: &str) -> NodeResourceSnapshot {
        let node_resources = {
            let resources = self.resources.read();
            resources.get(node_id).cloned()
        };
        let mut snapshot = NodeResourceSnapshot::default();
        if let Some(node_resources) = node_resources {
            let node_resources = node_resources.lock();
            for entry in node_resources.values() {
                snapshot.add_usage(entry.class, entry.usage());
            }
        }
        snapshot
    }

    fn remove_node_resources_if_current(&self, node_id: &str, expected: &SharedNodeResources) {
        let mut resources = self.resources.write();
        if resources
            .get(node_id)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            resources.remove(node_id);
        }
    }

    pub fn dump_json(&self) -> Result<String, StateError> {
        let m = self.inner.read();
        serde_json::to_string(&*m).map_err(Into::into)
    }

    pub fn load_json(&self, json: &str) -> Result<(), StateError> {
        let map = serde_json::from_str::<HashMap<String, serde_json::Value>>(json)?;
        let mut guard = self.inner.write();
        *guard = map;
        drop(guard);
        self.native.write().clear();
        self.resources.write().clear();
        Ok(())
    }
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
