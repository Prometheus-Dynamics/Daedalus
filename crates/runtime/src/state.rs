mod context;
mod resources;

pub use context::{ExecutionContext, RuntimeResources};
pub use resources::{
    ManagedByteBuffer, ManagedResource, NodeResourceSnapshot, ResourceClass,
    ResourceLifecycleEvent, ResourceUsage,
};

pub use crate::StateError;
use resources::{ResourceEntry, ResourceStorage, SharedNodeResources};
use std::any::Any;
use std::collections::{BTreeMap, HashMap, hash_map::Entry};
use std::sync::{Arc, RwLock};

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
        let mut node_resources = self
            .node_resources
            .lock()
            .map_err(|_| StateError::lock("state node resource"))?;
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
        match self.inner.read() {
            Ok(m) => m.get(key).cloned(),
            Err(_) => {
                tracing::warn!(
                    target: "daedalus_runtime::state",
                    key,
                    "state read lock poisoned"
                );
                None
            }
        }
    }

    /// Fallible getter for raw values.
    pub fn get_result(&self, key: &str) -> Result<Option<serde_json::Value>, StateError> {
        let guard = self.inner.read().map_err(|_| StateError::lock("state"))?;
        Ok(guard.get(key).cloned())
    }

    /// Fallible getter with error context.
    pub fn get_checked<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StateError> {
        let guard = self.inner.read().map_err(|_| StateError::lock("state"))?;
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
        let guard = self
            .native
            .read()
            .map_err(|_| StateError::lock("state native"))?;
        let Some(value) = guard.get(key) else {
            return Ok(None);
        };
        value
            .downcast_ref::<T>()
            .cloned()
            .map(Some)
            .ok_or_else(|| StateError::state_type_mismatch(key))
    }

    /// Fallible getter that moves a native typed value out of the store without cloning it.
    pub fn take_native<T: Send + Sync + 'static>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StateError> {
        let mut guard = self
            .native
            .write()
            .map_err(|_| StateError::lock("state native"))?;
        let Some(value) = guard.remove(key) else {
            return Ok(None);
        };
        match value.downcast::<T>() {
            Ok(value) => Ok(Some(*value)),
            Err(value) => {
                guard.insert(key.to_string(), value);
                Err(StateError::state_type_mismatch(key))
            }
        }
    }

    pub fn set(&self, key: &str, value: serde_json::Value) -> Result<(), StateError> {
        let mut m = self.inner.write().map_err(|_| StateError::lock("state"))?;
        m.insert(key.to_string(), value);
        drop(m);
        match self.native.write() {
            Ok(mut native) => {
                native.remove(key);
            }
            Err(_) => {
                tracing::warn!(
                    target: "daedalus_runtime::state",
                    key,
                    "state native lock poisoned while clearing stale native value"
                );
            }
        }
        Ok(())
    }

    pub fn set_typed<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<(), StateError> {
        let json = serde_json::to_value(value)?;
        self.set(key, json)
    }

    /// Store a native typed value without serializing it through `serde_json`.
    pub fn set_native<T: Send + Sync + 'static>(
        &self,
        key: &str,
        value: T,
    ) -> Result<(), StateError> {
        let mut native = self
            .native
            .write()
            .map_err(|_| StateError::lock("state native"))?;
        native.insert(key.to_string(), Box::new(value));
        drop(native);
        match self.inner.write() {
            Ok(mut json) => {
                json.remove(key);
            }
            Err(_) => {
                tracing::warn!(
                    target: "daedalus_runtime::state",
                    key,
                    "state lock poisoned while clearing stale json value"
                );
            }
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
    ) -> Result<(), StateError> {
        let node_resources = self.node_resources(node_id)?;
        let mut node_resources = node_resources
            .lock()
            .map_err(|_| StateError::lock("state node resource"))?;
        node_resources.insert(
            name.to_string(),
            ResourceEntry {
                class,
                storage: ResourceStorage::Usage(ResourceUsage::new(live_bytes, retained_bytes)),
            },
        );
        Ok(())
    }

    pub fn record_node_custom_metric(
        &self,
        node_id: &str,
        name: impl Into<String>,
        value: crate::executor::CustomMetricValue,
    ) -> Result<(), StateError> {
        let mut metrics = self
            .custom_metrics
            .write()
            .map_err(|_| StateError::lock("state custom metrics"))?;
        let entry = metrics.entry(node_id.to_string()).or_default();
        entry
            .entry(name.into())
            .and_modify(|existing| existing.merge(value.clone()))
            .or_insert(value);
        Ok(())
    }

    pub(crate) fn clear_node_custom_metrics(&self, node_id: &str) -> Result<(), StateError> {
        let mut metrics = self
            .custom_metrics
            .write()
            .map_err(|_| StateError::lock("state custom metrics"))?;
        metrics.remove(node_id);
        Ok(())
    }

    pub(crate) fn drain_node_custom_metrics(
        &self,
        node_id: &str,
    ) -> Result<BTreeMap<String, crate::executor::CustomMetricValue>, StateError> {
        let mut metrics = self
            .custom_metrics
            .write()
            .map_err(|_| StateError::lock("state custom metrics"))?;
        Ok(metrics.remove(node_id).unwrap_or_default())
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
        let node_resources = self.node_resources(node_id)?;
        let managed = {
            let mut node_resources = node_resources
                .lock()
                .map_err(|_| StateError::lock("state node resource"))?;
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

    fn node_resources(&self, node_id: &str) -> Result<SharedNodeResources, StateError> {
        {
            let resources = self
                .resources
                .read()
                .map_err(|_| StateError::lock("state resource"))?;
            if let Some(node_resources) = resources.get(node_id) {
                return Ok(Arc::clone(node_resources));
            }
        }

        let mut resources = self
            .resources
            .write()
            .map_err(|_| StateError::lock("state resource"))?;
        Ok(Arc::clone(
            resources
                .entry(node_id.to_string())
                .or_insert_with(|| Arc::new(std::sync::Mutex::new(HashMap::new()))),
        ))
    }

    pub fn begin_node_resource_frame(&self, node_id: &str) -> Result<(), StateError> {
        self.apply_node_resource_lifecycle(node_id, ResourceLifecycleEvent::BeforeFrame)
    }

    pub fn release_node_resources(&self, node_id: &str) -> Result<(), StateError> {
        let node_resources = self
            .resources
            .write()
            .map_err(|_| StateError::lock("state resource"))?
            .remove(node_id);
        if let Some(node_resources) = node_resources {
            let mut node_resources = node_resources
                .lock()
                .map_err(|_| StateError::lock("state node resource"))?;
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
    ) -> Result<(), StateError> {
        if matches!(event, ResourceLifecycleEvent::Stop) {
            return self.release_node_resources(node_id);
        }
        let Some(node_resources) = ({
            let resources = self
                .resources
                .read()
                .map_err(|_| StateError::lock("state resource"))?;
            resources.get(node_id).cloned()
        }) else {
            return Ok(());
        };

        let remove_node = {
            let mut node_resources = node_resources
                .lock()
                .map_err(|_| StateError::lock("state node resource"))?;
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
            self.remove_node_resources_if_current(node_id, &node_resources)?;
        }
        Ok(())
    }

    pub fn apply_resource_lifecycle(
        &self,
        event: ResourceLifecycleEvent,
    ) -> Result<(), StateError> {
        if matches!(event, ResourceLifecycleEvent::Stop) {
            let node_resources = self
                .resources
                .write()
                .map_err(|_| StateError::lock("state resource"))?
                .drain()
                .map(|(_, node_resources)| node_resources)
                .collect::<Vec<_>>();
            for node_resources in node_resources {
                let mut node_resources = node_resources
                    .lock()
                    .map_err(|_| StateError::lock("state node resource"))?;
                for entry in node_resources.values_mut() {
                    entry.apply_lifecycle(ResourceLifecycleEvent::Stop);
                }
            }
            return Ok(());
        }

        let node_resource_sets = {
            let resources = self
                .resources
                .read()
                .map_err(|_| StateError::lock("state resource"))?;
            resources
                .iter()
                .map(|(node_id, node_resources)| (node_id.clone(), Arc::clone(node_resources)))
                .collect::<Vec<_>>()
        };
        let mut empty_nodes = Vec::new();
        for (node_id, node_resources) in node_resource_sets {
            let mut node_resources_guard = node_resources
                .lock()
                .map_err(|_| StateError::lock("state node resource"))?;
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
            self.remove_node_resources_if_current(&node_id, &node_resources)?;
        }
        Ok(())
    }

    pub fn snapshot_node_resources(
        &self,
        node_id: &str,
    ) -> Result<NodeResourceSnapshot, StateError> {
        let node_resources = {
            let resources = self
                .resources
                .read()
                .map_err(|_| StateError::lock("state resource"))?;
            resources.get(node_id).cloned()
        };
        let mut snapshot = NodeResourceSnapshot::default();
        if let Some(node_resources) = node_resources {
            let node_resources = node_resources
                .lock()
                .map_err(|_| StateError::lock("state node resource"))?;
            for entry in node_resources.values() {
                snapshot.add_usage(entry.class, entry.usage());
            }
        }
        Ok(snapshot)
    }

    fn remove_node_resources_if_current(
        &self,
        node_id: &str,
        expected: &SharedNodeResources,
    ) -> Result<(), StateError> {
        let mut resources = self
            .resources
            .write()
            .map_err(|_| StateError::lock("state resource"))?;
        if resources
            .get(node_id)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            resources.remove(node_id);
        }
        Ok(())
    }

    pub fn dump_json(&self) -> Result<String, StateError> {
        let m = self.inner.read().map_err(|_| StateError::lock("state"))?;
        serde_json::to_string(&*m).map_err(Into::into)
    }

    pub fn load_json(&self, json: &str) -> Result<(), StateError> {
        let map = serde_json::from_str::<HashMap<String, serde_json::Value>>(json)?;
        let mut guard = self.inner.write().map_err(|_| StateError::lock("state"))?;
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

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
