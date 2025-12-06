use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

/// Shared runtime state store keyed by node id.
#[derive(Default, Clone)]
pub struct StateStore {
    inner: Arc<RwLock<HashMap<String, serde_json::Value>>>,
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

    pub fn set(&self, key: &str, value: serde_json::Value) -> Result<(), String> {
        let mut m = self
            .inner
            .write()
            .map_err(|_| "state lock poisoned".to_string())?;
        m.insert(key.to_string(), value);
        Ok(())
    }

    pub fn set_typed<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<(), String> {
        let json = serde_json::to_value(value).map_err(|e| format!("serde error: {e}"))?;
        self.set(key, json)
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
        Ok(())
    }
}

/// Execution context passed to nodes.
pub struct ExecutionContext {
    pub state: StateStore,
    pub metadata: BTreeMap<String, daedalus_data::model::Value>,
    /// Graph-level metadata (typed values) shared by all nodes in the graph.
    pub graph_metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
    #[cfg(feature = "gpu")]
    pub gpu: Option<GpuContextHandle>,
}

#[cfg(feature = "gpu")]
pub type GpuContextHandle = daedalus_gpu::GpuContextHandle;
