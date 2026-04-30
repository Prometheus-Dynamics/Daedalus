use std::sync::{Arc, OnceLock, RwLock};

pub type RuntimeDataSizeInspector = fn(&dyn std::any::Any) -> Option<u64>;

#[derive(Clone, Debug, Default)]
pub struct RuntimeDataSizeInspectors {
    inspectors: Arc<RwLock<Vec<RuntimeDataSizeInspector>>>,
}

impl RuntimeDataSizeInspectors {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn global() -> Self {
        static INSPECTORS: OnceLock<RuntimeDataSizeInspectors> = OnceLock::new();
        INSPECTORS.get_or_init(Self::new).clone()
    }

    pub fn register(&self, inspector: RuntimeDataSizeInspector) {
        let mut inspectors = self
            .inspectors
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let inspector_addr = inspector as usize;
        if inspectors
            .iter()
            .any(|existing| *existing as usize == inspector_addr)
        {
            return;
        }
        inspectors.push(inspector);
    }

    pub fn estimate_payload_bytes(&self, payload: &daedalus_transport::Payload) -> Option<u64> {
        if let Some(bytes) = payload.bytes_estimate() {
            return Some(bytes);
        }
        let value = payload.value_any()?;
        let inspectors = self
            .inspectors
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inspectors.iter().find_map(|inspector| inspector(value))
    }
}

pub fn register_runtime_data_size_inspector(inspector: RuntimeDataSizeInspector) {
    RuntimeDataSizeInspectors::global().register(inspector);
}

pub fn estimate_payload_bytes(payload: &daedalus_transport::Payload) -> Option<u64> {
    RuntimeDataSizeInspectors::global().estimate_payload_bytes(payload)
}

#[cfg(test)]
mod tests {
    use super::RuntimeDataSizeInspectors;

    struct SizedPayload;

    fn sized_payload_bytes(value: &dyn std::any::Any) -> Option<u64> {
        value.is::<SizedPayload>().then_some(42)
    }

    #[test]
    fn owned_runtime_data_size_inspectors_do_not_leak_entries() {
        let left = RuntimeDataSizeInspectors::new();
        let right = RuntimeDataSizeInspectors::new();
        left.register(sized_payload_bytes);

        let payload = daedalus_transport::Payload::owned("test:sized_payload", SizedPayload);

        assert_eq!(left.estimate_payload_bytes(&payload), Some(42));
        assert_eq!(right.estimate_payload_bytes(&payload), None);
    }
}
