use daedalus_ffi_core::{BackendConfig, WireValue};
use thiserror::Error;

use crate::{DecodedInvokeResponse, RunnerPool, RunnerPoolError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateSyncPolicy {
    Optional,
    Required,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StateSyncResult {
    pub imported: bool,
    pub state: Option<WireValue>,
}

#[derive(Debug, Error)]
pub enum StateSyncError {
    #[error("response is missing required state for node `{node_id}`")]
    MissingState { node_id: String },
    #[error(transparent)]
    Runner(#[from] RunnerPoolError),
}

pub fn sync_response_state(
    pool: &RunnerPool,
    config: &BackendConfig,
    node_id: &str,
    decoded: &DecodedInvokeResponse,
    policy: StateSyncPolicy,
) -> Result<StateSyncResult, StateSyncError> {
    let Some(state) = decoded.state().cloned() else {
        return match policy {
            StateSyncPolicy::Optional => Ok(StateSyncResult {
                imported: false,
                state: None,
            }),
            StateSyncPolicy::Required => Err(StateSyncError::MissingState {
                node_id: node_id.into(),
            }),
        };
    };

    pool.import_state(config, node_id, state.clone())?;
    Ok(StateSyncResult {
        imported: true,
        state: Some(state),
    })
}

pub fn export_runner_state(
    pool: &RunnerPool,
    config: &BackendConfig,
    node_id: &str,
) -> Result<Option<WireValue>, StateSyncError> {
    pool.export_state(config, node_id).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use daedalus_ffi_core::{
        BackendKind, BackendRuntimeModel, InvokeRequest, InvokeResponse, WORKER_PROTOCOL_VERSION,
        WirePayloadHandle,
    };
    use daedalus_transport::{AccessMode, TypeKey};

    use super::*;
    use crate::{BackendRunner, RunnerHealth};

    #[derive(Clone, Default)]
    struct StateRunner {
        state: Arc<Mutex<Option<WireValue>>>,
    }

    impl BackendRunner for StateRunner {
        fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
            Ok(InvokeResponse {
                protocol_version: request.protocol_version,
                correlation_id: request.correlation_id,
                outputs: BTreeMap::new(),
                state: self
                    .state
                    .lock()
                    .map_err(|_| RunnerPoolError::LockPoisoned)?
                    .clone(),
                events: Vec::new(),
            })
        }

        fn export_state(&self, _node_id: &str) -> Result<Option<WireValue>, RunnerPoolError> {
            self.state
                .lock()
                .map_err(|_| RunnerPoolError::LockPoisoned)
                .map(|state| state.clone())
        }

        fn import_state(&self, _node_id: &str, state: WireValue) -> Result<(), RunnerPoolError> {
            *self
                .state
                .lock()
                .map_err(|_| RunnerPoolError::LockPoisoned)? = Some(state);
            Ok(())
        }
    }

    struct StoppedRunner;

    impl BackendRunner for StoppedRunner {
        fn health(&self) -> RunnerHealth {
            RunnerHealth::Stopped
        }

        fn invoke(&self, _request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
            unreachable!("stopped runner should not be invoked in state sync tests")
        }
    }

    fn backend_config() -> BackendConfig {
        BackendConfig {
            backend: BackendKind::Python,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some("plugin.py".into()),
            entry_class: None,
            entry_symbol: Some("add".into()),
            executable: Some("python".into()),
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        }
    }

    fn decoded_response(state: Option<WireValue>) -> DecodedInvokeResponse {
        crate::decode_response(
            InvokeResponse {
                protocol_version: WORKER_PROTOCOL_VERSION,
                correlation_id: Some("req-1".into()),
                outputs: BTreeMap::new(),
                state,
                events: Vec::new(),
            },
            Some("req-1"),
        )
        .expect("decode response")
    }

    #[test]
    fn imports_decoded_response_state_into_runner() {
        let config = backend_config();
        let mut pool = RunnerPool::new();
        pool.insert(&config, StateRunner::default())
            .expect("insert runner");

        let result = sync_response_state(
            &pool,
            &config,
            "demo:add",
            &decoded_response(Some(WireValue::Int(12))),
            StateSyncPolicy::Required,
        )
        .expect("sync state");

        assert_eq!(
            result,
            StateSyncResult {
                imported: true,
                state: Some(WireValue::Int(12)),
            }
        );
        assert_eq!(
            export_runner_state(&pool, &config, "demo:add").expect("export state"),
            Some(WireValue::Int(12))
        );
    }

    #[test]
    fn optional_state_sync_ignores_missing_state() {
        let config = backend_config();
        let mut pool = RunnerPool::new();
        pool.insert(&config, StateRunner::default())
            .expect("insert runner");

        let result = sync_response_state(
            &pool,
            &config,
            "demo:add",
            &decoded_response(None),
            StateSyncPolicy::Optional,
        )
        .expect("sync optional state");

        assert_eq!(
            result,
            StateSyncResult {
                imported: false,
                state: None,
            }
        );
        assert_eq!(
            export_runner_state(&pool, &config, "demo:add").expect("export state"),
            None
        );
    }

    #[test]
    fn required_state_sync_rejects_missing_state() {
        let config = backend_config();
        let pool = RunnerPool::new();

        assert!(matches!(
            sync_response_state(
                &pool,
                &config,
                "demo:add",
                &decoded_response(None),
                StateSyncPolicy::Required,
            ),
            Err(StateSyncError::MissingState { node_id }) if node_id == "demo:add"
        ));
    }

    #[test]
    fn preserves_worker_side_state_handles() {
        let config = backend_config();
        let mut pool = RunnerPool::new();
        pool.insert(&config, StateRunner::default())
            .expect("insert runner");
        let handle = WireValue::Handle(WirePayloadHandle {
            id: "state-1".into(),
            type_key: TypeKey::new("demo:state"),
            access: AccessMode::Modify,
            residency: None,
            layout: None,
            capabilities: vec!["stateful".into()],
            metadata: BTreeMap::new(),
        });

        sync_response_state(
            &pool,
            &config,
            "demo:add",
            &decoded_response(Some(handle.clone())),
            StateSyncPolicy::Required,
        )
        .expect("sync state handle");

        assert_eq!(
            export_runner_state(&pool, &config, "demo:add").expect("export state"),
            Some(handle)
        );
    }

    #[test]
    fn propagates_runner_errors_for_present_state() {
        let config = backend_config();
        let mut pool = RunnerPool::new();
        pool.insert(&config, StoppedRunner).expect("insert runner");

        assert!(matches!(
            sync_response_state(
                &pool,
                &config,
                "demo:add",
                &decoded_response(Some(WireValue::Int(1))),
                StateSyncPolicy::Required,
            ),
            Err(StateSyncError::Runner(RunnerPoolError::RunnerNotReady(
                RunnerHealth::Stopped
            )))
        ));
    }
}
