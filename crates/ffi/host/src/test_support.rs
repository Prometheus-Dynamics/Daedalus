//! Runner doubles and schema fixtures shared by this crate's unit tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_ffi_core::{
    BackendConfig, BackendKind, InvokeRequest, InvokeResponse, WirePort, WireValue,
};
use daedalus_transport::AccessMode;

use crate::{BackendRunner, BackendRunnerFactory, RunnerPoolError};

#[derive(Default)]
pub(crate) struct RunnerCounters {
    pub builds: AtomicUsize,
    pub shutdowns: AtomicUsize,
}

/// Answers every request with `out = String(node_id)` and counts shutdowns.
pub(crate) struct EchoRunner {
    counters: Arc<RunnerCounters>,
    supported_nodes: Option<Vec<String>>,
}

impl BackendRunner for EchoRunner {
    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        Ok(InvokeResponse {
            protocol_version: request.protocol_version,
            correlation_id: request.correlation_id,
            outputs: BTreeMap::from([("out".into(), WireValue::String(request.node_id))]),
            state: None,
            events: Vec::new(),
        })
    }

    fn supported_nodes(&self) -> Option<Vec<String>> {
        self.supported_nodes.clone()
    }

    fn shutdown(&self) -> Result<(), RunnerPoolError> {
        self.counters.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Builds [`EchoRunner`]s, counting builds; fails for backends launched with `fail_executable`.
#[derive(Default)]
pub(crate) struct EchoFactory {
    pub counters: Arc<RunnerCounters>,
    pub supported_nodes: Option<Vec<String>>,
    pub fail_executable: Option<&'static str>,
}

impl EchoFactory {
    pub fn supporting(nodes: &[&str]) -> Self {
        Self {
            supported_nodes: Some(nodes.iter().map(|node| (*node).to_owned()).collect()),
            ..Self::default()
        }
    }

    pub fn builds(&self) -> usize {
        self.counters.builds.load(Ordering::SeqCst)
    }
}

impl BackendRunnerFactory for EchoFactory {
    fn build_runner(
        &self,
        _node_id: &str,
        backend: &BackendConfig,
    ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        if self.fail_executable.is_some() && backend.executable.as_deref() == self.fail_executable {
            return Err(RunnerPoolError::Runner("worker failed to start".into()));
        }
        self.counters.builds.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(EchoRunner {
            counters: self.counters.clone(),
            supported_nodes: self.supported_nodes.clone(),
        }))
    }
}

/// A required, read-access `Int` port.
pub(crate) fn int_port(name: &str) -> WirePort {
    WirePort {
        access: AccessMode::Read,
        ..WirePort::new(name, TypeExpr::Scalar(ValueType::Int))
    }
}

/// A persistent Python worker running `symbol` from `module` via `executable`.
pub(crate) fn python_worker(executable: &str, module: &str, symbol: &str) -> BackendConfig {
    BackendConfig::persistent_worker(BackendKind::Python, executable, symbol)
        .with_entry_module(module)
}

/// An in-process Rust plugin library backend.
pub(crate) fn rust_in_process() -> BackendConfig {
    BackendConfig::in_process(BackendKind::Rust, "daedalus_plugin_register")
        .with_entry_module("libdemo_plugin.so")
}
