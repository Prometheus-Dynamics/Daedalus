//! The worker: builds the plugin's schema and serves the worker protocol over a reader and writer.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, Write};
use std::sync::Arc;

use daedalus::data::model::Value;
use daedalus::data::to_value::ToValue;
use daedalus::runtime::plugins::{PluginInstallable, PluginRegistry};
use daedalus::runtime::type_index::TypeIndex;
use daedalus::runtime::{ExecutionContext, StateStore};
use daedalus_ffi_core::{
    BackendKind, FfiContractError, NodeSchema, PluginSchema, WORKER_PROTOCOL_VERSION, WorkerError,
    WorkerHello, WorkerMessage, WorkerMessagePayload,
};
use daedalus_ffi_host::{SchemaExportError, export_registry_plugin_schema};
use thiserror::Error;

/// Converts a node's state to and from its wire value (see [`Worker::with_state`]).
pub(crate) struct StateCodec {
    pub(crate) import: StateImport,
    pub(crate) export: StateExport,
}

/// Seeds a node's state from its wire value; `false` when the value does not decode.
pub(crate) type StateImport = Box<dyn Fn(&ExecutionContext, &Value) -> bool + Send + Sync>;
/// The node's state as a wire value, `None` when it has no state yet.
pub(crate) type StateExport = Box<dyn Fn(&ExecutionContext) -> Option<Value> + Send + Sync>;

/// Why a plugin cannot be served: its install failed, or its schema does not export.
#[derive(Debug, Error)]
pub enum WorkerBuildError {
    #[error("plugin install failed: {0}")]
    Install(String),
    #[error("plugin schema export failed: {0}")]
    Schema(#[from] SchemaExportError),
    #[error("plugin schema is invalid: {0}")]
    Contract(#[from] FfiContractError),
}

/// A plugin served as a persistent worker. Build it once, then call [`Worker::serve`].
///
/// The worker owns the plugin's registry: node handlers, const coercers and value serializers are
/// the ones the in-process runtime uses, and `state(T)` nodes keep their state here across calls.
pub struct Worker {
    pub(crate) worker_id: String,
    pub(crate) state_codecs: BTreeMap<String, StateCodec>,
    pub(crate) registry: PluginRegistry,
    pub(crate) types: TypeIndex,
    pub(crate) capabilities: Arc<daedalus::runtime::capabilities::CapabilityRegistry>,
    pub(crate) schemas: Vec<PluginSchema>,
    pub(crate) nodes: BTreeMap<String, Arc<NodeSchema>>,
    pub(crate) state: StateStore,
}

impl Worker {
    /// A worker for `plugin`, installed into a fresh registry (the same install a host performs).
    pub fn from_plugin<P: PluginInstallable + ?Sized>(
        plugin: &P,
    ) -> Result<Self, WorkerBuildError> {
        let mut registry = PluginRegistry::new();
        registry
            .install(plugin)
            .map_err(|err| WorkerBuildError::Install(err.to_string()))?;
        Self::new(registry)
    }

    /// A worker for every plugin installed in `registry`.
    pub fn new(registry: PluginRegistry) -> Result<Self, WorkerBuildError> {
        // The same export the host and the dynamic-plugin path use: the transport capabilities
        // plus each installed manifest (`daedalus::dylib::registry_plugin_schema`).
        let mut capabilities = registry.transport_capabilities.clone();
        for manifest in registry.plugin_manifests.values() {
            if capabilities.plugin_manifest(&manifest.id).is_none() {
                capabilities
                    .register_plugin(manifest.clone())
                    .map_err(|err| WorkerBuildError::Install(err.to_string()))?;
            }
        }
        // A fresh registry already holds the runtime's builtin plugins; the worker serves the rest.
        let builtin: BTreeSet<String> = PluginRegistry::new()
            .plugin_manifests
            .keys()
            .cloned()
            .collect();
        let mut schemas = Vec::with_capacity(registry.plugin_manifests.len());
        for plugin_id in registry.plugin_manifests.keys() {
            if builtin.contains(plugin_id) {
                continue;
            }
            schemas.push(export_registry_plugin_schema(&capabilities, plugin_id)?);
        }
        let mut nodes = BTreeMap::new();
        for schema in &schemas {
            for node in &schema.nodes {
                nodes.insert(node.id.clone(), Arc::new(node.clone()));
            }
        }
        let types = registry.type_index();
        let capabilities = Arc::new(registry.capabilities.clone());
        Ok(Self {
            worker_id: "daedalus-rust-worker".into(),
            state_codecs: BTreeMap::new(),
            types,
            capabilities,
            registry,
            schemas,
            nodes,
            state: StateStore::default(),
        })
    }

    /// The `worker_id` the hello announces (default `daedalus-rust-worker`).
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = worker_id.into();
        self
    }

    /// Declare the state of node `node_id` as a `state(T)` node's state, converted to and from the
    /// wire state (`WireValue`, which the host sends and stores) through `decode` and `ToValue`.
    /// The worker seeds the state before each call that carries one and exports it after each
    /// call. A node without a declaration keeps its state in the worker and exports none.
    pub fn with_state<T>(
        mut self,
        node_id: impl Into<String>,
        decode: fn(&Value) -> Option<T>,
    ) -> Self
    where
        T: Default + ToValue + Send + Sync + 'static,
    {
        // The macros keep a `state(T)` node's state as a tuple of its state slots: `(T,)` here.
        let import = Box::new(
            move |ctx: &ExecutionContext, value: &Value| match decode(value) {
                Some(state) => {
                    ctx.set_node_state((state,));
                    true
                }
                None => false,
            },
        );
        let export = Box::new(|ctx: &ExecutionContext| {
            let (state,) = ctx.take_node_state::<(T,)>()?;
            let value = state.to_value();
            ctx.set_node_state((state,));
            Some(value)
        });
        self.state_codecs
            .insert(node_id.into(), StateCodec { import, export });
        self
    }

    /// The exported schema of each installed plugin, in plugin id order.
    pub fn schemas(&self) -> &[PluginSchema] {
        &self.schemas
    }

    /// Serve the worker protocol on this process's stdin and stdout until stdin closes.
    pub fn serve(mut self) -> io::Result<()> {
        let stdin = io::stdin();
        let stdout = io::stdout();
        self.serve_io(stdin.lock(), stdout.lock())
    }

    /// Serve the worker protocol over `input` and `output`: say hello, then answer each invoke
    /// with a response or a typed error until `input` ends. Returns `Ok` on end of input, which is
    /// a clean exit. Only protocol frames are written to `output`; diagnostics go to stderr.
    pub fn serve_io<R: BufRead, W: Write>(&mut self, input: R, mut output: W) -> io::Result<()> {
        let hello = WorkerMessage::new(
            WorkerMessagePayload::Hello(self.hello()),
            Some("startup".into()),
        );
        write_message(&mut output, &hello)?;
        for line in input.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let message: WorkerMessage = match serde_json::from_str(&line) {
                Ok(message) => message,
                Err(err) => {
                    let error = worker_error("bad_message", format!("malformed message: {err}"));
                    write_message(&mut output, &error_message(error, None))?;
                    continue;
                }
            };
            let correlation_id = message.correlation_id.clone();
            if let Err(err) = message.validate_protocol() {
                let error = worker_error("unsupported_protocol", err.to_string());
                write_message(&mut output, &error_message(error, correlation_id))?;
                continue;
            }
            let reply = match message.payload {
                WorkerMessagePayload::Ack(_) => continue,
                WorkerMessagePayload::Invoke(request) => match self.invoke(&request) {
                    Ok(response) => WorkerMessage::new(
                        WorkerMessagePayload::Response(response),
                        request.correlation_id,
                    ),
                    Err(failure) => {
                        let error = worker_error(failure.code, failure.message);
                        error_message(error, request.correlation_id)
                    }
                },
                _ => {
                    let error = worker_error("bad_message", "expected an invoke message".into());
                    error_message(error, correlation_id)
                }
            };
            write_message(&mut output, &reply)?;
        }
        Ok(())
    }

    fn hello(&self) -> WorkerHello {
        WorkerHello {
            protocol_version: WORKER_PROTOCOL_VERSION,
            min_protocol_version: WORKER_PROTOCOL_VERSION,
            worker_id: Some(self.worker_id.clone()),
            backend: Some(BackendKind::Rust),
            supported_nodes: self.nodes.keys().cloned().collect(),
            capabilities: vec!["persistent_worker".into(), "events".into()],
            metadata: BTreeMap::from([
                ("sdk".into(), serde_json::json!("daedalus-ffi-rust")),
                (
                    "sdk_version".into(),
                    serde_json::json!(env!("CARGO_PKG_VERSION")),
                ),
            ]),
        }
    }
}

fn worker_error(code: &str, message: String) -> WorkerError {
    WorkerError {
        code: code.into(),
        message,
        metadata: BTreeMap::new(),
    }
}

fn error_message(error: WorkerError, correlation_id: Option<String>) -> WorkerMessage {
    WorkerMessage::new(WorkerMessagePayload::Error(error), correlation_id)
}

/// One JSON object per line, flushed at once: the host reads a line at a time.
fn write_message<W: Write>(output: &mut W, message: &WorkerMessage) -> io::Result<()> {
    serde_json::to_writer(&mut *output, message).map_err(io::Error::other)?;
    output.write_all(b"\n")?;
    output.flush()
}
