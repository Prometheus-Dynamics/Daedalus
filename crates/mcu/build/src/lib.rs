//! Host-side plan compiler for `daedalus-mcu` (see docs/mcu.md).
//!
//! Plans a graph with the regular Daedalus planner (registry declarations, type checks, adapter
//! resolution, edge policies, schedule) into one [`McuPlan`], then renders it for one of the
//! device modes:
//!
//! - **Compiled** ([`compile`], [`McuPlan::to_rust`]): a `Graph` struct with one
//!   fixed-capacity queue per edge and one state slot per node, `push_*`/`pop_*` methods per
//!   host port and a `tick` calling the node functions directly in schedule order. Constants
//!   marked tunable (node metadata [`PARAMS_META_KEY`]) become parameters: a `Tunable` impl,
//!   typed setters and `PARAM_NAMES`/`PARAMS` tables.
//! - **Loaded** ([`library`], [`compile_loaded`], [`McuPlan::to_blob`]): a node library
//!   generated once per firmware, and plan blobs compiled against its [`LibraryManifest`] and
//!   loaded by `daedalus_mcu::loaded::Interpreter` at run time.
//!
//! [`PlanManifest`] (JSON) names a plan's host ports and parameters by id for host tools; the
//! `daedalus-mcu` binary compiles blobs and encodes parameter updates from the command line.
//!
//! Several edges into one input (fan-in) and user adapters (`#[daedalus_mcu::adapt]`) are planned
//! and rendered in every mode; see docs/mcu.md.
//!
//! ```ignore
//! // build.rs of the device crate; `my_nodes` is also a regular dependency.
//! fn main() {
//!     let nodes = [my_nodes::scale::NODE, my_nodes::lowpass::NODE];
//!     let adapters = [my_nodes::to_volts::ADAPTER];
//!     let json = std::fs::read_to_string("graph.json").unwrap();
//!     let source = daedalus_mcu_build::compile_document(&json, &nodes, &adapters, &Default::default())
//!         .unwrap_or_else(|err| panic!("{err}"));
//!     daedalus_mcu_build::write_out_dir("graph.rs", &source).unwrap();
//!     println!("cargo:rerun-if-changed=graph.json");
//! }
//! ```

mod blob;
mod emit;
mod library;
mod lower;
#[cfg(test)]
mod tests;

use std::path::PathBuf;

use daedalus_mcu::{AdapterDesc, NodeDesc};
use daedalus_planner::GraphDocumentError;
use daedalus_registry::capability::{NODE_FIRE_META_KEY, NodeDecl, PortDecl};
use daedalus_registry::transport_key_typeexpr;
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_transport::{AdaptRequest, Payload, TransportError, TypeKey};
use serde::{Deserialize, Serialize};

pub use blob::{ParamManifest, PlanManifest, PortManifest};
pub use daedalus_data::model::Value;
pub use daedalus_mcu::{Overflow, Scalar, ScalarKind};
pub use daedalus_planner::{Graph, GraphDocument};
pub use library::LibraryManifest;
pub use lower::{
    Endpoint, HostPort, InputSource, McuPlan, PARAMS_META_KEY, PlanAdapter, PlanEdge, PlanNode,
    PlanParam, scalar_value,
};

/// Code generation choices that the graph does not carry.
#[derive(Clone, Debug)]
pub struct CompileOptions {
    /// Name of the generated graph struct.
    pub graph_name: String,
    /// Capacity of edges whose policy does not bound them (FIFO, the default policy, and
    /// `drop_newest` / `drop_oldest` / `error_on_full`). A full FIFO edge fails the push with
    /// `McuError::QueueFull`.
    pub fifo_capacity: usize,
    /// Ignore the graph's parameter markers: every constant stays a literal (a production
    /// build of a graph tuned with a tunable one).
    pub freeze_params: bool,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            graph_name: "Graph".into(),
            fifo_capacity: 4,
            freeze_params: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompileError {
    #[error(transparent)]
    Document(#[from] GraphDocumentError),
    #[error("node declaration `{id}`: {message}")]
    Registry { id: String, message: String },
    #[error("planning failed:\n{0}")]
    Planner(String),
    #[error("not supported by the MCU profile: {0}")]
    Unsupported(String),
    #[error("manifest: {0}")]
    Manifest(String),
    #[error("writing the generated plan: {0}")]
    Io(#[from] std::io::Error),
}

/// A device node's declaration as the host compiler reads it: from a [`NodeDesc`] in a build
/// script, or from a [`LibraryManifest`] in host tools.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpec {
    pub id: String,
    /// Module path of the node's generated glue.
    pub path: String,
    pub inputs: Vec<PortSpec>,
    pub outputs: Vec<PortSpec>,
    pub fire_all: bool,
}

/// A user adapter as the host compiler reads it: from an [`AdapterDesc`], or from a
/// [`LibraryManifest`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterSpec {
    pub id: String,
    /// Module path of the adapter's generated glue.
    pub path: String,
    /// Transport key of the input type.
    pub from: String,
    /// Transport key of the output type.
    pub to: String,
    /// Planner cost.
    pub cost: u32,
}

impl From<&AdapterDesc> for AdapterSpec {
    fn from(desc: &AdapterDesc) -> Self {
        Self {
            id: desc.id.into(),
            path: desc.path.into(),
            from: desc.from.into(),
            to: desc.to.into(),
            cost: desc.cost,
        }
    }
}

fn adapter_specs(adapters: &[AdapterDesc]) -> Vec<AdapterSpec> {
    adapters.iter().map(AdapterSpec::from).collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortSpec {
    pub name: String,
    /// Transport key of the port type.
    pub key: String,
    /// An optional input or a conditional output.
    pub optional: bool,
}

impl From<&NodeDesc> for NodeSpec {
    fn from(desc: &NodeDesc) -> Self {
        let ports = |ports: &[daedalus_mcu::PortDesc]| {
            ports
                .iter()
                .map(|port| PortSpec {
                    name: port.name.into(),
                    key: port.key.into(),
                    optional: port.optional,
                })
                .collect()
        };
        Self {
            id: desc.id.into(),
            path: desc.path.into(),
            inputs: ports(desc.inputs),
            outputs: ports(desc.outputs),
            fire_all: desc.fire_all,
        }
    }
}

fn specs(nodes: &[NodeDesc]) -> Vec<NodeSpec> {
    nodes.iter().map(NodeSpec::from).collect()
}

/// Plan `graph` against `nodes` and `adapters` and render the compiled device module.
pub fn compile(
    graph: Graph,
    nodes: &[NodeDesc],
    adapters: &[AdapterDesc],
    options: &CompileOptions,
) -> Result<String, CompileError> {
    Ok(plan(graph, nodes, adapters, options)?.to_rust(options))
}

/// [`compile`] a persisted graph document (`daedalus.graph` JSON).
pub fn compile_document(
    json: &str,
    nodes: &[NodeDesc],
    adapters: &[AdapterDesc],
    options: &CompileOptions,
) -> Result<String, CompileError> {
    Ok(plan_document(json, nodes, adapters, options)?.to_rust(options))
}

/// [`plan`] a persisted graph document.
pub fn plan_document(
    json: &str,
    nodes: &[NodeDesc],
    adapters: &[AdapterDesc],
    options: &CompileOptions,
) -> Result<McuPlan, CompileError> {
    plan(
        GraphDocument::from_json(json)?.graph,
        nodes,
        adapters,
        options,
    )
}

/// Plan `graph` into the device plan (inspectable; [`McuPlan::to_rust`] renders it).
pub fn plan(
    graph: Graph,
    nodes: &[NodeDesc],
    adapters: &[AdapterDesc],
    options: &CompileOptions,
) -> Result<McuPlan, CompileError> {
    plan_specs(graph, &specs(nodes), &adapter_specs(adapters), options)
}

/// [`plan`] against node and adapter specs.
#[doc(hidden)]
pub fn plan_specs(
    graph: Graph,
    nodes: &[NodeSpec],
    adapters: &[AdapterSpec],
    options: &CompileOptions,
) -> Result<McuPlan, CompileError> {
    lower::lower(graph, nodes, adapters, &registry(nodes, adapters)?, options)
}

/// The node library of a loaded-mode firmware, from the nodes and user adapters it includes (in
/// entry order). [`LibraryManifest::to_rust`] renders its `LIBRARY`; the JSON manifest lets host
/// tools compile plans for it.
pub fn library(
    nodes: &[NodeDesc],
    adapters: &[AdapterDesc],
) -> Result<LibraryManifest, CompileError> {
    LibraryManifest::new(specs(nodes), adapter_specs(adapters))
}

/// Plan `graph` for a loaded-mode firmware with `library`; [`McuPlan::to_blob`] encodes it.
pub fn compile_loaded(
    graph: Graph,
    library: &LibraryManifest,
    options: &CompileOptions,
) -> Result<McuPlan, CompileError> {
    plan_specs(graph, &library.nodes, &library.adapters, options)
}

/// [`compile_loaded`] a graph document and encode the blob and its manifest.
pub fn compile_loaded_document(
    json: &str,
    library: &LibraryManifest,
    options: &CompileOptions,
) -> Result<(Vec<u8>, PlanManifest), CompileError> {
    let plan = compile_loaded(GraphDocument::from_json(json)?.graph, library, options)?;
    Ok((plan.to_blob(library)?, plan.manifest(Some(library))))
}

/// The plugin registry the planner sees: the runtime builtins (host bridge, primitive types,
/// numeric widening adapters) plus one declaration per device node and per user adapter.
pub fn registry(
    nodes: &[NodeSpec],
    adapters: &[AdapterSpec],
) -> Result<PluginRegistry, CompileError> {
    let mut registry = PluginRegistry::new();
    for spec in nodes {
        registry
            .register_node_decl(node_decl(spec))
            .map_err(|err| CompileError::Registry {
                id: spec.id.clone(),
                message: err.to_string(),
            })?;
    }
    for spec in adapters {
        // The planner needs the declaration only: the device runs the function.
        let not_run_here = |_: Payload, _: &AdaptRequest| {
            Err(TransportError::Unsupported(
                "a device adapter runs on the device, not in the host planner".into(),
            ))
        };
        registry
            .register_transport_adapter_fn_with_cost(
                spec.id.clone(),
                transport_key_typeexpr(&TypeKey::new(spec.from.as_str())),
                transport_key_typeexpr(&TypeKey::new(spec.to.as_str())),
                u64::from(spec.cost),
                not_run_here,
            )
            .map_err(|err| CompileError::Registry {
                id: spec.id.clone(),
                message: err.to_string(),
            })?;
    }
    Ok(registry)
}

/// The planner declaration of a device node: the same ports, keys, optional inputs, fire mode
/// and conditional outputs a `#[node]` of the full runtime declares.
pub fn node_decl(spec: &NodeSpec) -> NodeDecl {
    let port = |port: &PortSpec| {
        let key = TypeKey::new(port.key.as_str());
        let decl =
            PortDecl::new(port.name.as_str(), key.clone()).schema(transport_key_typeexpr(&key));
        if port.optional { decl.optional() } else { decl }
    };
    let mut decl = NodeDecl::new(spec.id.as_str());
    for input in &spec.inputs {
        decl = decl.input(port(input));
    }
    for output in &spec.outputs {
        decl = decl.output(PortDecl {
            optional: false,
            ..port(output)
        });
        if output.optional {
            decl = decl.metadata(
                format!("outputs.{}.conditional", output.name),
                Value::Bool(true),
            );
        }
    }
    if spec.fire_all {
        decl = decl.metadata(NODE_FIRE_META_KEY, Value::String("all".into()));
    }
    decl
}

/// Write `contents` to `$OUT_DIR/<file_name>` (from a build script) and return the path.
pub fn write_out_dir(file_name: &str, contents: impl AsRef<[u8]>) -> Result<PathBuf, CompileError> {
    let dir = std::env::var_os("OUT_DIR").ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "OUT_DIR is not set (not a build script)",
        )
    })?;
    let path = PathBuf::from(dir).join(file_name);
    std::fs::write(&path, contents)?;
    Ok(path)
}
