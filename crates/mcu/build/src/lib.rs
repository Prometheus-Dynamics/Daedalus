//! Host-side plan compiler for `daedalus-mcu` (see docs/mcu.md).
//!
//! A device crate's `build.rs` plans its graph here with the regular Daedalus planner (registry
//! declarations, type checks, adapter resolution, edge policies, schedule) and writes the result
//! as Rust source: a `Graph` struct with one fixed-capacity queue per edge and one state slot per
//! node, `push_*`/`pop_*` methods per host port, and a `tick` that calls the node functions
//! directly in schedule order. All tables are `const` data and code, so nothing is parsed or
//! allocated on the device.
//!
//! ```ignore
//! // build.rs of the device crate; `my_nodes` is also a regular dependency.
//! fn main() {
//!     let nodes = [my_nodes::scale::NODE, my_nodes::lowpass::NODE];
//!     let json = std::fs::read_to_string("graph.json").unwrap();
//!     let source = daedalus_mcu_build::compile_document(&json, &nodes, &Default::default())
//!         .unwrap_or_else(|err| panic!("{err}"));
//!     daedalus_mcu_build::write_out_dir("graph.rs", &source).unwrap();
//!     println!("cargo:rerun-if-changed=graph.json");
//! }
//! ```

mod emit;
mod lower;
#[cfg(test)]
mod tests;

use std::path::PathBuf;

use daedalus_data::model::Value;
use daedalus_mcu::{NodeDesc, PortDesc};
use daedalus_planner::{GraphDocument, GraphDocumentError};
use daedalus_registry::capability::{NODE_FIRE_META_KEY, NodeDecl, PortDecl};
use daedalus_registry::transport_key_typeexpr;
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_transport::TypeKey;

pub use daedalus_mcu::Overflow;
pub use daedalus_planner::Graph;
pub use lower::{HostPort, InputSource, McuPlan, PlanEdge, PlanNode};

/// Code generation choices that the graph does not carry.
#[derive(Clone, Debug)]
pub struct CompileOptions {
    /// Name of the generated graph struct.
    pub graph_name: String,
    /// Capacity of edges whose policy does not bound them (FIFO, the default policy, and
    /// `drop_newest` / `drop_oldest` / `error_on_full`). A full FIFO edge fails the push with
    /// `McuError::QueueFull`.
    pub fifo_capacity: usize,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            graph_name: "Graph".into(),
            fifo_capacity: 4,
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
    #[error("writing the generated plan: {0}")]
    Io(#[from] std::io::Error),
}

/// Plan `graph` against `nodes` and render the device module.
pub fn compile(
    graph: Graph,
    nodes: &[NodeDesc],
    options: &CompileOptions,
) -> Result<String, CompileError> {
    Ok(plan(graph, nodes, options)?.to_rust(options))
}

/// [`compile`] a persisted graph document (`daedalus.graph` JSON).
pub fn compile_document(
    json: &str,
    nodes: &[NodeDesc],
    options: &CompileOptions,
) -> Result<String, CompileError> {
    compile(GraphDocument::from_json(json)?.graph, nodes, options)
}

/// Plan `graph` into the device plan (inspectable; [`McuPlan::to_rust`] renders it).
pub fn plan(
    graph: Graph,
    nodes: &[NodeDesc],
    options: &CompileOptions,
) -> Result<McuPlan, CompileError> {
    lower::lower(graph, nodes, &registry(nodes)?, options)
}

/// The plugin registry the planner sees: the runtime builtins (host bridge, primitive types,
/// numeric widening adapters) plus one declaration per device node.
pub fn registry(nodes: &[NodeDesc]) -> Result<PluginRegistry, CompileError> {
    let mut registry = PluginRegistry::new();
    for desc in nodes {
        registry
            .register_node_decl(node_decl(desc))
            .map_err(|err| CompileError::Registry {
                id: desc.id.into(),
                message: err.to_string(),
            })?;
    }
    Ok(registry)
}

/// The planner declaration of a device node: the same ports, keys, optional inputs, fire mode
/// and conditional outputs a `#[node]` of the full runtime declares.
pub fn node_decl(desc: &NodeDesc) -> NodeDecl {
    let port = |port: &PortDesc| {
        let key = TypeKey::new(port.key);
        let decl = PortDecl::new(port.name, key.clone()).schema(transport_key_typeexpr(&key));
        if port.optional { decl.optional() } else { decl }
    };
    let mut decl = NodeDecl::new(desc.id);
    for input in desc.inputs {
        decl = decl.input(port(input));
    }
    for output in desc.outputs {
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
    if desc.fire_all {
        decl = decl.metadata(NODE_FIRE_META_KEY, Value::String("all".into()));
    }
    decl
}

/// Write `source` to `$OUT_DIR/<file_name>` (from a build script) and return the path.
pub fn write_out_dir(file_name: &str, source: &str) -> Result<PathBuf, CompileError> {
    let dir = std::env::var_os("OUT_DIR").ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "OUT_DIR is not set (not a build script)",
        )
    })?;
    let path = PathBuf::from(dir).join(file_name);
    std::fs::write(&path, source)?;
    Ok(path)
}
