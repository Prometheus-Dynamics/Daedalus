//! Rewrites `generated/graph.rs` from `graph.json`. It lives here so it can run while the old
//! module doesn't compile.

use daedalus_cpu::source::SourceBackend;
use daedalus_graph::GraphDoc;
use daedalus_planner::{Config, Form, plan};
use daedalus_registry::Registry;

/// The module `graph.json` plans to.
pub fn module() -> String {
    let text = include_str!("../../graph.json");
    let doc = GraphDoc::parse(text).expect("graph.json parses");
    let mut registry = Registry::new();
    registry
        .register_all(&daedalus_chip_blink_nodes::NODES)
        .expect("the nodes register");
    let backend = SourceBackend {
        name: "Graph",
        root: None,
    };
    let config = Config {
        form: Form::Source,
        ..Config::default()
    };
    plan(&doc.graph, &registry, &[&backend], &config)
        .and_then(|planned| planned.source())
        .expect("the graph plans")
}

#[allow(dead_code)]
fn main() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../generated/graph.rs");
    std::fs::write(path, module()).expect("write generated/graph.rs");
}
