//! Rewrites everything in `generated/`.

use daedalus_chip_shared::{LIBRARY, NODES};
use daedalus_cpu::blob::BlobBackend;
use daedalus_cpu::source::SourceBackend;
use daedalus_graph::{GraphBody, GraphDoc};
use daedalus_planner::{Config, Form, plan};
use daedalus_registry::Registry;

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.register_all(&NODES).expect("the nodes register");
    registry
}

fn module(graph: &GraphBody) -> Vec<u8> {
    let backend = SourceBackend {
        name: "Graph",
        root: None,
    };
    let config = Config {
        form: Form::Source,
        ..Config::default()
    };
    plan(graph, &registry(), &[&backend], &config)
        .and_then(|planned| planned.source())
        .expect("the graph plans")
        .into_bytes()
}

fn stored(graph: &GraphBody) -> Vec<u8> {
    let backend = BlobBackend { library: &LIBRARY };
    let config = Config {
        form: Form::Blob,
        ..Config::default()
    };
    plan(graph, &registry(), &[&backend], &config)
        .and_then(|planned| planned.blob())
        .expect("the graph plans")
}

/// Every file in `generated/`, by name.
pub fn files() -> Vec<(&'static str, Vec<u8>)> {
    let graph = GraphDoc::parse(include_str!("../graph.json"))
        .expect("the graph parses")
        .graph;
    vec![("graph.rs", module(&graph)), ("plan.bin", stored(&graph))]
}

#[allow(dead_code)]
fn main() {
    let folder = concat!(env!("CARGO_MANIFEST_DIR"), "/generated");
    for (name, bytes) in files() {
        std::fs::write(format!("{folder}/{name}"), bytes).expect("write a generated file");
    }
}
