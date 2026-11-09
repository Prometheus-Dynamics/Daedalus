//! Rewrites everything in `generated/`. It lives here so it can run while the old modules don't
//! compile.

use daedalus_chip_blink_nodes::{LIBRARY, NODES};
use daedalus_cpu::blob::BlobBackend;
use daedalus_cpu::source::SourceBackend;
use daedalus_graph::{GraphBody, GraphDoc, Lit, LitField};
use daedalus_planner::{Config, Form, plan};
use daedalus_registry::Registry;

fn parsed(text: &str) -> GraphBody {
    GraphDoc::parse(text).expect("the graph parses").graph
}

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

fn range(low: f64, high: f64) -> Lit {
    Lit::List(vec![Lit::Float(low), Lit::Float(high)])
}

fn field(name: &str, value: Lit) -> LitField {
    LitField {
        name: name.to_string(),
        value,
    }
}

/// `graph` with `lowpass.alpha`, `threshold.on`, `threshold.off` and `blink.period` marked.
fn tunable(mut graph: GraphBody) -> GraphBody {
    for node in &mut graph.nodes {
        let marks = match node.id.as_str() {
            "blink.lowpass" => vec![field("alpha", range(0.0, 1.0))],
            "blink.threshold" => vec![field("on", range(0.0, 5.0)), field("off", range(0.0, 5.0))],
            "blink.blink" => vec![field(
                "period",
                Lit::List(vec![Lit::Int(1), Lit::Int(1000)]),
            )],
            _ => continue,
        };
        node.metadata
            .insert("daedalus.chip.params".to_string(), Lit::Struct(marks));
    }
    graph
}

/// Every file in `generated/`, by name.
pub fn files() -> Vec<(&'static str, Vec<u8>)> {
    let a = parsed(include_str!("../../graph.json"));
    let b = parsed(include_str!("../../graph_b.json"));
    let fanin = parsed(include_str!("../../graph_fanin.json"));
    let fanin_b = parsed(include_str!("../../graph_fanin_b.json"));
    vec![
        ("graph.rs", module(&a)),
        ("tunable.rs", module(&tunable(a.clone()))),
        ("fanin.rs", module(&fanin)),
        ("fanin_tunable.rs", module(&tunable(fanin.clone()))),
        ("plan_a.bin", stored(&a)),
        ("plan_b.bin", stored(&b)),
        ("fanin_a.bin", stored(&fanin)),
        ("fanin_b.bin", stored(&fanin_b)),
    ]
}

#[allow(dead_code)]
fn main() {
    let folder = concat!(env!("CARGO_MANIFEST_DIR"), "/../generated");
    for (name, bytes) in files() {
        std::fs::write(format!("{folder}/{name}"), bytes).expect("write a generated file");
    }
}
