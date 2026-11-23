//! A node's metrics: declared on the node with `#[node(metrics(..))]`, and listed by a host from
//! the plan, without running the graph.

use daedalus_backend::Backend;
use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost, MetricInfo};
use daedalus_macros::node;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{MetricDecl, MetricUnit, NodeRecord, Registry};

/// A body with stages of its own to report on.
#[node(
    id = "m.detect",
    inputs(x),
    outputs(out),
    metrics(threshold = "ns", decode = "ns", candidates = "count")
)]
fn detect(x: i64) -> i64 {
    x + 1
}

#[node(id = "m.plain", inputs(x), outputs(out))]
fn plain(x: i64) -> i64 {
    x
}

const NODES: [&NodeRecord; 2] = [&DETECT_NODE, &PLAIN_NODE];

fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    registry
}

/// The host's `x` through `m.detect` and `m.plain` to the host's `out`.
fn planned() -> Planned {
    let mut host = NodeDoc::new("host");
    host.kind = NodeKindDoc::HostBridge;
    host.port_types.insert("x".into(), "i64".into());
    host.port_types.insert("out".into(), "i64".into());
    let graph = GraphBody {
        nodes: vec![host, NodeDoc::new("m.detect"), NodeDoc::new("m.plain")],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(1, "out", 2, "x"),
            edge(2, "out", 0, "out"),
        ],
        groups: Vec::new(),
    };
    plan(&graph, &registry(), &[&CpuBackend], &Config::default()).unwrap()
}

fn listed(host: &LoadedHost) -> Vec<(String, String, MetricUnit)> {
    host.metrics()
        .into_iter()
        .map(|MetricInfo { node, name, unit }| (node, name, unit))
        .collect()
}

fn expected() -> Vec<(String, String, MetricUnit)> {
    [
        ("threshold", MetricUnit::Ns),
        ("decode", MetricUnit::Ns),
        ("candidates", MetricUnit::Count),
    ]
    .into_iter()
    .map(|(name, unit)| ("m.detect#0".to_string(), name.to_string(), unit))
    .collect()
}

#[test]
fn a_nodes_declaration_lists_its_metrics_in_order_with_their_units() {
    let metric = |name, unit| MetricDecl { name, unit };
    assert_eq!(
        DETECT_DECL.metrics,
        [
            metric("threshold", MetricUnit::Ns),
            metric("decode", MetricUnit::Ns),
            metric("candidates", MetricUnit::Count),
        ]
    );
    assert!(PLAIN_DECL.metrics.is_empty());
}

#[test]
fn a_host_lists_a_graphs_metrics_from_the_plan_without_running_it() {
    let host = LoadedHost::load(planned(), LoadOptions::default()).unwrap();
    assert_eq!(listed(&host), expected());
}

#[test]
fn a_stored_plan_keeps_the_metrics_and_one_stored_before_them_lists_none() {
    let mut made = planned().made().unwrap();
    let backends: [&dyn Backend; 1] = [&CpuBackend];
    let load = |made: &daedalus_made::MadePlan| {
        let bytes = daedalus_made::encode(made).unwrap();
        LoadedHost::load_made(&bytes, &registry(), &backends, LoadOptions::default()).unwrap()
    };
    assert_eq!(listed(&load(&made)), expected());
    // What reading a plan stored before the metrics gives: see core/made/src/stored_outputs.rs.
    for node in &mut made.facts.nodes {
        node.metrics.clear();
    }
    assert!(listed(&load(&made)).is_empty());
}
