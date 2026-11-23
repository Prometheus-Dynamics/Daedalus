//! A wired input that declares a default: the node reads the default until a value arrives on
//! the edge and on any tick the edge is empty, and the plan says so.

use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::explain::explain_plan;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{NodeRecord, Registry};

/// `x` scaled by `gain`, which defaults to 10.
#[node(
    id = "d.scale",
    inputs(x, port(name = "gain", default = 10)),
    outputs(out)
)]
fn scale(x: i64, gain: i64) -> i64 {
    x * gain
}

const NODES: [&NodeRecord; 1] = [&SCALE_NODE];

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

/// The host feeds both `x` and `gain`.
fn planned() -> Planned {
    let mut host = NodeDoc::new("host");
    host.kind = NodeKindDoc::HostBridge;
    for port in ["x", "gain", "out"] {
        host.port_types.insert(port.into(), "i64".into());
    }
    let graph = GraphBody {
        nodes: vec![host, NodeDoc::new("d.scale")],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(0, "gain", 1, "gain"),
            edge(1, "out", 0, "out"),
        ],
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    plan(&graph, &registry, &[&CpuBackend], &Config::default()).unwrap()
}

#[test]
fn a_wired_defaulted_input_reads_its_default_whenever_its_edge_is_empty() {
    let mut host = LoadedHost::load(planned(), LoadOptions::default()).unwrap();
    let mut tick = |x: i64, gain: Option<i64>| {
        host.host().feed_typed("x", Arc::new(x)).unwrap();
        if let Some(gain) = gain {
            host.host().feed_typed("gain", Arc::new(gain)).unwrap();
        }
        host.step().unwrap();
        host.host().take_copy::<i64>("out").unwrap()
    };
    // Before any gain comes, the default; the node did not wait for the edge.
    assert_eq!(tick(2, None), Some(20));
    // A fed gain is used on the tick it comes.
    assert_eq!(tick(2, Some(3)), Some(6));
    // And the default again on a tick the edge is empty.
    assert_eq!(tick(2, None), Some(20));
}

#[test]
fn the_plan_marks_the_edge_into_a_defaulted_input_as_having_a_fallback() {
    let explain = explain_plan(&planned());
    let marked: Vec<(&str, bool)> = explain
        .edges
        .iter()
        .filter(|edge| edge.to_node == 1)
        .map(|edge| (edge.to_port.as_str(), edge.fallback))
        .collect();
    assert_eq!(marked, [("x", false), ("gain", true)]);
}
