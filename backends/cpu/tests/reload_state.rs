//! A reload keeps the state of a node whose step did not change, and says which nodes started
//! fresh. A node's state is in its step, so the step is what moves across.

#![cfg(feature = "merge")]

use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, Lit, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{NodeRecord, Registry};

#[node(id = "r.double", inputs(x), outputs(y))]
fn double(x: i64) -> i64 {
    x * 2
}

#[node(id = "r.negate", inputs(x), outputs(y))]
fn negate(x: i64) -> i64 {
    -x
}

#[derive(Default)]
struct Total(i64);

/// A running total, scaled by a constant.
#[node(id = "r.total", inputs(x, scale), outputs(sum), state(Total))]
fn total(x: i64, scale: i64, state: &mut Total) -> i64 {
    state.0 += x * scale;
    state.0
}

const NODES: [&NodeRecord; 3] = [&DOUBLE_NODE, &NEGATE_NODE, &TOTAL_NODE];

fn labelled(id: &str, label: &str) -> NodeDoc {
    let mut doc = NodeDoc::new(id);
    doc.label = Some(label.to_string());
    doc
}

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

/// `host.x -> shape... -> total -> host.sum`, with the stages given before the total.
fn planned(shapes: &[&str], scale: i64) -> Planned {
    let mut bridge = labelled("host", "io");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "sum"] {
        bridge
            .port_types
            .insert(port.to_string(), "i64".to_string());
    }
    let mut nodes = vec![bridge];
    let mut edges = Vec::new();
    for (at, id) in shapes.iter().enumerate() {
        let at = at as u32 + 1;
        nodes.push(labelled(id, &format!("shape{at}")));
        edges.push(match at {
            1 => edge(0, "x", 1, "x"),
            at => edge(at - 1, "y", at, "x"),
        });
    }
    let last = shapes.len() as u32 + 1;
    let mut total = labelled("r.total", "total");
    total.consts.insert("scale".to_string(), Lit::Int(scale));
    nodes.push(total);
    edges.push(edge(last - 1, "y", last, "x"));
    edges.push(edge(last, "sum", 0, "sum"));
    let graph = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    // Merging off: each node is its own step, so one node can change beside another.
    let config = Config {
        merge: false,
        ..Config::default()
    };
    plan(&graph, &registry, &[&CpuBackend], &config).unwrap()
}

fn through(host: &mut LoadedHost, x: i64) -> i64 {
    let io = host.host();
    io.feed_typed("x", Arc::new(x)).unwrap();
    io.step().unwrap();
    io.take_copy::<i64>("sum").unwrap().unwrap()
}

#[test]
fn a_reload_keeps_the_state_of_a_node_whose_step_did_not_change() {
    let mut host = LoadedHost::load(planned(&["r.double"], 1), LoadOptions::default()).unwrap();
    assert_eq!([1, 2, 3].map(|x| through(&mut host, x)), [2, 6, 12]);

    // The stage before the total is replaced. The total goes on from 12.
    let report = host.reload(planned(&["r.negate"], 1)).unwrap();
    assert_eq!(report.carried.names(), ["io", "total"]);
    assert_eq!(report.fresh.names(), ["shape1"]);
    assert_eq!((report.added.total(), report.removed.total()), (0, 0));
    assert_eq!([1, 2, 3].map(|x| through(&mut host, x)), [11, 9, 6]);

    // The same graph again: everything is kept.
    let report = host.reload(planned(&["r.negate"], 1)).unwrap();
    assert_eq!(report.carried.names(), ["io", "shape1", "total"]);
    assert_eq!(report.fresh.total(), 0);
    assert_eq!(through(&mut host, 1), 5);
}

#[test]
fn a_node_whose_constants_or_place_changed_starts_fresh_and_is_reported() {
    let mut host = LoadedHost::load(planned(&["r.double"], 1), LoadOptions::default()).unwrap();
    assert_eq!(through(&mut host, 5), 10);

    // A constant of the total changes. Its step holds the old one, so it is compiled again
    // and its state starts over, and the report says so.
    let report = host.reload(planned(&["r.double"], 3)).unwrap();
    assert_eq!(report.carried.names(), ["io", "shape1"]);
    assert_eq!(report.fresh.names(), ["total"]);
    assert_eq!(through(&mut host, 1), 6);

    // A stage is added before it. The total is at another place in the graph, which its step
    // names when it reports a failure, so it starts over too.
    let report = host.reload(planned(&["r.double", "r.negate"], 3)).unwrap();
    assert_eq!(report.carried.names(), ["io", "shape1"]);
    assert_eq!(report.fresh.names(), ["total"]);
    assert_eq!(report.added.names(), ["shape2"]);
    assert_eq!(through(&mut host, 1), -6);
}
