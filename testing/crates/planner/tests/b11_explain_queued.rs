//! The explanation's `queued` list: an edge that holds more than one value into a node that
//! waits for its other inputs.

use daedalus_graph::{EdgePolicyDoc, GraphBody, NodeKindDoc};
use daedalus_planner::explain::{explain_plan, json};
use daedalus_planner::{Config, plan};
use daedalus_planner_tests::{decl, edge, node, u32_ty};
use daedalus_registry::{Fire, Registry};

use super::c21_host_policy::Test;

fn registry() -> Registry {
    let mut registry = Registry::new();
    // `decl` nodes fire when every connected input has a new value.
    decl("t.pair")
        .input("frame", u32_ty())
        .input("trigger", u32_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    decl("t.any")
        .input("frame", u32_ty())
        .input("trigger", u32_ty())
        .output("out", u32_ty())
        .fire(Fire::Any)
        .body("t")
        .add(&mut registry);
    decl("t.add")
        .input("in", u32_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    registry
}

/// The host's `frame` and `trigger` into `reader`, with `policy` on the frame edge.
fn explained(reader: &str, policy: EdgePolicyDoc) -> daedalus_planner::explain::Explain {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    for port in ["frame", "trigger", "y"] {
        host.port_types.insert(port.to_string(), "u32".to_string());
    }
    let mut frame = edge(0, "frame", 1, "frame");
    frame.policy = policy;
    let mut edges = vec![frame, edge(1, "out", 0, "y")];
    if reader != "t.add" {
        edges.push(edge(0, "trigger", 1, "trigger"));
    } else {
        edges[0].to.port = "in".to_string();
    }
    let graph = GraphBody {
        nodes: vec![host, node(reader)],
        edges,
        groups: Vec::new(),
    };
    explain_plan(&plan(&graph, &registry(), &[&Test], &Config::default()).unwrap())
}

#[test]
fn a_fifo_into_a_node_that_waits_on_another_input_is_listed_with_what_it_waits_on() {
    let explain = explained("t.pair", EdgePolicyDoc::Fifo);
    // Both inputs queue while the node waits for the other.
    let ports: Vec<&str> = explain.queued.iter().map(|q| q.port.as_str()).collect();
    assert_eq!(ports, ["frame", "trigger"]);
    let frame = &explain.queued[0];
    assert_eq!((frame.node, frame.depth), (1, None));
    assert_eq!(frame.waits_on, ["trigger"]);
    assert_eq!(
        frame.note,
        "`t.pair` runs only when trigger also has a new value, and until then this edge holds up \
         to the FIFO depth (64 by default); if the value holds something scarce, give the edge \
         latest_only or per_tick"
    );
    // It survives the JSON form.
    let text = json::to_json(&explain).unwrap();
    assert_eq!(json::from_json(&text).unwrap(), explain);
}

#[test]
fn a_bounded_edge_deeper_than_one_is_listed_with_its_depth() {
    let bounded = EdgePolicyDoc::Bounded {
        capacity: 8,
        overflow: "drop_oldest".to_string(),
    };
    let explain = explained("t.pair", bounded);
    let frame = explain.queued.iter().find(|q| q.port == "frame").unwrap();
    assert_eq!(frame.depth, Some(8));
}

#[test]
fn an_edge_that_holds_one_value_or_a_node_that_does_not_wait_is_not_listed() {
    for policy in [EdgePolicyDoc::LatestOnly, EdgePolicyDoc::PerTick] {
        let explain = explained("t.pair", policy.clone());
        let ports: Vec<&str> = explain.queued.iter().map(|q| q.port.as_str()).collect();
        assert_eq!(ports, ["trigger"], "{policy:?}");
    }
    // A node that fires on any input drops what it can't use; a node with one input waits on
    // nothing.
    assert!(explained("t.any", EdgePolicyDoc::Fifo).queued.is_empty());
    assert!(explained("t.add", EdgePolicyDoc::Fifo).queued.is_empty());
    let text = json::to_json(&explained("t.add", EdgePolicyDoc::Fifo)).unwrap();
    assert!(!text.contains("queued"), "an empty list is left out");
}
