//! A `per_tick` edge at run time, set through a group's `edge_policies`: a value the reader did
//! not take in the tick it was sent in is gone, and released, when that tick ends.

use std::sync::Arc;

use daedalus_graph::{EdgePolicy, GraphBody, GroupDef, NodeKindDoc, StagePort};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_planner::{Config, plan};
use daedalus_planner_tests::{decl, edge, node, u32_ty};
use daedalus_registry::Registry;

use super::c21_group_input_policy::Other;
use super::c21_host_policy::Test;

const fn stage(stage: &'static str, port: &'static str) -> StagePort {
    StagePort { stage, port }
}

/// `opt` goes straight to the reader's optional input, so the caller's edge takes the policy.
const FED: GroupDef = GroupDef {
    stages: &[("reader", "t.reader")],
    inputs: &[
        ("trig", &[stage("reader", "trig")]),
        ("opt", &[stage("reader", "opt")]),
    ],
    outputs: &[("out", stage("reader", "out"))],
    edge_policies: &[(stage("reader", "opt"), EdgePolicy::PerTick)],
    ..GroupDef::new("t.fed")
};

/// `opt` goes through `pass`, on the other backend, so the connect takes the policy.
const CONNECTED: GroupDef = GroupDef {
    stages: &[("pass", "t.pass"), ("reader", "t.reader")],
    connects: &[(stage("pass", "out"), stage("reader", "opt"))],
    inputs: &[
        ("trig", &[stage("reader", "trig")]),
        ("opt", &[stage("pass", "in")]),
    ],
    outputs: &[("out", stage("reader", "out"))],
    edge_policies: &[(stage("reader", "opt"), EdgePolicy::PerTick)],
    ..GroupDef::new("t.connected")
};

/// `pass` feeds two readers, each with a trigger of its own: its output is one slot they share.
const SHARED: GroupDef = GroupDef {
    stages: &[
        ("pass", "t.pass"),
        ("first", "t.reader"),
        ("second", "t.reader"),
    ],
    connects: &[
        (stage("pass", "out"), stage("first", "opt")),
        (stage("pass", "out"), stage("second", "opt")),
    ],
    inputs: &[
        ("trig", &[stage("first", "trig")]),
        ("trig2", &[stage("second", "trig")]),
        ("opt", &[stage("pass", "in")]),
    ],
    outputs: &[
        ("out", stage("first", "out")),
        ("out2", stage("second", "out")),
    ],
    edge_policies: &[
        (stage("first", "opt"), EdgePolicy::PerTick),
        (stage("second", "opt"), EdgePolicy::PerTick),
    ],
    ..GroupDef::new("t.shared")
};

static GROUPS: [GroupDef; 6] = [
    SHARED,
    GroupDef {
        id: "t.shared_plain",
        edge_policies: &[],
        ..SHARED
    },
    FED,
    CONNECTED,
    GroupDef {
        id: "t.fed_plain",
        edge_policies: &[(stage("reader", "opt"), EdgePolicy::LatestOnly)],
        ..FED
    },
    GroupDef {
        id: "t.connected_plain",
        edge_policies: &[(stage("reader", "opt"), EdgePolicy::LatestOnly)],
        ..CONNECTED
    },
];

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.pass")
        .input("in", u32_ty())
        .output("out", u32_ty())
        .body("u")
        .add(&mut registry);
    // Runs when `trig` comes, with or without `opt`, and gives the sum of what it has.
    decl("t.reader")
        .input("trig", u32_ty())
        .optional("opt", u32_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    for group in &GROUPS {
        let mut group_decl = decl(group.id)
            .input("trig", u32_ty())
            .optional("opt", u32_ty())
            .output("out", u32_ty());
        if group.outputs.len() == 2 {
            group_decl = group_decl.input("trig2", u32_ty()).output("out2", u32_ty());
        }
        group_decl.group(group).body("t").add(&mut registry);
    }
    registry
}

fn loaded(group: &str) -> LoadedHost {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    for port in ["trig", "opt", "y"] {
        host.port_types.insert(port.to_string(), "u32".to_string());
    }
    let graph = GraphBody {
        nodes: vec![host, node(group)],
        edges: vec![
            edge(0, "trig", 1, "trig"),
            edge(0, "opt", 1, "opt"),
            edge(1, "out", 0, "y"),
        ],
        groups: Vec::new(),
    };
    let planned = plan(&graph, &registry(), &[&Test, &Other], &Config::default()).unwrap();
    LoadedHost::load(planned, LoadOptions::default()).unwrap()
}

fn y(host: &mut LoadedHost) -> Option<u32> {
    host.host()
        .take_value("y")
        .unwrap()
        .and_then(|value| value.typed::<u32>().copied())
}

/// Sends `opt` on one tick and `trig` on the next. What the reader gave, and how many handles to
/// the `opt` value were left after the first tick, the test's own included.
fn a_tick_apart(group: &str) -> (Option<u32>, usize) {
    let mut host = loaded(group);
    let sent = Arc::new(5_u32);
    host.host().feed_typed("opt", Arc::clone(&sent)).unwrap();
    host.step().unwrap();
    let held = Arc::strong_count(&sent);
    host.host().feed_typed("trig", Arc::new(100_u32)).unwrap();
    host.step().unwrap();
    (y(&mut host), held)
}

#[test]
fn a_per_tick_value_the_reader_did_not_take_is_gone_and_released_when_its_tick_ends() {
    // On the caller's edge to an input-fed stage.
    assert_eq!(a_tick_apart("t.fed"), (Some(100), 1));
    // On a connect inside the group: `pass` gave the reader a value of its own, also gone.
    assert_eq!(a_tick_apart("t.connected").0, Some(100));
}

#[test]
fn the_same_edges_as_latest_only_deliver_the_earlier_ticks_value() {
    assert_eq!(a_tick_apart("t.fed_plain"), (Some(105), 2));
    assert_eq!(a_tick_apart("t.connected_plain").0, Some(105));
}

#[test]
fn a_per_tick_value_is_read_in_the_tick_it_was_sent_in() {
    for group in ["t.fed", "t.connected"] {
        let mut host = loaded(group);
        host.host().feed_typed("opt", Arc::new(5_u32)).unwrap();
        host.host().feed_typed("trig", Arc::new(100_u32)).unwrap();
        host.step().unwrap();
        assert_eq!(y(&mut host), Some(105), "{group}");
    }
}

/// A graph around a two-reader group: `opt` and `trig` on one tick, so the first reader runs and
/// the second does not, then `trig2` on the next. What each reader gave.
fn two_readers(group: &str) -> (Option<u32>, Option<u32>) {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    for port in ["trig", "trig2", "opt", "y", "y2"] {
        host.port_types.insert(port.to_string(), "u32".to_string());
    }
    let graph = GraphBody {
        nodes: vec![host, node(group)],
        edges: vec![
            edge(0, "trig", 1, "trig"),
            edge(0, "trig2", 1, "trig2"),
            edge(0, "opt", 1, "opt"),
            edge(1, "out", 0, "y"),
            edge(1, "out2", 0, "y2"),
        ],
        groups: Vec::new(),
    };
    let planned = plan(&graph, &registry(), &[&Test, &Other], &Config::default()).unwrap();
    let mut host = LoadedHost::load(planned, LoadOptions::default()).unwrap();
    host.host().feed_typed("opt", Arc::new(5_u32)).unwrap();
    host.host().feed_typed("trig", Arc::new(100_u32)).unwrap();
    host.step().unwrap();
    let first = y(&mut host);
    host.host().feed_typed("trig2", Arc::new(200_u32)).unwrap();
    host.step().unwrap();
    let second = host
        .host()
        .take_value("y2")
        .unwrap()
        .and_then(|value| value.typed::<u32>().copied());
    (first, second)
}

#[test]
fn a_per_tick_output_with_two_readers_is_gone_for_both_when_its_tick_ends() {
    // The first reader ran in the tick and got it; the second runs a tick later and gets nothing.
    assert_eq!(two_readers("t.shared"), (Some(105), Some(200)));
}

#[test]
fn an_output_with_two_readers_and_no_policy_keeps_its_value_for_the_later_reader() {
    assert_eq!(two_readers("t.shared_plain"), (Some(105), Some(205)));
}
