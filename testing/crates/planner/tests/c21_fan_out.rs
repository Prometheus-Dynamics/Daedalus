//! An output with several readers, at run time: each reader has an edge of its own, so the number
//! of readers changes nothing about what a value means. A reader takes a value once; one that has
//! not run still finds it; each edge has its own depth and its own policy.

use std::sync::Arc;

use daedalus_graph::{EdgePolicyDoc, GraphBody, NodeKindDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_planner::{Config, plan};
use daedalus_planner_tests::{decl, edge, node, u32_ty};
use daedalus_registry::Registry;

use super::c21_group_input_policy::Other;
use super::c21_host_policy::Test;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.pass")
        .input("in", u32_ty())
        .output("out", u32_ty())
        .body("u")
        .add(&mut registry);
    decl("t.add")
        .input("in", u32_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    // Runs when `trig` comes, with or without `opt`, and gives the sum of what it has.
    decl("t.reader")
        .input("trig", u32_ty())
        .optional("opt", u32_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    registry
}

fn load(ports: &[&str], nodes: &[&str], edges: Vec<daedalus_graph::EdgeDoc>) -> LoadedHost {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    for port in ports {
        host.port_types.insert(port.to_string(), "u32".to_string());
    }
    let mut all = vec![host];
    all.extend(nodes.iter().map(|id| node(id)));
    let graph = GraphBody {
        nodes: all,
        edges,
        groups: Vec::new(),
    };
    let planned = plan(&graph, &registry(), &[&Test, &Other], &Config::default()).unwrap();
    LoadedHost::load(planned, LoadOptions::default()).unwrap()
}

fn feed(host: &mut LoadedHost, port: &str, value: u32) {
    host.host().feed_typed(port, Arc::new(value)).unwrap();
}

fn take(host: &mut LoadedHost, port: &str) -> Option<u32> {
    host.host()
        .take_value(port)
        .unwrap()
        .and_then(|value| value.typed::<u32>().copied())
}

#[test]
fn a_required_output_with_two_readers_does_not_fire_a_reader_that_already_took_it() {
    // `pass` (1) feeds the trigger of two readers (2 and 3); the host feeds the first one's `opt`.
    let mut host = load(
        &["x", "opt", "y", "y2"],
        &["t.pass", "t.reader", "t.reader"],
        vec![
            edge(0, "x", 1, "in"),
            edge(1, "out", 2, "trig"),
            edge(1, "out", 3, "trig"),
            edge(0, "opt", 2, "opt"),
            edge(2, "out", 0, "y"),
            edge(3, "out", 0, "y2"),
        ],
    );
    feed(&mut host, "x", 5);
    host.step().unwrap();
    assert_eq!(
        (take(&mut host, "y"), take(&mut host, "y2")),
        (Some(5), Some(5))
    );
    // A later tick wakes the first reader with only its optional input. Its trigger was taken
    // on the tick it came, so the reader waits for the next one.
    feed(&mut host, "opt", 1);
    host.step().unwrap();
    assert_eq!(take(&mut host, "y"), None);
    feed(&mut host, "x", 7);
    host.step().unwrap();
    assert_eq!(
        (take(&mut host, "y"), take(&mut host, "y2")),
        (Some(8), Some(7))
    );
}

/// `pass` (1) feeds `add` (2), which runs on every value, and the `opt` of a reader (3) that
/// waits for the host's `trig`. The reader's edge carries `policy`.
fn busy_and_waiting(policy: EdgePolicyDoc) -> LoadedHost {
    let mut waiting = edge(1, "out", 3, "opt");
    waiting.policy = policy;
    load(
        &["x", "trig", "y", "y2"],
        &["t.pass", "t.add", "t.reader"],
        vec![
            edge(0, "x", 1, "in"),
            edge(1, "out", 2, "in"),
            waiting,
            edge(0, "trig", 3, "trig"),
            edge(2, "out", 0, "y"),
            edge(3, "out", 0, "y2"),
        ],
    )
}

/// Sends 1 to `count` through `pass`, one a tick, checking that `add` got each, and then the
/// trigger. What the waiting reader gave.
fn send_then_trigger(host: &mut LoadedHost, count: u32) -> Option<u32> {
    for value in 1..=count {
        feed(host, "x", value);
        host.step().unwrap();
        assert_eq!(take(host, "y"), Some(value));
    }
    feed(host, "trig", 1000);
    host.step().unwrap();
    take(host, "y2")
}

#[test]
fn a_reader_that_has_not_run_finds_the_values_queued_on_its_own_edge() {
    // A FIFO of its own: the oldest of the ten, where a shared slot held only the newest.
    let mut host = busy_and_waiting(EdgePolicyDoc::Fifo);
    assert_eq!(send_then_trigger(&mut host, 10), Some(1001));
    // And the next trigger gets the next one.
    feed(&mut host, "trig", 2000);
    host.step().unwrap();
    assert_eq!(take(&mut host, "y2"), Some(2002));
}

#[test]
fn two_readers_of_one_output_each_follow_their_own_edges_policy() {
    let mut host = busy_and_waiting(EdgePolicyDoc::LatestOnly);
    assert_eq!(send_then_trigger(&mut host, 10), Some(1010));
    let mut host = busy_and_waiting(EdgePolicyDoc::PerTick);
    assert_eq!(send_then_trigger(&mut host, 10), Some(1000));
}

#[test]
fn a_fifo_edge_to_a_reader_that_never_runs_overflows_naming_both_ends_its_depth_and_policy() {
    let mut host = busy_and_waiting(EdgePolicyDoc::Fifo);
    let depth = LoadOptions::default().fifo_capacity;
    for value in 0..depth {
        feed(&mut host, "x", value);
        host.step().unwrap();
    }
    feed(&mut host, "x", depth);
    let error = host.step().unwrap_err();
    assert_eq!(
        error.to_string(),
        format!(
            "the edge from t.pass#0.out to t.reader#0.opt is full: it holds {depth} and its policy is \
             fifo, so the reader is not keeping up; give the edge a policy that drops \
             (latest_only, per_tick, or bounded with drop_oldest) or one that blocks"
        )
    );
}

#[test]
fn a_host_input_with_two_readers_is_still_one_kept_value() {
    // The host's `x` feeds the `opt` of two readers: no fan, one value kept for both, and a
    // second feed before a tick replaces the first.
    let mut host = load(
        &["x", "trig", "trig2", "y", "y2"],
        &["t.reader", "t.reader"],
        vec![
            edge(0, "x", 1, "opt"),
            edge(0, "x", 2, "opt"),
            edge(0, "trig", 1, "trig"),
            edge(0, "trig2", 2, "trig"),
            edge(1, "out", 0, "y"),
            edge(2, "out", 0, "y2"),
        ],
    );
    let steps = host.host().runner().counters().steps_called;
    feed(&mut host, "x", 5);
    feed(&mut host, "x", 6);
    feed(&mut host, "trig", 100);
    feed(&mut host, "trig2", 200);
    host.step().unwrap();
    assert_eq!(
        (take(&mut host, "y"), take(&mut host, "y2")),
        (Some(106), Some(206))
    );
    // Two steps ran, the two readers: nothing stands between the host and them.
    assert_eq!(host.host().runner().counters().steps_called - steps, 2);
}
