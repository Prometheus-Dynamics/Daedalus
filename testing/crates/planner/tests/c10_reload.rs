//! Loading a plan into a host and reloading it. These live here because a core crate can't use a
//! backend, and a reload needs steps from one.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use daedalus_graph::{GraphBody, NodeDoc, NodeKindDoc};
use daedalus_host::{LoadError, LoadOptions, LoadedHost, ReloadError};
use daedalus_planner::{Config, Form, Planned, plan};
use daedalus_planner_tests::{decl, edge, node, u8_ty};
use daedalus_registry::Registry;
use daedalus_sim::{BACKEND_ID, SimBackend, SimConfig, SimForm};
use daedalus_types::NodeId;

fn registry() -> Registry {
    let mut registry = Registry::new();
    for id in ["t.a", "t.b", "t.c"] {
        decl(id)
            .optional("in", u8_ty())
            .output("out", u8_ty())
            .body(BACKEND_ID)
            .add(&mut registry);
    }
    registry
}

fn labelled(id: &str, label: &str) -> NodeDoc {
    let mut doc = node(id);
    doc.label = Some(label.to_string());
    doc
}

fn bridge(label: &str) -> NodeDoc {
    let mut host = labelled("host", label);
    host.kind = NodeKindDoc::HostBridge;
    host.port_types.insert("out".to_string(), "u8".to_string());
    host
}

fn chain(nodes: Vec<NodeDoc>) -> GraphBody {
    let edges = (1..nodes.len() as u32)
        .map(|i| edge(i - 1, "out", i, "in"))
        .collect();
    GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    }
}

fn planned(graph: &GraphBody) -> Planned {
    planned_as(graph, SimForm::Steps, Form::Steps)
}

fn planned_as(graph: &GraphBody, sim: SimForm, form: Form) -> Planned {
    let backend = SimBackend::new(SimConfig {
        form: sim,
        ..SimConfig::default()
    });
    let config = Config {
        form,
        ..Config::default()
    };
    plan(graph, &registry(), &[&backend], &config).unwrap()
}

fn loaded(graph: &GraphBody) -> LoadedHost {
    LoadedHost::load(planned(graph), LoadOptions::default()).unwrap()
}

fn id(key: &'static str) -> NodeId {
    NodeId::new(key)
}

#[test]
fn a_plan_loads_and_its_steps_run() {
    let before = daedalus_sim::ran(0);
    let mut host = loaded(&chain(vec![labelled("t.a", "a"), labelled("t.b", "b")]));
    host.host().step().unwrap();
    assert!(daedalus_sim::ran(0) > before);
}

#[test]
fn reload_carries_state_of_kept_nodes() {
    let mut host = loaded(&chain(vec![labelled("t.a", "a"), labelled("t.b", "b")]));
    host.state().set(id("a"), 0, 41_u32);
    let report = host
        .reload(planned(&chain(vec![
            labelled("t.a", "a"),
            labelled("t.b", "b"),
        ])))
        .unwrap();
    assert!(report.carried.contains("a"));
    assert_eq!(report.carried.total(), 2);
    assert_eq!(report.fresh.total(), 0);
    assert_eq!(host.state().get::<u32>(id("a"), 0), Some(&41));
    host.host().step().unwrap();
}

#[test]
fn a_changed_body_starts_fresh_and_is_reported() {
    let mut host = loaded(&chain(vec![labelled("t.a", "a"), labelled("t.b", "b")]));
    host.state().set(id("b"), 0, 7_u32);
    let report = host
        .reload(planned(&chain(vec![
            labelled("t.a", "a"),
            labelled("t.c", "b"),
        ])))
        .unwrap();
    assert!(report.fresh.contains("b"));
    assert_eq!(host.state().get::<u32>(id("b"), 0), None);
}

#[test]
fn removed_node_state_is_dropped_and_reported() {
    let mut host = loaded(&chain(vec![labelled("t.a", "a"), labelled("t.b", "b")]));
    host.state().set(id("b"), 0, 7_u32);
    let report = host
        .reload(planned(&chain(vec![labelled("t.a", "a")])))
        .unwrap();
    assert!(report.removed.contains("b"));
    assert_eq!(report.removed.total(), 1);
    assert!(host.state().is_empty());
}

#[test]
fn a_new_node_is_reported_as_added() {
    let mut host = loaded(&chain(vec![labelled("t.a", "a")]));
    let report = host
        .reload(planned(&chain(vec![
            labelled("t.a", "a"),
            labelled("t.b", "b"),
        ])))
        .unwrap();
    assert!(report.added.contains("b"));
}

#[test]
fn failed_compile_keeps_the_running_graph() {
    let mut host = loaded(&chain(vec![labelled("t.a", "a")]));
    host.state().set(id("a"), 0, 5_u32);
    let blob = planned_as(
        &chain(vec![labelled("t.a", "a")]),
        SimForm::Blob,
        Form::Blob,
    );
    let error = host.reload(blob).unwrap_err();
    assert!(
        matches!(error, ReloadError::Load(LoadError::NotSteps)),
        "{error}"
    );
    assert_eq!(host.state().get::<u32>(id("a"), 0), Some(&5));
    let keys: Vec<&str> = host.node_keys().collect();
    assert_eq!(keys, ["a"]);
    host.host().step().unwrap();
}

#[test]
fn reload_rejects_a_graph_on_other_bridges() {
    let mut host = loaded(&chain(vec![bridge("left"), labelled("t.a", "a")]));
    host.state().set(id("a"), 0, 5_u32);
    let other = planned(&chain(vec![bridge("right"), labelled("t.a", "a")]));
    let error = host.reload(other).unwrap_err();
    assert!(matches!(error, ReloadError::OtherBridges { .. }), "{error}");
    assert_eq!(host.state().get::<u32>(id("a"), 0), Some(&5));
    let same = planned(&chain(vec![bridge("left"), labelled("t.a", "a")]));
    host.reload(same).unwrap();
}

struct Counted(Arc<AtomicU32>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn a_graph_reload_carries_held_frames_or_releases_them() {
    let drops = Arc::new(AtomicU32::new(0));
    let graph = chain(vec![labelled("t.a", "a"), labelled("t.b", "b")]);
    let mut host = loaded(&graph);
    host.state().set(id("b"), 0, Counted(drops.clone()));

    host.reload(planned(&graph)).unwrap();
    assert_eq!(
        drops.load(Ordering::Relaxed),
        0,
        "a kept node keeps what it holds"
    );
    assert!(host.state().get::<Counted>(id("b"), 0).is_some());

    let report = host
        .reload(planned(&chain(vec![labelled("t.a", "a")])))
        .unwrap();
    assert_eq!(report.removed.total(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 1, "released exactly once");

    host.reload(planned(&chain(vec![labelled("t.a", "a")])))
        .unwrap();
    drop(host);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn reloading_the_same_graph_many_times_runs_each_version() {
    let graph = chain(vec![labelled("t.a", "a"), labelled("t.b", "b")]);
    let mut host = loaded(&graph);
    for _ in 0..50 {
        host.reload(planned(&graph)).unwrap();
        host.host().step().unwrap();
    }
}

#[test]
fn a_removed_host_port_drops_its_queued_values_and_says_how_many() {
    let mut host = loaded(&chain(vec![bridge("left"), labelled("t.a", "a")]));
    host.host().feed("out", u8_value()).unwrap();
    let report = host
        .reload(planned(&chain(vec![labelled("t.a", "a")])))
        .unwrap();
    assert!(report.removed_ports.contains("out"));
    assert_eq!(report.dropped_values, 1);
}

fn u8_value() -> daedalus_value::Value {
    daedalus_value::Value {
        ty: daedalus_types::TypeIndex(0),
        space: daedalus_types::SpaceIndex(0),
        owner: daedalus_value::Owner::Static(b"x"),
    }
}
