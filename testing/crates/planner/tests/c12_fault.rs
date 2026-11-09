//! Failure reports from a loaded plan, and running unmerged to find the failing node.

use daedalus_graph::{GraphBody, NodeDoc};
use daedalus_host::{FaultReport, LoadOptions, LoadedHost, UnmergedError};
use daedalus_planner::{Config, Planned, plan};
use daedalus_planner_tests::{decl, edge, node, u8_ty};
use daedalus_registry::Registry;
use daedalus_sim::{BACKEND_ID, SimBackend, SimConfig};

const SECOND: u64 = 1_000_000_000;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.n")
        .optional("in", u8_ty())
        .output("out", u8_ty())
        .body(BACKEND_ID)
        .add(&mut registry);
    registry
}

fn three() -> GraphBody {
    let nodes: Vec<NodeDoc> = ["a", "b", "c"]
        .iter()
        .map(|label| {
            let mut doc = node("t.n");
            doc.label = Some((*label).to_string());
            doc
        })
        .collect();
    GraphBody {
        nodes,
        edges: vec![edge(0, "out", 1, "in"), edge(1, "out", 2, "in")],
        groups: Vec::new(),
    }
}

fn planned(merge: bool) -> Result<Planned, String> {
    let backend = SimBackend::new(SimConfig {
        merge: true,
        faults: &[1],
        ..SimConfig::default()
    });
    let config = Config {
        merge,
        ..Config::default()
    };
    plan(&three(), &registry(), &[&backend], &config).map_err(|error| format!("{error:?}"))
}

fn loaded(options: LoadOptions) -> LoadedHost {
    LoadedHost::load(planned(true).unwrap(), options).unwrap()
}

fn reports(host: &mut LoadedHost) -> Vec<FaultReport> {
    host.fault_reports().copied().collect()
}

#[test]
fn a_failing_node_in_a_merged_region_is_reported_with_its_covers_and_at() {
    let mut host = loaded(LoadOptions::default());
    host.step().unwrap();
    let all = reports(&mut host);
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].covers, ["a", "b", "c"]);
    assert_eq!(all[0].at, Some("b"));
    assert_eq!(all[0].consecutive, 1);
    assert_eq!(all[0].tick, 1);
    assert!(!all[0].reason().is_empty());
}

#[test]
fn the_unmerged_run_reports_the_single_failing_node() {
    let mut host = loaded(LoadOptions::default());
    host.set_planner(planned);
    host.step().unwrap();
    assert!(!host.unmerged());

    host.set_unmerged(true).unwrap();
    assert!(host.unmerged());
    host.step().unwrap();
    let all = reports(&mut host);
    let last = all.last().unwrap();
    assert_eq!(last.covers, ["b"]);
    assert_eq!(last.node(), Some("b"));

    host.set_unmerged(false).unwrap();
    assert!(!host.unmerged());
}

#[test]
fn unmerged_mode_needs_a_planner_and_leaves_the_graph_when_it_has_none() {
    let mut host = loaded(LoadOptions::default());
    assert!(matches!(
        host.set_unmerged(true),
        Err(UnmergedError::NoPlanner)
    ));
    assert!(!host.unmerged());
    host.step().unwrap();
}

#[test]
fn a_merged_region_is_never_unmerged_by_itself() {
    let mut host = loaded(LoadOptions::default());
    let mut now = 0;
    for _ in 0..8 {
        now += 60 * SECOND;
        host.host().set_clock(now);
        host.step().unwrap();
    }
    let all = reports(&mut host);
    assert_eq!(all.last().unwrap().covers, ["a", "b", "c"]);
    assert_eq!(all.last().unwrap().consecutive, 8);
    assert!(all.last().unwrap().until_ns > now, "it is backing off");
}

#[test]
fn the_ring_keeps_the_last_n() {
    let mut host = loaded(LoadOptions {
        fault_reports: 3,
        ..LoadOptions::default()
    });
    let mut now = 0;
    for _ in 0..7 {
        now += 60 * SECOND;
        host.host().set_clock(now);
        host.step().unwrap();
    }
    let ticks: Vec<u32> = reports(&mut host)
        .iter()
        .map(|report| report.tick)
        .collect();
    assert_eq!(ticks, [5, 6, 7]);
}
