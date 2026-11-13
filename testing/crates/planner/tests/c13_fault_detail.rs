//! What a step gives `StepCx::detail` before it faults is kept by the host with its failure.

use daedalus_graph::{GraphBody, NodeDoc};
use daedalus_host::{FaultReport, LoadOptions, LoadedHost};
use daedalus_planner::{Config, plan};
use daedalus_planner_tests::{decl, edge, node, u8_ty};
use daedalus_registry::Registry;
use daedalus_sim::{BACKEND_ID, SimBackend, SimConfig, write_details};

const SECOND: u64 = 1_000_000_000;
/// The lines a failing sim node writes after saying that it ran.
const LINES: usize = 40;

/// Three nodes in a row, each its own step, or all one step when `merge`. Node 1 fails.
fn loaded(merge: bool, options: LoadOptions) -> LoadedHost {
    write_details(LINES);
    let mut registry = Registry::new();
    decl("t.n")
        .optional("in", u8_ty())
        .output("out", u8_ty())
        .body(BACKEND_ID)
        .add(&mut registry);
    let nodes: Vec<NodeDoc> = ["a", "b", "c"]
        .iter()
        .map(|label| {
            let mut doc = node("t.n");
            doc.label = Some((*label).to_string());
            doc
        })
        .collect();
    let graph = GraphBody {
        nodes,
        edges: vec![edge(0, "out", 1, "in"), edge(1, "out", 2, "in")],
        groups: Vec::new(),
    };
    let backend = SimBackend::new(SimConfig {
        merge: true,
        faults: &[1],
        ..SimConfig::default()
    });
    let config = Config {
        merge,
        ..Config::default()
    };
    let planned = plan(&graph, &registry, &[&backend], &config).expect("a plan");
    LoadedHost::load(planned, options).expect("loads")
}

fn last(host: &mut LoadedHost) -> FaultReport {
    *host.fault_reports().last().expect("a failure")
}

/// What node 1 writes: that it ran, and then its lines.
fn written_by_the_failing_node() -> String {
    let mut text = String::from("node 1 ran\n");
    for line in 0..LINES {
        text.push_str(&format!("node 1 found line {line}\n"));
    }
    text
}

#[test]
fn a_failing_step_keeps_what_it_wrote_and_the_others_keep_nothing() {
    let mut host = loaded(false, LoadOptions::default());
    host.step().unwrap();
    let report = last(&mut host);
    assert_eq!(report.node(), Some("b"));
    assert_eq!(
        report.reason(),
        "sim fault",
        "the reason is still the headline"
    );
    assert!(report.detail, "and the report says there is more");
    let detail = host.fault_detail(report.step).expect("the detail");
    assert_eq!(detail.text, written_by_the_failing_node());
    assert!(detail.text.len() > daedalus_abi::REASON_CAPACITY);
    assert!(!detail.cut);
    assert_eq!(detail.tick, report.tick);
    // Node 0 ran and wrote, and didn't fail: nothing of its is kept.
    for step in 0..8 {
        if step != report.step {
            assert_eq!(host.fault_detail(step), None, "step {step}");
        }
    }
}

#[test]
fn a_merged_step_keeps_what_every_node_of_it_wrote_up_to_the_failure() {
    let mut host = loaded(true, LoadOptions::default());
    host.step().unwrap();
    let report = last(&mut host);
    assert_eq!(report.covers, ["a", "b", "c"]);
    let detail = host.fault_detail(report.step).expect("the detail");
    assert_eq!(
        detail.text,
        format!("node 0 ran\n{}", written_by_the_failing_node())
    );
}

#[test]
fn a_detail_is_the_last_failures_and_is_not_added_to() {
    let mut host = loaded(false, LoadOptions::default());
    let mut now = 0;
    for _ in 0..3 {
        now += 60 * SECOND;
        host.host().set_clock(now);
        host.step().unwrap();
    }
    let report = last(&mut host);
    assert_eq!(report.tick, 3);
    let detail = host.fault_detail(report.step).expect("the detail");
    assert_eq!(detail.tick, 3);
    assert_eq!(detail.text, written_by_the_failing_node());
    assert!(
        host.fault_reports().all(|report| report.detail),
        "each report of the three says it had one"
    );
}

#[test]
fn a_detail_past_the_bound_is_cut_and_says_so() {
    let mut host = loaded(
        false,
        LoadOptions {
            fault_detail_bytes: 100,
            ..LoadOptions::default()
        },
    );
    host.step().unwrap();
    let report = last(&mut host);
    let detail = host.fault_detail(report.step).expect("the detail");
    assert_eq!(detail.text, &written_by_the_failing_node()[..100]);
    assert!(detail.cut);
}

#[test]
fn a_host_that_keeps_no_detail_gets_none_and_the_step_isnt_asked() {
    let mut host = loaded(
        false,
        LoadOptions {
            fault_detail_bytes: 0,
            ..LoadOptions::default()
        },
    );
    host.step().unwrap();
    let report = last(&mut host);
    assert_eq!(report.reason(), "sim fault");
    assert!(!report.detail);
    assert_eq!(host.fault_detail(report.step), None);
}
