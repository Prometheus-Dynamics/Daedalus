//! Per-step recovery read from node metadata: fail by default, skip, restart, retries, refusals of
//! malformed settings, and the manual off that stops a failing node.

use daedalus_graph::{GraphBody, Lit, NodeDoc};
use daedalus_host::{HostError, LoadError, LoadOptions, LoadedHost, RECOVERY_KEY, RETRIES_KEY};
use daedalus_planner::{Config, plan};
use daedalus_planner_tests::{decl, edge, node, u8_ty};
use daedalus_registry::Registry;
use daedalus_runner::{FailurePolicy, RunError};
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

/// `a -> b -> c`, with the given metadata on `b`. The sim makes `b` fault on every call.
fn graph(b_metadata: &[(&str, Lit)]) -> GraphBody {
    let mut nodes: Vec<NodeDoc> = ["a", "b", "c"]
        .iter()
        .map(|label| {
            let mut doc = node("t.n");
            doc.label = Some((*label).to_string());
            doc
        })
        .collect();
    for (key, value) in b_metadata {
        nodes[1].metadata.insert((*key).to_string(), value.clone());
    }
    GraphBody {
        nodes,
        edges: vec![edge(0, "out", 1, "in"), edge(1, "out", 2, "in")],
        groups: Vec::new(),
    }
}

/// Loads the graph with one step per node, so `b` is a step of its own.
fn load(b_metadata: &[(&str, Lit)]) -> Result<LoadedHost, LoadError> {
    let backend = SimBackend::new(SimConfig {
        faults: &[1],
        ..SimConfig::default()
    });
    let config = Config {
        merge: false,
        ..Config::default()
    };
    let planned =
        plan(&graph(b_metadata), &registry(), &[&backend], &config).expect("the graph plans");
    LoadedHost::load(planned, LoadOptions::default())
}

fn fail_fast(host: &mut LoadedHost) {
    host.host()
        .set_failure_policy(FailurePolicy::DEFAULT.with_fail_fast(true));
}

fn text(value: &str) -> Lit {
    Lit::Str(value.to_string())
}

#[test]
fn a_fault_fails_the_run_by_default() {
    let mut host = load(&[]).unwrap();
    fail_fast(&mut host);
    assert!(matches!(
        host.step(),
        Err(HostError::Run(RunError::Fault { .. }))
    ));
}

#[test]
fn skip_drops_the_tick_and_the_run_goes_on() {
    let mut host = load(&[(RECOVERY_KEY, text("skip"))]).unwrap();
    fail_fast(&mut host);
    let report = host.step().expect("a skipped fault does not stop the run");
    assert_eq!(report.faults, 1);
    assert_eq!(report.retries, 0);
}

#[test]
fn restart_is_accepted_and_the_fault_is_contained() {
    let mut host = load(&[(RECOVERY_KEY, text("restart"))]).unwrap();
    let report = host.step().unwrap();
    assert_eq!(report.faults, 1);
}

#[test]
fn retries_run_the_body_again_in_the_same_tick() {
    let mut plain = load(&[]).unwrap();
    let base = plain.step().unwrap();
    let mut host = load(&[(RETRIES_KEY, Lit::Int(2))]).unwrap();
    let report = host.step().unwrap();
    assert_eq!(report.retries, 2, "two retries after the first call");
    assert_eq!(report.faults, 1, "the fault stands after the last retry");
    // The same tick without retries makes `base` calls; two retries add exactly two more.
    assert_eq!(report.step_calls, base.step_calls + 2);
}

#[test]
fn a_retried_fault_still_stops_a_fail_fast_run() {
    let mut host = load(&[(RETRIES_KEY, Lit::Int(1))]).unwrap();
    fail_fast(&mut host);
    assert!(matches!(
        host.step(),
        Err(HostError::Run(RunError::Fault { .. }))
    ));
}

#[test]
fn retries_and_recovery_together_are_refused() {
    let both = [(RECOVERY_KEY, text("skip")), (RETRIES_KEY, Lit::Int(2))];
    match load(&both) {
        Err(LoadError::Recovery { node, reason }) => {
            assert_eq!(node, "b");
            assert!(reason.contains("one of them"), "{reason}");
        }
        _ => panic!("both keys should be refused"),
    }
}

#[test]
fn an_unknown_recovery_name_is_refused() {
    assert!(matches!(
        load(&[(RECOVERY_KEY, text("explode"))]),
        Err(LoadError::Recovery { .. })
    ));
}

#[test]
fn a_recovery_that_is_not_text_is_refused() {
    assert!(matches!(
        load(&[(RECOVERY_KEY, Lit::Int(1))]),
        Err(LoadError::Recovery { .. })
    ));
}

#[test]
fn zero_or_non_integer_retries_are_refused() {
    assert!(matches!(
        load(&[(RETRIES_KEY, Lit::Int(0))]),
        Err(LoadError::Recovery { .. })
    ));
    assert!(matches!(
        load(&[(RETRIES_KEY, Lit::Int(-3))]),
        Err(LoadError::Recovery { .. })
    ));
    assert!(matches!(
        load(&[(RETRIES_KEY, text("3"))]),
        Err(LoadError::Recovery { .. })
    ));
}

#[test]
fn a_failing_node_is_never_switched_off_by_default() {
    let mut host = load(&[]).unwrap();
    let mut now = 0;
    for _ in 0..20 {
        // Each tick is long after the backoff, so the body is called every time.
        now += 60 * SECOND;
        host.host().set_clock(now);
        assert_eq!(host.step().unwrap().faults, 1);
    }
    assert_eq!(host.node_is_on("b"), Some(true));
}

#[test]
fn a_manual_off_stops_a_failing_node_and_on_brings_it_back() {
    let mut host = load(&[]).unwrap();
    fail_fast(&mut host);
    host.set_node_on("b", false).unwrap();
    assert_eq!(host.node_is_on("b"), Some(false));
    let report = host.step().expect("an off node is not called");
    assert_eq!(report.faults, 0);

    host.set_node_on("b", true).unwrap();
    assert!(matches!(
        host.step(),
        Err(HostError::Run(RunError::Fault { .. }))
    ));
}
