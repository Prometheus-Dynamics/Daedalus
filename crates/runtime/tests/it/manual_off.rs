//! Switching a node off by hand: an off node runs no frame, its consumer sees nothing new, and it
//! runs again once switched on. Uses the `resilience` module's fixtures.

use std::sync::Arc;
use std::time::Duration;

use daedalus_runtime::SchedulerConfig;
use daedalus_runtime::build_runtime;
use daedalus_runtime::executor::OwnedExecutor;

use super::resilience::{Flaky, chain};

#[test]
fn a_disabled_node_runs_no_frame_and_its_consumer_sees_nothing() {
    let rt = Arc::new(build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    ));
    let handler = Flaky::new(0);
    let seen = handler.seen.clone();
    let mut exec = OwnedExecutor::new(rt, handler);
    exec.try_set_node_enabled("flaky", false)
        .expect("a known node");

    let telemetry = exec.run_in_place().expect("tick");
    assert!(seen.lock().is_empty(), "no output from an off node");
    assert_eq!(telemetry.recovery().backoff_skips, 1);
    assert_eq!(telemetry.node_health.len(), 1);
    assert_eq!(telemetry.node_health[0].node_id, "flaky");
    assert_eq!(telemetry.node_health[0].state, "disabled");
    assert_eq!(telemetry.node_health[0].backoff_remaining, None);

    // Switched on, the node runs again: its frame reaches the sink.
    exec.try_set_node_enabled("flaky", true)
        .expect("a known node");
    let telemetry = exec.run_in_place().expect("tick");
    assert_eq!(*seen.lock(), vec![2]);
    assert!(
        telemetry.node_health.is_empty(),
        "a node that never failed has no report"
    );
}

/// Switching a node off keeps its failure record; switching it on clears its backoff at once, so
/// it runs on the next tick instead of after its delay (10 s here).
#[test]
fn switching_on_clears_the_backoff_and_keeps_the_failure_record() {
    let rt = Arc::new(build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    ));
    let mut exec = OwnedExecutor::new(rt, Flaky::new(u32::MAX))
        .with_backoff_after(1)
        .with_backoff(Duration::from_secs(10), Duration::from_secs(10));

    let failed = exec.run_in_place().expect("contained");
    assert_eq!(failed.node_health[0].state, "backed_off");

    exec.try_set_node_enabled("flaky", false)
        .expect("a known node");
    let off = exec.run_in_place().expect("tick");
    assert_eq!(off.node_health[0].state, "disabled");
    assert_eq!(off.node_health[0].failures, 1);

    exec.try_set_node_enabled("flaky", true)
        .expect("a known node");
    let on = exec.run_in_place().expect("contained");
    assert_eq!(on.recovery().backoff_skips, 0, "it ran at once");
    assert_eq!(on.node_health[0].failures, 2);
    assert_eq!(on.node_health[0].consecutive_failures, 1);
    assert_eq!(on.node_health[0].state, "backed_off");
}

#[test]
fn an_unknown_node_is_reported() {
    let rt = Arc::new(build_runtime(
        &chain(&["src", "flaky"]),
        &SchedulerConfig::default(),
    ));
    let mut exec = OwnedExecutor::new(rt, Flaky::new(0));
    let err = exec
        .try_set_node_enabled("nope", false)
        .expect_err("unknown node");
    assert_eq!(err.node, "nope");
}
