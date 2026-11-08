//! The auto-off policy: off by default; with `with_auto_off(n)` a node that fails `n` times in a
//! row is switched off (reported as `disabled` with `disabled_by: "auto_off"`) until it is switched
//! on again. Uses the `resilience` module's fixtures.

use std::sync::Arc;
use std::time::Duration;

use daedalus_runtime::SchedulerConfig;
use daedalus_runtime::build_runtime;
use daedalus_runtime::executor::OwnedExecutor;

use super::resilience::{Flaky, chain};

fn plan() -> Arc<daedalus_runtime::RuntimePlan> {
    Arc::new(build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    ))
}

/// Without the policy a node that always fails is backed off and tried again, never switched off.
#[test]
fn a_failing_node_is_never_switched_off_by_default() {
    let mut exec = OwnedExecutor::new(plan(), Flaky::new(u32::MAX))
        .with_backoff(Duration::ZERO, Duration::ZERO);
    let mut last = None;
    for _ in 0..5 {
        last = Some(exec.run_in_place().expect("contained"));
    }
    let report = &last.expect("ticks").node_health[0];
    assert_eq!(report.state, "backed_off", "retried, not off");
    assert_eq!(report.consecutive_failures, 5);
    assert_eq!(report.disabled_by, None);
}

/// With `with_auto_off(2)` the second failure in a row switches the node off; it then runs nothing
/// until it is switched on, and switching it on clears the off state.
#[test]
fn the_policy_switches_a_node_off_after_n_failures_and_on_again_clears_it() {
    let mut exec = OwnedExecutor::new(plan(), Flaky::new(u32::MAX))
        .with_backoff(Duration::ZERO, Duration::ZERO)
        .with_auto_off(2);

    let first = exec.run_in_place().expect("contained");
    assert_eq!(first.node_health[0].state, "backed_off");

    let second = exec.run_in_place().expect("contained");
    assert_eq!(second.node_health[0].state, "disabled");
    assert_eq!(
        second.node_health[0].disabled_by.as_deref(),
        Some("auto_off")
    );
    assert_eq!(second.node_health[0].failures, 2);

    // Off: the node runs nothing, so it records no new failure.
    let off = exec.run_in_place().expect("tick");
    assert_eq!(off.recovery().backoff_skips, 1);
    assert!(off.errors.is_empty());
    assert_eq!(off.node_health[0].failures, 2);

    // Switched on by hand: it runs again, and its reason is gone.
    exec.try_set_node_enabled("flaky", true)
        .expect("a known node");
    let on = exec.run_in_place().expect("contained");
    assert_eq!(on.node_health[0].failures, 3);
    assert_eq!(on.node_health[0].disabled_by, None);
}

/// A manual off is reported as `manual`, not `auto_off`.
#[test]
fn a_manual_off_is_reported_as_manual() {
    let mut exec = OwnedExecutor::new(plan(), Flaky::new(0)).with_auto_off(2);
    exec.try_set_node_enabled("flaky", false)
        .expect("a known node");
    let telemetry = exec.run_in_place().expect("tick");
    assert_eq!(telemetry.node_health[0].state, "disabled");
    assert_eq!(
        telemetry.node_health[0].disabled_by.as_deref(),
        Some("manual")
    );
}
