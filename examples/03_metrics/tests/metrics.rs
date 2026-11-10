//! The example's output, against the lines it is meant to print.

use std::process::Command;

use daedalus::prelude::Level;

const GOLDEN: &str = "\
metrics: outputs [4, 3]
level        Trace
ticks        5 (1 idle)
steps called 4
skipped      0
faults       2
backoff      0
deferrals    0
pending      0
cancelled    0
tick time    total 1250 ns, max 250 ns over 5 ticks
step  calls  failures  backoff  disabled  auto
0     4      1         false    false     false
event: tick 1, 1 step calls, 0 faults
event: tick 2, 1 step calls, 1 faults
event: tick 3, 1 step calls, 0 faults
event: tick 4, 1 step calls, 1 faults
event: tick 5, 0 step calls, 0 faults
fault: half: an odd number has no half
fault: half: an odd number has no half
";

#[test]
fn metrics_read_from_host() {
    let recorded = daedalus_example_metrics::record(Level::Trace);
    let counters = recorded.report.counters;
    assert_eq!((counters.ticks, counters.idle_ticks), (5, 1));
    assert_eq!((counters.steps_called, counters.faults), (4, 2));
    // One row for the one step: four calls, and it failed last time it ran.
    assert_eq!(recorded.report.nodes.len(), 1);
    assert_eq!(recorded.report.nodes[0].calls, 4);
    assert_eq!(recorded.report.nodes[0].failures, 1);
    assert!(
        !recorded.report.nodes[0].disabled,
        "a node that fails is never turned off"
    );
    assert_eq!(recorded.events.len(), 5);
    assert_eq!(recorded.faults.len(), 2);
    assert_eq!(recorded.outputs, [4, 3]);

    // At the basic level the counters are kept and the rows are not.
    let basic = daedalus_example_metrics::record(Level::Basic);
    assert_eq!(basic.report.counters.ticks, 5);
    assert!(basic.report.nodes.is_empty());

    assert_eq!(daedalus_example_metrics::run(), GOLDEN);
    let output = Command::new(env!("CARGO_BIN_EXE_daedalus-example-metrics"))
        .output()
        .expect("the example runs");
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), GOLDEN);
}
