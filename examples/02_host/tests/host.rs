//! The example's output, against the lines it is meant to print.

use std::process::Command;

const GOLDEN: &str = "\
host: doubling, the totals are [2, 6, 12]
host: reloaded: kept the state of [\"io\", \"total\"], started [\"shape\"] fresh, added 0, removed 0
host: negating, the totals are [11, 9, 6]
";

#[test]
fn host_reload_keeps_state_and_reports_change() {
    let mut host = daedalus_example_host::load();
    assert_eq!(
        daedalus_example_host::feed_step_drain(&mut host, &[1, 2, 3]),
        [2, 6, 12]
    );
    let report = daedalus_example_host::reload(&mut host);
    // The total's state came across: it goes on from 12.
    assert!(report.carried.contains("total"));
    assert_eq!(
        daedalus_example_host::feed_step_drain(&mut host, &[1]),
        [11]
    );
    // The replaced node is reported, and nothing was added or removed.
    assert_eq!(report.fresh.names(), ["shape"]);
    assert_eq!((report.added.total(), report.removed.total()), (0, 0));

    assert_eq!(daedalus_example_host::run(), GOLDEN);
    let output = Command::new(env!("CARGO_BIN_EXE_daedalus-example-host"))
        .output()
        .expect("the example runs");
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), GOLDEN);
}
