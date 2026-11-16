//! The counters binary on the host: it runs a command, prints the one line, and exits with the
//! command's status. Where the kernel refuses a counter, the line says `na` and the tool names
//! the reason on a line of its own.

#![cfg(target_os = "linux")]

use std::process::{Command, Output};

const NAMES: [&str; 5] = [
    "cycles",
    "instructions",
    "cache_misses",
    "llc_loads",
    "llc_misses",
];

fn counters(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_counters"))
        .args(args)
        .output()
        .expect("the counters binary runs")
}

#[test]
fn no_command_is_a_usage_error() {
    let out = counters(&[]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage: counters CMD..."));
}

#[test]
fn a_command_runs_and_the_line_has_every_counter() {
    let out = counters(&["true"]);
    assert_eq!(out.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let line = stderr.lines().last().expect("a line");
    let fields: Vec<&str> = line.split(' ').collect();
    assert_eq!(fields[0], "counters", "{stderr}");
    assert_eq!(fields.len(), 1 + 2 * NAMES.len(), "{stderr}");
    for (pair, name) in fields[1..].chunks(2).zip(NAMES) {
        assert_eq!(pair[0], name, "{stderr}");
        if pair[1] == "na" {
            let reason = format!("{name}: ");
            assert!(stderr.lines().any(|l| l.starts_with(&reason)), "{stderr}");
        } else {
            let count: u64 = pair[1].parse().expect("a count");
            if name == "instructions" {
                assert!(count > 0, "{stderr}");
            }
        }
    }
}

#[test]
fn the_exit_status_is_the_commands() {
    assert_eq!(counters(&["sh", "-c", "exit 7"]).status.code(), Some(7));
    assert_eq!(counters(&["false"]).status.code(), Some(1));
}

#[test]
fn a_command_that_cannot_run_exits_127() {
    let out = counters(&["/nonexistent/counters-test-command"]);
    assert_eq!(out.status.code(), Some(127));
}
