//! The bounded binary on the host: it passes a command's status through, stops one that runs
//! past its bound, pins to a core, and tells a missing command from a failing one.

#![cfg(target_os = "linux")]

use std::process::{Command, Output};
use std::time::{Duration, Instant};

fn bounded(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_bounded"))
        .args(args)
        .output()
        .expect("the bounded binary runs")
}

#[test]
fn the_commands_status_passes_through() {
    assert_eq!(bounded(&["5", "true"]).status.code(), Some(0));
    assert_eq!(bounded(&["5", "sh", "-c", "exit 7"]).status.code(), Some(7));
    assert_eq!(
        bounded(&["5", "sh", "-c", "kill -9 $$"]).status.code(),
        Some(128 + 9)
    );
}

#[test]
fn a_command_past_its_bound_is_stopped() {
    let started = Instant::now();
    let out = bounded(&["1", "sleep", "30"]);
    assert_eq!(out.status.code(), Some(124));
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(String::from_utf8_lossy(&out.stderr).contains("ran past 1 s"));
}

#[test]
fn a_missing_command_is_127_and_bad_usage_is_125() {
    assert_eq!(bounded(&["5", "/no/such/program"]).status.code(), Some(127));
    assert_eq!(bounded(&[]).status.code(), Some(125));
    assert_eq!(bounded(&["soon", "true"]).status.code(), Some(125));
    assert_eq!(bounded(&["-c", "x", "5", "true"]).status.code(), Some(125));
}

#[test]
fn the_command_runs_on_the_core_asked_for() {
    let out = bounded(&[
        "-c",
        "0",
        "5",
        "sh",
        "-c",
        "grep Cpus_allowed_list /proc/self/status",
    ]);
    assert_eq!(out.status.code(), Some(0));
    let line = String::from_utf8_lossy(&out.stdout).into_owned();
    assert_eq!(line.split_whitespace().last(), Some("0"), "{line}");
}
