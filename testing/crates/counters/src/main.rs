//! Runs a command under hardware counters, and counts over it and its children: user-space cycles,
//! instructions, cache misses, and last-level-cache loads and misses. When the command exits it
//! prints one line to stderr:
//!
//! ```text
//! counters cycles N instructions N cache_misses N llc_loads N llc_misses N
//! ```
//!
//! A counter that could not be opened prints `NAME: REASON` first, and `na` in its place.
//! The exit status is the command's, or 1 if it was killed by a signal. Exit 2 is no command, 3
//! is no pipe or no fork, 5 is a failed start signal, 4 is a child that could not wait for it, and
//! 127 is a command that cannot run.

#![allow(clippy::print_stderr)] // a command-line tool: its output is the line on stderr

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
fn main() -> std::process::ExitCode {
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() -> std::process::ExitCode {
    eprintln!("counters needs Linux: it reads hardware counters through perf_event_open");
    std::process::ExitCode::FAILURE
}
