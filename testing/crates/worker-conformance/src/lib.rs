//! The worker conformance suites: what a worker must do to be a Daedalus worker.
//!
//! There are two suites. Both drive a worker through the process backend, from a spec that starts it.
//!
//! - **Values** (`run_all`, `run_fixtures`): thirteen canonical fixture nodes, each run twice, with
//!   the bytes checked. The nodes are [`fixtures::fixtures`]; [`fixtures::declarations`] are their ports.
//! - **Behaviour** (`behaviour::behaviours`): six named cases of what a worker does beyond values.
//!   Each case is independent, so a language can opt in by name while its SDK catches up.
//!
//! # Adding a language to the behaviour suite
//!
//! 1. Serve the behaviour fixture nodes in your worker, next to the thirteen value fixtures. Their
//!    ids and ports are in [`fixtures::behaviour_declarations`], and each is documented there:
//!    - `fixture.wait_cancel(x: int) -> y: int`: runs until cancelled, then gives `x`.
//!    - `fixture.events(x: int) -> y: int`: emits progress, log and detail events, then gives `x`.
//!    - `fixture.chatty(x: int) -> y: int`: prints a line to stdout, then gives `x + 1`.
//!    - `fixture.stateful_label(in: string) -> out: string`, with a string state: appends `in` to
//!      the state and gives the state.
//!
//!    A worker that serves only some of them can opt in to just those behaviours. The suite
//!    registers all of [`fixtures::behaviour_declarations`] and the value fixtures, so a worker may
//!    declare any of them; a node outside those lists is refused.
//! 2. Add a test that calls the suite with your worker's spec, and asserts no failures:
//!
//!    ```ignore
//!    let spec = WorkerSpec::new(path_to_your_worker);
//!    let failures = daedalus_worker_conformance::behaviour::behaviours(spec, &[]).unwrap();
//!    assert!(failures.is_empty(), "{failures:#?}");
//!    ```
//!
//!    Pass names (`&["events"]`) to run only some. An empty list runs them all.
//! 3. To learn which transport your large values took, call
//!    `behaviour::large_transport(&spec)`. Your doc line for large values should say `plain` or
//!    `by reference`, and match it.
//!
//! # What each behaviour requires
//!
//! - `cancel`: while a run is going, poll `cancelled()` (or the host's cancel flag). Once the
//!   host cancels, end the run with status `cancelled`, within five seconds. A worker that does
//!   not answer in time is killed by the host, and that fails this case.
//! - `events`: for `fixture.events`, send progress 0.5 "half", log info "after the progress",
//!   detail "a detail line", progress 1.0 "done", in that order, each before the outputs. Then
//!   the outputs.
//! - `state`: an export of a node with no state set is a fault that names the node, never a
//!   value (in particular never `0` for a string). An export after a run is the state the node
//!   holds. An import sets the state, and the next run starts from it. Both integer and string
//!   state are checked.
//! - `chatty`: whatever a node body prints must not reach the packet stream. The worker keeps
//!   stdout for packets, and sends a node's text through its log or to stderr.
//! - `malformed`: a packet the worker cannot decode is answered with a `malformed packet` fault.
//!   The worker keeps running, and the next valid run gives its outputs.
//! - `large`: a `bytes` input of 1 MiB comes back as its bytes reversed, twice in a row, whether
//!   it travels as a plain packet or by reference. [`behaviour::large_transport`] says which.
//!
//! # The self-check
//!
//! `daedalus-worker-sabotage` (`src/bin/sabotage.rs`) is an honest worker by default. Set
//! `DAEDALUS_SABOTAGE` to a behaviour's name, and it breaks that behaviour in one way. The tests
//! in `tests/self_check.rs` show each sabotage fails its own case, so the cases are not vacuous.

#![forbid(unsafe_code)]

pub mod behaviour;
pub mod fixtures;
pub mod large;
pub mod runner;

pub use behaviour::{BEHAVIOURS, behaviours, large_transport};
pub use fixtures::{
    CUSTOM_KEY, DIAGNOSTIC_CODE, DIAGNOSTIC_DETAIL, Expect, Fixture, Port, Round,
    behaviour_declarations, declarations, fixtures, hello, slot_bytes,
};
pub use large::{Large, Transport, large_list, large_values};
pub use runner::{Failure, run_all, run_fixtures};
