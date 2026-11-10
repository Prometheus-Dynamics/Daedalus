//! The canonical fixtures, through the Node worker, in two rounds each.

use daedalus_lang_node::{fixture_worker, spec};
use daedalus_worker_conformance::run_all;

#[test]
fn all_thirteen_fixtures_pass_through_the_node_worker() {
    let failures = run_all(spec(&fixture_worker())).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}
