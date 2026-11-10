//! The canonical fixtures, through the fixture worker, in two rounds each.

mod support;

use daedalus_worker_conformance::run_all;

#[test]
fn all_thirteen_fixtures_pass_in_two_rounds() {
    let failures = run_all(support::spec(&[])).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}
