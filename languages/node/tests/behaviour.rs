//! The worker behaviour suite, through the Node worker: all six behaviours, and the transport the
//! worker's large values take. Node has no fd passing in pure JavaScript, so the transport is
//! plain: that is a property of the platform, and the SDK's docs say so.

use daedalus_lang_node::{fixture_worker, spec};
use daedalus_worker_conformance::behaviour::{behaviours, large_transport};
use daedalus_worker_conformance::large::Transport;

#[test]
fn all_six_behaviours_pass_through_the_node_worker() {
    let failures = behaviours(spec(&fixture_worker()), &[]).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn the_node_worker_takes_large_values_as_plain_packets() {
    let transport = large_transport(&spec(&fixture_worker())).expect("the large value round trips");
    assert_eq!(transport, Transport::Plain);
}
