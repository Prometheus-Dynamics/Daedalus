//! The JSON of the service's values, pinned byte for byte. The golden file was made from
//! daedalus-ctl's output before its rendering moved here, so a change to any field name, order or
//! value shows up here, and in `daedalus-ctl --json` and the C surface's watch events, which share
//! this code.

#![cfg(feature = "json")]

mod golden {
    include!("golden/fixtures.rs");
}

use daedalus_client::json;
use golden::{Fixture, fixtures};

#[test]
fn every_value_renders_as_the_golden_file_says() {
    let mut actual = String::new();
    for (name, fixture) in fixtures() {
        let value = match &fixture {
            Fixture::Event(event) => json::event(event),
            Fixture::Inspection(found) => json::inspection(found),
            Fixture::Plan(summary) => json::plan(summary),
            Fixture::Diagnostics(found) => json::diagnostics(found),
            Fixture::Plugin(info) => json::plugin(info),
            Fixture::Stepped(summary) => json::stepped(summary),
            Fixture::Carry(report) => json::carry(report),
            Fixture::Trace(entries) => json::trace(entries),
            Fixture::Typed(value) => json::typed(value),
        };
        actual.push_str(&format!("{name}\t{value}\n"));
    }
    assert_eq!(actual, include_str!("golden/json.txt"));
}

#[test]
fn the_scalar_names_are_the_ones_a_port_is_fed_by() {
    for (name, scalar) in json::SCALAR_NAMES {
        assert_eq!(
            json::type_name(&daedalus_client::WireType::Scalar(scalar)),
            name
        );
    }
    assert_eq!(json::hex(&[0, 171, 255]), "00abff");
}
