//! Parameters: a generated module's setters, and updates from a host.

#![cfg(feature = "source")]

#[path = "shared/source_fixture.rs"]
mod nodes;

#[allow(dead_code)]
mod generated {
    include!("fixtures/blink_tunable.rs");
}

use daedalus_cpu::chip::{
    ParamError, ParamSpec, ParamUpdate, Scalar, ScalarKind, Tunable, checked,
};
use daedalus_cpu::param_host::encode_update;
use generated::{Graph, PARAM_NAMES, PARAMS};

fn spec() -> ParamSpec {
    ParamSpec {
        kind: ScalarKind::F32,
        min: Scalar::F32(0.0),
        max: Scalar::F32(1.0),
    }
}

#[test]
fn a_spec_converts_and_bounds() {
    let spec = spec();
    assert_eq!(spec.check(Scalar::F64(0.5)), Ok(Scalar::F32(0.5)));
    assert_eq!(spec.check(Scalar::I64(1)), Ok(Scalar::F32(1.0)));
    assert_eq!(spec.check(Scalar::F32(1.5)), Err(ParamError::Range));
    assert_eq!(spec.check(Scalar::F32(f32::NAN)), Err(ParamError::Range));
    assert_eq!(spec.check(Scalar::Bool(true)), Err(ParamError::Type));
    assert_eq!(checked::<f32>(&spec, Scalar::F32(0.25)), Ok(0.25));
}

#[test]
fn every_kind_rejects_out_of_range_and_wrong_types() {
    for kind in ScalarKind::ALL {
        let (low, high) = Scalar::bounds(kind);
        let spec = ParamSpec {
            kind,
            min: high,
            max: high,
        };
        assert_eq!(spec.check(high), Ok(high), "{kind:?}");
        if low != high {
            assert_eq!(spec.check(low), Err(ParamError::Range), "{kind:?}");
        }
        if kind != ScalarKind::Bool {
            assert_eq!(
                spec.check(Scalar::Bool(true)),
                Err(ParamError::Type),
                "{kind:?}"
            );
        }
    }
}

#[test]
fn ids_follow_run_order_then_input_order() {
    assert_eq!(
        PARAM_NAMES,
        [
            "lowpass.alpha",
            "threshold.on",
            "threshold.off",
            "blink.period"
        ]
    );
    assert_eq!(PARAMS[0].kind, ScalarKind::F32);
    assert_eq!(PARAMS[3], ParamSpec::full(ScalarKind::U64));
}

#[test]
fn a_new_value_is_used_from_the_next_tick() {
    let mut graph = Box::new(Graph::new());
    assert_eq!(graph.param(1), Some(Scalar::F32(2.0)));
    graph.push_sample(3000).unwrap();
    graph.tick().unwrap();
    assert_eq!(
        graph.pop_led(),
        Some(true),
        "3 V is above the 2 V threshold"
    );

    graph.set_threshold_on(4.0).unwrap();
    graph.set_threshold_off(3.5).unwrap();
    assert_eq!(graph.param(1), Some(Scalar::F32(4.0)));
    graph.push_sample(3000).unwrap();
    graph.tick().unwrap();
    assert_eq!(
        graph.pop_led(),
        Some(false),
        "3 V is now below the off level"
    );
}

#[test]
fn setters_check_the_range_and_the_id() {
    let mut graph = Box::new(Graph::new());
    assert_eq!(graph.set_lowpass_alpha(1.5), Err(ParamError::Range));
    assert_eq!(graph.set_lowpass_alpha(f32::NAN), Err(ParamError::Range));
    assert_eq!(
        graph.param(0),
        Some(Scalar::F32(0.25)),
        "a refused value changes nothing"
    );
    assert_eq!(graph.set_param(9, Scalar::F32(0.0)), Err(ParamError::Id));
    assert_eq!(graph.set_param(3, Scalar::I64(-1)), Err(ParamError::Type));
    assert_eq!(graph.param(9), None);
}

#[test]
fn host_and_device_share_one_codec() {
    let mut graph = Box::new(Graph::new());
    let message = encode_update(&PARAM_NAMES, &PARAMS, "threshold.on", Scalar::F32(2.5)).unwrap();
    assert_eq!(message, [0x01, 0x09, 0x00, 0x00, 0x20, 0x40]);
    assert_eq!(ParamUpdate::decode(&message).unwrap().id, 1);
    graph.apply_update(&message).unwrap();
    assert_eq!(graph.param(1), Some(Scalar::F32(2.5)));
    assert_eq!(
        graph.apply_update(&message[..3]),
        Err(ParamError::Malformed)
    );
}

#[test]
fn the_host_applies_the_devices_checks() {
    let encode = |name: &str, value| encode_update(&PARAM_NAMES, &PARAMS, name, value);
    assert_eq!(encode("nope", Scalar::F32(0.0)), Err(ParamError::Id));
    assert_eq!(
        encode("threshold.on", Scalar::F32(9.0)),
        Err(ParamError::Range)
    );
    assert_eq!(
        encode("blink.period", Scalar::Bool(true)),
        Err(ParamError::Type)
    );
}

#[test]
fn freezing_gives_the_module_with_no_markers() {
    let frozen = nodes::generate_frozen(&nodes::tunable()).unwrap();
    assert_eq!(frozen, nodes::generate(&nodes::graph()).unwrap());
    assert!(!frozen.contains("Tunable"));
}

#[test]
fn a_mark_must_name_a_constant_in_its_range() {
    use daedalus_graph::Lit;
    let mut bad = nodes::tunable();
    bad.nodes[4].metadata.insert(
        nodes::PARAMS_KEY.to_string(),
        Lit::List(vec![Lit::Str("active".to_string())]),
    );
    let error = nodes::generate(&bad).unwrap_err().to_string();
    assert!(
        error.contains("`active`") && error.contains("constant"),
        "{error}"
    );

    let mut bad = nodes::tunable();
    let range = Lit::List(vec![Lit::Float(0.5), Lit::Float(1.0)]);
    bad.nodes[2].metadata.insert(
        nodes::PARAMS_KEY.to_string(),
        Lit::Struct(vec![daedalus_graph::LitField {
            name: "alpha".to_string(),
            value: range,
        }]),
    );
    let error = nodes::generate(&bad).unwrap_err().to_string();
    assert!(error.contains("outside its range"), "{error}");

    let mut bad = nodes::tunable();
    bad.nodes[3].metadata.insert(
        nodes::PARAMS_KEY.to_string(),
        Lit::List(vec![Lit::Str("nope".to_string())]),
    );
    let error = nodes::generate(&bad).unwrap_err().to_string();
    assert!(error.contains("no such input"), "{error}");
}
