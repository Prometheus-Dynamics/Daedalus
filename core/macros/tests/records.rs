//! What the macro writes, read back from the records.

#![allow(dead_code)]

use daedalus_author_fixture::FanIn;
use daedalus_author_fixture::sample::{ADD_NODE, NODES, PICK_U8_DECL};
use daedalus_macros::node;
use daedalus_registry::{Conversion, Fire, NodeRecord, Registry, TypeRef};
use daedalus_types::{Scalar, Typed};
use daedalus_value::Access;

use daedalus_author_fixture as fixture;

const U8: TypeRef = TypeRef::Key(Scalar::U8.key());
const U32: TypeRef = TypeRef::Key(Scalar::U32.key());

#[derive(Default)]
struct Count(u32);

#[node(
    id = "t.full",
    summary = "s",
    description = "d",
    inputs(port(name = "gain", default = -2, label = "Gain", unit = "dB", group = "g", advanced), held),
    outputs(port(name = "kept", description = "maybe")),
    fire = "any",
    state(Count),
    memory(kept = "fixture.space"),
    backend = fixture
)]
fn full(gain: Option<u32>, held: &mut u8, state: &mut Count) -> Option<u8> {
    state.0 += gain.unwrap_or(0);
    Some(*held)
}

#[test]
fn every_attribute_reaches_the_record() {
    let decl = &FULL_DECL;
    assert_eq!(
        (decl.id, decl.summary, decl.description),
        ("t.full", "s", "d")
    );
    assert_eq!(decl.fire, Fire::Any);
    assert!(
        decl.at.0.starts_with("core/macros/tests/records.rs:"),
        "{}",
        decl.at.0
    );
    let gain = &decl.inputs[0];
    assert_eq!((gain.name, gain.ty.clone()), ("gain", U32));
    assert!(gain.optional && gain.advanced);
    assert_eq!(gain.access, Access::Move);
    assert_eq!(gain.default, Some(daedalus_types::Literal::Int(-2)));
    assert_eq!((gain.label, gain.unit, gain.group), ("Gain", "dB", "g"));
    assert_eq!(decl.inputs[1].access, Access::Modify);
    assert_eq!(decl.inputs.len(), 2, "state isn't a port");
    let kept = &decl.outputs[0];
    assert!(kept.conditional);
    assert_eq!((kept.ty.clone(), kept.description), (U8, "maybe"));
    let body = &FULL_NODE.body;
    let state = body.state.unwrap();
    assert_eq!((state.size, state.align, state.name), (4, 4, "Count"));
    assert_eq!(body.memory, [("kept", "fixture.space")]);
}

#[node(id = "t.names", backend = fixture)]
fn names(left: &u32, right: u8) -> (u8, u32) {
    (right, *left)
}

#[test]
fn ports_default_to_the_signature() {
    let names: Vec<_> = NAMES_DECL.inputs.iter().map(|p| p.name).collect();
    assert_eq!(names, ["left", "right"]);
    assert_eq!(NAMES_DECL.inputs[0].access, Access::Read);
    let outs: Vec<_> = NAMES_DECL
        .outputs
        .iter()
        .map(|p| (p.name, p.ty.clone()))
        .collect();
    assert_eq!(outs, [("out0", U8), ("out1", U32)]);
}

#[node(id = "t.fan", backend = fixture)]
fn fan(items: FanIn<u32>) -> u32 {
    items.0
}

#[test]
fn a_fan_in_parameter_is_not_a_port() {
    assert!(FAN_DECL.inputs.is_empty());
    assert_eq!(FAN_DECL.fanin[0].prefix, "items");
    assert_eq!(FAN_DECL.fanin[0].ty, U32);
}

#[derive(Clone)]
struct Settings;

impl Typed for Settings {
    const KEY: daedalus_types::TypeKey = daedalus_types::TypeKey::new("t.settings");
    const EXPR: daedalus_types::TypeExpr = daedalus_types::TypeExpr::Opaque(Self::KEY);
}

impl Settings {
    const FIELDS: &'static [daedalus_registry::PortDecl] = &[];
    const RULES: &'static [daedalus_registry::FieldRule] = &[];
}

#[node(id = "t.cfg", inputs(value, config = Settings), backend = fixture)]
fn cfg(value: u32, settings: &Settings) -> u32 {
    let _ = settings;
    value
}

#[test]
fn a_config_parameter_is_recorded_and_is_not_a_port() {
    assert_eq!(CFG_DECL.inputs.len(), 1);
    assert_eq!(CFG_DECL.config[0].ty, TypeRef::Key(Settings::KEY));
}

#[node(id = "t.widen", conversion(cost = 3, lossy), shareable, reusable, backend = fixture)]
fn widen(value: u8) -> u32 {
    value as u32
}

#[test]
fn conversion_shareable_and_reusable_are_recorded() {
    assert_eq!(
        WIDEN_DECL.conversion,
        Some(Conversion {
            cost: 3,
            lossy: true
        })
    );
    assert_eq!((WIDEN_DECL.shareable, WIDEN_DECL.reusable), (true, true));
}

#[node(id = "t.text", inputs(a), outputs(b), backend = fixture(a: u8, b: u32))]
const TEXT: &str = "source";

#[test]
fn a_const_body_supplies_its_port_types() {
    assert_eq!(TEXT_DECL.inputs[0].ty, TypeRef::Body);
    let mut registry = Registry::new();
    registry.register(&TEXT_NODE).unwrap();
    let entry = registry.entry("t.text").unwrap();
    assert_eq!(entry.port_type(&entry.decl.inputs[0]), U8);
    assert_eq!(entry.port_type(&entry.decl.outputs[0]), U32);
    assert_eq!(TEXT_NODE.body.text, "source");
}

#[test]
fn generics_make_one_node_per_instance_and_lists_join_the_registry() {
    assert_eq!(PICK_U8_DECL.id, "fixture.pick.u8");
    assert_eq!(PICK_U8_DECL.inputs[0].ty, U8);
    let list: &[&'static NodeRecord] = NODES;
    let mut registry = Registry::new();
    registry.register_all(list).unwrap();
    assert_eq!(registry.entries().count(), 3);
    assert_eq!(ADD_NODE.decl.outputs[0].name, "sum");
}
