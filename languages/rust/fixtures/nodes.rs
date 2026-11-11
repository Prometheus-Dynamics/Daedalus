//! The canonical fixture nodes this SDK serves, in the shapes the conformance fixtures declare. All
//! are declared with `#[node]`.

#![allow(non_camel_case_types)]

use std::collections::BTreeMap;

use daedalus_lang_rust::{Fault, Pair, Wire};
use daedalus_macros::node;
use daedalus_registry::NodeRecord;
use daedalus_types::{Field, Scalar, TypeExpr, TypeKey, Typed, Variant};
use daedalus_wire::{FaultCode, WireValue};

/// The key of the fixture's custom type, as the conformance fixtures name it.
const CUSTOM_KEY: &str = "fixture.custom";

/// A struct with a name and a count: the `struct_record` fixture's value. Its port is the structure
/// itself, not a name, so the type is structural.
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub name: String,
    pub count: i64,
}

const RECORD_FIELDS: &[Field] = &[
    Field {
        name: "name",
        ty: TypeExpr::Scalar(Scalar::String),
    },
    Field {
        name: "count",
        ty: TypeExpr::Scalar(Scalar::Int),
    },
];

impl Typed for Record {
    const KEY: TypeKey = TypeKey::new("structural");
    const EXPR: TypeExpr = TypeExpr::Struct(RECORD_FIELDS);
    const STRUCTURAL: bool = true;
}

impl Wire for Record {
    fn to_wire(&self) -> WireValue {
        WireValue::Struct(vec![self.name.to_wire(), self.count.to_wire()])
    }
    fn from_wire(value: &WireValue) -> Option<Record> {
        match value {
            WireValue::Struct(fields) if fields.len() == 2 => Some(Record {
                name: String::from_wire(&fields[0])?,
                count: i64::from_wire(&fields[1])?,
            }),
            _ => None,
        }
    }
}

/// An enum with a unit variant and a variant that carries a `u32`: the `enum_choice` fixture's value.
#[derive(Clone, Debug, PartialEq)]
pub enum Choice {
    idle,
    busy(u32),
}

const CHOICE_VARIANTS: &[Variant] = &[
    Variant {
        name: "idle",
        payload: None,
    },
    Variant {
        name: "busy",
        payload: Some(TypeExpr::Scalar(Scalar::U32)),
    },
];

impl Typed for Choice {
    const KEY: TypeKey = TypeKey::new("structural");
    const EXPR: TypeExpr = TypeExpr::Enum(CHOICE_VARIANTS);
    const STRUCTURAL: bool = true;
}

impl Wire for Choice {
    fn to_wire(&self) -> WireValue {
        match self {
            Choice::idle => WireValue::Enum {
                variant: 0,
                payload: None,
            },
            Choice::busy(value) => WireValue::Enum {
                variant: 1,
                payload: Some(Box::new(value.to_wire())),
            },
        }
    }
    fn from_wire(value: &WireValue) -> Option<Choice> {
        match value {
            WireValue::Enum {
                variant: 0,
                payload: None,
            } => Some(Choice::idle),
            WireValue::Enum {
                variant: 1,
                payload: Some(inner),
            } => u32::from_wire(inner).map(Choice::busy),
            _ => None,
        }
    }
}

/// A type the worker has no structure for: its bytes are its value, under the custom key.
#[derive(Clone, Debug, PartialEq)]
pub struct Custom(pub Vec<u8>);

impl Typed for Custom {
    const KEY: TypeKey = TypeKey::new(CUSTOM_KEY);
    const EXPR: TypeExpr = TypeExpr::Scalar(Scalar::Bytes);
}

impl Wire for Custom {
    fn to_wire(&self) -> WireValue {
        WireValue::Opaque(self.0.clone())
    }
    fn from_wire(value: &WireValue) -> Option<Custom> {
        match value {
            WireValue::Opaque(bytes) => Some(Custom(bytes.clone())),
            _ => None,
        }
    }
}

#[node(id = "fixture.scalar_add", inputs(a, b), outputs(out), backend = daedalus_lang_rust)]
pub fn scalar_add(a: i64, b: i64) -> i64 {
    a.wrapping_add(b)
}

#[node(id = "fixture.multi_output", inputs(x), outputs(a, b), backend = daedalus_lang_rust)]
pub fn multi_output(x: u32) -> (u32, u32) {
    (x, x.wrapping_mul(2))
}

#[node(id = "fixture.raw_io", inputs(payload), outputs(out), backend = daedalus_lang_rust)]
pub fn raw_io(payload: Vec<u8>) -> Vec<u8> {
    payload.into_iter().rev().collect()
}

#[node(id = "fixture.bytes_echo", inputs("in"), outputs("out"), backend = daedalus_lang_rust)]
pub fn bytes_echo(r#in: Vec<u8>) -> Vec<u8> {
    r#in
}

#[node(id = "fixture.struct_record", inputs("in"), outputs("out"), backend = daedalus_lang_rust)]
pub fn struct_record(r#in: Record) -> Record {
    r#in
}

#[node(id = "fixture.enum_choice", inputs("in"), outputs("out"), backend = daedalus_lang_rust)]
pub fn enum_choice(r#in: Choice) -> Choice {
    r#in
}

// The port is the optional value itself, so it names its type: a bare `Option` parameter is an
// optional port of its inner type.
#[node(
    id = "fixture.optional_value",
    inputs(port(name = "in", ty = Option<i64>)),
    outputs(port(name = "out", ty = Option<i64>)),
    backend = daedalus_lang_rust
)]
pub fn optional_value(r#in: Option<i64>) -> Option<i64> {
    r#in
}

#[node(id = "fixture.list_reverse", inputs("in"), outputs("out"), backend = daedalus_lang_rust)]
pub fn list_reverse(r#in: Vec<u32>) -> Vec<u32> {
    r#in.into_iter().rev().collect()
}

#[node(
    id = "fixture.tuple_pair",
    inputs(port(name = "in", ty = (String, bool))),
    outputs(port(name = "out", ty = (String, bool))),
    backend = daedalus_lang_rust
)]
pub fn tuple_pair(r#in: Pair) -> Pair {
    r#in
}

#[node(id = "fixture.map_pairs", inputs("in"), outputs("out"), backend = daedalus_lang_rust)]
pub fn map_pairs(r#in: BTreeMap<String, i64>) -> BTreeMap<String, i64> {
    r#in
}

#[node(id = "fixture.custom_type_key", inputs("in"), outputs("out"), backend = daedalus_lang_rust)]
pub fn custom_type_key(r#in: Custom) -> Custom {
    r#in
}

/// The running total a stateful counter keeps: its state. The host declares it as an integer.
#[derive(Default)]
pub struct Count(pub i64);

impl Typed for Count {
    const KEY: TypeKey = Scalar::Int.key();
    const EXPR: TypeExpr = TypeExpr::Scalar(Scalar::Int);
}

impl Wire for Count {
    fn to_wire(&self) -> WireValue {
        self.0.to_wire()
    }
    fn from_wire(value: &WireValue) -> Option<Count> {
        i64::from_wire(value).map(Count)
    }
}

/// Each run adds its input to a running total, which is the node's state.
#[node(id = "fixture.stateful_counter", inputs(delta), outputs(total), state(Count), backend = daedalus_lang_rust)]
pub fn stateful_counter(delta: i64, state: &mut Count) -> i64 {
    state.0 += delta;
    state.0
}

/// Always fails with `node_error`, and says why.
#[node(id = "fixture.failure_diagnostic", inputs(value), outputs(out), backend = daedalus_lang_rust)]
pub fn failure_diagnostic(value: i64) -> Result<i64, Fault> {
    Err(Fault {
        code: FaultCode::NodeError,
        reason: format!("value {value} is refused"),
    })
}

/// The nodes this worker serves: the thirteen canonical fixtures.
pub const NODES: &[&NodeRecord] = &[
    &SCALAR_ADD_NODE,
    &MULTI_OUTPUT_NODE,
    &RAW_IO_NODE,
    &BYTES_ECHO_NODE,
    &STRUCT_RECORD_NODE,
    &ENUM_CHOICE_NODE,
    &OPTIONAL_VALUE_NODE,
    &LIST_REVERSE_NODE,
    &TUPLE_PAIR_NODE,
    &MAP_PAIRS_NODE,
    &CUSTOM_TYPE_KEY_NODE,
    &FAILURE_DIAGNOSTIC_NODE,
    &STATEFUL_COUNTER_NODE,
];
