//! The canonical fixture nodes this SDK serves, in the shapes the conformance fixtures declare.
//! Most are declared with `#[node]`. A node that returns an error, and a stateful node, are declared
//! by hand, because `#[node]` does not yet pass a fallible result or a state to its body.

#![allow(non_camel_case_types)]

use std::collections::BTreeMap;

use daedalus_lang_rust::codec::input;
use daedalus_lang_rust::{Context, NodeError, Pack, Pair, RustBody, Wire, state_type};
use daedalus_macros::node;
use daedalus_registry::{
    BodyRecord, Fire, NodeDecl, NodeRecord, PortDecl, SourceAt, StateDecl, TypeRef,
};
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

/// A node that always fails with `node_error`, and says why.
fn failure_run(
    inputs: &[(&'static str, WireValue)],
    _cx: &mut Context,
) -> Result<Vec<WireValue>, NodeError> {
    let value: i64 = input(inputs, "value")?;
    Err(NodeError::new(
        FaultCode::NodeError,
        format!("value {value} is refused"),
    ))
}

static FAILURE_RUN: RustBody = RustBody {
    run: failure_run,
    state: None,
};

static FAILURE_IN: [PortDecl; 1] = [PortDecl::new("value", TypeRef::Key(Scalar::Int.key()))];
static FAILURE_OUT: [PortDecl; 1] = [PortDecl::new("out", TypeRef::Key(Scalar::Int.key()))];
static FAILURE_DECL: NodeDecl = decl("fixture.failure_diagnostic", &FAILURE_IN, &FAILURE_OUT);
pub static FAILURE_NODE: NodeRecord = record(&FAILURE_DECL, &FAILURE_RUN, None);

/// A stateful counter: each run adds its input to a running total, which is the node's state. The
/// hello declares the state as an `i64`, and the host can export it and import another.
fn counter_run(
    inputs: &[(&'static str, WireValue)],
    cx: &mut Context,
) -> Result<Vec<WireValue>, NodeError> {
    let delta: i64 = input(inputs, "delta")?;
    let total = cx.state().and_then(i64::from_wire).unwrap_or(0) + delta;
    cx.set_state(total.to_wire());
    Ok(total.pack())
}

static COUNTER_RUN: RustBody = RustBody {
    run: counter_run,
    state: Some(state_type::<i64>),
};

static COUNTER_IN: [PortDecl; 1] = [PortDecl::new("delta", TypeRef::Key(Scalar::Int.key()))];
static COUNTER_OUT: [PortDecl; 1] = [PortDecl::new("total", TypeRef::Key(Scalar::Int.key()))];
static COUNTER_DECL: NodeDecl = decl("fixture.stateful_counter", &COUNTER_IN, &COUNTER_OUT);
pub static COUNTER_NODE: NodeRecord = record(
    &COUNTER_DECL,
    &COUNTER_RUN,
    Some(StateDecl {
        size: std::mem::size_of::<i64>(),
        align: std::mem::align_of::<i64>(),
        name: "i64",
    }),
);

/// A declaration with no optional parts, for a node written by hand.
const fn decl(
    id: &'static str,
    inputs: &'static [PortDecl],
    outputs: &'static [PortDecl],
) -> NodeDecl {
    NodeDecl {
        id,
        at: SourceAt(concat!(file!(), ":", line!())),
        summary: "",
        description: "",
        inputs,
        outputs,
        config: &[],
        fanin: &[],
        fire: Fire::All,
        reusable: false,
        shareable: false,
        conversion: None,
        group: None,
    }
}

/// A record for a node written by hand: its body is the Rust body given, with the state it declares.
const fn record(
    decl: &'static NodeDecl,
    body: &'static RustBody,
    state: Option<StateDecl>,
) -> NodeRecord {
    NodeRecord {
        decl,
        body: BodyRecord {
            backend: "rust",
            at: decl.at,
            needs: &[],
            state,
            ports: &[],
            memory: &[],
            code: body,
            text: "",
        },
    }
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
    &FAILURE_NODE,
    &COUNTER_NODE,
];
