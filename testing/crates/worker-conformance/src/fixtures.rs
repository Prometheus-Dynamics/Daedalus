//! The canonical fixtures: thirteen nodes, each with its declared ports, and for each of two
//! rounds the input bytes and the expected output bytes (or the expected fault).
//!
//! A value's bytes are its slot form: a fixed-width scalar is its little-endian bytes, a string
//! or byte field is its raw bytes, and a compound value is its wire encoding.

use daedalus_types::Scalar;
use daedalus_wire::codec::Writer;
use daedalus_wire::{Declaration, Hello, PortSpec, Schema, WireType, WireValue, encode_value};

/// The codec key the custom-type fixture uses. A worker must have a codec for it.
pub const CUSTOM_KEY: &str = "fixture.custom";

/// One port of a fixture node.
#[derive(Clone, Debug)]
pub struct Port {
    pub name: &'static str,
    pub ty: WireType,
}

/// What a round must produce.
#[derive(Clone, Debug)]
pub enum Expect {
    /// Each output port's slot bytes.
    Outputs(Vec<(&'static str, Vec<u8>)>),
    /// A fault whose code and reason text contain this.
    Fault(&'static str),
}

/// One run: its inputs, and what it must produce.
#[derive(Clone, Debug)]
pub struct Round {
    pub inputs: Vec<(&'static str, Vec<u8>)>,
    pub expect: Expect,
}

#[derive(Clone, Debug)]
pub struct Fixture {
    pub name: &'static str,
    pub node: &'static str,
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
    /// The state type, for a stateful node.
    pub state: Option<WireType>,
    /// Two rounds. A stateful node's second round sees its first round's state.
    pub rounds: [Round; 2],
}

/// The slot form of a value of `ty`.
pub fn slot_bytes(ty: &WireType, value: &WireValue) -> Vec<u8> {
    match (ty, value) {
        (WireType::Scalar(Scalar::String), WireValue::String(text)) => text.as_bytes().to_vec(),
        (WireType::Scalar(Scalar::Bytes), WireValue::Bytes(bytes)) => bytes.clone(),
        (WireType::Opaque(_), WireValue::Opaque(bytes)) => bytes.clone(),
        _ => {
            let mut out = Writer::new();
            encode_value(ty, value, &custom_schema(), &mut out).expect("a fixture value encodes");
            out.buf
        }
    }
}

/// A schema with the custom codec, for encoding fixture values.
fn custom_schema() -> Schema {
    let mut schema = Schema::new();
    schema.codec(CUSTOM_KEY);
    schema
}

fn i64_bytes(value: i64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn u32_bytes(value: u32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn scalar(scalar: Scalar) -> WireType {
    WireType::Scalar(scalar)
}

fn string() -> WireType {
    scalar(Scalar::String)
}

fn bytes_ty() -> WireType {
    scalar(Scalar::Bytes)
}

fn struct_ty() -> WireType {
    WireType::Struct(vec![
        ("name".into(), string()),
        ("count".into(), scalar(Scalar::Int)),
    ])
}

fn enum_ty() -> WireType {
    WireType::Enum(vec![
        ("idle".into(), None),
        ("busy".into(), Some(scalar(Scalar::U32))),
    ])
}

fn map_ty() -> WireType {
    WireType::Map(Box::new(string()), Box::new(scalar(Scalar::Int)))
}

fn tuple_ty() -> WireType {
    WireType::Tuple(vec![string(), scalar(Scalar::Bool)])
}

fn list_ty() -> WireType {
    WireType::List(Box::new(scalar(Scalar::U32)))
}

fn optional_ty() -> WireType {
    WireType::Optional(Box::new(scalar(Scalar::Int)))
}

fn value_pair(ty: &WireType, value: &WireValue) -> Vec<u8> {
    slot_bytes(ty, value)
}

/// The thirteen canonical fixtures.
pub fn fixtures() -> Vec<Fixture> {
    let same = |name: &'static str, node: &'static str, ty: WireType, value: WireValue| {
        let bytes = value_pair(&ty, &value);
        Fixture {
            name,
            node,
            inputs: vec![Port {
                name: "in",
                ty: ty.clone(),
            }],
            outputs: vec![Port {
                name: "out",
                ty: ty.clone(),
            }],
            state: None,
            rounds: [
                Round {
                    inputs: vec![("in", bytes.clone())],
                    expect: Expect::Outputs(vec![("out", bytes.clone())]),
                },
                Round {
                    inputs: vec![("in", bytes.clone())],
                    expect: Expect::Outputs(vec![("out", bytes)]),
                },
            ],
        }
    };
    let mut out = vec![
        Fixture {
            name: "scalar_add",
            node: "fixture.scalar_add",
            inputs: vec![
                Port {
                    name: "a",
                    ty: scalar(Scalar::Int),
                },
                Port {
                    name: "b",
                    ty: scalar(Scalar::Int),
                },
            ],
            outputs: vec![Port {
                name: "out",
                ty: scalar(Scalar::Int),
            }],
            state: None,
            rounds: [
                Round {
                    inputs: vec![("a", i64_bytes(2)), ("b", i64_bytes(40))],
                    expect: Expect::Outputs(vec![("out", i64_bytes(42))]),
                },
                Round {
                    inputs: vec![("a", i64_bytes(-1)), ("b", i64_bytes(1))],
                    expect: Expect::Outputs(vec![("out", i64_bytes(0))]),
                },
            ],
        },
        same(
            "bytes_echo",
            "fixture.bytes_echo",
            bytes_ty(),
            WireValue::Bytes(vec![1, 2, 3, 4]),
        ),
        same(
            "struct_record",
            "fixture.struct_record",
            struct_ty(),
            WireValue::Struct(vec![WireValue::String("demo".into()), WireValue::I64(3)]),
        ),
        same(
            "enum_choice",
            "fixture.enum_choice",
            enum_ty(),
            WireValue::Enum {
                variant: 1,
                payload: Some(Box::new(WireValue::U32(7))),
            },
        ),
        same(
            "optional_value",
            "fixture.optional_value",
            optional_ty(),
            WireValue::Optional(Some(Box::new(WireValue::I64(9)))),
        ),
        Fixture {
            name: "list_reverse",
            node: "fixture.list_reverse",
            inputs: vec![Port {
                name: "in",
                ty: list_ty(),
            }],
            outputs: vec![Port {
                name: "out",
                ty: list_ty(),
            }],
            state: None,
            rounds: [
                Round {
                    inputs: vec![(
                        "in",
                        slot_bytes(
                            &list_ty(),
                            &WireValue::List(vec![
                                WireValue::U32(1),
                                WireValue::U32(2),
                                WireValue::U32(3),
                            ]),
                        ),
                    )],
                    expect: Expect::Outputs(vec![(
                        "out",
                        slot_bytes(
                            &list_ty(),
                            &WireValue::List(vec![
                                WireValue::U32(3),
                                WireValue::U32(2),
                                WireValue::U32(1),
                            ]),
                        ),
                    )]),
                },
                Round {
                    inputs: vec![("in", slot_bytes(&list_ty(), &WireValue::List(vec![])))],
                    expect: Expect::Outputs(vec![(
                        "out",
                        slot_bytes(&list_ty(), &WireValue::List(vec![])),
                    )]),
                },
            ],
        },
        same(
            "tuple_pair",
            "fixture.tuple_pair",
            tuple_ty(),
            WireValue::Tuple(vec![WireValue::String("x".into()), WireValue::Bool(true)]),
        ),
        same(
            "map_pairs",
            "fixture.map_pairs",
            map_ty(),
            WireValue::Map(vec![(WireValue::String("a".into()), WireValue::I64(1))]),
        ),
        Fixture {
            name: "multi_output",
            node: "fixture.multi_output",
            inputs: vec![Port {
                name: "x",
                ty: scalar(Scalar::U32),
            }],
            outputs: vec![
                Port {
                    name: "a",
                    ty: scalar(Scalar::U32),
                },
                Port {
                    name: "b",
                    ty: scalar(Scalar::U32),
                },
            ],
            state: None,
            rounds: [
                Round {
                    inputs: vec![("x", u32_bytes(21))],
                    expect: Expect::Outputs(vec![("a", u32_bytes(21)), ("b", u32_bytes(42))]),
                },
                Round {
                    inputs: vec![("x", u32_bytes(0))],
                    expect: Expect::Outputs(vec![("a", u32_bytes(0)), ("b", u32_bytes(0))]),
                },
            ],
        },
        Fixture {
            name: "raw_io",
            node: "fixture.raw_io",
            inputs: vec![Port {
                name: "payload",
                ty: bytes_ty(),
            }],
            outputs: vec![Port {
                name: "out",
                ty: bytes_ty(),
            }],
            state: None,
            rounds: [
                Round {
                    inputs: vec![("payload", vec![1, 2, 3])],
                    expect: Expect::Outputs(vec![("out", vec![3, 2, 1])]),
                },
                Round {
                    // Longer than an inline value, so the output travels as a shared buffer.
                    inputs: vec![("payload", (0u8..40).collect())],
                    expect: Expect::Outputs(vec![("out", (0u8..40).rev().collect())]),
                },
            ],
        },
        Fixture {
            name: "stateful_counter",
            node: "fixture.stateful_counter",
            inputs: vec![Port {
                name: "delta",
                ty: scalar(Scalar::Int),
            }],
            outputs: vec![Port {
                name: "total",
                ty: scalar(Scalar::Int),
            }],
            state: Some(scalar(Scalar::Int)),
            rounds: [
                Round {
                    inputs: vec![("delta", i64_bytes(5))],
                    expect: Expect::Outputs(vec![("total", i64_bytes(5))]),
                },
                Round {
                    inputs: vec![("delta", i64_bytes(5))],
                    expect: Expect::Outputs(vec![("total", i64_bytes(10))]),
                },
            ],
        },
        Fixture {
            name: "custom_type_key",
            node: "fixture.custom_type_key",
            inputs: vec![Port {
                name: "in",
                ty: WireType::Opaque(CUSTOM_KEY.into()),
            }],
            outputs: vec![Port {
                name: "out",
                ty: WireType::Opaque(CUSTOM_KEY.into()),
            }],
            state: None,
            rounds: [
                Round {
                    inputs: vec![("in", vec![9, 8, 7])],
                    expect: Expect::Outputs(vec![("out", vec![9, 8, 7])]),
                },
                Round {
                    inputs: vec![("in", vec![])],
                    expect: Expect::Outputs(vec![("out", vec![])]),
                },
            ],
        },
        Fixture {
            name: "failure_diagnostic",
            node: "fixture.failure_diagnostic",
            inputs: vec![Port {
                name: "value",
                ty: scalar(Scalar::Int),
            }],
            outputs: vec![Port {
                name: "out",
                ty: scalar(Scalar::Int),
            }],
            state: None,
            rounds: [
                Round {
                    inputs: vec![("value", i64_bytes(7))],
                    expect: Expect::Fault("node_error"),
                },
                Round {
                    inputs: vec![("value", i64_bytes(8))],
                    expect: Expect::Fault("node_error"),
                },
            ],
        },
    ];
    out.sort_by_key(|fixture| fixture.name);
    out
}

/// The declarations the canonical worker serves: one per fixture node.
pub fn declarations() -> Vec<Declaration> {
    fixtures()
        .into_iter()
        .map(|fixture| Declaration {
            id: fixture.node.to_owned(),
            inputs: fixture
                .inputs
                .iter()
                .map(|port| PortSpec::new(port.name, port.ty.clone()))
                .collect(),
            outputs: fixture
                .outputs
                .iter()
                .map(|port| PortSpec::new(port.name, port.ty.clone()))
                .collect(),
            config: Vec::new(),
            state: fixture.state.clone(),
            capabilities: Vec::new(),
        })
        .collect()
}

/// The hello the canonical worker sends.
pub fn hello(packet_cap: u32) -> Hello {
    Hello::new("daedalus-worker-fixture", packet_cap, declarations())
}
