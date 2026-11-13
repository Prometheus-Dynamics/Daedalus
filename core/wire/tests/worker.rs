//! Round trips of every message and value kind, and the refusals the protocol names.

use daedalus_types::Scalar;
use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::{
    BoundaryValue, Declaration, Dir, Fault, FaultCode, Hello, HostMsg, LAYOUT_HASH, LogLevel,
    Outputs, PROTOCOL_VERSION, PortSpec, Progress, REASON_CAP, Run, RunStatus, Schema, WireError,
    WireType, WireValue, WorkerMsg, decode_value, encode_value, truncate_reason,
};

const SCALARS: [(Scalar, fn() -> WireValue); 16] = [
    (Scalar::Unit, || WireValue::Unit),
    (Scalar::Bool, || WireValue::Bool(true)),
    (Scalar::I8, || WireValue::I8(i8::MIN)),
    (Scalar::I16, || WireValue::I16(-2)),
    (Scalar::I32, || WireValue::I32(i32::MAX)),
    (Scalar::Int, || WireValue::I64(i64::MIN)),
    (Scalar::ISize, || WireValue::ISize(-7)),
    (Scalar::U8, || WireValue::U8(u8::MAX)),
    (Scalar::U16, || WireValue::U16(u16::MAX)),
    (Scalar::U32, || WireValue::U32(u32::MAX)),
    (Scalar::U64, || WireValue::U64(u64::MAX)),
    (Scalar::USize, || WireValue::USize(0)),
    (Scalar::F32, || WireValue::F32(1.5)),
    (Scalar::Float, || WireValue::F64(-0.25)),
    (Scalar::String, || WireValue::String("héllo".to_owned())),
    (Scalar::Bytes, || WireValue::Bytes(Vec::new())),
];

/// A worker that serves one node with every kind of port.
fn fixture() -> (Hello, Schema) {
    let mut inputs: Vec<PortSpec> = SCALARS
        .iter()
        .map(|(scalar, _)| PortSpec::new(scalar.key().as_str(), WireType::Scalar(*scalar)))
        .collect();
    inputs.push(PortSpec::new(
        "nested",
        WireType::Map(
            Box::new(WireType::Scalar(Scalar::String)),
            Box::new(WireType::List(Box::new(WireType::Optional(Box::new(
                WireType::Scalar(Scalar::U64),
            ))))),
        ),
    ));
    inputs.push(PortSpec::new(
        "record",
        WireType::Struct(vec![
            (
                "pair".to_owned(),
                WireType::Tuple(vec![
                    WireType::Scalar(Scalar::Bool),
                    WireType::Scalar(Scalar::I32),
                ]),
            ),
            (
                "choice".to_owned(),
                WireType::Enum(vec![
                    ("none".to_owned(), None),
                    ("some".to_owned(), Some(WireType::Scalar(Scalar::U8))),
                ]),
            ),
        ]),
    ));
    inputs.push(PortSpec::new(
        "blob",
        WireType::Opaque("rust:demo::Blob".to_owned()),
    ));
    let node = Declaration {
        id: "demo.node".to_owned(),
        inputs: inputs.clone(),
        outputs: vec![PortSpec::new("out", WireType::Scalar(Scalar::U64))],
        config: vec![PortSpec {
            name: "gain".to_owned(),
            ty: WireType::Scalar(Scalar::Float),
            default: Some(WireValue::F64(2.0)),
        }],
        state: Some(WireType::Scalar(Scalar::Int)),
        capabilities: vec!["cpu".to_owned()],
    };
    let hello = Hello::new("test-worker", 1024 * 1024, vec![node]);
    let mut schema = Schema::from_hello(&hello, &["rust:demo::Blob"]);
    schema.codec("rust:demo::Blob");
    (hello, schema)
}

fn input_values() -> Vec<BoundaryValue> {
    let mut values: Vec<BoundaryValue> = SCALARS
        .iter()
        .map(|(scalar, make)| BoundaryValue {
            node: "demo.node".to_owned(),
            port: scalar.key().as_str().to_owned(),
            value: make(),
        })
        .collect();
    values.push(BoundaryValue {
        node: "demo.node".to_owned(),
        port: "nested".to_owned(),
        value: WireValue::Map(vec![(
            WireValue::String("a".to_owned()),
            WireValue::List(vec![
                WireValue::Optional(Some(Box::new(WireValue::U64(u64::MAX)))),
                WireValue::Optional(None),
            ]),
        )]),
    });
    values.push(BoundaryValue {
        node: "demo.node".to_owned(),
        port: "record".to_owned(),
        value: WireValue::Struct(vec![
            WireValue::Tuple(vec![WireValue::Bool(false), WireValue::I32(-9)]),
            WireValue::Enum {
                variant: 1,
                payload: Some(Box::new(WireValue::U8(200))),
            },
        ]),
    });
    values.push(BoundaryValue {
        node: "demo.node".to_owned(),
        port: "blob".to_owned(),
        value: WireValue::Opaque(vec![1, 2, 3]),
    });
    values
}

fn round_trip_host(msg: HostMsg, schema: &Schema) {
    let mut out = Writer::new();
    msg.encode(schema, &mut out).expect("encode");
    let back = HostMsg::decode(&out.buf, schema).expect("decode");
    assert_eq!(back, msg);
}

fn round_trip_worker(msg: WorkerMsg, schema: &Schema) {
    let mut out = Writer::new();
    msg.encode(schema, &mut out).expect("encode");
    let back = WorkerMsg::decode(&out.buf, schema).expect("decode");
    assert_eq!(back, msg);
}

#[test]
fn every_scalar_and_compound_value_round_trips_through_a_run() {
    let (_, schema) = fixture();
    round_trip_host(
        HostMsg::Run(Run {
            run: u64::MAX,
            region: 3,
            nodes: vec!["demo.node".to_owned()],
            inputs: input_values(),
        }),
        &schema,
    );
}

#[test]
fn every_host_message_round_trips() {
    let (_, schema) = fixture();
    round_trip_host(HostMsg::Cancel { run: 9 }, &schema);
    round_trip_host(
        HostMsg::StateExport {
            node: "demo.node".into(),
        },
        &schema,
    );
    round_trip_host(
        HostMsg::StateImport {
            node: "demo.node".into(),
            value: WireValue::I64(-1),
        },
        &schema,
    );
    round_trip_host(HostMsg::Ping, &schema);
    round_trip_host(HostMsg::Shutdown, &schema);
}

#[test]
fn every_worker_message_round_trips() {
    let (hello, schema) = fixture();
    round_trip_worker(WorkerMsg::Hello(hello), &schema);
    round_trip_worker(
        WorkerMsg::Outputs(Outputs {
            run: 1,
            status: RunStatus::Ok,
            outputs: vec![BoundaryValue {
                node: "demo.node".into(),
                port: "out".into(),
                value: WireValue::U64(u64::MAX),
            }],
        }),
        &schema,
    );
    for status in [RunStatus::Cancelled, RunStatus::Fault] {
        round_trip_worker(
            WorkerMsg::Outputs(Outputs {
                run: 2,
                status,
                outputs: Vec::new(),
            }),
            &schema,
        );
    }
    round_trip_worker(
        WorkerMsg::Progress(Progress {
            run: 1,
            node_index: 0,
            message: "half".into(),
            fraction: Some(0.5),
        }),
        &schema,
    );
    round_trip_worker(
        WorkerMsg::Progress(Progress {
            run: 1,
            node_index: 4,
            message: String::new(),
            fraction: None,
        }),
        &schema,
    );
    for level in [
        LogLevel::Error,
        LogLevel::Warn,
        LogLevel::Info,
        LogLevel::Debug,
        LogLevel::Trace,
    ] {
        round_trip_worker(
            WorkerMsg::Log {
                level,
                text: "line".into(),
            },
            &schema,
        );
    }
    round_trip_worker(
        WorkerMsg::Fault(Fault {
            run: 5,
            node: Some("demo.node".into()),
            code: FaultCode::NodePanic,
            reason: "boom".into(),
        }),
        &schema,
    );
    round_trip_worker(
        WorkerMsg::Fault(Fault {
            run: 5,
            node: None,
            code: FaultCode::MalformedPacket,
            reason: String::new(),
        }),
        &schema,
    );
    round_trip_worker(
        WorkerMsg::StateValue {
            node: "demo.node".into(),
            value: WireValue::I64(42),
        },
        &schema,
    );
    round_trip_worker(WorkerMsg::Pong, &schema);
}

#[test]
fn every_fault_code_has_its_own_number_and_name() {
    let mut numbers: Vec<u16> = FaultCode::ALL.iter().map(|code| code.code()).collect();
    numbers.sort_unstable();
    numbers.dedup();
    assert_eq!(numbers.len(), FaultCode::ALL.len());
    for code in FaultCode::ALL {
        assert_eq!(FaultCode::from_code(code.code()).unwrap(), code);
        assert!(!code.name().is_empty());
    }
    assert!(FaultCode::from_code(0).is_err());
}

#[test]
fn a_long_fault_reason_is_cut_at_a_character_boundary() {
    // Each character is three bytes, so 512 bytes ends mid-character after 170 of them.
    let reason = "€".repeat(300);
    let cut = truncate_reason(&reason, REASON_CAP).to_owned();
    assert!(cut.len() <= REASON_CAP);
    assert_eq!(cut.len(), 510);
    let (_, schema) = fixture();
    let msg = WorkerMsg::Fault(Fault {
        run: 1,
        node: None,
        code: FaultCode::NodeError,
        reason,
    });
    let mut out = Writer::new();
    msg.encode(&schema, &mut out).unwrap();
    match WorkerMsg::decode(&out.buf, &schema).unwrap() {
        WorkerMsg::Fault(fault) => assert_eq!(fault.reason, cut),
        other => panic!("expected a fault, got {other:?}"),
    }
}

#[test]
fn a_hello_with_another_protocol_version_is_refused_and_names_both() {
    let (mut hello, _) = fixture();
    hello.protocol = 2;
    match hello.check() {
        Err(WireError::Protocol { expected, found }) => {
            assert_eq!((expected, found), (PROTOCOL_VERSION, 2));
            let text = WireError::Protocol { expected, found }.to_string();
            assert!(text.contains('1') && text.contains('2'));
        }
        other => panic!("expected a protocol error, got {other:?}"),
    }
}

#[test]
fn a_hello_with_another_layout_hash_is_refused_and_names_both() {
    let (mut hello, _) = fixture();
    hello.layout = LAYOUT_HASH ^ 1;
    match hello.check() {
        Err(error @ WireError::Layout { .. }) => {
            let text = error.to_string();
            assert!(text.contains(&format!("{:#018x}", LAYOUT_HASH)), "{text}");
            assert!(
                text.contains(&format!("{:#018x}", LAYOUT_HASH ^ 1)),
                "{text}"
            );
        }
        other => panic!("expected a layout error, got {other:?}"),
    }
}

#[test]
fn a_hello_round_trips_and_checks_clean() {
    let (hello, _) = fixture();
    hello.check().expect("this build's hello checks");
    let mut out = Writer::new();
    WorkerMsg::Hello(hello.clone())
        .encode(&Schema::new(), &mut out)
        .unwrap();
    match WorkerMsg::decode(&out.buf, &Schema::new()).unwrap() {
        WorkerMsg::Hello(back) => assert_eq!(back, hello),
        other => panic!("expected a hello, got {other:?}"),
    }
}

#[test]
fn an_opaque_value_with_no_codec_is_refused_both_ways() {
    let (_, mut schema) = fixture();
    let blob = WireType::Opaque("rust:demo::Blob".to_owned());
    let mut out = Writer::new();
    assert!(matches!(
        encode_value(&blob, &WireValue::Opaque(vec![1]), &Schema::new(), &mut out),
        Err(WireError::NoCodec(_))
    ));
    // With a codec it encodes; the same bytes without one are refused on decode.
    encode_value(&blob, &WireValue::Opaque(vec![1]), &schema, &mut out).unwrap();
    let mut input = Reader::new(&out.buf);
    assert!(matches!(
        decode_value(&blob, &mut input, &Schema::new()),
        Err(WireError::NoCodec(_))
    ));
    schema.codec("rust:demo::Blob");
}

#[test]
fn a_value_of_the_wrong_shape_is_refused() {
    let mut out = Writer::new();
    let err = encode_value(
        &WireType::Scalar(Scalar::U8),
        &WireValue::U16(1),
        &Schema::new(),
        &mut out,
    )
    .unwrap_err();
    assert_eq!(
        err,
        WireError::ValueShape {
            expected: "u8",
            found: "u16"
        }
    );
}

#[test]
fn a_port_the_schema_does_not_know_is_refused() {
    let (_, schema) = fixture();
    let msg = HostMsg::Run(Run {
        run: 1,
        region: 0,
        nodes: vec!["demo.node".into()],
        inputs: vec![BoundaryValue {
            node: "demo.node".into(),
            port: "missing".into(),
            value: WireValue::Unit,
        }],
    });
    let mut out = Writer::new();
    assert!(matches!(
        msg.encode(&schema, &mut out),
        Err(WireError::UnknownPort { .. })
    ));
    assert!(schema.lookup("demo.node", Dir::Output, "out").is_ok());
}

#[test]
fn a_type_nested_past_the_limit_is_refused() {
    let mut out = Writer::new();
    for _ in 0..40 {
        out.u8(2);
    }
    out.u8(0);
    out.u8(Scalar::Bool as u8);
    let mut input = Reader::new(&out.buf);
    assert_eq!(WireType::decode(&mut input, 0), Err(WireError::TooDeep));
}

#[test]
fn the_decoder_is_total_over_every_truncation_and_some_noise() {
    let (_, schema) = fixture();
    let mut out = Writer::new();
    HostMsg::Run(Run {
        run: 7,
        region: 1,
        nodes: vec!["demo.node".into()],
        inputs: input_values(),
    })
    .encode(&schema, &mut out)
    .unwrap();
    let full = out.buf;
    for cut in 0..full.len() {
        assert!(
            HostMsg::decode(&full[..cut], &schema).is_err(),
            "prefix of {cut} bytes decoded"
        );
    }
    // A simple linear congruential generator makes noise. Every result is Ok or Err, never a panic.
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    for round in 0..20_000 {
        let len = (round % 97) + 1;
        let noise: Vec<u8> = (0..len)
            .map(|_| {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (seed >> 33) as u8
            })
            .collect();
        let _ = HostMsg::decode(&noise, &schema);
        let _ = WorkerMsg::decode(&noise, &schema);
    }
}

#[test]
fn a_bad_tag_names_what_it_was_reading() {
    let mut input = Reader::new(&[0xee]);
    assert_eq!(
        WireType::decode(&mut input, 0),
        Err(WireError::BadTag {
            what: "type",
            tag: 0xee
        })
    );
    assert_eq!(LogLevel::Info as u8, 2);
    assert_eq!(Scalar::Bytes as u8, 15);
}

#[test]
fn a_packet_round_trips_and_a_clean_end_is_closed() {
    use daedalus_wire::{PacketError, read_packet, write_packet};
    let mut wire = Vec::new();
    write_packet(&mut wire, b"one", 64).unwrap();
    write_packet(&mut wire, b"", 64).unwrap();
    let mut input = &wire[..];
    let mut buf = Vec::new();
    read_packet(&mut input, 64, &mut buf).unwrap();
    assert_eq!(buf, b"one");
    read_packet(&mut input, 64, &mut buf).unwrap();
    assert!(buf.is_empty());
    assert!(matches!(
        read_packet(&mut input, 64, &mut buf),
        Err(PacketError::Closed)
    ));
}

#[test]
fn a_detail_round_trips_and_a_host_that_predates_it_would_only_ignore_it() {
    let schema = Schema::new();
    round_trip_worker(
        WorkerMsg::Detail {
            run: 9,
            text: "line 1 is out of place\nline 2 is out of place\n".to_owned(),
        },
        &schema,
    );
    // The kind is past the ones the layout hash counts, so adding it moved no hash.
    assert_eq!(daedalus_wire::kind::DETAIL, 0x47);
}
