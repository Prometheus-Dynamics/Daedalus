use super::*;

#[test]
fn wire_value_roundtrips_typed_payloads() {
    let value = WireValue::Record(BTreeMap::from([
        (
            String::from("bytes"),
            WireValue::Bytes(BytePayload {
                data: vec![1, 2, 3],
                encoding: ByteEncoding::Raw,
            }),
        ),
        (
            String::from("image"),
            WireValue::Image(ImagePayload {
                data: vec![255; 16],
                width: 2,
                height: 2,
                channels: 4,
                dtype: ScalarDType::U8,
                layout: ImageLayout::Hwc,
            }),
        ),
        (
            String::from("handle"),
            WireValue::Handle(WirePayloadHandle {
                id: "payload-1".into(),
                type_key: TypeKey::new("demo:frame"),
                access: AccessMode::Read,
                residency: Some(Residency::Cpu),
                layout: Some(Layout::new("hwc")),
                capabilities: vec!["borrow_ref".into()],
                metadata: BTreeMap::from([("bytes".into(), serde_json::json!(1024))]),
            }),
        ),
        (
            String::from("mode"),
            WireValue::Enum(WireEnumValue {
                name: String::from("fast"),
                value: None,
            }),
        ),
    ]));

    let json = serde_json::to_string(&value).expect("serialize wire value");
    let decoded: WireValue = serde_json::from_str(&json).expect("deserialize wire value");
    assert_eq!(decoded, value);
}

#[cfg(feature = "image-payload")]
#[test]
fn image_payload_validation_checks_layout_dtype_and_length() {
    let payload = ImagePayload {
        data: vec![0; 2 * 3 * 4 * 2],
        width: 2,
        height: 3,
        channels: 4,
        dtype: ScalarDType::U16,
        layout: ImageLayout::Hwc,
    };

    assert_eq!(payload.expected_data_len().expect("expected len"), 48);
    payload.validate().expect("valid payload");
}

#[cfg(feature = "image-payload")]
#[test]
fn image_payload_validation_rejects_bad_shapes() {
    let zero_width = ImagePayload {
        data: Vec::new(),
        width: 0,
        height: 1,
        channels: 4,
        dtype: ScalarDType::U8,
        layout: ImageLayout::Hwc,
    };
    assert!(matches!(
        zero_width.validate(),
        Err(ImagePayloadValidationError::InvalidDimension {
            field: "width",
            value: 0
        })
    ));

    let bad_channels = ImagePayload {
        data: vec![0; 5],
        width: 1,
        height: 1,
        channels: 5,
        dtype: ScalarDType::U8,
        layout: ImageLayout::Chw,
    };
    assert!(matches!(
        bad_channels.validate(),
        Err(ImagePayloadValidationError::InvalidChannels { channels: 5 })
    ));

    let bad_len = ImagePayload {
        data: vec![0; 3],
        width: 1,
        height: 1,
        channels: 1,
        dtype: ScalarDType::F32,
        layout: ImageLayout::Chw,
    };
    assert!(matches!(
        bad_len.validate(),
        Err(ImagePayloadValidationError::InvalidDataLength {
            expected: 4,
            actual: 3
        })
    ));
}

#[test]
fn wire_value_converts_to_runtime_value() {
    let wire = WireValue::Record(BTreeMap::from([
        (String::from("unit"), WireValue::Unit),
        (String::from("ok"), WireValue::Bool(true)),
        (
            String::from("bytes"),
            WireValue::Bytes(BytePayload {
                data: vec![1, 2, 3],
                encoding: ByteEncoding::Raw,
            }),
        ),
        (
            String::from("items"),
            WireValue::List(vec![WireValue::Int(1), WireValue::String("two".into())]),
        ),
        (
            String::from("variant"),
            WireValue::Enum(WireEnumValue {
                name: "Some".into(),
                value: Some(Box::new(WireValue::Float(3.5))),
            }),
        ),
    ]));

    let value = wire.into_value().expect("wire converts to value");

    assert_eq!(
        value,
        Value::Struct(vec![
            StructFieldValue {
                name: "bytes".into(),
                value: Value::Bytes(Cow::Owned(vec![1, 2, 3])),
            },
            StructFieldValue {
                name: "items".into(),
                value: Value::List(vec![Value::Int(1), Value::String(Cow::Owned("two".into()))]),
            },
            StructFieldValue {
                name: "ok".into(),
                value: Value::Bool(true),
            },
            StructFieldValue {
                name: "unit".into(),
                value: Value::Unit,
            },
            StructFieldValue {
                name: "variant".into(),
                value: Value::Enum(EnumValue {
                    name: "Some".into(),
                    value: Some(Box::new(Value::Float(3.5))),
                }),
            },
        ])
    );
}

#[test]
fn runtime_value_converts_to_wire_value() {
    let value = Value::Struct(vec![
        StructFieldValue {
            name: "name".into(),
            value: Value::String(Cow::Owned("demo".into())),
        },
        StructFieldValue {
            name: "tuple".into(),
            value: Value::Tuple(vec![Value::Int(1), Value::Bool(false)]),
        },
        StructFieldValue {
            name: "mode".into(),
            value: Value::Enum(EnumValue {
                name: "Ready".into(),
                value: None,
            }),
        },
    ]);

    let wire = WireValue::from_value(value).expect("value converts to wire");

    assert_eq!(
        wire,
        WireValue::Record(BTreeMap::from([
            (
                "mode".into(),
                WireValue::Enum(WireEnumValue {
                    name: "Ready".into(),
                    value: None
                })
            ),
            ("name".into(), WireValue::String("demo".into())),
            (
                "tuple".into(),
                WireValue::List(vec![WireValue::Int(1), WireValue::Bool(false)])
            ),
        ]))
    );
}

#[test]
fn wire_value_conformance_covers_structured_shapes_and_optional_absence() {
    let mut record = BTreeMap::new();
    record.insert("unit".into(), WireValue::Unit);
    record.insert("optional_absent".into(), WireValue::Unit);
    record.insert(
        "list".into(),
        WireValue::List(vec![WireValue::Int(1), WireValue::Int(2)]),
    );
    record.insert(
        "tuple".into(),
        WireValue::List(vec![
            WireValue::String("left".into()),
            WireValue::Bool(true),
        ]),
    );
    record.insert(
        "map".into(),
        WireValue::Record(BTreeMap::from([
            ("a".into(), WireValue::Float(1.0)),
            ("b".into(), WireValue::Float(2.0)),
        ])),
    );
    record.insert(
        "enum_none".into(),
        WireValue::Enum(WireEnumValue {
            name: "None".into(),
            value: None,
        }),
    );
    record.insert(
        "enum_some".into(),
        WireValue::Enum(WireEnumValue {
            name: "Some".into(),
            value: Some(Box::new(WireValue::String("payload".into()))),
        }),
    );

    let wire = WireValue::Record(record);
    let value = wire.clone().into_value().expect("wire to value");
    let decoded = WireValue::from_value(value).expect("value back to wire");

    assert_eq!(decoded, wire);
}

#[test]
fn value_to_wire_conformance_covers_map_tuple_enum_and_unit() {
    let value = Value::Map(vec![
        (Value::String(Cow::Owned("unit".into())), Value::Unit),
        (
            Value::String(Cow::Owned("tuple".into())),
            Value::Tuple(vec![
                Value::String(Cow::Owned("left".into())),
                Value::Bool(false),
            ]),
        ),
        (
            Value::String(Cow::Owned("list".into())),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        ),
        (
            Value::String(Cow::Owned("enum".into())),
            Value::Enum(EnumValue {
                name: "Ready".into(),
                value: Some(Box::new(Value::Unit)),
            }),
        ),
    ]);

    let wire = WireValue::from_value(value).expect("value to wire");

    assert_eq!(
        wire,
        WireValue::Record(BTreeMap::from([
            (
                "enum".into(),
                WireValue::Enum(WireEnumValue {
                    name: "Ready".into(),
                    value: Some(Box::new(WireValue::Unit))
                })
            ),
            (
                "list".into(),
                WireValue::List(vec![WireValue::Int(1), WireValue::Int(2)])
            ),
            (
                "tuple".into(),
                WireValue::List(vec![
                    WireValue::String("left".into()),
                    WireValue::Bool(false)
                ])
            ),
            ("unit".into(), WireValue::Unit),
        ]))
    );
}

#[test]
fn wire_value_conversion_reports_unsupported_shapes() {
    let image = WireValue::Image(ImagePayload {
        data: vec![255; 4],
        width: 1,
        height: 1,
        channels: 4,
        dtype: ScalarDType::U8,
        layout: ImageLayout::Hwc,
    });
    assert!(matches!(
        image.into_value(),
        Err(WireValueConversionError::UnsupportedWireValue { kind: "image", .. })
    ));

    let handle = WireValue::Handle(WirePayloadHandle {
        id: "payload-1".into(),
        type_key: TypeKey::new("demo:frame"),
        access: AccessMode::Read,
        residency: None,
        layout: None,
        capabilities: Vec::new(),
        metadata: BTreeMap::new(),
    });
    assert!(matches!(
        handle.into_value(),
        Err(WireValueConversionError::UnsupportedWireValue { kind: "handle", .. })
    ));

    let map = Value::Map(vec![(
        Value::Int(1),
        Value::String(Cow::Owned("bad".into())),
    )]);
    assert!(matches!(
        WireValue::from_value(map),
        Err(WireValueConversionError::UnsupportedValue {
            kind: "map with non-string key",
            ..
        })
    ));
}

#[test]
fn wire_bytes_convert_to_raw_transport_payload() {
    let payload = WireValue::Bytes(BytePayload {
        data: vec![1, 2, 3, 4],
        encoding: ByteEncoding::Raw,
    })
    .into_payload("demo:bytes")
    .expect("wire bytes convert to payload");

    assert_eq!(payload.type_key().as_str(), "demo:bytes");
    assert_eq!(payload.bytes_estimate(), Some(4));

    let wire = WireValue::from_payload(&payload).expect("payload converts to wire bytes");
    assert_eq!(
        wire,
        WireValue::Bytes(BytePayload {
            data: vec![1, 2, 3, 4],
            encoding: ByteEncoding::Raw,
        })
    );
}

#[test]
fn payload_refs_route_through_transport_payload_metadata() {
    let payload = Payload::bytes_with_type_key(
        "example.Bytes",
        std::sync::Arc::<[u8]>::from(vec![1, 2, 3, 4]),
    );
    let wire = WireValue::payload_ref_from_payload("lease-1", &payload, AccessMode::Read);
    let WireValue::Handle(handle) = wire else {
        panic!("expected handle");
    };

    assert_eq!(handle.id, "lease-1");
    assert_eq!(handle.type_key, TypeKey::new("example.Bytes"));
    assert_eq!(handle.residency, Some(Residency::Cpu));
    assert_eq!(
        handle.metadata.get("bytes_estimate"),
        Some(&serde_json::json!(4))
    );
}

#[test]
fn payload_refs_validate_access_residency_layout_and_boundary_contracts() {
    let spec = boundary_contract_fixture_spec();
    let fixture =
        generate_language_fixture(&spec, FixtureLanguage::Python).expect("boundary fixture");
    let node = &fixture.schema.nodes[0];

    fixture
        .request
        .validate_against_node_with_boundaries(node, &fixture.schema.boundary_contracts)
        .expect("valid boundary handle");
    fixture
        .expected_response
        .validate_against_node_with_boundaries(node, &fixture.schema.boundary_contracts)
        .expect("valid boundary output handle");

    let mut bad = fixture.request.clone();
    let WireValue::Handle(handle) = bad.args.get_mut("frame").expect("frame arg") else {
        panic!("expected handle");
    };
    handle.residency = Some(Residency::Cpu);

    assert!(matches!(
        bad.validate_against_node_with_boundaries(node, &fixture.schema.boundary_contracts),
        Err(InvokeContractError::PayloadResidencyMismatch { .. })
    ));

    assert!(matches!(
        fixture
            .request
            .validate_against_node_with_boundaries(node, &[]),
        Err(InvokeContractError::MissingBoundaryContract { .. })
    ));
}

#[test]
fn structured_wire_values_convert_through_transport_payloads() {
    let wire = WireValue::Record(BTreeMap::from([
        ("count".into(), WireValue::Int(2)),
        (
            "tags".into(),
            WireValue::List(vec![
                WireValue::String("a".into()),
                WireValue::String("b".into()),
            ]),
        ),
    ]));

    let payload = wire
        .clone()
        .into_payload("demo:record")
        .expect("wire record converts to payload");
    assert_eq!(payload.type_key().as_str(), "demo:record");
    assert!(payload.get_ref::<Value>().is_some());

    let decoded = WireValue::from_payload(&payload).expect("payload converts back to wire");
    assert_eq!(decoded, wire);
}

#[test]
fn payload_to_wire_reports_unsupported_payload_storage() {
    let payload = Payload::owned("demo:u32", 42_u32);

    assert!(matches!(
        WireValue::from_payload(&payload),
        Err(WireValueConversionError::UnsupportedPayload { type_key, .. })
            if type_key == "demo:u32"
    ));
}
