use super::*;

#[test]
fn invoke_contract_preserves_structured_state_and_outputs() {
    let request = InvokeRequest {
        protocol_version: WORKER_PROTOCOL_VERSION,
        node_id: String::from("demo.normalize"),
        correlation_id: Some(String::from("req-1")),
        args: BTreeMap::from([(
            String::from("config"),
            WireValue::Record(BTreeMap::from([(
                String::from("size"),
                WireValue::Record(BTreeMap::from([(
                    String::from("width"),
                    WireValue::Int(512),
                )])),
            )])),
        )]),
        state: Some(WireValue::Record(BTreeMap::from([(
            String::from("previous"),
            WireValue::Float(0.5),
        )]))),
        context: BTreeMap::from([(
            String::from("trace_id"),
            serde_json::Value::String(String::from("abc123")),
        )]),
    };

    let response = InvokeResponse {
        protocol_version: WORKER_PROTOCOL_VERSION,
        correlation_id: Some(String::from("req-1")),
        outputs: BTreeMap::from([(
            String::from("result"),
            WireValue::Record(BTreeMap::from([(
                String::from("shape"),
                WireValue::Record(BTreeMap::from([
                    (String::from("width"), WireValue::Int(512)),
                    (String::from("height"), WireValue::Int(512)),
                ])),
            )])),
        )]),
        state: Some(WireValue::Record(BTreeMap::from([(
            String::from("previous"),
            WireValue::Float(0.75),
        )]))),
        events: vec![InvokeEvent {
            level: InvokeEventLevel::Info,
            message: String::from("normalized"),
            metadata: BTreeMap::new(),
        }],
    };

    let request_json = serde_json::to_string(&request).expect("serialize request");
    let response_json = serde_json::to_string(&response).expect("serialize response");

    let decoded_request: InvokeRequest =
        serde_json::from_str(&request_json).expect("deserialize request");
    let decoded_response: InvokeResponse =
        serde_json::from_str(&response_json).expect("deserialize response");

    assert_eq!(decoded_request, request);
    assert_eq!(decoded_response, response);
    decoded_request
        .validate_protocol()
        .expect("request protocol is supported");
    decoded_response
        .validate_protocol()
        .expect("response protocol is supported");
}

#[test]
fn invoke_contract_validates_requests_responses_events_and_wire_values() {
    let int_port = |name: &str, optional: bool| WirePort {
        name: name.into(),
        ty: TypeExpr::scalar(daedalus_data::model::ValueType::Int),
        type_key: None,
        optional,
        access: AccessMode::Read,
        residency: None,
        layout: None,
        source: None,
        const_value: None,
    };
    let node = NodeSchema {
        id: "demo.contract".into(),
        backend: BackendKind::Python,
        entrypoint: "run".into(),
        label: None,
        stateful: false,
        feature_flags: Vec::new(),
        inputs: vec![int_port("a", false), int_port("maybe", true)],
        outputs: vec![int_port("out", false)],
        metadata: BTreeMap::new(),
    };
    let request = InvokeRequest {
        protocol_version: WORKER_PROTOCOL_VERSION,
        node_id: "demo.contract".into(),
        correlation_id: Some("req-1".into()),
        args: BTreeMap::from([("a".into(), WireValue::Int(1))]),
        state: Some(WireValue::Record(BTreeMap::from([(
            "state".into(),
            WireValue::Enum(WireEnumValue {
                name: "Ready".into(),
                value: None,
            }),
        )]))),
        context: BTreeMap::new(),
    };
    request
        .validate_against_node(&node)
        .expect("request contract");

    let response = InvokeResponse {
        protocol_version: WORKER_PROTOCOL_VERSION,
        correlation_id: Some("req-1".into()),
        outputs: BTreeMap::from([("out".into(), WireValue::Int(2))]),
        state: None,
        events: vec![InvokeEvent {
            level: InvokeEventLevel::Info,
            message: "ok".into(),
            metadata: BTreeMap::new(),
        }],
    };
    response
        .validate_against_node(&node)
        .expect("response contract");

    let mut missing = request.clone();
    missing.args.clear();
    assert!(matches!(
        missing.validate_against_node(&node),
        Err(InvokeContractError::MissingArgument(name)) if name == "a"
    ));

    let mut unexpected = response.clone();
    unexpected.outputs.insert("extra".into(), WireValue::Unit);
    assert!(matches!(
        unexpected.validate_against_node(&node),
        Err(InvokeContractError::UnexpectedOutput(name)) if name == "extra"
    ));

    let empty_event = InvokeEvent {
        level: InvokeEventLevel::Warn,
        message: " ".into(),
        metadata: BTreeMap::new(),
    };
    assert!(matches!(
        empty_event.validate_contract(),
        Err(InvokeContractError::EmptyField {
            field: "event.message"
        })
    ));

    assert!(matches!(
        WireValue::Enum(WireEnumValue {
            name: String::new(),
            value: None,
        })
        .validate_contract(),
        Err(InvokeContractError::EmptyField { field: "enum.name" })
    ));
}

#[test]
fn worker_protocol_negotiates_supported_version_and_capability_summary() {
    let hello = WorkerHello {
        protocol_version: WORKER_PROTOCOL_VERSION,
        min_protocol_version: WORKER_PROTOCOL_VERSION,
        worker_id: Some("python-worker-1".into()),
        backend: Some(BackendKind::Python),
        supported_nodes: vec!["demo.add".into()],
        capabilities: vec!["stateful".into(), "raw_io".into()],
        metadata: BTreeMap::from([("pid".into(), serde_json::json!(1234))]),
    };

    assert_eq!(
        hello.negotiated_protocol_version().expect("negotiate"),
        WORKER_PROTOCOL_VERSION
    );

    let ack = WorkerProtocolAck::from_hello(&hello).expect("ack");
    assert_eq!(ack.protocol_version, WORKER_PROTOCOL_VERSION);
    assert_eq!(ack.worker_id.as_deref(), Some("python-worker-1"));
    ack.validate_protocol().expect("ack protocol");

    let json = serde_json::to_string(&hello).expect("serialize hello");
    let decoded: WorkerHello = serde_json::from_str(&json).expect("deserialize hello");
    assert_eq!(decoded, hello);
}

#[test]
fn worker_protocol_rejects_unsupported_versions() {
    let request = InvokeRequest {
        protocol_version: WORKER_PROTOCOL_VERSION + 1,
        node_id: "demo.add".into(),
        correlation_id: Some("req-unsupported".into()),
        args: BTreeMap::new(),
        state: None,
        context: BTreeMap::new(),
    };
    assert!(matches!(
        request.validate_protocol(),
        Err(WorkerProtocolError::UnsupportedVersion { found, .. })
            if found == WORKER_PROTOCOL_VERSION + 1
    ));

    let hello = WorkerHello {
        protocol_version: WORKER_PROTOCOL_VERSION + 2,
        min_protocol_version: WORKER_PROTOCOL_VERSION + 1,
        worker_id: None,
        backend: Some(BackendKind::Node),
        supported_nodes: Vec::new(),
        capabilities: Vec::new(),
        metadata: BTreeMap::new(),
    };
    assert!(matches!(
        hello.negotiated_protocol_version(),
        Err(WorkerProtocolError::UnsupportedVersion { .. })
    ));
}

#[test]
fn worker_messages_wrap_every_payload_with_protocol_and_correlation_id() {
    let hello = WorkerMessage::new(
        WorkerMessagePayload::Hello(WorkerHello {
            protocol_version: WORKER_PROTOCOL_VERSION,
            min_protocol_version: WORKER_PROTOCOL_VERSION,
            worker_id: Some("node-worker-1".into()),
            backend: Some(BackendKind::Node),
            supported_nodes: vec!["demo.add".into()],
            capabilities: vec!["persistent_worker".into()],
            metadata: BTreeMap::new(),
        }),
        Some("startup-1".into()),
    );
    let request = WorkerMessage::new(
        WorkerMessagePayload::Invoke(InvokeRequest {
            protocol_version: WORKER_PROTOCOL_VERSION,
            node_id: "demo.add".into(),
            correlation_id: Some("invoke-1".into()),
            args: BTreeMap::new(),
            state: None,
            context: BTreeMap::new(),
        }),
        Some("invoke-1".into()),
    );
    let event = WorkerMessage::new(
        WorkerMessagePayload::Event(InvokeEvent {
            level: InvokeEventLevel::Info,
            message: "loaded".into(),
            metadata: BTreeMap::new(),
        }),
        Some("invoke-1".into()),
    );
    let error = WorkerMessage::new(
        WorkerMessagePayload::Error(WorkerError {
            code: "method_not_found".into(),
            message: "missing demo.add".into(),
            metadata: BTreeMap::new(),
        }),
        Some("invoke-1".into()),
    );

    for message in [hello, request, event, error] {
        message.validate_protocol().expect("message protocol");
        assert!(message.correlation_id.is_some());
        let json = serde_json::to_string(&message).expect("serialize message");
        let decoded: WorkerMessage = serde_json::from_str(&json).expect("deserialize message");
        assert_eq!(decoded, message);
    }
}
