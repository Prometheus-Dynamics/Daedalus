//! The worker protocol over in-memory pipes: the hello, the ack, invokes and their responses,
//! typed errors, a node panic that leaves the worker serving, state across calls, events, the
//! schema export and the package it ships in.

use std::collections::BTreeMap;

use daedalus::declare_plugin;
use daedalus::macros::node;
use daedalus::runtime::NodeError;
use daedalus_ffi_core::{
    BackendKind, BackendRuntimeModel, ByteEncoding, BytePayload, InvokeEventLevel, InvokeRequest,
    WORKER_PROTOCOL_VERSION, WireValue, WorkerMessage, WorkerMessagePayload,
};
use daedalus_ffi_rust::{Worker, emit_event, event_info, rust_worker_package};

#[node(id = "add", inputs("a", "b"), outputs("out"))]
fn add(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a + b)
}

#[node(id = "split", inputs("value"), outputs("positive", "negative"))]
fn split(value: i64) -> Result<(i64, i64), NodeError> {
    Ok((value, -value))
}

#[derive(Default)]
struct AccumState {
    sum: i64,
}

#[node(id = "accum", inputs("value"), outputs("sum"), state(AccumState))]
fn accum(value: i64, state: &mut AccumState) -> Result<i64, NodeError> {
    state.sum += value;
    Ok(state.sum)
}

#[node(id = "boom", inputs("value"), outputs("out"))]
fn boom(value: i64) -> Result<i64, NodeError> {
    if value < 0 {
        panic!("negative input");
    }
    Ok(value)
}

#[node(id = "reject", inputs("value"), outputs("out"))]
fn reject(_value: i64) -> Result<i64, NodeError> {
    Err(NodeError::Handler("value rejected".into()))
}

#[node(id = "noisy", inputs("value"), outputs("out"))]
fn noisy(value: i64) -> Result<i64, NodeError> {
    event_info("starting");
    emit_event(InvokeEventLevel::Warn, "careful", BTreeMap::new());
    Ok(value)
}

#[node(id = "bytes_len", inputs("payload"), outputs("len"))]
fn bytes_len(payload: Vec<u8>) -> Result<u64, NodeError> {
    Ok(payload.len() as u64)
}

declare_plugin!(
    ProtocolPlugin,
    "test.protocol",
    [add, split, accum, boom, reject, noisy, bytes_len]
);

/// Serve `messages` (after the ack) and return every message the worker wrote.
fn serve(worker: &mut Worker, messages: &[WorkerMessage]) -> Vec<WorkerMessage> {
    let mut input = String::new();
    input.push_str(&line(&WorkerMessage::new(
        WorkerMessagePayload::Ack(daedalus_ffi_core::WorkerProtocolAck {
            protocol_version: WORKER_PROTOCOL_VERSION,
            worker_id: None,
        }),
        Some("startup".into()),
    )));
    for message in messages {
        input.push_str(&line(message));
    }
    let mut output = Vec::new();
    worker
        .serve_io(input.as_bytes(), &mut output)
        .expect("serve_io returns on end of input");
    String::from_utf8(output)
        .expect("utf-8 output")
        .lines()
        .map(|line| serde_json::from_str(line).expect("protocol frame"))
        .collect()
}

fn line(message: &WorkerMessage) -> String {
    let mut text = serde_json::to_string(message).expect("encode");
    text.push('\n');
    text
}

fn invoke(node: &str, args: &[(&str, WireValue)], correlation: &str) -> WorkerMessage {
    WorkerMessage::new(
        WorkerMessagePayload::Invoke(InvokeRequest {
            protocol_version: WORKER_PROTOCOL_VERSION,
            node_id: node.into(),
            correlation_id: Some(correlation.into()),
            args: args
                .iter()
                .map(|(name, value)| ((*name).to_string(), value.clone()))
                .collect(),
            state: None,
            context: BTreeMap::new(),
        }),
        Some(correlation.into()),
    )
}

fn worker() -> Worker {
    Worker::from_plugin(&ProtocolPlugin::new()).expect("worker")
}

/// The reply to the one invoke in `messages` (after the hello).
fn reply(messages: &[WorkerMessage]) -> &WorkerMessagePayload {
    &messages[1].payload
}

#[test]
fn hello_announces_every_node_and_the_protocol() {
    let mut worker = worker().with_worker_id("protocol-test");
    let out = serve(&mut worker, &[]);
    let WorkerMessagePayload::Hello(hello) = &out[0].payload else {
        panic!("first frame is not a hello: {:?}", out[0]);
    };
    assert_eq!(out[0].correlation_id.as_deref(), Some("startup"));
    assert_eq!(hello.protocol_version, WORKER_PROTOCOL_VERSION);
    assert_eq!(hello.min_protocol_version, WORKER_PROTOCOL_VERSION);
    assert_eq!(hello.worker_id.as_deref(), Some("protocol-test"));
    assert_eq!(hello.backend, Some(BackendKind::Rust));
    assert_eq!(
        hello.supported_nodes,
        [
            "test.protocol:accum",
            "test.protocol:add",
            "test.protocol:boom",
            "test.protocol:bytes_len",
            "test.protocol:noisy",
            "test.protocol:reject",
            "test.protocol:split",
        ]
    );
    assert!(
        hello
            .capabilities
            .contains(&"persistent_worker".to_string())
    );
    assert_eq!(hello.negotiated_protocol_version().ok(), Some(1));
}

#[test]
fn invoke_answers_with_the_correlation_id_and_typed_outputs() {
    let mut worker = worker();
    let out = serve(
        &mut worker,
        &[invoke(
            "test.protocol:add",
            &[("a", WireValue::Int(2)), ("b", WireValue::Int(40))],
            "req-1",
        )],
    );
    let WorkerMessagePayload::Response(response) = reply(&out) else {
        panic!("expected a response: {:?}", reply(&out));
    };
    assert_eq!(out[1].correlation_id.as_deref(), Some("req-1"));
    assert_eq!(response.correlation_id.as_deref(), Some("req-1"));
    assert_eq!(response.outputs.get("out"), Some(&WireValue::Int(42)));
    assert!(response.state.is_none());
    assert!(response.events.is_empty());
}

#[test]
fn multi_output_nodes_name_each_output() {
    let mut worker = worker();
    let out = serve(
        &mut worker,
        &[invoke(
            "test.protocol:split",
            &[("value", WireValue::Int(7))],
            "s",
        )],
    );
    let WorkerMessagePayload::Response(response) = reply(&out) else {
        panic!("expected a response: {:?}", reply(&out));
    };
    assert_eq!(response.outputs.get("positive"), Some(&WireValue::Int(7)));
    assert_eq!(response.outputs.get("negative"), Some(&WireValue::Int(-7)));
}

#[test]
fn stateful_nodes_keep_state_across_invokes() {
    let mut worker = worker();
    let messages = [
        invoke("test.protocol:accum", &[("value", WireValue::Int(3))], "a"),
        invoke("test.protocol:accum", &[("value", WireValue::Int(5))], "b"),
    ];
    let out = serve(&mut worker, &messages);
    let sums: Vec<_> = out[1..]
        .iter()
        .map(|message| match &message.payload {
            WorkerMessagePayload::Response(response) => response.outputs["sum"].clone(),
            other => panic!("expected a response: {other:?}"),
        })
        .collect();
    assert_eq!(sums, [WireValue::Int(3), WireValue::Int(8)]);
}

#[test]
fn a_node_panic_is_an_error_and_the_worker_keeps_serving() {
    let mut worker = worker();
    let out = serve(
        &mut worker,
        &[
            invoke("test.protocol:boom", &[("value", WireValue::Int(-1))], "p"),
            invoke(
                "test.protocol:add",
                &[("a", WireValue::Int(1)), ("b", WireValue::Int(1))],
                "q",
            ),
        ],
    );
    let WorkerMessagePayload::Error(error) = &out[1].payload else {
        panic!("expected an error: {:?}", out[1]);
    };
    assert_eq!(error.code, "node_panic");
    assert!(
        error.message.contains("negative input"),
        "{}",
        error.message
    );
    assert_eq!(out[1].correlation_id.as_deref(), Some("p"));
    let WorkerMessagePayload::Response(response) = &out[2].payload else {
        panic!("worker stopped serving after a panic: {:?}", out[2]);
    };
    assert_eq!(response.outputs.get("out"), Some(&WireValue::Int(2)));
}

#[test]
fn node_errors_are_typed() {
    let mut worker = worker();
    let out = serve(
        &mut worker,
        &[invoke(
            "test.protocol:reject",
            &[("value", WireValue::Int(1))],
            "r",
        )],
    );
    let WorkerMessagePayload::Error(rejected) = &out[1].payload else {
        panic!("expected an error: {:?}", out[1]);
    };
    assert_eq!(rejected.code, "node_error");
    assert!(rejected.message.contains("value rejected"));
}

#[test]
fn bad_requests_name_their_fault() {
    let mut worker = worker();
    let out = serve(
        &mut worker,
        &[
            invoke("test.protocol:nope", &[], "u"),
            invoke("test.protocol:add", &[("a", WireValue::Int(1))], "m"),
            invoke(
                "test.protocol:add",
                &[
                    ("a", WireValue::Int(1)),
                    ("b", WireValue::Int(1)),
                    ("c", WireValue::Int(1)),
                ],
                "x",
            ),
            WorkerMessage::new(
                WorkerMessagePayload::Invoke(InvokeRequest {
                    protocol_version: WORKER_PROTOCOL_VERSION + 1,
                    node_id: "test.protocol:add".into(),
                    correlation_id: Some("v".into()),
                    args: BTreeMap::new(),
                    state: None,
                    context: BTreeMap::new(),
                }),
                Some("v".into()),
            ),
        ],
    );
    let codes: Vec<_> = out[1..]
        .iter()
        .map(|message| match &message.payload {
            WorkerMessagePayload::Error(error) => error.code.clone(),
            other => panic!("expected an error: {other:?}"),
        })
        .collect();
    assert_eq!(
        codes,
        [
            "unknown_node",
            "bad_request",
            "bad_request",
            "unsupported_protocol"
        ]
    );
}

#[test]
fn a_malformed_line_is_an_error_and_the_next_message_still_runs() {
    let mut worker = worker();
    let mut output = Vec::new();
    let input = format!(
        "not-json\n{}",
        line(&invoke(
            "test.protocol:add",
            &[("a", WireValue::Int(2)), ("b", WireValue::Int(2))],
            "ok"
        ))
    );
    worker
        .serve_io(input.as_bytes(), &mut output)
        .expect("serve_io");
    let frames: Vec<WorkerMessage> = String::from_utf8(output)
        .expect("utf-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("frame"))
        .collect();
    let WorkerMessagePayload::Error(error) = &frames[1].payload else {
        panic!("expected an error: {:?}", frames[1]);
    };
    assert_eq!(error.code, "bad_message");
    assert!(matches!(
        frames[2].payload,
        WorkerMessagePayload::Response(_)
    ));
}

#[test]
fn events_travel_in_the_response_in_emission_order() {
    let mut worker = worker();
    let out = serve(
        &mut worker,
        &[invoke(
            "test.protocol:noisy",
            &[("value", WireValue::Int(1))],
            "e",
        )],
    );
    let WorkerMessagePayload::Response(response) = reply(&out) else {
        panic!("expected a response: {:?}", reply(&out));
    };
    let events: Vec<_> = response
        .events
        .iter()
        .map(|event| (event.level, event.message.as_str()))
        .collect();
    assert_eq!(
        events,
        [
            (InvokeEventLevel::Info, "starting"),
            (InvokeEventLevel::Warn, "careful")
        ]
    );
}

#[test]
fn byte_payloads_reach_the_node_and_a_u64_result_is_uint() {
    let mut worker = worker();
    let payload = WireValue::Bytes(BytePayload {
        data: vec![1, 2, 3, 4],
        encoding: ByteEncoding::Base64,
    });
    let out = serve(
        &mut worker,
        &[invoke(
            "test.protocol:bytes_len",
            &[("payload", payload)],
            "b",
        )],
    );
    let WorkerMessagePayload::Response(response) = reply(&out) else {
        panic!("expected a response: {:?}", reply(&out));
    };
    assert_eq!(response.outputs.get("len"), Some(&WireValue::UInt(4)));
}

#[test]
fn end_of_input_is_a_clean_exit() {
    let mut worker = worker();
    let mut output = Vec::new();
    assert!(worker.serve_io(&b""[..], &mut output).is_ok());
    assert_eq!(String::from_utf8(output).expect("utf-8").lines().count(), 1);
}

#[test]
fn the_schema_is_the_plugin_schema_the_host_installs() {
    let worker = worker();
    let schema = &worker.schemas()[0];
    assert_eq!(schema.plugin.name, "test.protocol");
    let add = schema
        .nodes
        .iter()
        .find(|node| node.id == "test.protocol:add")
        .expect("add node");
    let inputs: Vec<_> = add.inputs.iter().map(|port| port.name.as_str()).collect();
    assert_eq!(inputs, ["a", "b"]);
    assert_eq!(add.backend, BackendKind::Rust);
    schema.validate().expect("valid schema");
}

#[test]
fn the_package_runs_every_node_in_a_persistent_worker() {
    let worker = worker();
    let schema = worker.schemas()[0].clone();
    let package =
        rust_worker_package(schema.clone(), "/opt/plugin/worker", ["--serve"]).expect("package");
    package.validate().expect("valid package");
    assert_eq!(package.backends.len(), schema.nodes.len());
    for backend in package.backends.values() {
        assert_eq!(backend.backend, BackendKind::Rust);
        assert_eq!(backend.runtime_model, BackendRuntimeModel::PersistentWorker);
        assert_eq!(backend.executable.as_deref(), Some("/opt/plugin/worker"));
        assert_eq!(backend.args, ["--serve"]);
    }
}
