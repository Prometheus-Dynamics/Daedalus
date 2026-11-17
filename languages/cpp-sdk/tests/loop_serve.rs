//! The worker loop over in-memory packets: the hello, runs with their events and outputs, state
//! kept and exported and imported, and the faults for a run that cannot go on. Each node is a C-ABI
//! function, as a C worker's node is, and calls the SDK's exports.

mod support;

use std::ffi::c_int;

use daedalus_lang_cpp_sdk::DdCtx;
use daedalus_lang_cpp_sdk::ffi::DdValue;
use daedalus_types::Scalar;
use daedalus_wire::codec::Writer;
use daedalus_wire::{
    BoundaryValue, Declaration, Fault, FaultCode, LAYOUT_HASH, LogLevel, Outputs, Progress,
    RunStatus, Schema, WireType, WireValue, WorkerMsg, encode_value, kind,
};
use support::*;

/// Reads the i64 at `index` of a C array of values.
///
/// # Safety
/// `values` points to at least `index + 1` readable values, each holding an integer.
unsafe fn int_at(values: *const DdValue, index: usize) -> i64 {
    // SAFETY: the caller's contract makes the value at `index` readable and an integer.
    unsafe { (*values.add(index)).data.i }
}

/// Writes an i64 into the output at `index`.
///
/// # Safety
/// `outputs` points to at least `index + 1` writable values.
unsafe fn set_int(outputs: *mut DdValue, index: usize, v: i64) {
    // SAFETY: the caller's contract makes the value at `index` writable.
    unsafe { (*outputs.add(index)).data.i = v };
}

/// # Safety
/// Called by the loop with the two inputs of `test.add` and its output.
unsafe extern "C" fn add(_cx: *mut DdCtx, inputs: *const DdValue, outputs: *mut DdValue) -> c_int {
    // SAFETY: the loop passes the node's two inputs.
    let (a, b) = unsafe { (int_at(inputs, 0), int_at(inputs, 1)) };
    // SAFETY: the loop passes the node's output.
    unsafe { set_int(outputs, 0, a + b) };
    0
}

/// # Safety
/// Called by the loop with the input of `test.events_add` and its output.
unsafe extern "C" fn events_add(
    cx: *mut DdCtx,
    inputs: *const DdValue,
    outputs: *mut DdValue,
) -> c_int {
    // SAFETY: the loop passes the node's two inputs.
    let (a, b) = unsafe { (int_at(inputs, 0), int_at(inputs, 1)) };
    // SAFETY: `cx` is the context the loop made for this call.
    unsafe { report_progress(cx) };
    // SAFETY: the loop passes the node's output.
    unsafe { set_int(outputs, 0, a + b) };
    0
}

/// Reports a progress and a log line, as the node would.
///
/// # Safety
/// `cx` is the context the loop made for the running node.
unsafe fn report_progress(cx: *mut DdCtx) {
    // SAFETY: the context is the running node's, as the caller's contract says.
    unsafe { abi::dd_progress(cx, 0.5, c_str("half")) };
    // SAFETY: as above.
    unsafe { abi::dd_log(cx, c_str("info"), c_str("adding")) };
}

/// # Safety
/// Called by the loop with the delta input, and its state is an i64 kept between runs.
unsafe extern "C" fn counter(
    cx: *mut DdCtx,
    inputs: *const DdValue,
    outputs: *mut DdValue,
) -> c_int {
    // SAFETY: the loop passes the node's one input.
    let delta = unsafe { int_at(inputs, 0) };
    // SAFETY: `cx` is the running node's context.
    let state = unsafe { abi::dd_state(cx) };
    let current = if state.is_null() {
        0
    } else {
        // SAFETY: a non-null state is the value the loop stored, which is an i64.
        unsafe { (*state).data.i }
    };
    let total = current + delta;
    // SAFETY: `cx` is the running node's context, and the value is an i64.
    unsafe { abi::dd_set_state(cx, int_value(total)) };
    // SAFETY: the loop passes the node's output.
    unsafe { set_int(outputs, 0, total) };
    0
}

/// # Safety
/// Called by the loop, which passes the node's context and its unused outputs.
unsafe extern "C" fn fails(
    cx: *mut DdCtx,
    _inputs: *const DdValue,
    _outputs: *mut DdValue,
) -> c_int {
    // SAFETY: `cx` is the running node's context.
    unsafe { abi::dd_detail(cx, c_str("one")) };
    // SAFETY: as above.
    unsafe { abi::dd_detail(cx, c_str("two")) };
    // SAFETY: as above.
    unsafe { abi::dd_fault_code(cx, 77) };
    // SAFETY: as above.
    unsafe { abi::dd_fault(cx, c_str("node_error"), c_str("rejected")) };
    1
}

/// # Safety
/// Called by the loop with the node's output, which is an i8 and gets 300.
unsafe extern "C" fn too_big(
    _cx: *mut DdCtx,
    _inputs: *const DdValue,
    outputs: *mut DdValue,
) -> c_int {
    // SAFETY: the loop passes the node's output.
    unsafe { set_int(outputs, 0, 300) };
    0
}

fn i64_type() -> *const daedalus_lang_cpp_sdk::ffi::DdType {
    scalar(DD_I64)
}

/// The nodes the tests serve.
fn nodes() -> Vec<daedalus_lang_cpp_sdk::ffi::DdNode> {
    let delta = port("delta", i64_type());
    let out = port("out", i64_type());
    vec![
        node(
            "test.add",
            &[port("a", i64_type()), port("b", i64_type())],
            std::slice::from_ref(&out),
            add,
        ),
        node(
            "test.events_add",
            &[port("a", i64_type()), port("b", i64_type())],
            std::slice::from_ref(&out),
            events_add,
        ),
        stateful(
            "test.counter",
            std::slice::from_ref(&delta),
            &[port("total", i64_type())],
            i64_type(),
            counter,
        ),
        node(
            "test.stateless",
            std::slice::from_ref(&delta),
            std::slice::from_ref(&out),
            add,
        ),
        node(
            "test.fails",
            std::slice::from_ref(&delta),
            std::slice::from_ref(&out),
            fails,
        ),
        node("test.too_big", &[], &[port("out", scalar(DD_I8))], too_big),
    ]
}

/// A run message: its id, its nodes, and its inputs, each an i64 on a node's port.
fn run_message(run: u64, node_ids: &[&str], inputs: &[(&str, &str, i64)]) -> Vec<u8> {
    let mut out = Writer::new();
    out.u8(kind::RUN);
    out.u64(run);
    out.u32(0);
    out.u32(node_ids.len() as u32);
    for id in node_ids {
        out.str(id).expect("a short id");
    }
    out.u32(inputs.len() as u32);
    for (node, port, value) in inputs {
        out.str(node).expect("a short id");
        out.str(port).expect("a short port");
        i64_bytes(&mut out, *value);
    }
    out.buf
}

fn i64_bytes(out: &mut Writer, value: i64) {
    encode_value(
        &WireType::Scalar(Scalar::Int),
        &WireValue::I64(value),
        &Schema::new(),
        out,
    )
    .expect("an i64 encodes");
}

/// A state import or export of the named node, with an i64 value.
fn state_import(node_id: &str, value: i64) -> Vec<u8> {
    let mut out = Writer::new();
    out.u8(kind::STATE_IMPORT);
    out.str(node_id).expect("a short id");
    i64_bytes(&mut out, value);
    out.buf
}

fn state_export(node_id: &str) -> Vec<u8> {
    let mut out = Writer::new();
    out.u8(kind::STATE_EXPORT);
    out.str(node_id).expect("a short id");
    out.buf
}

/// The packets a served stream wrote, decoded: the hello first, then each message as the wire reads it.
fn decode(packets: &[Vec<u8>]) -> Vec<WorkerMsg> {
    let schema = schema_of(&packets[0]);
    packets
        .iter()
        .map(|body| WorkerMsg::decode(body, &schema).expect("a packet decodes"))
        .collect()
}

fn outputs(run: u64, status: RunStatus, values: Vec<BoundaryValue>) -> WorkerMsg {
    WorkerMsg::Outputs(Outputs {
        run,
        status,
        outputs: values,
    })
}

fn fault(run: u64, node: Option<&str>, code: FaultCode, reason: &str) -> WorkerMsg {
    WorkerMsg::Fault(Fault {
        run,
        node: node.map(str::to_owned),
        code,
        reason: reason.to_owned(),
    })
}

#[test]
fn the_hello_declares_every_node_and_port() {
    let packets = serve(&nodes(), &frame(&[kind::SHUTDOWN]));
    assert_eq!(
        packets.len(),
        1,
        "a shutdown writes nothing after the hello"
    );
    let WorkerMsg::Hello(hello) = decode(&packets)[0].clone() else {
        panic!("the first packet is the hello");
    };
    assert_eq!(hello.worker, "test-worker");
    assert_eq!(hello.layout, LAYOUT_HASH);
    let ids: Vec<&str> = hello
        .nodes
        .iter()
        .map(|node: &Declaration| node.id.as_str())
        .collect();
    assert_eq!(
        ids,
        [
            "test.add",
            "test.events_add",
            "test.counter",
            "test.stateless",
            "test.fails",
            "test.too_big"
        ]
    );
    assert!(
        hello.nodes[2].state.is_some(),
        "the counter declares its state"
    );
}

#[test]
fn a_run_sends_its_events_before_its_outputs() {
    let input = frame(&run_message(
        4,
        &["test.events_add"],
        &[("test.events_add", "a", 2), ("test.events_add", "b", 40)],
    ));
    let msgs = decode(&serve(&nodes(), &input));
    assert_eq!(
        msgs[1],
        WorkerMsg::Progress(Progress {
            run: 4,
            node_index: 0,
            message: "half".to_owned(),
            fraction: Some(0.5),
        })
    );
    assert_eq!(
        msgs[2],
        WorkerMsg::Log {
            level: LogLevel::Info,
            text: "adding".to_owned(),
        }
    );
    let WorkerMsg::Outputs(done) = &msgs[3] else {
        panic!("the outputs come last");
    };
    assert_eq!(done.status, RunStatus::Ok);
    assert_eq!(done.outputs[0].node, "test.events_add");
    assert_eq!(done.outputs[0].port, "out");
    assert_eq!(done.outputs[0].value, WireValue::I64(42));
    assert_eq!(msgs.len(), 4);
}

#[test]
fn state_is_kept_between_runs_exported_and_imported() {
    let mut input = Vec::new();
    input.extend(frame(&run_message(
        1,
        &["test.counter"],
        &[("test.counter", "delta", 5)],
    )));
    input.extend(frame(&run_message(
        2,
        &["test.counter"],
        &[("test.counter", "delta", 7)],
    )));
    input.extend(frame(&state_export("test.counter")));
    input.extend(frame(&state_import("test.counter", 100)));
    input.extend(frame(&run_message(
        3,
        &["test.counter"],
        &[("test.counter", "delta", 1)],
    )));
    input.extend(frame(&[kind::SHUTDOWN]));
    let msgs = decode(&serve(&nodes(), &input));
    let totals: Vec<WireValue> = msgs
        .iter()
        .filter_map(|msg| match msg {
            WorkerMsg::Outputs(done) => done.outputs.first().map(|out| out.value.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        totals,
        [WireValue::I64(5), WireValue::I64(12), WireValue::I64(101)]
    );
    assert!(
        msgs.contains(&WorkerMsg::StateValue {
            node: "test.counter".to_owned(),
            value: WireValue::I64(12),
        }),
        "the export sends the state as it stood: {msgs:?}"
    );
}

#[test]
fn a_state_import_of_the_wrong_size_keeps_the_stored_state() {
    let mut bad = Writer::new();
    bad.u8(kind::STATE_IMPORT);
    bad.str("test.counter").expect("a short id");
    bad.buf.extend_from_slice(&[1, 2, 3]);
    let mut input = Vec::new();
    input.extend(frame(&run_message(
        1,
        &["test.counter"],
        &[("test.counter", "delta", 5)],
    )));
    input.extend(frame(&bad.buf));
    input.extend(frame(&state_export("test.counter")));
    let msgs = decode(&serve(&nodes(), &input));
    assert!(
        msgs.contains(&fault(
            0,
            Some("test.counter"),
            FaultCode::MalformedPacket,
            "the state value does not match the node's state type"
        )),
        "{msgs:?}"
    );
    assert!(
        msgs.contains(&WorkerMsg::StateValue {
            node: "test.counter".to_owned(),
            value: WireValue::I64(5),
        }),
        "the stored state is the run's: {msgs:?}"
    );
}

#[test]
fn a_node_with_no_state_refuses_an_export() {
    let input = frame(&state_export("test.add"));
    let msgs = decode(&serve(&nodes(), &input));
    assert_eq!(
        msgs[1],
        fault(
            0,
            Some("test.add"),
            FaultCode::BadInput,
            "the node has no state to export"
        )
    );
}

#[test]
fn an_unknown_node_and_an_unknown_port_each_end_their_run_with_a_fault() {
    let mut input = Vec::new();
    input.extend(frame(&run_message(5, &["test.missing"], &[])));
    input.extend(frame(&run_message(
        6,
        &["test.add"],
        &[("test.add", "nope", 1)],
    )));
    let msgs = decode(&serve(&nodes(), &input));
    assert_eq!(
        msgs[1..],
        [
            fault(5, None, FaultCode::UnknownNode, "no such node"),
            outputs(5, RunStatus::Fault, vec![]),
            fault(
                6,
                None,
                FaultCode::BadInput,
                "an input names no declared port"
            ),
            outputs(6, RunStatus::Fault, vec![]),
        ]
    );
}

#[test]
fn a_failing_node_sends_its_details_its_code_and_its_fault() {
    let input = frame(&run_message(
        7,
        &["test.fails"],
        &[("test.fails", "delta", 1)],
    ));
    let msgs = decode(&serve(&nodes(), &input));
    assert_eq!(
        msgs[1..],
        [
            WorkerMsg::Detail {
                run: 7,
                text: "one".to_owned(),
            },
            WorkerMsg::Detail {
                run: 7,
                text: "two".to_owned(),
            },
            WorkerMsg::NodeCode { run: 7, code: 77 },
            fault(7, Some("test.fails"), FaultCode::NodeError, "rejected"),
            outputs(7, RunStatus::Fault, vec![]),
        ]
    );
}

#[test]
fn an_output_that_does_not_fit_its_type_fails_the_run_without_a_fault() {
    let input = frame(&run_message(8, &["test.too_big"], &[]));
    let msgs = decode(&serve(&nodes(), &input));
    assert_eq!(msgs[1..], [outputs(8, RunStatus::Fault, vec![])]);
}

#[test]
fn a_malformed_packet_and_an_unknown_kind_leave_the_worker_serving() {
    let mut input = Vec::new();
    input.extend(frame(&[0x7f]));
    input.extend(frame(&[kind::PING]));
    input.extend(frame(&[kind::SHUTDOWN]));
    input.extend(frame(&run_message(9, &["test.add"], &[])));
    let msgs = decode(&serve(&nodes(), &input));
    assert_eq!(
        msgs[1..],
        [
            fault(0, None, FaultCode::MalformedPacket, "unknown message kind"),
            WorkerMsg::Pong,
        ],
        "shutdown ends the loop, so the run after it is not served"
    );
}

#[test]
fn a_node_name_that_is_not_text_names_no_node() {
    // Node names are compared as bytes, so a name that is not UTF-8 is simply not one the worker serves.
    let mut out = Writer::new();
    out.u8(kind::RUN);
    out.u64(10);
    out.u32(0);
    out.u32(1);
    out.bytes(&[0xff, 0xfe]).expect("a short id");
    out.u32(0);
    let input = frame(&out.buf);
    let msgs = decode(&serve(&nodes(), &input));
    assert_eq!(
        msgs[1..],
        [
            fault(10, None, FaultCode::UnknownNode, "no such node"),
            outputs(10, RunStatus::Fault, vec![]),
        ]
    );
}

#[test]
fn a_hello_with_nothing_to_serve_is_still_a_hello() {
    let packets = serve(&[], &frame(&[kind::SHUTDOWN]));
    assert_eq!(packets.len(), 1);
    assert!(matches!(decode(&packets)[0], WorkerMsg::Hello(_)));
}
