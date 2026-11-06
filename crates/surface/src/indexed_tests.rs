//! The by-index calls on a real graph (the `stable_abi` nodes installed the in-process way): index
//! lookup, pushes and takes by index against the by-name calls, and the batch's all-or-nothing
//! check. (A warm bytes push allocating nothing is checked in `daedalus-host`'s shared-mode test,
//! which counts the surface's allocations.)

use std::ptr;

use daedalus::GraphDocument;
use daedalus::data::daedalus_type::DaedalusTypeExpr;
use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::SharedBytes;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use lemnos_abi::{Handle, MessageSink, Status};

use crate::graph::{
    daedalus_surface_graph_compile, daedalus_surface_graph_input_index,
    daedalus_surface_graph_output_index, daedalus_surface_graph_release,
};
use crate::host::{daedalus_surface_host_create, daedalus_surface_host_release};
use crate::indexed::*;
use crate::io::{daedalus_surface_push_i64, daedalus_surface_tick};
use crate::state::lock;

/// `step` -> `accumulate` -> `total`, and `bytes` -> `shared_sum` -> `tail`.
fn document() -> String {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    let graph = registry
        .graph_builder()
        .unwrap()
        .node_id("stable_abi:accumulate", "accumulate")
        .node_id("stable_abi:shared_sum", "shared_sum")
        .input_as("step", TypeExpr::Scalar(ValueType::Int))
        .input_as("bytes", <SharedBytes as DaedalusTypeExpr>::type_expr())
        .connect("step", "accumulate.step")
        .connect("bytes", "shared_sum.bytes")
        .connect("accumulate.total", "total")
        .connect("shared_sum.tail", "tail")
        .build();
    GraphDocument::new(graph).to_json().unwrap()
}

fn setup() -> (Handle, Handle) {
    let mut host = Handle::NULL;
    // Safety: a valid out-pointer.
    assert_eq!(
        unsafe { daedalus_surface_host_create(&mut host) },
        Status::OK
    );
    lock()
        .hosts
        .get_mut(host)
        .unwrap()
        .registry
        .install_plugin(&StableAbiPlugin::new())
        .unwrap();
    let document = document();
    let mut graph = Handle::NULL;
    // Safety: the document and out-pointer are valid for the call.
    let status = unsafe {
        daedalus_surface_graph_compile(
            host,
            document.as_ptr(),
            document.len(),
            &mut graph,
            MessageSink::NONE,
        )
    };
    assert_eq!(status, Status::OK);
    (host, graph)
}

fn teardown(host: Handle, graph: Handle) {
    // Safety: both are live handles of this test.
    unsafe {
        assert_eq!(daedalus_surface_graph_release(graph), Status::OK);
        assert_eq!(daedalus_surface_host_release(host), Status::OK);
    }
}

fn input(graph: Handle, name: &str) -> Result<u32, Status> {
    let mut index = u32::MAX;
    // Safety: the name and out-pointer are valid for the call.
    let status =
        unsafe { daedalus_surface_graph_input_index(graph, name.as_ptr(), name.len(), &mut index) };
    if status == Status::OK {
        Ok(index)
    } else {
        Err(status)
    }
}

fn output(graph: Handle, name: &str) -> u32 {
    let mut index = u32::MAX;
    // Safety: the name and out-pointer are valid for the call.
    let status = unsafe {
        daedalus_surface_graph_output_index(graph, name.as_ptr(), name.len(), &mut index)
    };
    assert_eq!(status, Status::OK, "output {name}");
    index
}

fn tick(graph: Handle) {
    // Safety: a live handle; the sink drops messages.
    assert_eq!(
        unsafe { daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::OK
    );
}

#[test]
fn ports_by_index_match_ports_by_name() {
    let (host, graph) = setup();
    let step = input(graph, "step").unwrap();
    let bytes = input(graph, "bytes").unwrap();
    let (total, tail) = (output(graph, "total"), output(graph, "tail"));
    assert_eq!(input(graph, "nope"), Err(Status::NOT_FOUND));
    // Safety (whole test): every pointer is valid for the call it is passed to.
    unsafe {
        // Kind and range are checked; nothing reaches the engine.
        assert_eq!(
            daedalus_surface_push_f64_at(graph, step, 1.0),
            Status::INVALID_ARGUMENT
        );
        assert_eq!(
            daedalus_surface_push_i64_at(graph, 99, 1),
            Status::NOT_FOUND
        );
        assert_eq!(
            daedalus_surface_take_i64_at(graph, total, ptr::null_mut()),
            Status::INVALID_ARGUMENT
        );
        let mut value = 0i64;
        assert_eq!(
            daedalus_surface_take_i64_at(graph, total, &mut value),
            Status::AGAIN
        );

        // By index and by name feed the same port.
        assert_eq!(daedalus_surface_push_i64_at(graph, step, 5), Status::OK);
        tick(graph);
        assert_eq!(
            daedalus_surface_take_i64_at(graph, total, &mut value),
            Status::OK
        );
        assert_eq!(value, 5);
        let name = "step";
        assert_eq!(
            daedalus_surface_push_i64(graph, name.as_ptr(), name.len(), 2),
            Status::OK
        );
        tick(graph);
        assert_eq!(
            daedalus_surface_take_i64_at(graph, total, &mut value),
            Status::OK
        );
        assert_eq!(value, 7);

        // Bytes by index, with the needed-size protocol.
        let data = [1u8, 2, 3, 4];
        assert_eq!(
            daedalus_surface_push_bytes_at(graph, bytes, data.as_ptr(), data.len()),
            Status::OK
        );
        tick(graph);
        let (mut buf, mut needed) = ([0u8; 8], 0usize);
        assert_eq!(
            daedalus_surface_take_bytes_at(graph, tail, buf.as_mut_ptr(), 1, &mut needed),
            Status::BUFFER_TOO_SMALL
        );
        assert_eq!(needed, 3);
        assert_eq!(
            daedalus_surface_take_bytes_at(graph, tail, buf.as_mut_ptr(), buf.len(), &mut needed),
            Status::OK
        );
        assert_eq!(&buf[..needed], &[2, 3, 4]);
    }
    teardown(host, graph);
}

#[test]
fn a_batch_is_checked_whole_before_anything_is_pushed() {
    let (host, graph) = setup();
    let step = input(graph, "step").unwrap();
    let bytes = input(graph, "bytes").unwrap();
    let total = output(graph, "total");
    let data = [9u8, 8, 7];
    let entry = |port, kind| PushEntry {
        port,
        kind,
        int_value: 3,
        float_value: 0.0,
        data: data.as_ptr(),
        len: data.len(),
    };
    let mut done = usize::MAX;
    let mut value = 0i64;
    // Safety (whole test): the entries, buffers and out-pointers are valid for each call.
    unsafe {
        // The last entry names a port of another kind: nothing is pushed.
        let bad = [entry(step, PUSH_I64), entry(bytes, PUSH_I64)];
        assert_eq!(
            daedalus_surface_push_batch(graph, bad.as_ptr(), bad.len(), &mut done),
            Status::INVALID_ARGUMENT
        );
        assert_eq!(done, 0);
        let unknown_kind = [entry(step, 77)];
        assert_eq!(
            daedalus_surface_push_batch(graph, unknown_kind.as_ptr(), 1, &mut done),
            Status::INVALID_ARGUMENT
        );
        tick(graph);
        assert_eq!(
            daedalus_surface_take_i64_at(graph, total, &mut value),
            Status::AGAIN,
            "the refused batch pushed nothing"
        );

        let good = [entry(step, PUSH_I64), entry(bytes, PUSH_BYTES)];
        assert_eq!(
            daedalus_surface_push_batch(graph, good.as_ptr(), good.len(), &mut done),
            Status::OK
        );
        assert_eq!(done, 2);
        tick(graph);
        assert_eq!(
            daedalus_surface_take_i64_at(graph, total, &mut value),
            Status::OK
        );
        assert_eq!(value, 3);
        // An empty batch and a null `done` are fine.
        assert_eq!(
            daedalus_surface_push_batch(graph, ptr::null(), 0, ptr::null_mut()),
            Status::OK
        );
        assert_eq!(
            daedalus_surface_push_batch(graph, ptr::null(), 1, ptr::null_mut()),
            Status::INVALID_ARGUMENT
        );
    }
    teardown(host, graph);
}
