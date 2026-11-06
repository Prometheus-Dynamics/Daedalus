//! Contract tests on the raw entry points: stale and null handles, the needed-size protocol,
//! refusals that must not reach the engine, and the empty graph's JSON.

use std::ptr;

use lemnos_abi::{AbiVersion, Handle, LAYOUT_HASH, MessageSink, Status};

use crate::*;

const EMPTY_GRAPH: &str = r#"{"format":"daedalus.graph","schema_version":1,"requires":[],
    "graph":{"nodes":[],"edges":[],"metadata":{}}}"#;

fn host() -> Handle {
    let mut host = Handle::NULL;
    // Safety: `host` is a valid out-pointer.
    assert_eq!(
        unsafe { host::daedalus_surface_host_create(&mut host) },
        Status::OK
    );
    host
}

fn compile(host: Handle, document: &str) -> Result<Handle, Status> {
    let mut graph = Handle::NULL;
    // Safety: the document and the out-pointer are valid for the call.
    let status = unsafe {
        graph::daedalus_surface_graph_compile(
            host,
            document.as_ptr(),
            document.len(),
            &mut graph,
            MessageSink::NONE,
        )
    };
    if status == Status::OK {
        Ok(graph)
    } else {
        Err(status)
    }
}

/// Reads a JSON output through the needed-size protocol, as the wrapper does.
fn json(f: impl Fn(*mut u8, usize, *mut usize) -> Status) -> String {
    let mut buf = Vec::new();
    let mut needed = 0;
    let status = f(buf.as_mut_ptr(), buf.len(), &mut needed);
    assert_eq!(
        status,
        Status::BUFFER_TOO_SMALL,
        "a zero-capacity buffer is too small"
    );
    buf.resize(needed, 0);
    let status = f(buf.as_mut_ptr(), buf.len(), &mut needed);
    assert_eq!(status, Status::OK);
    String::from_utf8(buf).unwrap()
}

#[test]
fn version_and_features_are_written() {
    let mut version = AbiVersion::new(0, 0, 0);
    // Safety: a valid out-pointer.
    assert_eq!(
        unsafe { abi::daedalus_surface_version(&mut version) },
        Status::OK
    );
    assert_eq!(version.layout, LAYOUT_HASH);
    assert_eq!(version.major, SURFACE_MAJOR);
    assert_eq!(version.minor, SURFACE_MINOR);
    assert_eq!(version.accepts(&version), Status::OK);
    let mut bits = 0;
    // Safety: a valid out-pointer.
    assert_eq!(
        unsafe { abi::daedalus_surface_features(&mut bits) },
        Status::OK
    );
    assert_eq!(bits, feature_set());
    // Safety: null out-pointers are refused, not written.
    assert_eq!(
        unsafe { abi::daedalus_surface_version(ptr::null_mut()) },
        Status::INVALID_ARGUMENT
    );
}

#[test]
fn stale_and_null_handles_are_refused() {
    let host = host();
    // Safety: the handle is valid until released below.
    assert_eq!(
        unsafe { host::daedalus_surface_host_release(host) },
        Status::OK
    );
    // Released: every use is refused, and a second release too.
    assert_eq!(
        unsafe { host::daedalus_surface_host_release(host) },
        Status::STALE_HANDLE
    );
    assert_eq!(compile(host, EMPTY_GRAPH), Err(Status::STALE_HANDLE));
    assert_eq!(
        unsafe { host::daedalus_surface_host_release(Handle::NULL) },
        Status::STALE_HANDLE
    );
    let mut buf = [0u8; 1];
    let mut needed = 0;
    // Safety: the buffer and out-pointer are valid; the handle is stale.
    assert_eq!(
        unsafe { host::daedalus_surface_host_schemas(host, buf.as_mut_ptr(), 1, &mut needed) },
        Status::STALE_HANDLE
    );
    // A graph released with its host alive; the same stale-handle rule applies to the graph.
    let host = self::host();
    let graph = compile(host, EMPTY_GRAPH).unwrap();
    // Safety: the handle is valid until released.
    assert_eq!(
        unsafe { graph::daedalus_surface_graph_release(graph) },
        Status::OK
    );
    assert_eq!(
        unsafe { io::daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::STALE_HANDLE
    );
    assert_eq!(
        unsafe { host::daedalus_surface_host_release(host) },
        Status::OK
    );
}

#[test]
fn a_host_with_live_graphs_is_busy_until_they_are_released() {
    let host = host();
    let graph = compile(host, EMPTY_GRAPH).unwrap();
    // Safety: the handle is valid.
    assert_eq!(
        unsafe { host::daedalus_surface_host_release(host) },
        Status::BUSY
    );
    assert_eq!(
        unsafe { graph::daedalus_surface_graph_release(graph) },
        Status::OK
    );
    assert_eq!(
        unsafe { host::daedalus_surface_host_release(host) },
        Status::OK
    );
}

#[test]
fn an_empty_graph_runs_and_reports_json() {
    let host = host();
    let graph = compile(host, EMPTY_GRAPH).unwrap();
    assert_eq!(
        json(|b, c, n| unsafe { graph::daedalus_surface_graph_ports(graph, b, c, n) }),
        r#"{"inputs":[],"outputs":[]}"#
    );
    assert_eq!(
        json(|b, c, n| unsafe { graph::daedalus_surface_graph_telemetry(graph, b, c, n) }),
        "null"
    );
    // Safety: the handle is valid; the sink drops messages.
    assert_eq!(
        unsafe { io::daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::OK
    );
    let telemetry =
        json(|b, c, n| unsafe { graph::daedalus_surface_graph_telemetry(graph, b, c, n) });
    assert!(
        telemetry.starts_with('{'),
        "telemetry after a tick: {telemetry}"
    );
    assert!(
        json(|b, c, n| unsafe { graph::daedalus_surface_graph_explain(graph, b, c, n) })
            .starts_with('{')
    );
    assert_eq!(
        json(|b, c, n| unsafe { host::daedalus_surface_host_schemas(host, b, c, n) }),
        "[]"
    );
    let port = "nope";
    let mut value = 0i64;
    // Safety: valid port bytes and out-pointer.
    assert_eq!(
        unsafe { io::daedalus_surface_take_i64(graph, port.as_ptr(), port.len(), &mut value) },
        Status::NOT_FOUND
    );
    assert_eq!(
        unsafe { io::daedalus_surface_push_i64(graph, port.as_ptr(), port.len(), 1) },
        Status::NOT_FOUND
    );
    // Null pointers are refused without being read.
    assert_eq!(
        unsafe { io::daedalus_surface_take_i64(graph, port.as_ptr(), port.len(), ptr::null_mut()) },
        Status::INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { io::daedalus_surface_push_i64(graph, ptr::null(), 3, 1) },
        Status::INVALID_ARGUMENT
    );
    // Invalid UTF-8 names are refused.
    let bad = [0xffu8];
    assert_eq!(
        unsafe { io::daedalus_surface_push_i64(graph, bad.as_ptr(), 1, 1) },
        Status::INVALID_ARGUMENT
    );
    // A frame needs a descriptor: a null one is refused, and so is a scalar port. Off Linux the
    // frame calls are not supported at all.
    let expected = if cfg!(target_os = "linux") {
        Status::INVALID_ARGUMENT
    } else {
        Status::NOT_SUPPORTED
    };
    assert_eq!(
        unsafe {
            frames::daedalus_surface_push_frame(
                graph,
                port.as_ptr(),
                port.len(),
                ptr::null(),
                ptr::null(),
            )
        },
        expected
    );
    assert_eq!(
        unsafe { graph::daedalus_surface_graph_release(graph) },
        Status::OK
    );
    assert_eq!(
        unsafe { host::daedalus_surface_host_release(host) },
        Status::OK
    );
}

#[test]
fn a_bad_document_and_a_missing_plugin_are_refused_with_a_message() {
    let text = std::sync::Mutex::new(String::new());
    extern "C" fn collect(context: *mut std::ffi::c_void, bytes: *const u8, len: usize) {
        // Safety: the test passes a live `Mutex<String>`, and `bytes` is valid for `len`.
        unsafe {
            let text = &*context.cast::<std::sync::Mutex<String>>();
            let piece = std::slice::from_raw_parts(bytes, len);
            text.lock()
                .unwrap()
                .push_str(&String::from_utf8_lossy(piece));
        }
    }
    let sink = MessageSink {
        context: (&text as *const std::sync::Mutex<String>).cast_mut().cast(),
        write: Some(collect),
    };
    let host = host();
    let mut graph = Handle::NULL;
    let bad = "{not json";
    // Safety: the document is valid for its length.
    let status = unsafe {
        graph::daedalus_surface_graph_compile(host, bad.as_ptr(), bad.len(), &mut graph, sink)
    };
    assert_eq!(status, Status::INVALID_ARGUMENT);
    assert!(
        !text.lock().unwrap().is_empty(),
        "the reason goes to the sink"
    );
    let path = "/nonexistent/libnope.so";
    // Safety: the path is valid for its length.
    let status =
        unsafe { host::daedalus_surface_host_install(host, path.as_ptr(), path.len(), sink) };
    assert_eq!(status, Status::NOT_FOUND);
    assert!(text.lock().unwrap().contains("no plugin library"));
    assert_eq!(
        unsafe { host::daedalus_surface_host_release(host) },
        Status::OK
    );
}
