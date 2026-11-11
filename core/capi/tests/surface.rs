//! The surface called from Rust, as C would call it: the whole flow on a real plugin, and
//! every way a call is refused.

mod support;

use std::sync::{Mutex, MutexGuard, PoisonError};

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_capi::*;
use support::{fixture_library, graph_json};

#[global_allocator]
static COUNTING: CountingAlloc = CountingAlloc;

/// The tests count live handles, which are the process's: one at a time.
static TURN: Mutex<()> = Mutex::new(());

fn turn() -> MutexGuard<'static, ()> {
    TURN.lock().unwrap_or_else(PoisonError::into_inner)
}

const OK: Status = Status::OK;

fn host() -> Handle {
    let mut host = 0;
    // SAFETY: `host` is a handle to write.
    assert_eq!(unsafe { daedalus_host_create(&mut host) }, OK);
    let path = fixture_library();
    let path = path.to_str().expect("a UTF-8 path");
    // SAFETY: the path is `len` bytes of UTF-8.
    assert_eq!(
        unsafe { daedalus_host_install(host, path.as_ptr(), path.len()) },
        OK,
        "{}",
        last_error()
    );
    host
}

fn compile(host: Handle, json: &str) -> Result<Handle, Status> {
    let mut graph = 0;
    // SAFETY: the text is `len` bytes of UTF-8, and `graph` is a handle to write.
    match unsafe { daedalus_graph_compile(host, json.as_ptr(), json.len(), &mut graph) } {
        Status::OK => Ok(graph),
        status => Err(status),
    }
}

/// Text from a call that fills a buffer: asked for its size, then read.
fn text(call: impl Fn(*mut u8, usize, *mut usize) -> Status) -> String {
    let mut len = 0;
    let sized = call(std::ptr::null_mut(), 0, &mut len);
    assert!(sized == Status::BUFFER_TOO_SMALL || len == 0, "{sized:?}");
    let mut buf = vec![0; len];
    if len > 0 {
        assert_eq!(call(buf.as_mut_ptr(), buf.len(), &mut len), OK);
    }
    String::from_utf8(buf).expect("UTF-8")
}

fn last_error() -> String {
    // SAFETY: `text` gives a null buffer or one of the size it says.
    text(|buf, cap, len| unsafe { daedalus_last_error(buf, cap, len) })
}

fn push(graph: Handle, name: &str, value: i64) -> Status {
    // SAFETY: the name is `len` bytes of UTF-8.
    unsafe { daedalus_push_i64(graph, name.as_ptr(), name.len(), value) }
}

fn take(graph: Handle, name: &str) -> Result<Option<i64>, Status> {
    let (mut out, mut present) = (0, 0);
    // SAFETY: the name is `len` bytes of UTF-8, and the rest are values to write.
    match unsafe { daedalus_take_i64(graph, name.as_ptr(), name.len(), &mut out, &mut present) } {
        Status::OK => Ok((present == 1).then_some(out)),
        status => Err(status),
    }
}

fn tick(graph: Handle) -> TickInfo {
    let mut info = TickInfo::default();
    // SAFETY: `info` is a tick struct to write.
    assert_eq!(
        unsafe { daedalus_tick(graph, &mut info) },
        OK,
        "{}",
        last_error()
    );
    info
}

#[test]
fn the_version_is_the_abi_and_every_status_has_a_name() {
    let mut version = Version::default();
    // SAFETY: `version` is a version to write.
    assert_eq!(unsafe { daedalus_version(&mut version) }, OK);
    let abi = daedalus_abi::DAEDALUS_ABI;
    assert_eq!(
        (version.major, version.minor, version.layout_hash),
        (u32::from(abi.major), u32::from(abi.minor), abi.layout)
    );
    // SAFETY: a null pointer is refused, not written.
    assert_eq!(
        unsafe { daedalus_version(std::ptr::null_mut()) },
        Status::NULL_POINTER
    );
    for (status, name) in Status::ALL {
        // SAFETY: a status name is NUL-terminated and lives for the process.
        let got = unsafe { std::ffi::CStr::from_ptr(daedalus_status_name(*status)) };
        assert_eq!(got.to_str(), Ok(*name));
    }
}

#[test]
fn a_graph_over_a_plugin_is_compiled_pushed_ticked_and_taken() {
    let _turn = turn();
    let host = host();
    // SAFETY: `text` gives a null buffer or one of the size it says.
    let schemas = text(|buf, cap, len| unsafe { daedalus_host_schemas(host, buf, cap, len) });
    assert!(schemas.contains(r#""id":"fixture.pass""#), "{schemas}");
    assert!(schemas.contains(r#""type":"u32""#), "{schemas}");

    let graph = compile(host, &graph_json("fixture.pass")).expect("compiles");
    let mut count = 0;
    // SAFETY: `count` is a size to write.
    assert_eq!(unsafe { daedalus_graph_port_count(graph, &mut count) }, OK);
    assert_eq!(count, 2);
    let port = |index: usize| -> (String, String, u8) {
        let mut info = std::mem::MaybeUninit::<PortInfo>::uninit();
        // SAFETY: `info` is a port to write, and it is written before it is read.
        let info = unsafe {
            assert_eq!(daedalus_graph_port(graph, index, info.as_mut_ptr()), OK);
            info.assume_init()
        };
        // SAFETY: the strings are the graph's, which is live.
        let read = |data: *const u8, len: usize| unsafe {
            String::from_utf8_lossy(std::slice::from_raw_parts(data, len)).into_owned()
        };
        (
            read(info.name, info.name_len),
            read(info.type_key, info.type_key_len),
            info.is_input,
        )
    };
    assert_eq!(port(0), ("in".into(), "u32".into(), 1), "sorted by name");
    assert_eq!(port(1), ("out".into(), "u32".into(), 0));
    let mut index = 9;
    // SAFETY: the name is `len` bytes, and `index` is a size to write.
    assert_eq!(
        unsafe { daedalus_graph_port_index(graph, "out".as_ptr(), 3, &mut index) },
        OK
    );
    assert_eq!(index, 1);

    // SAFETY: `text` gives a null buffer or one of the size it says.
    let explain = text(|buf, cap, len| unsafe { daedalus_graph_explain(graph, buf, cap, len) });
    assert!(
        explain.contains("fixture.pass") && explain.contains("fixture.cpu"),
        "{explain}"
    );

    assert_eq!(take(graph, "out"), Ok(None), "nothing yet");
    assert_eq!(push(graph, "in", 41), OK);
    let ran = tick(graph);
    assert_eq!((ran.idle, ran.step_calls, ran.faults), (0, 1, 0));
    assert_eq!(take(graph, "out"), Ok(Some(41)));
    assert_eq!(take(graph, "out"), Ok(None), "taken once");
    assert_eq!(tick(graph).idle, 1, "nothing new");

    // By index, with the largest number the port holds.
    assert_eq!(daedalus_push_i64_at(graph, 0, i64::from(u32::MAX)), OK);
    tick(graph);
    let (mut out, mut present) = (0, 0);
    // SAFETY: both are values to write.
    assert_eq!(
        unsafe { daedalus_take_i64_at(graph, 1, &mut out, &mut present) },
        OK
    );
    assert_eq!((out, present), (i64::from(u32::MAX), 1));

    let mut telemetry = Telemetry::default();
    // SAFETY: `telemetry` is a struct to write.
    assert_eq!(
        unsafe { daedalus_graph_telemetry(graph, &mut telemetry) },
        OK
    );
    assert_eq!(
        telemetry,
        Telemetry {
            ticks: 3,
            idle_ticks: 1,
            step_calls: 2,
            faults: 0,
            pushes: 2,
            takes: 2,
        }
    );

    // The host is busy while its graph lives, and both go in order.
    assert_eq!(daedalus_host_release(host), Status::BUSY);
    assert!(last_error().contains("graphs"));
    assert_eq!(daedalus_graph_release(graph), OK);
    assert_eq!(daedalus_host_release(host), OK);
    assert_eq!(live_handles(), (0, 0));
}

#[test]
fn what_a_port_wont_take_is_refused_with_its_reason() {
    let _turn = turn();
    let host = host();
    let graph = compile(host, &graph_json("fixture.pass")).expect("compiles");
    assert_eq!(push(graph, "in", -1), Status::OUT_OF_RANGE, "a u32 port");
    assert_eq!(push(graph, "in", 1 << 32), Status::OUT_OF_RANGE);
    assert_eq!(push(graph, "nowhere", 1), Status::UNKNOWN_PORT);
    assert_eq!(push(graph, "out", 1), Status::WRONG_DIRECTION);
    assert_eq!(take(graph, "in"), Err(Status::WRONG_DIRECTION));
    assert_eq!(daedalus_push_i64_at(graph, 7, 1), Status::UNKNOWN_PORT);
    // SAFETY: the name is `len` bytes.
    assert_eq!(
        unsafe { daedalus_push_f64(graph, "in".as_ptr(), 2, 1.0) },
        Status::TYPE_MISMATCH
    );
    // SAFETY: the name is `len` bytes.
    assert_eq!(
        unsafe { daedalus_push_bool(graph, "in".as_ptr(), 2, 1) },
        Status::TYPE_MISMATCH
    );
    // SAFETY: the name and the bytes are as long as said.
    assert_eq!(
        unsafe { daedalus_push_bytes(graph, "in".as_ptr(), 2, [1u8].as_ptr(), 1) },
        Status::TYPE_MISMATCH
    );
    let (mut float, mut present) = (0.0, 0);
    // SAFETY: the name is `len` bytes, and the rest are values to write.
    assert_eq!(
        unsafe { daedalus_take_f64(graph, "out".as_ptr(), 3, &mut float, &mut present) },
        Status::TYPE_MISMATCH
    );
    // Null pointers, and text that isn't text.
    // SAFETY: a null name is refused before it is read.
    assert_eq!(
        unsafe { daedalus_push_i64(graph, std::ptr::null(), 2, 1) },
        Status::NULL_POINTER
    );
    // SAFETY: the bytes are `len` long.
    assert_eq!(
        unsafe { daedalus_push_i64(graph, [0xff, 0xfe].as_ptr(), 2, 1) },
        Status::BAD_TEXT
    );
    // SAFETY: null outputs are refused before anything is written.
    assert_eq!(
        unsafe {
            daedalus_take_i64(
                graph,
                "out".as_ptr(),
                3,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        },
        Status::NULL_POINTER
    );
    // Nothing above reached the graph.
    assert_eq!(tick(graph).idle, 1);

    // A graph that can't be read, and one that can't be planned, each say why.
    assert_eq!(compile(host, "not json"), Err(Status::GRAPH_INVALID));
    assert!(!last_error().is_empty());
    assert_eq!(
        compile(host, &graph_json("no.such.node")),
        Err(Status::PLAN_FAILED)
    );
    assert!(last_error().contains("no.such.node"), "{}", last_error());
    // A plugin that isn't there.
    // SAFETY: the path is `len` bytes.
    assert_eq!(
        unsafe { daedalus_host_install(host, "/nowhere/lib.so".as_ptr(), 15) },
        Status::PLUGIN_FAILED
    );
    assert!(last_error().contains("/nowhere/lib.so"), "{}", last_error());

    assert_eq!(daedalus_graph_release(graph), OK);
    assert_eq!(daedalus_host_release(host), OK);
    assert_eq!(live_handles(), (0, 0), "the failed compiles left nothing");
}

#[test]
fn stale_and_null_handles_are_statuses() {
    let _turn = turn();
    let host = host();
    let graph = compile(host, &graph_json("fixture.pass")).expect("compiles");
    assert_eq!(daedalus_graph_release(graph), OK);
    for (handle, want) in [
        (graph, Status::STALE_HANDLE),
        (0, Status::BAD_HANDLE),
        (host, Status::BAD_HANDLE),
        (0xdead_beef, Status::BAD_HANDLE),
    ] {
        assert_eq!(push(handle, "in", 1), want, "{handle:#x}");
        assert_eq!(take(handle, "out"), Err(want));
        assert_eq!(daedalus_graph_release(handle), want);
        // SAFETY: a null tick is allowed, and a bad handle is refused first.
        assert_eq!(unsafe { daedalus_tick(handle, std::ptr::null_mut()) }, want);
        let mut count = 0;
        // SAFETY: `count` is a size to write.
        assert_eq!(
            unsafe { daedalus_graph_port_count(handle, &mut count) },
            want
        );
    }
    assert_eq!(
        daedalus_host_release(graph),
        Status::BAD_HANDLE,
        "a graph is no host"
    );
    assert_eq!(daedalus_host_release(host), OK);
    assert_eq!(daedalus_host_release(host), Status::STALE_HANDLE);
    assert_eq!(
        compile(host, &graph_json("fixture.pass")),
        Err(Status::STALE_HANDLE)
    );
    // A new host takes the place, and the old number still isn't it.
    let again = self::host();
    assert_ne!(again, host);
    assert_eq!(daedalus_host_release(host), Status::STALE_HANDLE);
    assert_eq!(daedalus_host_release(again), OK);
}

#[test]
fn a_batch_with_one_bad_entry_pushes_nothing() {
    let _turn = turn();
    let host = host();
    let graph = compile(host, &graph_json("fixture.pass")).expect("compiles");
    let entry = |port: usize, kind: u32, value: i64| Push {
        port,
        kind,
        value: PushValue { i64_value: value },
    };
    // Good, good, and one that doesn't fit the port.
    let batch = [
        entry(0, KIND_I64, 1),
        entry(0, KIND_I64, 2),
        entry(0, KIND_I64, -5),
    ];
    let mut bad = usize::MAX;
    // SAFETY: three entries, each with the member its kind names, and `bad` is a size.
    assert_eq!(
        unsafe { daedalus_push_batch(graph, batch.as_ptr(), 3, &mut bad) },
        Status::BAD_BATCH
    );
    assert_eq!(bad, 2);
    assert!(
        last_error().contains("entry 2") && last_error().contains("OUT_OF_RANGE"),
        "{}",
        last_error()
    );
    assert_eq!(tick(graph).idle, 1, "nothing was pushed");
    // Each other way an entry is bad: no such port, an output, another kind, no kind at all.
    for wrong in [
        entry(9, KIND_I64, 1),
        entry(1, KIND_I64, 1),
        entry(0, KIND_F64, 1),
        entry(0, 77, 1),
    ] {
        let batch = [entry(0, KIND_I64, 1), wrong];
        // SAFETY: two entries, and a null `bad` is allowed.
        assert_eq!(
            unsafe { daedalus_push_batch(graph, batch.as_ptr(), 2, std::ptr::null_mut()) },
            Status::BAD_BATCH
        );
    }
    assert_eq!(tick(graph).idle, 1);
    // A good batch goes in whole. The port holds one value at a time, so one arrives.
    let batch = [entry(0, KIND_I64, 8)];
    // SAFETY: one entry.
    assert_eq!(
        unsafe { daedalus_push_batch(graph, batch.as_ptr(), 1, std::ptr::null_mut()) },
        OK
    );
    tick(graph);
    assert_eq!(take(graph, "out"), Ok(Some(8)));
    // SAFETY: no entries are read.
    assert_eq!(
        unsafe { daedalus_push_batch(graph, std::ptr::null(), 0, std::ptr::null_mut()) },
        OK
    );
    // SAFETY: a null batch with a count is refused before it is read.
    assert_eq!(
        unsafe { daedalus_push_batch(graph, std::ptr::null(), 2, std::ptr::null_mut()) },
        Status::NULL_POINTER
    );
    assert_eq!(daedalus_graph_release(graph), OK);
    assert_eq!(daedalus_host_release(host), OK);
}

#[test]
fn a_panic_is_a_status_and_a_node_that_panics_is_a_fault() {
    let _turn = turn();
    assert_eq!(daedalus_selftest_panic(), Status::PANIC);
    assert!(last_error().contains("panicked"), "{}", last_error());
    // A node that panics doesn't take the caller down: the tick reports a fault, there is no
    // output, and the graph and the host are still good.
    let host = host();
    let graph = compile(host, &graph_json("fixture.boom")).expect("compiles");
    assert_eq!(push(graph, "in", 1), OK);
    let ran = tick(graph);
    assert_eq!(ran.faults, 1);
    assert_eq!(take(graph, "out"), Ok(None));
    assert_eq!(push(graph, "in", 2), OK);
    assert_eq!(tick(graph).faults, 1);
    assert_eq!(daedalus_graph_release(graph), OK);
    assert_eq!(daedalus_host_release(host), OK);
}

#[test]
fn a_thousand_calls_leave_nothing_behind() {
    let _turn = turn();
    let host = host();
    let graph = compile(host, &graph_json("fixture.pass")).expect("compiles");
    for value in 0..16 {
        push(graph, "in", value);
        tick(graph);
        take(graph, "out").expect("a take");
    }
    // Warm, a push, a tick and a take of a number allocate nothing at all.
    let ((), allocations) = counted(|| {
        for value in 0..1000 {
            assert_eq!(push(graph, "in", value), OK);
            // SAFETY: a null tick is allowed.
            assert_eq!(unsafe { daedalus_tick(graph, std::ptr::null_mut()) }, OK);
            assert_eq!(take(graph, "out"), Ok(Some(value)));
        }
    });
    assert_eq!(allocations, 0);
    // Refused calls and panics don't either, beyond the reason they keep.
    for _ in 0..1000 {
        assert_eq!(push(graph, "out", 1), Status::WRONG_DIRECTION);
        assert_eq!(push(0, "in", 1), Status::BAD_HANDLE);
    }
    assert_eq!(daedalus_graph_release(graph), OK);
    // A thousand graphs made and released leave no handle live, and the places are reused.
    let mut highest = 0;
    for _ in 0..1000 {
        let graph = compile(host, &graph_json("fixture.pass")).expect("compiles");
        highest = highest.max(graph & 0xffff_ffff);
        assert_eq!(daedalus_graph_release(graph), OK);
    }
    assert_eq!(highest, 0, "one place, a thousand generations");
    assert_eq!(daedalus_host_release(host), OK);
    assert_eq!(live_handles(), (0, 0));
}

#[test]
fn a_value_whose_fence_is_pending_is_not_taken_until_the_fence_resolves() {
    let _turn = turn();
    let host = host();
    let graph = compile(host, &graph_json("fixture.gated")).expect("compiles");
    assert_eq!(push(graph, "in", 7), OK);

    // The node does its work on the first tick and returns a pending fence, so nothing is taken
    // while the fence waits. Each later tick polls it once, and it resolves on the fourth tick.
    tick(graph);
    assert_eq!(take(graph, "out"), Ok(None), "the fence is pending");
    tick(graph);
    assert_eq!(take(graph, "out"), Ok(None), "the fence is still pending");
    tick(graph);
    assert_eq!(take(graph, "out"), Ok(None), "the fence is still pending");
    tick(graph);
    assert_eq!(take(graph, "out"), Ok(Some(7)), "the fence resolved");
    assert_eq!(take(graph, "out"), Ok(None), "the value is taken once");

    assert_eq!(daedalus_graph_release(graph), OK);
    assert_eq!(daedalus_host_release(host), OK);
}
