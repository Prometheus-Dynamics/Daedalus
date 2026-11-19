//! The surface called from Rust, as C calls it: every function on a daemon in this process with
//! the fixture plugin loaded, and the way each call answers.

use crate::support;

use std::sync::{Mutex, MutexGuard, PoisonError};

use daedalus_client_capi::*;
use support::{
    config, connect, daemon, encode, feed_u32, graph_json, graph_json_as, json, last_error,
    last_error_code, load_fixture, put_graph, start, step, take_u32, u32_value,
};

/// The tests count live handles, which are the process's: one test at a time.
static TURN: Mutex<()> = Mutex::new(());

fn turn() -> MutexGuard<'static, ()> {
    TURN.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Waits for the next event of a watch, up to a second.
fn event(watch: Handle, timeout_ms: u32) -> (u32, Option<serde_json::Value>) {
    let mut len = 0;
    let mut state = u32::MAX;
    let call = |buf: *mut u8, cap: usize, len: &mut usize, state: &mut u32| {
        // SAFETY: the buffer is null or `cap` bytes, and `len` and `state` are written.
        unsafe { daedalus_client_watch_next(watch, timeout_ms, buf, cap, len, state) }
    };
    let status = call(std::ptr::null_mut(), 0, &mut len, &mut state);
    if state != WATCH_EVENT {
        assert_eq!(status, Status::OK, "{}", last_error());
        return (state, None);
    }
    let mut buf = vec![0; len];
    assert_eq!(
        call(buf.as_mut_ptr(), buf.len(), &mut len, &mut state),
        Status::OK
    );
    let value = serde_json::from_slice(&buf).expect("JSON");
    (state, Some(value))
}

#[test]
fn the_version_is_the_protocol_and_its_layout_and_each_status_has_a_name() {
    let mut version = Version::default();
    // SAFETY: `version` is a version to write.
    assert_eq!(unsafe { daedalus_client_version(&mut version) }, Status::OK);
    assert_eq!(version.protocol, daedalus_wire::service::SERVICE_VERSION);
    assert_eq!(
        version.layout_hash,
        daedalus_wire::service::SERVICE_LAYOUT_HASH
    );
    for (status, name) in Status::ALL {
        assert_eq!(status.name(), format!("{name}\0"));
    }
    // SAFETY: a status is a number, and the name is never freed.
    let name =
        unsafe { std::ffi::CStr::from_ptr(daedalus_client_status_name(Status::BAD_ARGUMENT)) };
    assert_eq!(name.to_str().unwrap(), "DAEDALUS_CLIENT_BAD_ARGUMENT");
}

#[test]
fn a_run_is_stored_started_fed_stepped_taken_and_stopped() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    assert_eq!(
        put_graph(client, "g", &graph_json("fixture.pass")),
        Status::OK,
        "{}",
        last_error()
    );
    let run = start(client, "g");
    assert_eq!(
        feed_u32(client, run, "in", 41),
        Status::OK,
        "{}",
        last_error()
    );
    let summary = step(client, run, 1);
    assert_eq!((summary.ticks, summary.tick), (1, 1));
    assert_eq!(take_u32(client, run, "out"), Some(41));
    assert_eq!(take_u32(client, run, "out"), None, "the port is empty now");

    // The feed's answer, when asked for.
    let mut fed = Fed::default();
    let bytes = encode(u32_value(42));
    // SAFETY: the port and the bytes are `len` bytes each, and `fed` is written.
    let status = unsafe {
        daedalus_client_feed_wire(
            client,
            run,
            b"in".as_ptr(),
            2,
            bytes.as_ptr(),
            bytes.len(),
            &mut fed,
        )
    };
    assert_eq!(status, Status::OK, "{}", last_error());
    assert_eq!((fed.woke, fed.dropped), (1, 0));

    let mut stopped = Stopped::default();
    // SAFETY: `stopped` is a value to write, and the run is this client's.
    let status = unsafe { daedalus_client_stop(client, run, STOP_DRAIN, &mut stopped) };
    assert_eq!(status, Status::OK, "{}", last_error());
    assert!(
        stopped.tick >= summary.tick,
        "the stop is at or after the step"
    );
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn a_taken_value_waits_for_a_buffer_that_fits_and_for_its_own_type() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    put_graph(client, "g", &graph_json("fixture.pass"));
    let run = start(client, "g");
    feed_u32(client, run, "in", 7);
    step(client, run, 1);

    // A typed take of the wrong type refuses, and keeps the value.
    let mut value = 0i64;
    let mut present = 0u8;
    // SAFETY: `value` and `present` are written, and the port is `len` bytes.
    let status = unsafe {
        daedalus_client_take_i64(client, run, b"out".as_ptr(), 3, &mut value, &mut present)
    };
    assert_eq!(status, Status::TYPE_MISMATCH, "{}", last_error());
    assert_eq!(present, 1, "the value exists, and it is kept");

    // The encoding of it is 2 bytes of type and 4 of value, and it comes out the same twice.
    assert_eq!(take_u32(client, run, "out"), Some(7));
    assert_eq!(take_u32(client, run, "out"), None);

    // Bytes: a buffer too small keeps the value, and the size is written.
    put_graph(client, "b", &graph_json_as("fixture.pass_bytes", "bytes"));
    let run = start(client, "b");
    let payload = b"hello";
    // SAFETY: the port and the payload are `len` bytes each, and the output is null.
    let status = unsafe {
        daedalus_client_feed_bytes(
            client,
            run,
            b"in".as_ptr(),
            2,
            payload.as_ptr(),
            payload.len(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(status, Status::OK, "{}", last_error());
    step(client, run, 1);
    let mut len = 0;
    let mut present = 0u8;
    // SAFETY: the buffer is null with zero, and `len` and `present` are written.
    let status = unsafe {
        daedalus_client_take_bytes(
            client,
            run,
            b"out".as_ptr(),
            3,
            std::ptr::null_mut(),
            0,
            &mut len,
            &mut present,
        )
    };
    assert_eq!(status, Status::BUFFER_TOO_SMALL);
    assert_eq!((len, present), (5, 1));
    let mut buf = [0u8; 5];
    // SAFETY: the buffer holds `len` bytes, and `len` and `present` are written.
    let status = unsafe {
        daedalus_client_take_bytes(
            client,
            run,
            b"out".as_ptr(),
            3,
            buf.as_mut_ptr(),
            buf.len(),
            &mut len,
            &mut present,
        )
    };
    assert_eq!(status, Status::OK);
    assert_eq!(&buf, b"hello");
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn a_port_that_takes_another_type_refuses_the_feed_with_the_servers_code() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    put_graph(client, "g", &graph_json("fixture.pass"));
    let run = start(client, "g");
    // SAFETY: the port is `len` bytes, and the output is null.
    let status = unsafe {
        daedalus_client_feed_i64(client, run, b"in".as_ptr(), 2, 5, std::ptr::null_mut())
    };
    assert_eq!(status, Status::SERVICE_ERROR, "{}", last_error());
    assert_eq!(last_error_code(), 5, "a value of the wrong type");
    assert!(last_error().starts_with("value_type"), "{}", last_error());
    // A success leaves the error as it was.
    assert_eq!(feed_u32(client, run, "in", 1), Status::OK);
    assert_eq!(last_error_code(), 5);
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn plans_and_made_plans_are_read_as_json_and_bytes_and_a_made_plan_starts_by_name() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    put_graph(client, "g", &graph_json("fixture.pass"));

    let options = PlanOptions {
        form: FORM_STEPS,
        inspect: 1,
        priority: std::ptr::null(),
        priority_len: 0,
    };
    let plan = json(|buf, cap, len| {
        // SAFETY: the name is `len` bytes, the options live for the call, and the buffer and
        // length are as the caller gave them.
        unsafe { daedalus_client_plan(client, b"g".as_ptr(), 1, &options, buf, cap, len) }
    });
    assert_eq!(plan["graph"], "g");
    assert_eq!(plan["form"], "steps");
    assert!(plan["diagnostics"].is_array());

    let mut hash = 0u64;
    let mut len = 0usize;
    let cfg = config();
    // SAFETY: the name is `len` bytes, the config lives for the call, `hash` and `len` are
    // written, and the buffer is null.
    let status = unsafe {
        daedalus_client_make_plan(
            client,
            b"g".as_ptr(),
            1,
            &cfg,
            &mut hash,
            std::ptr::null_mut(),
            0,
            &mut len,
        )
    };
    assert_eq!(status, Status::BUFFER_TOO_SMALL, "{}", last_error());
    let mut bytes = vec![0; len];
    // SAFETY: the buffer holds `len` bytes, and `hash` and `len` are written.
    let status = unsafe {
        daedalus_client_make_plan(
            client,
            b"g".as_ptr(),
            1,
            &cfg,
            &mut hash,
            bytes.as_mut_ptr(),
            bytes.len(),
            &mut len,
        )
    };
    assert_eq!(status, Status::OK, "{}", last_error());
    assert_ne!(hash, 0);
    assert!(!bytes.is_empty());

    // The made plan is stored under a name, and a run starts on that name.
    // SAFETY: the name and the bytes are `len` bytes each.
    let status = unsafe {
        daedalus_client_put_plan(client, b"made".as_ptr(), 4, bytes.as_ptr(), bytes.len())
    };
    assert_eq!(status, Status::OK, "{}", last_error());
    let run = start(client, "made");
    feed_u32(client, run, "in", 3);
    step(client, run, 1);
    assert_eq!(take_u32(client, run, "out"), Some(3));

    // A form that names no option is refused before the daemon is asked.
    let bad = PlanOptions { form: 9, ..options };
    let mut len = 0;
    // SAFETY: the options live for the call, and `len` is written.
    let status = unsafe {
        daedalus_client_plan(
            client,
            b"g".as_ptr(),
            1,
            &bad,
            std::ptr::null_mut(),
            0,
            &mut len,
        )
    };
    assert_eq!(status, Status::BAD_ARGUMENT, "{}", last_error());
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn inspect_gives_each_kind_as_json_and_trace_gives_the_served_requests() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    put_graph(client, "g", &graph_json("fixture.pass"));
    let run = start(client, "g");
    feed_u32(client, run, "in", 9);
    step(client, run, 1);

    let info = json(|buf, cap, len| {
        // SAFETY: the buffer is null or `cap` bytes, and `len` is written.
        unsafe { daedalus_client_inspect_run(client, run, buf, cap, len) }
    });
    assert_eq!(info["graph"], "g");
    assert_eq!(info["state"], "running");
    assert_eq!(info["tick"], 1);
    assert_eq!(info["ports"][0]["type"], "u32");

    let graph = json(|buf, cap, len| {
        // SAFETY: the name is `len` bytes, and the buffer and length are the caller's.
        unsafe { daedalus_client_inspect_graph(client, b"g".as_ptr(), 1, buf, cap, len) }
    });
    assert_eq!(graph["name"], "g");
    assert_eq!(graph["graph"]["format"], "daedalus.graph");

    let cache = json(|buf, cap, len| {
        // SAFETY: the buffer and length are the caller's.
        unsafe { daedalus_client_inspect_cache(client, buf, cap, len) }
    });
    assert!(cache["entries"].is_number());

    let telemetry = json(|buf, cap, len| {
        // SAFETY: the buffer and length are the caller's.
        unsafe { daedalus_client_inspect_telemetry(client, run, buf, cap, len) }
    });
    assert_eq!(telemetry["run"], run);

    let host = json(|buf, cap, len| {
        // SAFETY: the buffer and length are the caller's.
        unsafe { daedalus_client_inspect_host(client, buf, cap, len) }
    });
    assert_eq!(host["graphs"][0], "g");
    assert!(
        host["plugins"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p == "dylib-fixture")
    );

    let trace = json(|buf, cap, len| {
        // SAFETY: the buffer and length are the caller's.
        unsafe { daedalus_client_trace(client, 0, buf, cap, len) }
    });
    assert!(!trace["entries"].as_array().unwrap().is_empty());
    assert_eq!(daedalus_client_cache_clear(client), Status::OK);
    let mut version = 0;
    // SAFETY: `version` is a `uint32_t` to write.
    assert_eq!(
        unsafe { daedalus_client_ping(client, &mut version) },
        Status::OK
    );
    assert_eq!(version, daedalus_wire::service::SERVICE_VERSION);
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn a_watch_pulls_the_fault_of_a_step_then_idles_and_reports_the_end() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    put_graph(client, "boom", &graph_json("fixture.boom"));
    let run = start(client, "boom");

    let mut watch: Handle = 0;
    // SAFETY: `watch` is a handle to write.
    let status = unsafe { daedalus_client_watch_open(client, run, EVENT_FAULT, &mut watch) };
    assert_eq!(status, Status::OK, "{}", last_error());

    let (state, none) = event(watch, 20);
    assert_eq!(
        (state, none),
        (WATCH_IDLE, None),
        "nothing has happened yet"
    );

    feed_u32(client, run, "in", 4);
    let summary = step(client, run, 1);
    assert_eq!(summary.faults, 1);
    let (state, fault) = event(watch, 5000);
    assert_eq!(state, WATCH_EVENT);
    let fault = fault.expect("an event");
    assert_eq!(fault["event"], "fault");
    assert_eq!(fault["run"], run);
    assert!(
        fault["node"].is_string(),
        "the node that failed is named: {fault}"
    );
    assert!(
        fault["reason"].as_str().unwrap().contains("boom"),
        "{fault}"
    );

    // Stopping the run ends it: the watch reports the end, and then has nothing more.
    let mut stopped = Stopped::default();
    // SAFETY: `stopped` is a value to write.
    unsafe { daedalus_client_stop(client, run, STOP_IMMEDIATE, &mut stopped) };
    let (state, ended) = event(watch, 5000);
    assert_eq!(state, WATCH_EVENT);
    assert_eq!(ended.expect("an event")["event"], "ended");
    assert_eq!(daedalus_client_watch_close(watch), Status::OK);
    assert_eq!(daedalus_client_watch_close(watch), Status::STALE_HANDLE);
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn a_watch_event_too_big_for_its_buffer_waits_for_the_next_pull() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    put_graph(client, "boom", &graph_json("fixture.boom"));
    let run = start(client, "boom");
    let mut watch: Handle = 0;
    // SAFETY: `watch` is a handle to write.
    unsafe { daedalus_client_watch_open(client, run, EVENT_FAULT, &mut watch) };
    feed_u32(client, run, "in", 1);
    step(client, run, 1);

    let mut len = 0;
    let mut state = 0;
    // SAFETY: the buffer is null with zero, and `len` and `state` are written.
    let status = unsafe {
        daedalus_client_watch_next(watch, 5000, std::ptr::null_mut(), 0, &mut len, &mut state)
    };
    assert_eq!(status, Status::BUFFER_TOO_SMALL);
    assert_eq!(state, WATCH_EVENT);
    let mut buf = vec![0; len];
    // SAFETY: the buffer holds `len` bytes, and `len` and `state` are written.
    let status = unsafe {
        daedalus_client_watch_next(watch, 0, buf.as_mut_ptr(), buf.len(), &mut len, &mut state)
    };
    assert_eq!(status, Status::OK, "{}", last_error());
    let fault: serde_json::Value = serde_json::from_slice(&buf).expect("JSON");
    assert_eq!(fault["event"], "fault");
    assert_eq!(daedalus_client_watch_close(watch), Status::OK);
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn cancel_and_reload_and_wait_on_a_run() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    put_graph(client, "g", &graph_json("fixture.pass"));
    put_graph(client, "h", &graph_json("fixture.pass"));
    let run = start(client, "g");

    let mut carry = Carry::default();
    // SAFETY: the name is `len` bytes, and `carry` is written.
    let status = unsafe { daedalus_client_reload(client, run, b"h".as_ptr(), 1, &mut carry) };
    assert_eq!(status, Status::OK, "{}", last_error());
    assert_eq!(
        carry.carried, 2,
        "both nodes keep their state: the two graphs are the same"
    );

    // A cancelled run takes no reload, and says why.
    assert_eq!(
        daedalus_client_cancel(client, run),
        Status::OK,
        "{}",
        last_error()
    );
    // SAFETY: the name is `len` bytes, and the output is null.
    let status =
        unsafe { daedalus_client_reload(client, run, b"h".as_ptr(), 1, std::ptr::null_mut()) };
    assert_eq!(status, Status::SERVICE_ERROR, "{}", last_error());
    // The daemon refuses every request that needs a live run with the cancelled code (9).
    assert_eq!(last_error_code(), 9, "cancelled: the run is cancelled");

    let mut stopped = Stopped::default();
    // SAFETY: `stopped` is a value to write.
    assert_eq!(
        unsafe { daedalus_client_stop(client, run, STOP_DRAIN, &mut stopped) },
        Status::OK
    );
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

/// The constants are the numbers the header gives them.
#[test]
fn the_constants_are_the_numbers_the_surface_uses() {
    assert_eq!(FORM_SOURCE, 2);
    assert_eq!(TELEMETRY_TRACE, 3);
    assert_eq!(STOP_IMMEDIATE, 1);
    assert_eq!(INSPECT_HOST, 4);
    assert_eq!(EVENT_ALL, EVENT_FAULT | EVENT_TELEMETRY);
    assert_eq!(ALL_RUNS, 0);
}
