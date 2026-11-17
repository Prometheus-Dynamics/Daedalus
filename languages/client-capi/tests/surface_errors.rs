//! Every way a call on the surface is refused: bad endpoints, null pointers, handles that were
//! closed or never given out, bad text, bad numbers, the daemon's refusals, and a daemon that is
//! gone. None of them leaves a handle or a value behind.

mod support;

use std::sync::{Mutex, MutexGuard, PoisonError};

use daedalus_client_capi::*;
use support::{
    config, connect, daemon, encode, feed_u32, graph_json, last_error, last_error_code,
    load_fixture, put_graph, start, step, take_u32, tcp, u32_value,
};

/// The tests count live handles, which are the process's: one test at a time.
static TURN: Mutex<()> = Mutex::new(());

fn turn() -> MutexGuard<'static, ()> {
    TURN.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Connects to an endpoint text, and gives the status and the handle.
fn connect_text(endpoint: &str) -> (Status, Handle) {
    let mut handle = 0;
    // SAFETY: the endpoint is `len` bytes, and `handle` is a handle to write.
    let status =
        unsafe { daedalus_client_connect(endpoint.as_ptr(), endpoint.len(), 0, &mut handle) };
    (status, handle)
}

/// What a `unix:` endpoint gets on a platform with unix sockets: a socket error, since there is no
/// socket at the path. On a platform without them the endpoint is well formed but cannot be
/// served, which is `UNSUPPORTED`, not a bad endpoint.
#[cfg(unix)]
const UNIX_PATH_STATUS: Status = Status::IO;
#[cfg(not(unix))]
const UNIX_PATH_STATUS: Status = Status::UNSUPPORTED;

#[test]
fn an_endpoint_that_is_not_unix_or_loopback_tcp_is_refused_before_a_connection() {
    let _turn = turn();
    for text in ["", "http://x", "tcp:8.8.8.8:4100", "tcp:not-an-address"] {
        let (status, handle) = connect_text(text);
        assert_eq!(status, Status::BAD_ENDPOINT, "{text:?}: {}", last_error());
        assert_eq!(handle, 0, "{text:?} gave a handle");
    }
    let (status, handle) = connect_text("unix:");
    assert_eq!(status, UNIX_PATH_STATUS, "unix: {}", last_error());
    assert_eq!(handle, 0, "unix: gave a handle");
    // A socket nobody listens on, through a path.
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "daedalus-client-capi-none-{}.sock",
        std::process::id()
    ));
    let (status, handle) = connect_text(&format!("unix:{}", path.display()));
    assert_eq!(status, UNIX_PATH_STATUS, "{}", last_error());
    assert_eq!(handle, 0, "a path with no socket gave a handle");
    #[cfg(not(unix))]
    assert!(
        last_error().contains("unix sockets are not available"),
        "{}",
        last_error()
    );
}

#[cfg(unix)]
#[test]
fn a_connection_on_a_unix_socket_runs_the_same_calls_as_one_over_tcp() {
    let _turn = turn();
    let daemon = daemon();
    let path =
        std::env::temp_dir().join(format!("daedalus-client-capi-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let _socket = daemon.host.listen_unix(&path).expect("a unix listener");
    let endpoint = format!("unix:{}", path.display());
    let (status, client) = connect_text(&endpoint);
    assert_eq!(status, Status::OK, "{}", last_error());
    put_graph(client, "g", &graph_json("fixture.pass"));
    let mut version = 0;
    // SAFETY: `version` is a `uint32_t` to write.
    assert_eq!(
        unsafe { daedalus_client_ping(client, &mut version) },
        Status::OK
    );
    assert_eq!(version, daedalus_wire::service::SERVICE_VERSION);
    assert_eq!(daedalus_client_close(client), Status::OK);
    let _ = std::fs::remove_file(&path);
    drop(daemon);
}

#[test]
fn a_loopback_connection_to_nothing_is_an_io_status() {
    let _turn = turn();
    // A port nothing listens on: bind one, then let it go.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
        .port();
    let (status, handle) = connect_text(&tcp(port));
    assert_eq!(status, Status::IO, "{}", last_error());
    assert_eq!(handle, 0);
}

#[test]
fn null_pointers_and_bad_text_are_statuses_and_leave_the_value_in_place() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    put_graph(client, "g", &graph_json("fixture.pass"));
    let run = start(client, "g");

    // A null output where one is written.
    // SAFETY: the endpoint is `len` bytes, and the output is null, which the call refuses.
    let status =
        unsafe { daedalus_client_connect(tcp(daemon.port).as_ptr(), 17, 0, std::ptr::null_mut()) };
    assert_eq!(status, Status::NULL_POINTER, "{}", last_error());
    // SAFETY: the output is null, which the call refuses.
    assert_eq!(
        unsafe { daedalus_client_ping(client, std::ptr::null_mut()) },
        Status::NULL_POINTER
    );
    // SAFETY: the name is `len` bytes; the start's run is null, which the call refuses.
    let cfg = config();
    let status =
        unsafe { daedalus_client_start(client, b"g".as_ptr(), 1, &cfg, std::ptr::null_mut()) };
    assert_eq!(status, Status::NULL_POINTER);

    // A null text with a length.
    // SAFETY: the name is null with a length, which the call refuses.
    let status =
        unsafe { daedalus_client_put_graph(client, std::ptr::null(), 1, b"{}".as_ptr(), 2) };
    assert_eq!(status, Status::NULL_POINTER, "{}", last_error());
    // Empty text needs no memory: a null name with zero length is still refused by the daemon,
    // and not by the surface.
    // SAFETY: an empty name needs no memory, and the JSON is `len` bytes.
    let status =
        unsafe { daedalus_client_put_graph(client, std::ptr::null(), 0, b"{}".as_ptr(), 2) };
    assert_eq!(status, Status::SERVICE_ERROR, "{}", last_error());

    // Bad text: not UTF-8.
    let bad = [0xffu8, 0xfe];
    // SAFETY: the bytes are `len` bytes; the call refuses them as text.
    let status = unsafe { daedalus_client_load_plugin(client, bad.as_ptr(), bad.len()) };
    assert_eq!(status, Status::BAD_TEXT, "{}", last_error());
    // SAFETY: the bytes are `len` bytes; the call refuses them as text.
    let status = unsafe {
        daedalus_client_feed_str(
            client,
            run,
            b"in".as_ptr(),
            2,
            bad.as_ptr(),
            bad.len(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(status, Status::BAD_TEXT, "{}", last_error());

    // Bytes that are not a typed value.
    let garbage = [9u8, 9, 9];
    // SAFETY: the bytes are `len` bytes, and the output is null.
    let status = unsafe {
        daedalus_client_feed_wire(
            client,
            run,
            b"in".as_ptr(),
            2,
            garbage.as_ptr(),
            garbage.len(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(status, Status::WIRE, "{}", last_error());
    // A typed value with bytes after it is not one either.
    let mut extra = encode(u32_value(1));
    extra.push(0);
    // SAFETY: the bytes are `len` bytes, and the output is null.
    let status = unsafe {
        daedalus_client_feed_wire(
            client,
            run,
            b"in".as_ptr(),
            2,
            extra.as_ptr(),
            extra.len(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(status, Status::WIRE, "{}", last_error());

    // A take whose present flag is null is refused before the value is taken, so it stays.
    feed_u32(client, run, "in", 5);
    step(client, run, 1);
    // SAFETY: the port is `len` bytes, and the present flag is null, which the call refuses.
    let status = unsafe {
        daedalus_client_take_i64(
            client,
            run,
            b"out".as_ptr(),
            3,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(status, Status::NULL_POINTER, "{}", last_error());
    assert_eq!(
        take_u32(client, run, "out"),
        Some(5),
        "the value was not taken"
    );
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn a_handle_that_was_never_given_out_or_was_closed_is_refused() {
    let _turn = turn();
    let daemon = daemon();
    let first = connect(daemon.port);
    // Zero, and a number that was never a handle: both are bad.
    assert_eq!(daedalus_client_close(0), Status::BAD_HANDLE);
    assert_eq!(
        daedalus_client_close(12345),
        Status::BAD_HANDLE,
        "{}",
        last_error()
    );
    assert_eq!(daedalus_client_cache_clear(0), Status::BAD_HANDLE);
    assert_eq!(daedalus_client_cancel(0, 1), Status::BAD_HANDLE);

    // Closed, then stale, and closing it again is stale too.
    assert_eq!(daedalus_client_close(first), Status::OK);
    assert_eq!(
        daedalus_client_cache_clear(first),
        Status::STALE_HANDLE,
        "{}",
        last_error()
    );
    assert_eq!(daedalus_client_close(first), Status::STALE_HANDLE);

    // A handle is reused: the new connection is a new number, and the old one stays stale.
    let second = connect(daemon.port);
    assert_ne!(second, first);
    assert_eq!(
        daedalus_client_cache_clear(first),
        Status::STALE_HANDLE,
        "the old number stays stale"
    );
    assert_eq!(daedalus_client_cache_clear(second), Status::OK);
    assert_eq!(daedalus_client_close(second), Status::OK);
    drop(daemon);
}

#[test]
fn sessions_share_a_connection_and_closing_either_closes_both() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    let mut other: Handle = 0;
    // SAFETY: the name is `len` bytes, and `other` is a handle to write.
    let status = unsafe { daedalus_client_in_session(client, b"second".as_ptr(), 6, &mut other) };
    assert_eq!(status, Status::OK, "{}", last_error());
    put_graph(other, "only-here", &graph_json("fixture.pass"));
    // A session is a namespace for graph names: the first session does not see the name.
    let status = inspect_graph_status(client, b"only-here");
    assert_eq!(status, Status::SERVICE_ERROR, "{}", last_error());
    // An empty name is refused by the client.
    let mut empty: Handle = 0;
    // SAFETY: the name is empty, which needs no memory, and `empty` is a handle to write.
    let status = unsafe { daedalus_client_in_session(client, std::ptr::null(), 0, &mut empty) };
    assert_eq!(status, Status::BAD_SESSION, "{}", last_error());

    assert_eq!(daedalus_client_close(client), Status::OK);
    // The connection is closed for the other handle too.
    assert_eq!(
        daedalus_client_cache_clear(other),
        Status::DISCONNECTED,
        "{}",
        last_error()
    );
    assert_eq!(daedalus_client_close(other), Status::OK);
    drop(daemon);
}

/// Inspects a graph by name, for the session test: the status, not the text.
fn inspect_graph_status(client: Handle, name: &[u8]) -> Status {
    let mut len = 0;
    // SAFETY: the name is `len` bytes, the buffer is null, and `len` is written.
    unsafe {
        daedalus_client_inspect_graph(
            client,
            name.as_ptr(),
            name.len(),
            std::ptr::null_mut(),
            0,
            &mut len,
        )
    }
}

#[test]
fn the_daemons_refusals_carry_their_code_and_name() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    // An unknown run: the code of unknown_run is 3.
    let mut len = 0;
    // SAFETY: the buffer is null, and `len` is written.
    let status =
        unsafe { daedalus_client_inspect_run(client, 999, std::ptr::null_mut(), 0, &mut len) };
    assert_eq!(status, Status::SERVICE_ERROR, "{}", last_error());
    assert_eq!(last_error_code(), 3);
    assert!(
        last_error().starts_with("unknown_run: "),
        "{}",
        last_error()
    );

    // A stored graph that does not parse is an invalid request.
    let status = put_graph(client, "bad", "{ not json");
    assert_eq!(status, Status::SERVICE_ERROR, "{}", last_error());
    assert_eq!(last_error_code(), 1);

    // A failure with no server behind it has no code.
    let status = daedalus_client_close(0);
    assert_eq!(status, Status::BAD_HANDLE);
    assert_eq!(last_error_code(), 0, "not the server's");

    // A watch kind that names no event is refused by the surface, not sent.
    let mut watch: Handle = 0;
    // SAFETY: `watch` is a handle to write.
    let status = unsafe { daedalus_client_watch_open(client, 0, 9, &mut watch) };
    assert_eq!(status, Status::BAD_ARGUMENT, "{}", last_error());
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn a_daemon_that_is_gone_fails_its_clients_without_hanging() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    daemon.host.shutdown();
    let mut len = 0;
    // SAFETY: the buffer is null, and `len` is written.
    let status = unsafe { daedalus_client_inspect_host(client, std::ptr::null_mut(), 0, &mut len) };
    // The daemon may answer that it is shutting down before it closes the connection; any of the
    // three answers is a refusal, and each says what happened.
    match status {
        Status::SERVICE_ERROR => {
            assert_eq!(last_error_code(), 10, "stopped: {}", last_error());
            assert!(last_error().contains("shutting down"), "{}", last_error());
        }
        Status::DISCONNECTED | Status::UNKNOWN_DELIVERY => {}
        other => panic!("{other:?}: {}", last_error()),
    }
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn a_panic_is_a_status_and_the_last_error_keeps_its_text() {
    assert_eq!(daedalus_client_selftest_panic(), Status::PANIC);
    assert!(last_error().contains("asked to"), "{}", last_error());
}

#[test]
fn nothing_is_left_behind_once_every_handle_is_closed() {
    let _turn = turn();
    let before = daedalus_client_capi::live_handles();
    let daemon = daemon();
    for _ in 0..50 {
        let client = connect(daemon.port);
        let mut watch: Handle = 0;
        // SAFETY: `watch` is a handle to write.
        unsafe { daedalus_client_watch_open(client, 0, 3, &mut watch) };
        assert_eq!(daedalus_client_watch_close(watch), Status::OK);
        assert_eq!(daedalus_client_close(client), Status::OK);
    }
    assert_eq!(daedalus_client_capi::live_handles(), before);
    drop(daemon);
}

#[test]
fn a_value_that_a_port_does_not_carry_is_refused_by_type_not_shape() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    load_fixture(client);
    put_graph(client, "g", &graph_json("fixture.pass"));
    let run = start(client, "g");
    // SAFETY: the port is `len` bytes, and the output is null.
    let status = unsafe {
        daedalus_client_feed_bool(client, run, b"in".as_ptr(), 2, 1, std::ptr::null_mut())
    };
    assert_eq!(status, Status::SERVICE_ERROR, "{}", last_error());
    assert_eq!(last_error_code(), 5);
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}
