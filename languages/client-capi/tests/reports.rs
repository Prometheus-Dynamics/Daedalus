//! The calls that keep what the daemon answered: a plugin's description, a graph's diagnostics and
//! a reload's carry, read back by `daedalus_client_report`; and the two that give text as written,
//! `daedalus_client_plan_raw` and `daedalus_client_graph_text`.

use crate::support;

use std::sync::{Mutex, MutexGuard, PoisonError};

use daedalus_client_capi::*;
use support::{
    connect, daemon, fixture_library, graph_json, json, last_error, put_graph, start, text,
};

/// The tests count live handles, which are the process's: one test at a time.
static TURN: Mutex<()> = Mutex::new(());

fn turn() -> MutexGuard<'static, ()> {
    TURN.lock().unwrap_or_else(PoisonError::into_inner)
}

fn report(client: Handle) -> String {
    text(|buf, cap, len| {
        // SAFETY: the buffer is null or `cap` bytes, and `len` points to a size.
        unsafe { daedalus_client_report(client, buf, cap, len) }
    })
}

#[test]
fn a_plugin_a_graph_and_a_reload_keep_their_reports() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    assert_eq!(
        report(client),
        "",
        "no report before any call that keeps one"
    );

    let path = fixture_library();
    let path = path.to_str().expect("a UTF-8 path");
    // SAFETY: the path is `len` bytes of UTF-8.
    let status = unsafe { daedalus_client_load_plugin_report(client, path.as_ptr(), path.len()) };
    assert_eq!(status, Status::OK, "{}", last_error());
    let plugin: serde_json::Value = serde_json::from_str(&report(client)).expect("JSON");
    assert!(
        plugin["nodes"]
            .as_array()
            .is_some_and(|nodes| !nodes.is_empty()),
        "{plugin}"
    );
    assert_eq!(plugin["generation"], 1, "{plugin}");
    assert_eq!(report(client), report(client), "reading changes nothing");

    let graph = graph_json("fixture.pass");
    // SAFETY: both texts are `len` bytes of UTF-8.
    let status = unsafe {
        daedalus_client_put_graph_report(client, b"g".as_ptr(), 1, graph.as_ptr(), graph.len())
    };
    assert_eq!(status, Status::OK, "{}", last_error());
    assert_eq!(report(client), "[]", "a clean graph has no diagnostics");

    let run = start(client, "g");
    assert_eq!(put_graph(client, "h", &graph), Status::OK);
    // SAFETY: the name is `len` bytes of UTF-8.
    let status = unsafe { daedalus_client_reload_report(client, run, b"h".as_ptr(), 1) };
    assert_eq!(status, Status::OK, "{}", last_error());
    let carry: serde_json::Value = serde_json::from_str(&report(client)).expect("JSON");
    assert_eq!(carry["carried"]["total"], 2, "{carry}");
    assert_eq!(carry["dropped_values"], 0, "{carry}");
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}

#[test]
fn a_plan_explanation_and_a_stored_document_come_back_as_text() {
    let _turn = turn();
    let daemon = daemon();
    let client = connect(daemon.port);
    support::load_fixture(client);
    let graph = graph_json("fixture.pass");
    assert_eq!(put_graph(client, "g", &graph), Status::OK);

    let stored = text(|buf, cap, len| {
        // SAFETY: the name is `len` bytes, and the buffer is null or `cap` bytes.
        unsafe { daedalus_client_graph_text(client, b"g".as_ptr(), 1, buf, cap, len) }
    });
    let document: serde_json::Value = serde_json::from_str(&stored).expect("JSON");
    assert_eq!(document["format"], "daedalus.graph", "{document}");

    for (inspect, explained) in [(0, false), (1, true)] {
        let options = PlanOptions {
            form: FORM_STEPS,
            inspect,
            priority: std::ptr::null(),
            priority_len: 0,
        };
        let plan = json(|buf, cap, len| {
            // SAFETY: the name is `len` bytes, and `options` is read for the call.
            unsafe { daedalus_client_plan_raw(client, b"g".as_ptr(), 1, &options, buf, cap, len) }
        });
        assert!(plan["hash"].is_string(), "{plan}");
        match plan["explain"].as_str() {
            Some(text) => {
                assert!(explained, "an explanation that was not asked for: {plan}");
                let parsed: serde_json::Value =
                    serde_json::from_str(text).expect("the text is JSON");
                assert!(parsed["nodes"].is_array(), "{parsed}");
            }
            None => assert!(!explained && plan["explain"].is_null(), "{plan}"),
        }
    }
    assert_eq!(daedalus_client_close(client), Status::OK);
    drop(daemon);
}
