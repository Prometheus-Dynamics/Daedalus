//! The listener: several clients on one host, over TCP, a unix socket and stdio.

mod listen_support;

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::thread;

use daedalus_client::{Client, ClientError, Endpoint, Options};
use daedalus_host::listen::ListenOptions;
use daedalus_types::Scalar;
use daedalus_wire::service::{
    EndReason, ErrorCode, EventBody, EventKinds, InspectTarget, Inspection, Op, OpKind,
    PlanOptions, RunState, Severity, StartConfig, StopMode, TelemetryLevel, TypedValue,
    WatchTarget,
};
use daedalus_wire::{DEFAULT_CAP, WireType, WireValue, write_packet};
use listen_support::{WAITING, graph, graph_typed, host, raw, request, serving, u32_value, until};

fn connect(addr: SocketAddr) -> Client {
    Client::connect(Endpoint::tcp(addr).unwrap(), Options::default()).expect("connect")
}

fn code<T: std::fmt::Debug>(result: Result<T, ClientError>) -> ErrorCode {
    match result {
        Err(ClientError::Service(error)) => error.code,
        other => panic!("expected a service error, got {other:?}"),
    }
}

/// Stores the doubling graph as `g` and starts a run of it.
fn start_double(client: &Client) -> u64 {
    assert!(
        client
            .put_graph("g", &graph("t.double"))
            .unwrap()
            .is_empty()
    );
    client.start("g", StartConfig::default()).unwrap()
}

#[test]
fn two_clients_share_one_host() {
    let (host, addr) = serving(ListenOptions::default());
    let a = connect(addr);
    let b = connect(addr);
    until("both sessions", || host.connections() == 2);

    let run = start_double(&a);
    assert_eq!(a.feed(run, "in", u32_value(21)).unwrap().woke, 1);
    assert_eq!(a.step(run, 1).unwrap().ticks, 1);

    // B sees A's run and A's graph, and takes the value A's feed produced.
    let Inspection::Host(info) = b.inspect(InspectTarget::Host).unwrap() else {
        panic!("expected the host");
    };
    assert_eq!(info.runs.len(), 1);
    assert_eq!(info.runs[0].run, run);
    assert_eq!(info.runs[0].state, RunState::Running);
    assert_eq!(info.graphs, ["g"]);
    let Inspection::Run(info) = b.inspect(InspectTarget::Run(run)).unwrap() else {
        panic!("expected the run");
    };
    assert_eq!(info.graph, "g");
    assert_eq!(info.tick, 1);
    assert_eq!(info.nodes, ["io", "work"]);
    let ports: Vec<(&str, bool, &str)> = info
        .ports
        .iter()
        .map(|port| (port.name.as_str(), port.input, port.type_key.as_str()))
        .collect();
    assert_eq!(ports, [("in", true, "u32"), ("out", false, "u32")]);
    assert_eq!(b.take(run, "out").unwrap(), Some(u32_value(42)));
    assert_eq!(a.take(run, "out").unwrap(), None);

    // Graph names are per session. Runs are not.
    let other = b.in_session("other").unwrap();
    assert_eq!(
        code(other.start("g", StartConfig::default())),
        ErrorCode::UnknownGraph
    );
    other.feed(run, "in", u32_value(4)).unwrap();
    a.step(run, 1).unwrap();
    assert_eq!(other.take(run, "out").unwrap(), Some(u32_value(8)));
    host.shutdown();
}

#[test]
fn a_disconnect_leaves_the_run_running_and_ends_its_watches() {
    let (host, addr) = serving(ListenOptions::default());
    let a = connect(addr);
    let run = start_double(&a);
    let watch = a.watch(WatchTarget::Run(run), EventKinds::ALL).unwrap();
    a.feed(run, "in", u32_value(5)).unwrap();
    assert_eq!(host.watch_count(), 1);
    // The connection goes while the watch is still subscribed.
    a.close();
    assert_eq!(watch.next_event(), None);
    until("the session to end", || host.connections() == 0);
    assert_eq!(host.watch_count(), 0);
    assert_eq!(host.run_count(), 1);

    // A new client finds the run as it was left: the fed value is still waiting.
    let b = connect(addr);
    assert_eq!(b.step(run, 1).unwrap().step_calls, 1);
    assert_eq!(b.take(run, "out").unwrap(), Some(u32_value(10)));
    // A draining stop ticks until a tick finds nothing to do: one more here.
    assert_eq!(b.stop(run, StopMode::Drain).unwrap().tick, 2);
    assert_eq!(host.run_count(), 0);
    assert_eq!(code(b.step(run, 1)), ErrorCode::UnknownRun);
    host.shutdown();
}

#[test]
fn a_client_that_stops_reading_is_disconnected_without_stalling_step() {
    let (host, addr) = serving(ListenOptions {
        outbound_queue: 8,
        ..ListenOptions::default()
    });
    let stepper = connect(addr);
    let run = start_double(&stepper);
    // Fill the trace, so each answer to the stuck client is large.
    for _ in 0..1100 {
        stepper.ping().unwrap();
    }

    // The stuck client watches the run, asks for the trace over and over, and reads nothing.
    let mut stuck = raw(addr);
    let watch = Op::Watch {
        target: WatchTarget::Run(run),
        kinds: EventKinds::ALL,
    };
    write_packet(&mut stuck, &request(1, watch), DEFAULT_CAP).unwrap();
    until("the watch", || host.watch_count() == 1);
    let flood = thread::spawn(move || {
        let ask = request(2, Op::Trace { limit: 0 });
        // Ends when the host drops the connection, which makes a write fail.
        while write_packet(&mut stuck, &ask, DEFAULT_CAP).is_ok() {}
        stuck
    });

    // Meanwhile the run keeps stepping, and every step sends the stuck client an event.
    let mut total = 0;
    while host.connections() == 2 {
        stepper.feed(run, "in", u32_value(total)).unwrap();
        assert_eq!(stepper.step(run, 1).unwrap().ticks, 1);
        assert_eq!(
            stepper.take(run, "out").unwrap(),
            Some(u32_value(total * 2))
        );
        total += 1;
    }
    let _stuck = flood.join().unwrap();
    until("the stuck session to end", || host.connections() == 1);
    assert_eq!(host.watch_count(), 0);
    // The host is untouched: the run is there and steps.
    stepper.feed(run, "in", u32_value(7)).unwrap();
    stepper.step(run, 1).unwrap();
    assert_eq!(stepper.take(run, "out").unwrap(), Some(u32_value(14)));
    host.shutdown();
}

#[test]
fn a_connection_past_the_limit_is_told_the_host_is_busy() {
    let (host, addr) = serving(ListenOptions {
        connections: 2,
        max_runs: 1,
        ..ListenOptions::default()
    });
    assert_eq!(ListenOptions::default().connections, 64);
    let a = connect(addr);
    let b = connect(addr);
    until("both sessions", || host.connections() == 2);
    let refused = Client::connect(Endpoint::tcp(addr).unwrap(), Options::default());
    assert_eq!(code(refused.map(|_| ())), ErrorCode::Busy);
    // A slot that frees is given out again.
    drop(b);
    until("the slot to free", || host.connections() == 1);
    let c = connect(addr);
    assert_eq!(c.ping().unwrap(), 1);

    // The run limit is the same kind of refusal.
    let run = start_double(&a);
    assert_eq!(code(c.start("g", StartConfig::default())), ErrorCode::Busy);
    a.stop(run, StopMode::Immediate).unwrap();
    c.start("g", StartConfig::default()).unwrap();
    assert_eq!(c.server().unwrap().capabilities.max_runs, 1);
    host.shutdown();
}

#[test]
fn a_fault_reaches_every_watcher_and_a_cancel_stops_a_tick_in_progress() {
    let (host, addr) = serving(ListenOptions::default());
    let a = connect(addr);
    let b = connect(addr);
    a.put_graph("boom", &graph("t.boom")).unwrap();
    let config = StartConfig {
        telemetry: TelemetryLevel::Detailed,
        ..StartConfig::default()
    };
    let run = a.start("boom", config).unwrap();
    let on_run = b.watch(WatchTarget::Run(run), EventKinds::ALL).unwrap();
    let mut on_all = a.watch(WatchTarget::All, EventKinds::FAULT).unwrap();
    a.feed(run, "in", u32_value(1)).unwrap();
    let summary = a.step(run, 1).unwrap();
    assert_eq!((summary.faults, summary.step_calls), (1, 1));

    for watch in [&on_run, &on_all] {
        match watch.next_event().expect("a fault").body {
            EventBody::Fault(fault) => {
                assert_eq!(fault.run, run);
                assert_eq!(fault.node.as_deref(), Some("work"));
                assert_eq!(fault.covers, ["work"]);
                assert_eq!(fault.reason, "boom: the node failed on purpose");
                assert_eq!(fault.consecutive, 1);
            }
            other => panic!("expected a fault, got {other:?}"),
        }
    }
    // The watch that asked for telemetry gets the batch's totals. The other does not.
    assert!(matches!(
        on_run.next_event().expect("telemetry").body,
        EventBody::Telemetry(event) if event.faults == 1 && event.tick == 1
    ));
    let Inspection::Telemetry(info) = b.inspect(InspectTarget::Telemetry(run)).unwrap() else {
        panic!("expected telemetry");
    };
    assert_eq!(
        (info.tick, info.faults, info.level),
        (1, 1, TelemetryLevel::Detailed)
    );
    assert_eq!(info.rows.len(), 1);
    assert_eq!(info.rows[0].nodes, ["work"]);
    assert_eq!(info.rows[0].failures, 1);
    assert!(!info.rows[0].disabled && !info.rows[0].auto_off);
    // It keeps failing. The step backs off, and nothing ever turns it off.
    for _ in 0..8 {
        a.feed(run, "in", u32_value(1)).unwrap();
        a.step(run, 1).unwrap();
    }
    let Inspection::Telemetry(info) = b.inspect(InspectTarget::Telemetry(run)).unwrap() else {
        panic!("expected telemetry");
    };
    assert!(
        info.rows[0].backing_off,
        "a step that keeps failing backs off"
    );
    assert!(!info.rows[0].disabled && !info.rows[0].auto_off);
    assert!(info.faults >= 3, "{} faults", info.faults);
    drop(on_run);
    let on_run = b.watch(WatchTarget::Run(run), EventKinds::ALL).unwrap();
    a.stop(run, StopMode::Immediate).unwrap();
    assert!(matches!(
        on_run.next_event().expect("ended").body,
        EventBody::Ended {
            reason: EndReason::Stopped,
            ..
        }
    ));
    assert_eq!(on_run.next_event(), None, "a watch on one run ends with it");
    // The watch on every run saw each later fault too, and then the run ending.
    let ended = on_all
        .by_ref()
        .map(|event| event.body)
        .find(|body| !matches!(body, EventBody::Fault(_)))
        .expect("ended");
    assert_eq!(
        ended,
        EventBody::Ended {
            run,
            reason: EndReason::Stopped
        }
    );

    // A node that runs until it is cancelled: A's step is inside the tick when B cancels.
    a.put_graph("wait", &graph("t.wait")).unwrap();
    let run = a.start("wait", StartConfig::default()).unwrap();
    a.feed(run, "in", u32_value(1)).unwrap();
    let stepping = {
        let a = a.clone();
        thread::spawn(move || a.step(run, 3))
    };
    until("the body to be running", || WAITING.load(Ordering::SeqCst));
    b.cancel(run).unwrap();
    assert_eq!(code(stepping.join().unwrap()), ErrorCode::Cancelled);
    assert!(matches!(
        on_all.next_event().expect("ended").body,
        EventBody::Ended { run: ended, reason: EndReason::Cancelled } if ended == run
    ));
    // A cancelled run takes nothing more, and still answers inspect, take and stop.
    assert_eq!(code(a.feed(run, "in", u32_value(1))), ErrorCode::Cancelled);
    assert_eq!(code(a.step(run, 1)), ErrorCode::Cancelled);
    let Inspection::Run(info) = a.inspect(InspectTarget::Run(run)).unwrap() else {
        panic!("expected the run");
    };
    assert_eq!(info.state, RunState::Cancelled);
    assert_eq!(a.take(run, "out").unwrap(), None);
    a.stop(run, StopMode::Drain).unwrap();
    assert_eq!(code(a.cancel(run)), ErrorCode::UnknownRun);
    assert_eq!(
        on_all.next_event().expect("ended").body,
        EventBody::Ended {
            run,
            reason: EndReason::Stopped
        }
    );

    // Unwatching ends the stream, and the host forgets the watch.
    on_all.close();
    assert_eq!(on_all.next_event(), None);
    until("the unwatch", || host.watch_count() == 0);
    host.shutdown();
}

#[test]
fn each_refusal_has_its_code_and_a_panic_is_an_error_response() {
    let (host, addr) = serving(ListenOptions::default());
    let client = connect(addr);
    let run = start_double(&client);

    assert_eq!(
        code(client.put_graph("bad", "{ not json")),
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        code(client.put_graph("", &graph("t.double"))),
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        code(client.plan("absent", PlanOptions::default())),
        ErrorCode::UnknownGraph
    );
    assert_eq!(code(client.step(99, 1)), ErrorCode::UnknownRun);
    assert_eq!(
        code(client.feed(run, "nope", u32_value(1))),
        ErrorCode::UnknownPort
    );
    assert_eq!(
        code(client.feed(run, "out", u32_value(1))),
        ErrorCode::UnknownPort
    );
    assert_eq!(code(client.take(run, "in")), ErrorCode::UnknownPort);
    let text = TypedValue::new(
        WireType::Scalar(Scalar::String),
        WireValue::String("one".to_string()),
    );
    assert_eq!(code(client.feed(run, "in", text)), ErrorCode::ValueType);
    assert_eq!(code(client.load_plugin("bad")), ErrorCode::PluginError);
    assert_eq!(client.load_plugin("libdemo.so").unwrap().name, "libdemo.so");
    assert_eq!(
        code(client.patch_graph("g", "{\"version\": 9}")),
        ErrorCode::InvalidRequest
    );

    // A graph that names a node nothing declares is stored with its diagnostics, and fails to
    // plan with the same.
    let found = client.put_graph("lost", &graph("t.missing")).unwrap();
    assert!(
        found
            .iter()
            .any(|d| d.severity == Severity::Error && d.code == "node_missing")
    );
    match client.start("lost", StartConfig::default()) {
        Err(ClientError::Service(error)) => {
            assert_eq!(error.code, ErrorCode::PlanningFailed);
            assert!(!error.diagnostics.is_empty());
        }
        other => panic!("expected planning_failed, got {other:?}"),
    }

    // A port whose type has no wire codec: the run exists, and values for it are refused.
    client
        .put_graph("blob", &graph_typed("t.blob", "t:blob"))
        .unwrap();
    let blob = client.start("blob", StartConfig::default()).unwrap();
    assert_eq!(
        code(client.feed(blob, "in", u32_value(1))),
        ErrorCode::Unsupported
    );
    assert_eq!(code(client.take(blob, "out")), ErrorCode::Unsupported);

    // The loader panics. The request is answered with an error and the host goes on.
    match client.load_plugin("panic") {
        Err(ClientError::Service(error)) => {
            assert_eq!(error.code, ErrorCode::InvalidRequest);
            assert!(error.message.contains("panicked"), "{}", error.message);
            assert!(error.message.contains("on purpose"), "{}", error.message);
        }
        other => panic!("expected an error response, got {other:?}"),
    }
    assert_eq!(client.ping().unwrap(), 1);
    client.feed(run, "in", u32_value(3)).unwrap();
    client.step(run, 1).unwrap();
    assert_eq!(client.take(run, "out").unwrap(), Some(u32_value(6)));

    // The trace has every request, with the code of each one that failed.
    let trace = client.trace(0).unwrap();
    assert_eq!(trace.first().unwrap().op, OpKind::PutGraph);
    let failed: Vec<ErrorCode> = trace.iter().filter_map(|entry| entry.error).collect();
    assert!(failed.contains(&ErrorCode::PlanningFailed));
    assert!(failed.contains(&ErrorCode::Unsupported));
    assert_eq!(client.trace(2).unwrap().len(), 2);
    host.shutdown();
}

#[test]
fn plan_reload_patch_and_the_cache() {
    let (host, addr) = serving(ListenOptions::default());
    let client = connect(addr);
    let run = start_double(&client);

    let first = client.plan("g", PlanOptions::default()).unwrap();
    assert!(!first.cached);
    assert_eq!((first.nodes, first.regions), (2, 1));
    let again = client.plan("g", PlanOptions::default()).unwrap();
    assert!(again.cached);
    assert_eq!(again.hash, first.hash);
    let cache = client.cache_inspect().unwrap();
    assert_eq!((cache.entries, cache.hits, cache.misses), (1, 1, 1));
    let inspected = client
        .plan(
            "g",
            PlanOptions {
                inspect: true,
                ..PlanOptions::default()
            },
        )
        .unwrap();
    assert!(inspected.explain.unwrap().contains("regions"));
    client.cache_clear().unwrap();
    assert_eq!(client.cache_inspect().unwrap().entries, 0);
    assert!(!client.plan("g", PlanOptions::default()).unwrap().cached);

    // A reload to the same graph carries every node. One to another body starts it fresh.
    let same = client.reload(run, "g").unwrap();
    assert_eq!(same.carried.names, ["io", "work"]);
    assert_eq!(same.fresh.total, 0);
    client.put_graph("g2", &graph("t.boom")).unwrap();
    let changed = client.reload(run, "g2").unwrap();
    assert_eq!(changed.fresh.names, ["work"]);
    assert_eq!(changed.carried.names, ["io"]);
    let Inspection::Run(info) = client.inspect(InspectTarget::Run(run)).unwrap() else {
        panic!("expected the run");
    };
    assert_eq!(info.graph, "g2");

    // A patch that swaps the node back, and one whose selector matches nothing.
    let patch = r#"{"version":1,"ops":[{"type":"replace_node_id","node":{"index":1},"new_id":"t.double"}]}"#;
    assert!(client.patch_graph("g2", patch).unwrap().is_empty());
    client.reload(run, "g2").unwrap();
    client.feed(run, "in", u32_value(50)).unwrap();
    client.step(run, 1).unwrap();
    assert_eq!(client.take(run, "out").unwrap(), Some(u32_value(100)));
    let missing = r#"{"version":1,"ops":[{"type":"delete_node","node":{"index":9}}]}"#;
    let found = client.patch_graph("g2", missing).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].code, "selector_not_found");

    // An immediate stop reports what was still waiting in the ports.
    client.feed(run, "in", u32_value(1)).unwrap();
    assert_eq!(client.stop(run, StopMode::Immediate).unwrap().dropped, 1);
    host.shutdown();
}

#[test]
fn shutdown_ends_watches_and_closes_every_session() {
    let (host, addr) = serving(ListenOptions::default());
    let client = connect(addr);
    let run = start_double(&client);
    let watch = client.watch(WatchTarget::All, EventKinds::ALL).unwrap();
    assert_eq!(host.shutdown(), 1);
    assert!(host.is_stopping());
    assert!(matches!(
        watch.next_event().expect("ended").body,
        EventBody::Ended {
            run: 0,
            reason: EndReason::Shutdown
        }
    ));
    assert_eq!(watch.next_event(), None);
    until("the client to see the close", || !client.is_connected());
    assert!(matches!(
        client.step(run, 1),
        Err(ClientError::Disconnected)
    ));
    until("the sessions to end", || host.connections() == 0);
    // Nothing is listening any more: a new connection is not served.
    assert!(Client::connect(Endpoint::tcp(addr).unwrap(), Options::default()).is_err());
}

#[test]
fn only_loopback_is_offered() {
    let host = host(ListenOptions::default());
    let error = host
        .listen_tcp("0.0.0.0:0".parse().unwrap())
        .map(|_| ())
        .expect_err("a wildcard address is refused");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(host.listen_tcp("192.0.2.7:0".parse().unwrap()).is_err());
    let (_listener, addr) = host
        .listen_tcp("[::1]:0".parse().unwrap())
        .unwrap_or_else(|_| {
            // A machine without IPv6 loopback still has the IPv4 one.
            host.listen_tcp("127.0.0.1:0".parse().unwrap()).unwrap()
        });
    assert!(addr.ip().is_loopback());
    host.shutdown();
}

#[cfg(unix)]
#[test]
fn a_unix_socket_serves_clients_and_replaces_a_stale_file() {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("listen-unix");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("daedalusd.sock");
    // A file left behind by a host that died: nothing answers on it.
    drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
    assert!(path.exists());

    let host = host(ListenOptions::default());
    let listener = host.listen_unix(&path).expect("the stale file is replaced");
    assert_eq!(listener.address(), format!("unix:{}", path.display()));
    let client = Client::connect(Endpoint::unix(&path), Options::default()).unwrap();
    let run = start_double(&client);
    client.feed(run, "in", u32_value(2)).unwrap();
    client.step(run, 1).unwrap();
    assert_eq!(client.take(run, "out").unwrap(), Some(u32_value(4)));

    // A second host cannot take a socket a live one is serving.
    let other = listen_support::host(ListenOptions::default());
    let error = other.listen_unix(&path).map(|_| ()).expect_err("in use");
    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);

    host.shutdown();
    listener.join();
    assert!(!path.exists(), "the socket file goes with the listener");
    let _ = std::fs::remove_dir_all(&dir);
}
