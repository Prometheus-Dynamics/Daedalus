//! The socket path allocates nothing for a warm Feed, Step or Take. What is counted is the
//! connection's reader thread (reading a request, decoding it, serving it, encoding its answer and
//! queueing it) and its writer thread (writing the answer): see `SocketAllocations`. A body that
//! runs on a thread of its own is not counted, and nothing here claims it.

#![cfg(unix)]

mod listen_support;

use std::os::unix::net::UnixStream;

use daedalus_abi::counting::CountingAlloc;
use daedalus_client::{Client, Endpoint, Options};
use daedalus_host::listen::{
    KEPT_PACKET_BYTES, ListenOptions, SPARE_PACKETS, ServiceHost, SocketAllocations,
};
use daedalus_types::Scalar;
use daedalus_wire::service::{
    ClientMsg, DEFAULT_SESSION, ErrorCode, HostInfo, InspectTarget, Inspection, Op, OpKind, Reply,
    Request, Response, ServerMsg, ServiceHello, StartConfig, TRACE_RING, TypedValue,
};
use daedalus_wire::{DEFAULT_CAP, Schema, WireType, WireValue, read_packet, write_packet};
use listen_support::{graph, graph_typed, host, u32_value};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

const WARM: u32 = 50;
const COUNTED: u64 = 1000;

/// The counts once `takes` Takes are recorded and every request's answer is written. A request
/// is recorded after its answer is queued, and a packet after it is written, so a client can
/// hold the last answer a moment before both are counted. One connection, no events: each
/// request has one packet.
fn settled(host: &ServiceHost, takes: u64) -> SocketAllocations {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let counts = host.socket_allocations();
        let done =
            counts.requests(OpKind::Take) == takes && counts.packets == counts.all_requests();
        if done || std::time::Instant::now() > deadline {
            return counts;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Per request of each steady op, over `COUNTED` warm round trips of each.
fn per_request(counts: &SocketAllocations) -> Vec<(OpKind, u64, f64)> {
    [OpKind::Feed, OpKind::Step, OpKind::Take]
        .into_iter()
        .map(|kind| {
            let requests = counts.requests(kind);
            (
                kind,
                requests,
                counts.reader(kind) as f64 / requests.max(1) as f64,
            )
        })
        .collect()
}

#[test]
fn a_warm_feed_step_and_take_allocate_nothing_on_the_socket_path() {
    let served = Served::new("warm");
    let client = served.client();
    assert!(
        client
            .put_graph("g", &graph("t.double"))
            .unwrap()
            .is_empty()
    );
    let run = client.start("g", StartConfig::default()).unwrap();
    let round = |n: u32| {
        client.feed(run, "in", u32_value(n)).unwrap();
        client.step(run, 1).unwrap();
        assert_eq!(client.take(run, "out").unwrap(), Some(u32_value(2 * n)));
    };
    for n in 0..WARM {
        round(n);
    }
    let before = settled(&served.host, u64::from(WARM));
    for n in 0..COUNTED as u32 {
        round(n);
    }
    let counts = settled(&served.host, u64::from(WARM) + COUNTED).since(&before);
    let rows = per_request(&counts);
    let writer = counts.writer as f64 / counts.packets.max(1) as f64;
    let all: Vec<String> = rows
        .iter()
        .map(|(kind, requests, per)| format!("{kind:?} {requests} requests, {per:.2} each"))
        .collect();
    let all = format!(
        "reader: {}; writer: {} packets, {writer:.2} each",
        all.join(", "),
        counts.packets
    );
    for (kind, requests, per) in &rows {
        assert_eq!(*requests, COUNTED, "{kind:?} requests counted: {all}");
        assert_eq!(*per, 0.0, "{kind:?} allocates on the reader thread: {all}");
    }
    assert_eq!(writer, 0.0, "the writer thread allocates: {all}");
}

/// A connection that has said hello, for requests the client library doesn't send: other
/// sessions, unknown names, a cut-off body.
fn raw(path: &std::path::Path) -> UnixStream {
    let mut stream = UnixStream::connect(path).expect("connect");
    let hello = ClientMsg::Hello(ServiceHello::client())
        .encode(&Schema::new())
        .unwrap();
    write_packet(&mut stream, &hello, DEFAULT_CAP).unwrap();
    let mut buf = Vec::new();
    read_packet(&mut stream, DEFAULT_CAP, &mut buf).unwrap();
    stream
}

fn ask(stream: &mut UnixStream, body: &[u8]) -> Response {
    write_packet(stream, body, DEFAULT_CAP).unwrap();
    let mut buf = Vec::new();
    read_packet(stream, DEFAULT_CAP, &mut buf).unwrap();
    match ServerMsg::decode(&buf, &Schema::new()).unwrap() {
        ServerMsg::Response(response) => response,
        other => panic!("expected a response, got {other:?}"),
    }
}

fn encoded(id: u64, session: &str, op: Op) -> Vec<u8> {
    let request = Request {
        id,
        session: session.to_owned(),
        op,
    };
    ClientMsg::Request(request).encode(&Schema::new()).unwrap()
}

fn host_info(stream: &mut UnixStream) -> HostInfo {
    let op = Op::Inspect {
        target: InspectTarget::Host,
    };
    match ask(stream, &encoded(1, DEFAULT_SESSION, op)).result {
        Ok(Reply::Inspection(Inspection::Host(info))) => info,
        other => panic!("expected the host's inspection, got {other:?}"),
    }
}

struct Served {
    host: std::sync::Arc<ServiceHost>,
    listener: Option<daedalus_host::listen::Listener>,
    dir: std::path::PathBuf,
    path: std::path::PathBuf,
}

impl Served {
    fn new(name: &str) -> Served {
        let dir = std::env::temp_dir().join(format!("dd-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s");
        let host = host(ListenOptions::default());
        let listener = Some(host.listen_unix(&path).expect("listen"));
        Served {
            host,
            listener,
            dir,
            path,
        }
    }

    fn client(&self) -> Client {
        Client::connect(Endpoint::unix(&self.path), Options::default()).expect("connect")
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        self.host.shutdown();
        if let Some(listener) = self.listener.take() {
            listener.join();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn names_a_client_makes_up_are_looked_up_and_never_kept() {
    let served = Served::new("names");
    let client = served.client();
    assert!(
        client
            .put_graph("g", &graph("t.double"))
            .unwrap()
            .is_empty()
    );
    let run = client.start("g", StartConfig::default()).unwrap();
    let mut stream = raw(&served.path);
    let before = host_info(&mut stream);
    for i in 0..10_000u64 {
        let (session, port) = (format!("s{i}"), format!("p{i}"));
        let op = match i % 3 {
            0 => Op::Feed {
                run,
                port,
                value: u32_value(1),
            },
            1 => Op::Take { run, port },
            _ => Op::Step {
                run: run + 1 + i,
                count: 1,
            },
        };
        let answer = ask(&mut stream, &encoded(i, &session, op)).result;
        let code = answer.err().map(|error| error.code);
        let want = if i % 3 == 2 {
            ErrorCode::UnknownRun
        } else {
            ErrorCode::UnknownPort
        };
        assert_eq!(code, Some(want), "request {i}");
    }
    // Nothing the requests named was added: no session, graph, run or plugin.
    assert_eq!(host_info(&mut stream), before);
    // The trace holds the newest of them, in its fixed number of entries, as they were named.
    let trace = match ask(
        &mut stream,
        &encoded(2, DEFAULT_SESSION, Op::Trace { limit: 0 }),
    )
    .result
    {
        Ok(Reply::Trace(entries)) => entries,
        other => panic!("expected the trace, got {other:?}"),
    };
    assert_eq!(trace.len(), TRACE_RING);
    let last = trace
        .iter()
        .rev()
        .find(|entry| entry.session == "s9999")
        .unwrap();
    assert_eq!(
        (last.op, last.target.as_str()),
        (OpKind::Feed, format!("{run}:p9999").as_str())
    );
}

#[test]
fn one_large_answer_is_not_kept_for_the_rest_of_the_connection() {
    let served = Served::new("kept");
    let client = served.client();
    assert!(
        client
            .put_graph("g", &graph("t.double"))
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .put_graph("b", &graph_typed("t.typed.bytes", "bytes"))
            .unwrap()
            .is_empty()
    );
    let small = client.start("g", StartConfig::default()).unwrap();
    let large = client.start("b", StartConfig::default()).unwrap();
    let big = TypedValue::new(
        WireType::Scalar(Scalar::Bytes),
        WireValue::Bytes(vec![7; 1 << 20]),
    );
    client.feed(large, "in", big).unwrap();
    client.step(large, 1).unwrap();
    assert!(client.take(large, "out").unwrap().is_some());
    // The answers after it are written after it, so its buffer has been handed back by then.
    for n in 0..20 {
        client.feed(small, "in", u32_value(n)).unwrap();
        client.step(small, 1).unwrap();
        client.take(small, "out").unwrap();
    }
    let kept = served.host.retained_packet_bytes();
    assert!(
        kept <= SPARE_PACKETS * KEPT_PACKET_BYTES,
        "{kept} bytes kept"
    );
    assert!(
        kept < 1 << 20,
        "the 1 MiB answer's buffer is kept: {kept} bytes"
    );
}

#[test]
fn a_feed_cut_short_changes_nothing() {
    let served = Served::new("cut");
    let client = served.client();
    assert!(
        client
            .put_graph("g", &graph("t.double"))
            .unwrap()
            .is_empty()
    );
    let run = client.start("g", StartConfig::default()).unwrap();
    client.feed(run, "in", u32_value(7)).unwrap();
    let mut stream = raw(&served.path);
    let feed = Op::Feed {
        run,
        port: "in".to_owned(),
        value: u32_value(9),
    };
    let whole = encoded(3, DEFAULT_SESSION, feed);
    // The value's last two bytes are missing.
    let answer = ask(&mut stream, &whole[..whole.len() - 2]).result;
    assert!(answer.is_err(), "a feed cut short was served: {answer:?}");
    client.step(run, 1).unwrap();
    assert_eq!(client.take(run, "out").unwrap(), Some(u32_value(14)));
}
