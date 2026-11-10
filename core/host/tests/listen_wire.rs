//! The listener at the level of packets: a stdio session, accept errors, malformed input and a
//! writer that fails.

mod listen_support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;

use daedalus_host::listen::{ACCEPT_BACKOFF_CAP, Acceptor, Channel, ListenOptions, accept_backoff};
use daedalus_wire::service::{
    ClientMsg, ErrorCode, InspectTarget, Inspection, Op, Reply, Request, ServerMsg, ServiceHello,
    StartConfig,
};
use daedalus_wire::{DEFAULT_CAP, Schema, read_packet, write_packet};
use listen_support::{graph, host, raw, request, serving, u32_value, until};

#[test]
fn a_stdio_session_round_trips_a_graph() {
    let host = host(ListenOptions::default());
    let (host_reads, mut client_writes) = std::io::pipe().unwrap();
    let (mut client_reads, host_writes) = std::io::pipe().unwrap();
    let serving = {
        let host = Arc::clone(&host);
        thread::spawn(move || host.serve_stream(host_reads, host_writes))
    };
    let mut buf = Vec::new();
    let mut ask = |message: ClientMsg| -> ServerMsg {
        let body = message.encode(&Schema::new()).unwrap();
        write_packet(&mut client_writes, &body, DEFAULT_CAP).unwrap();
        read_packet(&mut client_reads, DEFAULT_CAP, &mut buf).unwrap();
        ServerMsg::decode(&buf, &Schema::new()).unwrap()
    };
    let mut next_id = 0;
    let mut call = |ask: &mut dyn FnMut(ClientMsg) -> ServerMsg, op: Op| -> Reply {
        next_id += 1;
        match ask(ClientMsg::Request(Request::new(next_id, op))) {
            ServerMsg::Response(response) => {
                assert_eq!(response.id, next_id);
                response.result.expect("the op succeeds")
            }
            other => panic!("expected a response, got {other:?}"),
        }
    };
    assert!(matches!(
        ask(ClientMsg::Hello(ServiceHello::client())),
        ServerMsg::Hello(hello) if hello.check().is_ok()
    ));

    let json = graph("t.double");
    let put = Op::PutGraph {
        name: "g".to_string(),
        json: json.clone(),
    };
    assert!(
        matches!(call(&mut ask, put), Reply::Graph { diagnostics, .. } if diagnostics.is_empty())
    );
    let inspect = Op::Inspect {
        target: InspectTarget::Graph("g".to_string()),
    };
    match call(&mut ask, inspect) {
        Reply::Inspection(Inspection::Graph { name, json: back }) => {
            assert_eq!(name, "g");
            assert_eq!(back, json, "the graph comes back as it went in");
        }
        other => panic!("expected the graph, got {other:?}"),
    }
    let start = Op::Start {
        graph: "g".to_string(),
        config: StartConfig::default(),
    };
    let Reply::Started { run } = call(&mut ask, start) else {
        panic!("expected a run");
    };
    let feed = Op::Feed {
        run,
        port: "in".to_string(),
        value: u32_value(8),
    };
    assert!(matches!(call(&mut ask, feed), Reply::Fed { woke: 1, .. }));
    call(&mut ask, Op::Step { run, count: 1 });
    let take = Op::Take {
        run,
        port: "out".to_string(),
    };
    assert_eq!(call(&mut ask, take), Reply::Value(Some(u32_value(16))));

    // Closing the client's side ends the session, and the run stays in the host.
    drop(ask);
    drop(client_writes);
    serving.join().unwrap();
    let mut rest = Vec::new();
    client_reads.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty());
    assert_eq!(host.run_count(), 1);
    host.shutdown();
}

/// An acceptor that fails a set number of times, then hands out what it is sent.
struct Flaky {
    failures: u32,
    incoming: Receiver<Option<Channel>>,
    wake: Sender<Option<Channel>>,
}

impl Acceptor for Flaky {
    fn accept(&mut self) -> std::io::Result<Channel> {
        if self.failures > 0 {
            self.failures -= 1;
            return Err(std::io::Error::other("too many open files"));
        }
        match self.incoming.recv() {
            Ok(Some(channel)) => Ok(channel),
            _ => Err(std::io::Error::other("woken")),
        }
    }

    fn waker(&self) -> Box<dyn Fn() + Send + Sync> {
        let wake = Mutex::new(self.wake.clone());
        Box::new(move || {
            let _ = wake.lock().unwrap().send(None);
        })
    }

    fn describe(&self) -> String {
        "test:flaky".to_string()
    }
}

#[test]
fn accept_errors_back_off_within_the_cap_and_the_listener_keeps_going() {
    let host = host(ListenOptions::default());
    let (send, incoming) = channel();
    let delays = Arc::new(Mutex::new(Vec::new()));
    let listener = host.listen_on(
        Flaky {
            failures: 12,
            incoming,
            wake: send.clone(),
        },
        {
            let delays = Arc::clone(&delays);
            move |delay| delays.lock().unwrap().push(delay)
        },
    );
    assert_eq!(listener.address(), "test:flaky");

    // After twelve failed accepts the listener is still there, and serves a connection.
    let (host_reads, mut client_writes) = std::io::pipe().unwrap();
    let (mut client_reads, host_writes) = std::io::pipe().unwrap();
    send.send(Some(Channel {
        reader: Box::new(host_reads),
        writer: Box::new(host_writes),
        close: Box::new(|| {}),
    }))
    .unwrap();
    let hello = ClientMsg::Hello(ServiceHello::client())
        .encode(&Schema::new())
        .unwrap();
    write_packet(&mut client_writes, &hello, DEFAULT_CAP).unwrap();
    let mut buf = Vec::new();
    read_packet(&mut client_reads, DEFAULT_CAP, &mut buf).unwrap();
    write_packet(&mut client_writes, &request(9, Op::Ping), DEFAULT_CAP).unwrap();
    client_writes.flush().unwrap();
    read_packet(&mut client_reads, DEFAULT_CAP, &mut buf).unwrap();
    assert!(matches!(
        ServerMsg::decode(&buf, &Schema::new()),
        Ok(ServerMsg::Response(response)) if response.id == 9 && response.result.is_ok()
    ));

    let millis: Vec<u64> = delays
        .lock()
        .unwrap()
        .iter()
        .map(|delay| delay.as_millis() as u64)
        .collect();
    assert_eq!(
        millis,
        [
            50, 100, 200, 400, 800, 1600, 3200, 6400, 8000, 8000, 8000, 8000
        ]
    );
    assert!(
        delays
            .lock()
            .unwrap()
            .iter()
            .all(|d| *d <= ACCEPT_BACKOFF_CAP)
    );
    for failures in [0, 1, 9, 10_000, u32::MAX] {
        assert!(accept_backoff(failures) <= ACCEPT_BACKOFF_CAP);
    }
    assert_eq!(accept_backoff(u32::MAX), ACCEPT_BACKOFF_CAP);

    drop(client_writes);
    host.shutdown();
    listener.join();
}

fn response(stream: &mut TcpStream) -> (u64, Result<Reply, ErrorCode>) {
    let mut buf = Vec::new();
    read_packet(stream, DEFAULT_CAP, &mut buf).expect("a response");
    match ServerMsg::decode(&buf, &Schema::new()).expect("decodes") {
        ServerMsg::Response(response) => (response.id, response.result.map_err(|e| e.code)),
        other => panic!("expected a response, got {other:?}"),
    }
}

#[test]
fn a_malformed_or_oversize_packet_gets_a_typed_error() {
    let (host, addr) = serving(ListenOptions::default());

    // A request that does not decode is answered under its id, and the next one is served.
    let mut stream = raw(addr);
    let mut bad = request(7, Op::Ping);
    bad.push(0);
    write_packet(&mut stream, &bad, DEFAULT_CAP).unwrap();
    write_packet(&mut stream, &request(8, Op::Ping), DEFAULT_CAP).unwrap();
    assert_eq!(response(&mut stream), (7, Err(ErrorCode::InvalidRequest)));
    assert!(matches!(response(&mut stream), (8, Ok(Reply::Pong { .. }))));
    // A second hello is refused, and the session goes on.
    let hello = ClientMsg::Hello(ServiceHello::client())
        .encode(&Schema::new())
        .unwrap();
    write_packet(&mut stream, &hello, DEFAULT_CAP).unwrap();
    assert_eq!(response(&mut stream), (0, Err(ErrorCode::InvalidRequest)));

    // A length over the cap: the error, and then the connection is dropped, since the body
    // was never read.
    stream.write_all(&(DEFAULT_CAP + 1).to_le_bytes()).unwrap();
    assert_eq!(response(&mut stream), (0, Err(ErrorCode::FrameTooLarge)));
    let mut rest = Vec::new();
    let _ = stream.read_to_end(&mut rest);
    assert!(rest.is_empty());

    // A client with another layout is told so in place of a hello, and dropped.
    let mut other = TcpStream::connect(addr).unwrap();
    let mut hello = ServiceHello::client();
    hello.layout ^= 1;
    let body = ClientMsg::Hello(hello).encode(&Schema::new()).unwrap();
    write_packet(&mut other, &body, DEFAULT_CAP).unwrap();
    assert_eq!(response(&mut other), (0, Err(ErrorCode::Unsupported)));
    // So is one whose first packet is not a hello.
    let mut eager = TcpStream::connect(addr).unwrap();
    write_packet(&mut eager, &request(1, Op::Ping), DEFAULT_CAP).unwrap();
    assert_eq!(response(&mut eager), (0, Err(ErrorCode::InvalidRequest)));
    until("the refused sessions to end", || host.connections() == 0);
    host.shutdown();
}

/// A writer that takes the hello and one more packet, then fails as a timed-out socket does.
struct Stalls {
    writes: u32,
    sink: std::io::PipeWriter,
}

impl Write for Stalls {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writes += 1;
        // A packet is two writes: its length, then its body.
        if self.writes > 4 {
            return Err(std::io::ErrorKind::TimedOut.into());
        }
        self.sink.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.sink.flush()
    }
}

#[test]
fn a_write_that_times_out_disconnects_the_client() {
    let host = host(ListenOptions::default());
    assert_eq!(
        host.options().write_timeout,
        std::time::Duration::from_secs(10)
    );
    let (send, incoming) = channel();
    let listener = host.listen_on(
        Flaky {
            failures: 0,
            incoming,
            wake: send.clone(),
        },
        |_| {},
    );
    let (host_reads, mut client_writes) = std::io::pipe().unwrap();
    let (mut client_reads, host_writes) = std::io::pipe().unwrap();
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    send.send(Some(Channel {
        reader: Box::new(host_reads),
        writer: Box::new(Stalls {
            writes: 0,
            sink: host_writes,
        }),
        close: Box::new({
            let closed = Arc::clone(&closed);
            move || closed.store(true, Ordering::SeqCst)
        }),
    }))
    .unwrap();
    let hello = ClientMsg::Hello(ServiceHello::client())
        .encode(&Schema::new())
        .unwrap();
    write_packet(&mut client_writes, &hello, DEFAULT_CAP).unwrap();
    let mut buf = Vec::new();
    read_packet(&mut client_reads, DEFAULT_CAP, &mut buf).unwrap();
    for id in 1..=2 {
        write_packet(&mut client_writes, &request(id, Op::Ping), DEFAULT_CAP).unwrap();
    }
    // The first answer arrives. The write of the second fails, and the connection is closed.
    read_packet(&mut client_reads, DEFAULT_CAP, &mut buf).unwrap();
    until("the close", || closed.load(Ordering::SeqCst));
    assert!(read_packet(&mut client_reads, DEFAULT_CAP, &mut buf).is_err());
    drop(client_writes);
    until("the session to end", || host.connections() == 0);
    host.shutdown();
    listener.join();
}
