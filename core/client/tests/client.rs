//! The client against a small server that speaks the service framing, written here.

use std::io::Write;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use daedalus_client::protocol::{
    CacheInfo, Capabilities, CarryReport, ClientMsg, EndReason, ErrorCode, Event, EventBody,
    EventKinds, InspectTarget, Inspection, Op, OpKind, PlanOptions, PlanSummary, PluginInfo, Reply,
    Request, Response, SERVICE_LAYOUT_HASH, SERVICE_VERSION, ServerMsg, ServiceError, ServiceHello,
    StartConfig, StepSummary, StopMode, TelemetryEvent, TypedValue, WatchTarget,
};
use daedalus_client::{
    BACKOFF_CAP, Backoff, Client, ClientError, Clock, Endpoint, Options, WireError, WireType,
    WireValue,
};
use daedalus_wire::{DEFAULT_CAP, Schema, read_packet, write_packet};

/// How the test server treats requests.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// Answers each request as it arrives.
    Answer,
    /// Reads requests and answers none.
    Hold,
    /// Collects this many requests, then answers them newest first.
    Reverse(usize),
}

struct Shared {
    mode: Mutex<Mode>,
    layout: u64,
    /// Every request read, in order, across restarts.
    log: Mutex<Vec<Request>>,
    /// The live connections, for writing events and for killing.
    streams: Mutex<Vec<TcpStream>>,
    /// The subscriptions made on live connections: the stream and the server's number.
    subs: Mutex<Vec<(TcpStream, u64)>>,
    stopped: AtomicBool,
    /// While set, the server is down: a connection is accepted and closed at once, before any
    /// hello, which a client sees as a failed attempt.
    down: AtomicBool,
}

/// A server on a loopback port. It can be killed and brought back. The port stays bound while it
/// is down, so no other test can be given it.
struct Server {
    addr: SocketAddr,
    shared: Arc<Shared>,
}

fn send(stream: &mut TcpStream, message: &ServerMsg) -> bool {
    let body = message.encode(&Schema::new()).expect("encode");
    write_packet(stream, &body, DEFAULT_CAP).is_ok()
}

fn reply_for(request: &Request, sub: u64) -> Result<Reply, ServiceError> {
    Ok(match &request.op {
        Op::Ping => Reply::Pong {
            version: SERVICE_VERSION,
        },
        Op::LoadPlugin { path } => Reply::Plugin(PluginInfo {
            name: path.clone(),
            ..PluginInfo::default()
        }),
        Op::PutGraph { name, .. } => Reply::Graph {
            name: name.clone(),
            diagnostics: Vec::new(),
        },
        Op::PatchGraph { graph, .. } => Reply::Graph {
            name: graph.clone(),
            diagnostics: Vec::new(),
        },
        Op::Plan { graph, .. } => Reply::Plan(PlanSummary {
            graph: graph.clone(),
            hash: 9,
            ..PlanSummary::default()
        }),
        Op::Start { .. } => Reply::Started { run: 7 },
        Op::Feed { .. } => Reply::Fed {
            woke: 1,
            dropped: false,
        },
        Op::Step { run: 404, .. } => {
            return Err(ServiceError::new(ErrorCode::UnknownRun, "no run 404"));
        }
        Op::Step { count, .. } => Reply::Stepped(StepSummary {
            ticks: *count,
            ..StepSummary::default()
        }),
        Op::Take { .. } => Reply::Value(Some(TypedValue::new(
            WireType::Scalar(daedalus_types::Scalar::U32),
            WireValue::U32(42),
        ))),
        Op::Watch { .. } => Reply::Watching { sub },
        Op::Reload { .. } => Reply::Reloaded(CarryReport::default()),
        Op::Stop { run, .. } => Reply::Stopped {
            run: *run,
            tick: 3,
            dropped: 0,
        },
        Op::Inspect { .. } => Reply::Inspection(Inspection::Cache(CacheInfo {
            entries: 1,
            hits: 2,
            misses: 3,
        })),
        Op::Trace { .. } => Reply::Trace(Vec::new()),
        Op::Unwatch { .. } | Op::Cancel { .. } | Op::Cache { .. } => Reply::Ack,
    })
}

fn serve(shared: &Shared, mut stream: TcpStream) {
    let mut buf = Vec::new();
    if read_packet(&mut stream, DEFAULT_CAP, &mut buf).is_err() {
        return;
    }
    let mut hello = ServiceHello::server(Capabilities::all_ops());
    hello.layout = shared.layout;
    if !send(&mut stream, &ServerMsg::Hello(hello)) {
        return;
    }
    let mut held = Vec::new();
    let mut next_sub = 100;
    while read_packet(&mut stream, DEFAULT_CAP, &mut buf).is_ok() {
        let Ok(ClientMsg::Request(request)) = ClientMsg::decode(&buf, &Schema::new()) else {
            return;
        };
        shared.log.lock().unwrap().push(request.clone());
        let mode = *shared.mode.lock().unwrap();
        match mode {
            Mode::Hold => continue,
            Mode::Reverse(count) => {
                held.push(request);
                if held.len() < count {
                    continue;
                }
            }
            Mode::Answer => held.push(request),
        }
        for request in held.drain(..).rev() {
            if matches!(request.op, Op::Watch { .. }) {
                next_sub += 1;
                let stream = stream.try_clone().expect("clone");
                shared.subs.lock().unwrap().push((stream, next_sub));
            }
            let response = Response {
                id: request.id,
                result: reply_for(&request, next_sub),
            };
            if !send(&mut stream, &ServerMsg::Response(response)) {
                return;
            }
        }
    }
}

impl Server {
    fn start() -> Server {
        Server::with_layout(SERVICE_LAYOUT_HASH)
    }

    fn with_layout(layout: u64) -> Server {
        let shared = Arc::new(Shared {
            mode: Mutex::new(Mode::Answer),
            layout,
            log: Mutex::new(Vec::new()),
            streams: Mutex::new(Vec::new()),
            subs: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
            down: AtomicBool::new(false),
        });
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let accepting = Arc::clone(&shared);
        thread::spawn(move || {
            let shared = accepting;
            for stream in listener.incoming() {
                if shared.stopped.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                if shared.down.load(Ordering::SeqCst) {
                    continue;
                }
                shared
                    .streams
                    .lock()
                    .unwrap()
                    .push(stream.try_clone().expect("clone"));
                let shared = Arc::clone(&shared);
                thread::spawn(move || serve(&shared, stream));
            }
        });
        Server { addr, shared }
    }

    /// Drops every connection and serves none until `restart`, as a killed server does.
    fn kill(&self) {
        self.shared.down.store(true, Ordering::SeqCst);
        for stream in self.shared.streams.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
        self.shared.subs.lock().unwrap().clear();
    }

    fn restart(&self) {
        self.shared.down.store(false, Ordering::SeqCst);
    }

    fn set_mode(&self, mode: Mode) {
        *self.shared.mode.lock().unwrap() = mode;
    }

    fn log(&self) -> Vec<Request> {
        self.shared.log.lock().unwrap().clone()
    }

    fn count(&self, kind: OpKind) -> usize {
        self.log().iter().filter(|r| r.op.kind() == kind).count()
    }

    /// Sends one event to every subscription on a live connection.
    fn emit(&self, body: EventBody) {
        for (stream, sub) in self.shared.subs.lock().unwrap().iter_mut() {
            let event = Event {
                sub: *sub,
                body: body.clone(),
            };
            assert!(send(stream, &ServerMsg::Event(event)));
            stream.flush().unwrap();
        }
    }

    fn endpoint(&self) -> Endpoint {
        Endpoint::tcp(self.addr).expect("loopback")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        for stream in self.shared.streams.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

/// Waits for `done`, however slow the machine is. Fails only if it never happens.
fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(2));
    }
}

type Hook = Box<dyn FnMut(usize) + Send>;

/// A clock that never sleeps. It records each delay it was asked for, and runs a hook with the
/// number of delays so far, which is where a test brings the server back.
#[derive(Default)]
struct FakeClock {
    delays: Mutex<Vec<Duration>>,
    hook: Mutex<Option<Hook>>,
}

impl FakeClock {
    fn on_sleep(&self, hook: impl FnMut(usize) + Send + 'static) {
        *self.hook.lock().unwrap() = Some(Box::new(hook));
    }

    fn delays(&self) -> Vec<Duration> {
        self.delays.lock().unwrap().clone()
    }
}

impl Clock for FakeClock {
    fn sleep(&self, delay: Duration) {
        let count = {
            let mut delays = self.delays.lock().unwrap();
            delays.push(delay);
            delays.len()
        };
        if let Some(hook) = self.hook.lock().unwrap().as_mut() {
            hook(count);
        }
        // A real pause, far shorter than any delay asked for, so a client that is left
        // retrying at the end of a test does not spin.
        thread::sleep(Duration::from_millis(1));
    }
}

fn reconnecting(clock: &Arc<FakeClock>) -> Options {
    Options {
        reconnect: true,
        clock: Arc::clone(clock) as Arc<dyn Clock>,
    }
}

fn tick(tick: u64) -> EventBody {
    EventBody::Telemetry(TelemetryEvent {
        run: 7,
        tick,
        step_calls: 1,
        faults: 0,
        skipped: 0,
    })
}

#[test]
fn every_op_has_a_typed_method() {
    let server = Server::start();
    let client = Client::connect(server.endpoint(), Options::default()).expect("connect");
    assert!(client.is_connected());
    assert_eq!(client.connects(), 1);
    assert!(
        client
            .server()
            .expect("hello")
            .capabilities
            .serves(OpKind::Ping)
    );

    let tools = client.in_session("tools").expect("session");
    assert_eq!(tools.ping().unwrap(), SERVICE_VERSION);
    assert_eq!(tools.load_plugin("libdemo.so").unwrap().name, "libdemo.so");
    assert!(tools.put_graph("g", "{}").unwrap().is_empty());
    assert!(tools.patch_graph("g", "{}").unwrap().is_empty());
    assert_eq!(tools.plan("g", PlanOptions::default()).unwrap().hash, 9);
    let run = tools.start("g", StartConfig::default()).unwrap();
    assert_eq!(run, 7);
    let value = TypedValue::new(
        WireType::Scalar(daedalus_types::Scalar::U32),
        WireValue::U32(41),
    );
    assert_eq!(tools.feed(run, "in", value).unwrap().woke, 1);
    assert_eq!(tools.step(run, 5).unwrap().ticks, 5);
    let taken = tools.take(run, "out").unwrap().expect("a value");
    assert_eq!(taken.value, WireValue::U32(42));
    let watch = tools.watch(WatchTarget::Run(run), EventKinds::ALL).unwrap();
    tools.reload(run, "g").unwrap();
    tools.cancel(run).unwrap();
    assert_eq!(tools.stop(run, StopMode::Drain).unwrap().tick, 3);
    assert!(matches!(
        tools.inspect(InspectTarget::Run(run)).unwrap(),
        Inspection::Cache(_)
    ));
    assert_eq!(tools.cache_inspect().unwrap().misses, 3);
    tools.cache_clear().unwrap();
    assert!(tools.trace(10).unwrap().is_empty());
    drop(watch);
    until("the unwatch", || server.count(OpKind::Unwatch) == 1);

    // Every op kind went over the wire, each in the session the handle names.
    let log = server.log();
    for kind in OpKind::ALL {
        assert!(log.iter().any(|r| r.op.kind() == kind), "{}", kind.name());
    }
    assert!(log.iter().all(|request| request.session == "tools"));
    assert_eq!(client.session(), "default");

    // A refusal from the server is a typed error with its code.
    match client.step(404, 1) {
        Err(ClientError::Service(error)) => assert_eq!(error.code, ErrorCode::UnknownRun),
        other => panic!("expected unknown_run, got {other:?}"),
    }
    assert!(matches!(
        client.in_session(&"a".repeat(65)),
        Err(ClientError::BadSession(65))
    ));
}

#[test]
fn pipelined_requests_are_matched_by_id() {
    let server = Server::start();
    server.set_mode(Mode::Reverse(4));
    let client = Client::connect(server.endpoint(), Options::default()).expect("connect");
    // Four requests go out before any answer is read. The server answers them newest first.
    let pending: Vec<_> = (1..=4)
        .map(|count| client.send(Op::Step { run: 7, count }).expect("send"))
        .collect();
    for (at, pending) in pending.into_iter().enumerate() {
        match pending.wait().expect("answer") {
            Reply::Stepped(summary) => assert_eq!(summary.ticks, at as u32 + 1),
            other => panic!("unexpected {other:?}"),
        }
    }
    // The same from several threads at once.
    server.set_mode(Mode::Answer);
    let threads: Vec<_> = (10..18)
        .map(|count| {
            let client = client.clone();
            thread::spawn(move || client.step(7, count).expect("step").ticks)
        })
        .collect();
    let got: Vec<u32> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(got, (10..18).collect::<Vec<u32>>());
}

#[test]
fn a_server_with_another_layout_is_refused() {
    let server = Server::with_layout(SERVICE_LAYOUT_HASH ^ 1);
    match Client::connect(server.endpoint(), Options::default()) {
        Err(ClientError::Refused(WireError::Layout { expected, found })) => {
            assert_eq!(expected, SERVICE_LAYOUT_HASH);
            assert_eq!(found, SERVICE_LAYOUT_HASH ^ 1);
        }
        Ok(_) => panic!("the server should have been refused"),
        Err(other) => panic!("expected a layout refusal, got {other:?}"),
    }
}

#[test]
fn an_address_that_is_not_loopback_is_refused() {
    let addr: SocketAddr = "192.0.2.1:4100".parse().unwrap();
    assert!(matches!(Endpoint::tcp(addr), Err(ClientError::NotLoopback(a)) if a == addr));
    assert!(Endpoint::tcp("[::1]:4100".parse().unwrap()).is_ok());
}

#[test]
fn a_watch_is_a_stream_that_ends_on_ended() {
    let server = Server::start();
    let client = Client::connect(server.endpoint(), Options::default()).expect("connect");
    let watch = client.watch(WatchTarget::Run(7), EventKinds::ALL).unwrap();
    server.emit(tick(1));
    server.emit(tick(2));
    server.emit(EventBody::Ended {
        run: 7,
        reason: EndReason::Stopped,
    });
    let id = watch.id();
    let events: Vec<Event> = watch.collect();
    assert_eq!(events.len(), 3);
    assert!(events.iter().all(|event| event.sub == id));
    assert_eq!(events[0].body, tick(1));
    assert_eq!(events[1].body, tick(2));
    assert!(matches!(events[2].body, EventBody::Ended { run: 7, .. }));
}

#[test]
fn a_watch_ends_on_disconnect_when_the_client_does_not_reconnect() {
    let server = Server::start();
    let client = Client::connect(server.endpoint(), Options::default()).expect("connect");
    let watch = client.watch(WatchTarget::All, EventKinds::FAULT).unwrap();
    server.emit(tick(1));
    assert_eq!(watch.next_event().expect("event").body, tick(1));
    server.kill();
    assert_eq!(watch.next_event(), None);
    until("the disconnect", || !client.is_connected());
    assert!(matches!(client.ping(), Err(ClientError::Disconnected)));
    assert_eq!(client.connects(), 1);
}

#[test]
fn reconnect_backs_off_to_the_8_s_cap_and_resubscribes_watches() {
    let server = Arc::new(Server::start());
    let clock = Arc::new(FakeClock::default());
    let client = Client::connect(server.endpoint(), reconnecting(&clock)).expect("connect");
    let watch = client.watch(WatchTarget::All, EventKinds::ALL).unwrap();
    server.emit(tick(1));
    assert_eq!(watch.next_event().expect("event").body, tick(1));

    // The server comes back during the ninth wait. Until then every attempt is refused, and a
    // call made while disconnected fails at once.
    let during = Arc::new(Mutex::new(Vec::new()));
    {
        let (server, client, during) = (Arc::clone(&server), client.clone(), Arc::clone(&during));
        clock.on_sleep(move |count| {
            during.lock().unwrap().push(client.ping().is_err());
            if count == 9 {
                server.restart();
            }
        });
    }
    server.kill();
    until("the reconnect", || client.connects() == 2);

    let delays: Vec<u64> = clock
        .delays()
        .iter()
        .map(|d| d.as_millis() as u64)
        .collect();
    assert_eq!(delays, [250, 500, 1000, 2000, 4000, 8000, 8000, 8000, 8000]);
    assert!(clock.delays().iter().all(|delay| *delay <= BACKOFF_CAP));
    assert_eq!(*during.lock().unwrap(), [true; 9]);

    // The watch was subscribed again without the caller doing anything, and its stream goes on
    // under the same number.
    until("the resubscription", || server.count(OpKind::Watch) == 2);
    until("the subscription on the new connection", || {
        !server.shared.subs.lock().unwrap().is_empty()
    });
    assert_eq!(client.ping().unwrap(), SERVICE_VERSION);
    server.emit(tick(2));
    let event = watch.next_event().expect("event after the reconnect");
    assert_eq!(event.sub, watch.id());
    assert_eq!(event.body, tick(2));

    // A later disconnect starts the backoff from the beginning.
    clock.on_sleep({
        let server = Arc::clone(&server);
        move |_| server.restart()
    });
    server.kill();
    until("the second reconnect", || client.connects() == 3);
    assert_eq!(clock.delays().last(), Some(&Duration::from_millis(250)));

    // Turned off by hand, the client stops trying and the stream ends.
    client.set_reconnect(false);
    clock.on_sleep(|_| {});
    server.kill();
    assert_eq!(watch.next_event(), None);
    assert!(matches!(client.ping(), Err(ClientError::Disconnected)));
}

#[test]
fn a_request_in_flight_at_a_disconnect_is_unknown_delivery_and_is_not_resent() {
    let server = Arc::new(Server::start());
    let clock = Arc::new(FakeClock::default());
    let client = Client::connect(server.endpoint(), reconnecting(&clock)).expect("connect");
    server.set_mode(Mode::Hold);
    let step = client.send(Op::Step { run: 7, count: 1 }).expect("send");
    let ping = client.send(Op::Ping).expect("send");
    let inspect = client
        .send(Op::Inspect {
            target: InspectTarget::Cache,
        })
        .expect("send");
    until("the server to read all three", || server.log().len() == 3);

    clock.on_sleep({
        let server = Arc::clone(&server);
        move |count| {
            if count == 3 {
                server.set_mode(Mode::Answer);
                server.restart();
            }
        }
    });
    server.kill();

    // The step may or may not have run: the client says so and does not send it again.
    assert!(matches!(step.wait(), Err(ClientError::UnknownDelivery)));
    // The ping and the inspect are harmless to repeat: they are resent and answered.
    assert!(matches!(ping.wait(), Ok(Reply::Pong { .. })));
    assert!(matches!(inspect.wait(), Ok(Reply::Inspection(_))));

    let log = server.log();
    assert_eq!(server.count(OpKind::Step), 1, "the step was sent once");
    assert_eq!(server.count(OpKind::Ping), 2);
    assert_eq!(server.count(OpKind::Inspect), 2);
    // A resent request keeps its id.
    let pings: Vec<u64> = log
        .iter()
        .filter(|r| r.op.kind() == OpKind::Ping)
        .map(|r| r.id)
        .collect();
    assert_eq!(pings[0], pings[1]);

    // Without reconnecting, nothing is resent: every in-flight request is settled at once.
    client.set_reconnect(false);
    let once = Client::connect(server.endpoint(), Options::default()).expect("connect");
    server.set_mode(Mode::Hold);
    let before = server.log().len();
    let step = once.send(Op::Step { run: 7, count: 1 }).expect("send");
    let ping = once.send(Op::Ping).expect("send");
    until("the server to read both", || {
        server.log().len() == before + 2
    });
    server.kill();
    assert!(matches!(step.wait(), Err(ClientError::UnknownDelivery)));
    assert!(matches!(ping.wait(), Err(ClientError::Disconnected)));
}

#[test]
fn the_backoff_never_exceeds_the_cap() {
    let mut backoff = Backoff::new();
    let mut last = Duration::ZERO;
    for _ in 0..200 {
        let delay = backoff.failed();
        assert!(delay >= last && delay <= BACKOFF_CAP);
        last = delay;
    }
    assert_eq!(last, BACKOFF_CAP);
    assert_eq!(Backoff::delay_for(u32::MAX), BACKOFF_CAP);
    backoff.succeeded();
    assert_eq!(backoff.failures(), 0);
    assert_eq!(backoff.failed(), Duration::from_millis(250));
}

#[cfg(unix)]
#[test]
fn a_unix_socket_is_the_default_endpoint() {
    use std::os::unix::net::UnixListener;

    let dir = std::env::temp_dir().join(format!("daedalus-client-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("unix.sock");
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).expect("bind");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buf = Vec::new();
        read_packet(&mut stream, DEFAULT_CAP, &mut buf).expect("hello");
        let hello = ServerMsg::Hello(ServiceHello::server(Capabilities::all_ops()));
        let body = hello.encode(&Schema::new()).unwrap();
        write_packet(&mut stream, &body, DEFAULT_CAP).unwrap();
        read_packet(&mut stream, DEFAULT_CAP, &mut buf).expect("request");
        let Ok(ClientMsg::Request(request)) = ClientMsg::decode(&buf, &Schema::new()) else {
            panic!("expected a request");
        };
        let response = ServerMsg::Response(Response {
            id: request.id,
            result: Ok(Reply::Pong {
                version: SERVICE_VERSION,
            }),
        });
        let body = response.encode(&Schema::new()).unwrap();
        write_packet(&mut stream, &body, DEFAULT_CAP).unwrap();
    });
    let client = Client::connect(Endpoint::unix(&path), Options::default()).expect("connect");
    assert_eq!(client.ping().unwrap(), SERVICE_VERSION);
    assert_eq!(
        client.endpoint().to_string(),
        format!("unix:{}", path.display())
    );
    server.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
