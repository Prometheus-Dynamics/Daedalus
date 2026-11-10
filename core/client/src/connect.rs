//! The connection: the hello, the reader thread, the pending requests, and what happens to them
//! when the connection drops.

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use daedalus_wire::service::{
    ClientMsg, DEFAULT_SESSION, EndReason, Event, EventBody, EventKinds, Op, Reply, Request,
    Response, ServerMsg, ServiceError, ServiceHello, WatchTarget,
};
use daedalus_wire::{DEFAULT_CAP, PacketError, Schema, WireError, read_packet, write_packet};

use crate::reconnect::{Backoff, Clock, SystemClock};
use crate::watch::Watch;

/// Where the server listens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    /// A unix socket. The default kind.
    #[cfg(unix)]
    Unix(PathBuf),
    /// TCP on the loopback interface. No other address is accepted.
    Tcp(SocketAddr),
}

impl Endpoint {
    /// A TCP endpoint. An address that is not loopback is refused: the service does not listen on
    /// the network, and the client does not reach across it.
    pub fn tcp(addr: SocketAddr) -> Result<Endpoint, ClientError> {
        if !addr.ip().is_loopback() {
            return Err(ClientError::NotLoopback(addr));
        }
        Ok(Endpoint::Tcp(addr))
    }

    #[cfg(unix)]
    pub fn unix(path: impl Into<PathBuf>) -> Endpoint {
        Endpoint::Unix(path.into())
    }

    fn open(&self) -> io::Result<Channel> {
        match self {
            #[cfg(unix)]
            Endpoint::Unix(path) => {
                let stream = UnixStream::connect(path)?;
                let (reader, closer) = (stream.try_clone()?, stream.try_clone()?);
                Ok(Channel {
                    reader: Box::new(reader),
                    writer: Box::new(stream),
                    close: Box::new(move || {
                        let _ = closer.shutdown(Shutdown::Both);
                    }),
                })
            }
            Endpoint::Tcp(addr) => {
                let stream = TcpStream::connect(addr)?;
                let _ = stream.set_nodelay(true);
                let (reader, closer) = (stream.try_clone()?, stream.try_clone()?);
                Ok(Channel {
                    reader: Box::new(reader),
                    writer: Box::new(stream),
                    close: Box::new(move || {
                        let _ = closer.shutdown(Shutdown::Both);
                    }),
                })
            }
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(unix)]
            Endpoint::Unix(path) => write!(f, "unix:{}", path.display()),
            Endpoint::Tcp(addr) => write!(f, "tcp:{addr}"),
        }
    }
}

struct Channel {
    reader: Box<dyn Read + Send>,
    writer: Box<dyn Write + Send>,
    close: Box<dyn Fn() + Send>,
}

/// How a client behaves.
#[derive(Clone)]
pub struct Options {
    /// Keep trying to reconnect after a disconnect. Off by default: a command line tool wants the
    /// error, and a long-lived client turns this on.
    pub reconnect: bool,
    /// What the client waits on between attempts.
    pub clock: Arc<dyn Clock>,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            reconnect: false,
            clock: Arc::new(SystemClock),
        }
    }
}

impl Options {
    /// Options for a client that reconnects.
    pub fn reconnecting() -> Options {
        Options {
            reconnect: true,
            ..Options::default()
        }
    }
}

/// Why a call failed.
#[derive(Debug)]
pub enum ClientError {
    /// There is no connection. Nothing was sent.
    Disconnected,
    /// The connection dropped after the request was written and before its answer arrived. The
    /// server may or may not have acted on it, and the client did not send it again.
    UnknownDelivery,
    /// The server refused the request.
    Service(ServiceError),
    /// The server speaks another protocol version or layout.
    Refused(WireError),
    /// A message could not be encoded or decoded.
    Wire(WireError),
    /// A request larger than the server reads.
    TooLarge {
        len: usize,
        cap: u32,
    },
    Io(io::Error),
    /// A TCP address that is not on the loopback interface.
    NotLoopback(SocketAddr),
    /// A session name that is empty or over the bound.
    BadSession(usize),
    /// The server answered with a reply that does not belong to the op.
    Unexpected {
        op: &'static str,
        reply: String,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Disconnected => f.write_str("disconnected"),
            ClientError::UnknownDelivery => {
                f.write_str("unknown_delivery: the connection dropped before the answer")
            }
            ClientError::Service(error) => error.fmt(f),
            ClientError::Refused(error) => write!(f, "the server was refused: {error}"),
            ClientError::Wire(error) => error.fmt(f),
            ClientError::TooLarge { len, cap } => {
                write!(
                    f,
                    "a request of {len} bytes is over the server's cap of {cap}"
                )
            }
            ClientError::Io(error) => error.fmt(f),
            ClientError::NotLoopback(addr) => write!(f, "{addr} is not a loopback address"),
            ClientError::BadSession(len) => {
                write!(f, "a session name of {len} bytes is empty or too long")
            }
            ClientError::Unexpected { op, reply } => {
                write!(f, "{op} was answered with {reply}")
            }
        }
    }
}

impl std::error::Error for ClientError {}

type Answer = Result<Reply, ClientError>;

/// A watch the client holds, so it can subscribe it again.
struct WatchEntry {
    target: WatchTarget,
    kinds: EventKinds,
    session: Arc<str>,
    events: Sender<Event>,
    /// The server's number for the subscription on this connection.
    sub: Option<u64>,
}

/// A request waiting for its answer.
struct Waiter {
    answer: Option<Sender<Answer>>,
    /// Kept for a request that may be sent again.
    resend: Option<Request>,
    /// The client watch this request subscribes.
    watch: Option<u64>,
}

struct State {
    link: Option<Link>,
    codecs: Schema,
    cap: u32,
    hello: Option<ServiceHello>,
    pending: HashMap<u64, Waiter>,
    watches: HashMap<u64, WatchEntry>,
    /// Server subscription numbers to client watch numbers, for this connection.
    subs: HashMap<u64, u64>,
    connects: u64,
}

struct Link {
    writer: Box<dyn Write + Send>,
    close: Box<dyn Fn() + Send>,
}

struct Inner {
    endpoint: Endpoint,
    options: Options,
    state: Mutex<State>,
    next_id: AtomicU64,
    closing: AtomicBool,
    reconnect: AtomicBool,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn keeps_trying(&self) -> bool {
        self.reconnect.load(Ordering::Relaxed) && !self.closing.load(Ordering::Relaxed)
    }
}

/// Closes the connection when the last client handle goes.
struct Handle {
    inner: Arc<Inner>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.inner.closing.store(true, Ordering::Relaxed);
        if let Some(link) = self.inner.lock().link.take() {
            (link.close)();
        }
    }
}

/// A connection to a server. Cloning it gives another handle to the same connection, and the
/// connection closes when the last handle is dropped.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
    _handle: Arc<Handle>,
    session: Arc<str>,
}

/// Runs the hello on a fresh channel. Returns the server's hello.
fn hello(channel: &mut Channel) -> Result<ServiceHello, ClientError> {
    let body = ClientMsg::Hello(ServiceHello::client())
        .encode(&Schema::new())
        .map_err(ClientError::Wire)?;
    write_packet(&mut channel.writer, &body, DEFAULT_CAP).map_err(packet_error)?;
    let mut buf = Vec::new();
    read_packet(&mut channel.reader, DEFAULT_CAP, &mut buf).map_err(packet_error)?;
    match ServerMsg::decode(&buf, &Schema::new()).map_err(ClientError::Wire)? {
        ServerMsg::Hello(hello) => {
            hello.check().map_err(ClientError::Refused)?;
            Ok(hello)
        }
        // A server that will not take the connection says why in place of its hello.
        ServerMsg::Response(Response {
            result: Err(error), ..
        }) => Err(ClientError::Service(error)),
        other => Err(ClientError::Unexpected {
            op: "hello",
            reply: format!("{other:?}"),
        }),
    }
}

fn packet_error(error: PacketError) -> ClientError {
    match error {
        PacketError::Closed | PacketError::Truncated => ClientError::Disconnected,
        PacketError::TooLarge { len, cap } => ClientError::TooLarge {
            len: len as usize,
            cap,
        },
        PacketError::Io(error) => ClientError::Io(error),
    }
}

impl Client {
    /// Connects and runs the hello. A server with another protocol version or layout is refused.
    pub fn connect(endpoint: Endpoint, options: Options) -> Result<Client, ClientError> {
        let mut channel = endpoint.open().map_err(ClientError::Io)?;
        let server = hello(&mut channel)?;
        let inner = Arc::new(Inner {
            endpoint,
            reconnect: AtomicBool::new(options.reconnect),
            options,
            state: Mutex::new(State {
                link: None,
                codecs: Schema::new(),
                cap: DEFAULT_CAP,
                hello: None,
                pending: HashMap::new(),
                watches: HashMap::new(),
                subs: HashMap::new(),
                connects: 0,
            }),
            next_id: AtomicU64::new(1),
            closing: AtomicBool::new(false),
        });
        let reader = adopt(&inner, channel, server);
        let thread = Arc::clone(&inner);
        std::thread::Builder::new()
            .name("daedalus-client".to_owned())
            .spawn(move || read_loop(&thread, reader))
            .map_err(ClientError::Io)?;
        Ok(Client {
            _handle: Arc::new(Handle {
                inner: Arc::clone(&inner),
            }),
            inner,
            session: Arc::from(DEFAULT_SESSION),
        })
    }

    pub(crate) fn with_session(&self, name: &str) -> Client {
        Client {
            inner: Arc::clone(&self.inner),
            _handle: Arc::clone(&self._handle),
            session: Arc::from(name),
        }
    }

    /// The session this handle's requests are in.
    pub fn session(&self) -> &str {
        &self.session
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.inner.endpoint
    }

    pub fn is_connected(&self) -> bool {
        self.inner.lock().link.is_some()
    }

    /// How many times the client has connected: one, plus one per reconnect.
    pub fn connects(&self) -> u64 {
        self.inner.lock().connects
    }

    /// The hello of the server on the current or last connection.
    pub fn server(&self) -> Option<ServiceHello> {
        self.inner.lock().hello.clone()
    }

    /// Turns reconnecting on or off by hand. Off stops the attempts after the one in progress.
    pub fn set_reconnect(&self, on: bool) {
        self.inner.reconnect.store(on, Ordering::Relaxed);
    }

    /// Closes the connection now, for every handle. Calls after this fail with `disconnected`,
    /// every watch stream ends, and the client does not reconnect.
    pub fn close(&self) {
        self.inner.closing.store(true, Ordering::Relaxed);
        if let Some(link) = self.inner.lock().link.take() {
            (link.close)();
        }
    }

    /// Sends a request without waiting for its answer, so several can be in flight at once.
    pub fn send(&self, op: Op) -> Result<Pending, ClientError> {
        let (answer, wait) = channel();
        let waiter = Waiter {
            answer: Some(answer),
            resend: None,
            watch: None,
        };
        self.send_with(op, waiter)?;
        Ok(Pending { answer: wait })
    }

    /// Sends a request and waits for its answer.
    pub fn call(&self, op: Op) -> Result<Reply, ClientError> {
        self.send(op)?.wait()
    }

    fn send_with(&self, op: Op, mut waiter: Waiter) -> Result<u64, ClientError> {
        let request = Request {
            id: self.inner.id(),
            session: self.session.to_string(),
            op,
        };
        let idempotent = request.op.kind().idempotent();
        let mut state = self.inner.lock();
        if state.link.is_none() {
            return Err(ClientError::Disconnected);
        }
        let id = request.id;
        let message = ClientMsg::Request(request);
        let body = message.encode(&state.codecs).map_err(ClientError::Wire)?;
        if body.len() > state.cap as usize {
            return Err(ClientError::TooLarge {
                len: body.len(),
                cap: state.cap,
            });
        }
        if idempotent && let ClientMsg::Request(request) = message {
            waiter.resend = Some(request);
        }
        state.pending.insert(id, waiter);
        let cap = state.cap;
        let link = state.link.as_mut().ok_or(ClientError::Disconnected)?;
        if write_packet(&mut link.writer, &body, cap).is_err() {
            // The reader sees the closed connection and settles whatever else is pending. This
            // request is settled here: part of it may have reached the server.
            (link.close)();
            state.pending.remove(&id);
            return Err(match idempotent {
                true => ClientError::Disconnected,
                false => ClientError::UnknownDelivery,
            });
        }
        Ok(id)
    }

    pub(crate) fn subscribe(
        &self,
        target: WatchTarget,
        kinds: EventKinds,
    ) -> Result<Watch, ClientError> {
        let (events, stream) = channel();
        let (answer, wait) = channel();
        let watch = self.inner.id();
        self.inner.lock().watches.insert(
            watch,
            WatchEntry {
                target,
                kinds,
                session: Arc::clone(&self.session),
                events,
                sub: None,
            },
        );
        let waiter = Waiter {
            answer: Some(answer),
            resend: None,
            watch: Some(watch),
        };
        let sent = self
            .send_with(Op::Watch { target, kinds }, waiter)
            .and_then(|_| Pending { answer: wait }.wait());
        match sent {
            Ok(Reply::Watching { .. }) => Ok(Watch {
                id: watch,
                events: stream,
                client: self.clone(),
            }),
            Ok(other) => {
                self.inner.lock().watches.remove(&watch);
                Err(ClientError::Unexpected {
                    op: "watch",
                    reply: format!("{other:?}"),
                })
            }
            Err(error) => {
                self.inner.lock().watches.remove(&watch);
                Err(error)
            }
        }
    }

    /// Ends a watch: forgets it here, and tells the server if there is a connection. The answer
    /// is not waited for.
    pub(crate) fn unwatch(&self, watch: u64) {
        let sub = {
            let mut state = self.inner.lock();
            let Some(entry) = state.watches.remove(&watch) else {
                return;
            };
            if let Some(sub) = entry.sub {
                state.subs.remove(&sub);
            }
            entry.sub
        };
        if let Some(sub) = sub {
            let waiter = Waiter {
                answer: None,
                resend: None,
                watch: None,
            };
            let _ = self.send_with(Op::Unwatch { sub }, waiter);
        }
    }
}

/// A request that was sent and not yet answered.
pub struct Pending {
    answer: Receiver<Answer>,
}

impl Pending {
    /// Waits for the answer.
    pub fn wait(self) -> Result<Reply, ClientError> {
        self.answer.recv().unwrap_or(Err(ClientError::Disconnected))
    }

    /// Waits up to `timeout`. Gives the request back when nothing arrived in time.
    pub fn wait_timeout(self, timeout: Duration) -> Result<Result<Reply, ClientError>, Pending> {
        match self.answer.recv_timeout(timeout) {
            Ok(answer) => Ok(answer),
            Err(RecvTimeoutError::Disconnected) => Ok(Err(ClientError::Disconnected)),
            Err(RecvTimeoutError::Timeout) => Err(self),
        }
    }
}

/// Makes `channel` the client's connection: subscribes its watches again and resends what may be
/// resent. Returns the half the reader thread reads.
fn adopt(inner: &Inner, channel: Channel, server: ServiceHello) -> Box<dyn Read + Send> {
    let mut state = inner.lock();
    state.codecs = server.codecs();
    state.cap = server.packet_cap.min(DEFAULT_CAP);
    state.hello = Some(server);
    state.connects += 1;
    state.subs.clear();
    let mut link = Link {
        writer: channel.writer,
        close: channel.close,
    };
    let mut outgoing = Vec::new();
    // Every watch the client still holds is subscribed again, under a new request id.
    let watches: Vec<u64> = state.watches.keys().copied().collect();
    for watch in watches {
        let Some(entry) = state.watches.get_mut(&watch) else {
            continue;
        };
        entry.sub = None;
        let request = Request {
            id: inner.id(),
            session: entry.session.to_string(),
            op: Op::Watch {
                target: entry.target,
                kinds: entry.kinds,
            },
        };
        state.pending.insert(
            request.id,
            Waiter {
                answer: None,
                resend: None,
                watch: Some(watch),
            },
        );
        outgoing.push(request);
    }
    // What was in flight and may be sent twice goes out again, under its old id.
    outgoing.extend(
        state
            .pending
            .values()
            .filter_map(|waiter| waiter.resend.clone()),
    );
    let (codecs, cap) = (state.codecs.clone(), state.cap);
    for request in outgoing {
        let sent = ClientMsg::Request(request)
            .encode(&codecs)
            .ok()
            .is_some_and(|body| write_packet(&mut link.writer, &body, cap).is_ok());
        if !sent {
            // The reader finds the connection closed and starts over.
            (link.close)();
            break;
        }
    }
    state.link = Some(link);
    channel.reader
}

/// The reader thread: delivers answers and events, and reconnects for as long as it is asked to.
fn read_loop(inner: &Arc<Inner>, mut reader: Box<dyn Read + Send>) {
    let mut buf = Vec::new();
    let mut backoff = Backoff::new();
    loop {
        while read_packet(&mut reader, DEFAULT_CAP, &mut buf).is_ok() {
            let codecs = inner.lock().codecs.clone();
            match ServerMsg::decode(&buf, &codecs) {
                Ok(ServerMsg::Response(response)) => answer(inner, response),
                Ok(ServerMsg::Event(event)) => deliver(inner, event),
                // A second hello or a body that does not decode: the connection cannot be
                // trusted to stay in step, so it is dropped.
                Ok(ServerMsg::Hello(_)) | Err(_) => break,
            }
        }
        drop_link(inner);
        loop {
            if !inner.keeps_trying() {
                end(inner);
                return;
            }
            inner.options.clock.sleep(backoff.failed());
            if !inner.keeps_trying() {
                end(inner);
                return;
            }
            let Ok(mut channel) = inner.endpoint.open() else {
                continue;
            };
            // A server that is refused may be replaced by one that is not: keep trying.
            let Ok(server) = hello(&mut channel) else {
                (channel.close)();
                continue;
            };
            backoff.succeeded();
            reader = adopt(inner, channel, server);
            break;
        }
    }
}

/// The connection is gone: closes it and settles every request that cannot be sent again.
fn drop_link(inner: &Inner) {
    let keep = inner.keeps_trying();
    let mut state = inner.lock();
    if let Some(link) = state.link.take() {
        (link.close)();
    }
    state.subs.clear();
    state.pending.retain(|_, waiter| {
        if keep && waiter.resend.is_some() {
            return true;
        }
        if let Some(answer) = waiter.answer.take() {
            let error = match waiter.resend {
                Some(_) => ClientError::Disconnected,
                None => ClientError::UnknownDelivery,
            };
            let _ = answer.send(Err(error));
        }
        false
    });
}

/// The client stops for good: every watch stream ends and nothing stays pending.
fn end(inner: &Inner) {
    let mut state = inner.lock();
    state.watches.clear();
    state.subs.clear();
    for (_, mut waiter) in state.pending.drain() {
        if let Some(answer) = waiter.answer.take() {
            let _ = answer.send(Err(ClientError::Disconnected));
        }
    }
}

fn answer(inner: &Inner, response: Response) {
    let mut state = inner.lock();
    let Some(mut waiter) = state.pending.remove(&response.id) else {
        return;
    };
    if let Some(watch) = waiter.watch {
        match &response.result {
            Ok(Reply::Watching { sub }) => {
                // Recorded before the caller hears of it, and before the next packet is read, so
                // no event of this subscription can arrive unknown.
                if let Some(entry) = state.watches.get_mut(&watch) {
                    entry.sub = Some(*sub);
                    state.subs.insert(*sub, watch);
                }
            }
            // The server no longer has what the watch followed. Its stream ends.
            _ => {
                state.watches.remove(&watch);
            }
        }
    }
    if let Some(answer) = waiter.answer.take() {
        let _ = answer.send(response.result.map_err(ClientError::Service));
    }
}

fn deliver(inner: &Inner, event: Event) {
    let mut state = inner.lock();
    let Some(&watch) = state.subs.get(&event.sub) else {
        return;
    };
    let Some(entry) = state.watches.get(&watch) else {
        return;
    };
    // The last event of a subscription: the run a single-run watch follows ended, or the watch
    // itself did.
    let last = match (&event.body, entry.target) {
        (EventBody::Ended { .. }, WatchTarget::Run(_)) => true,
        (EventBody::Ended { run: 0, .. }, WatchTarget::All) => true,
        (EventBody::Ended { reason, .. }, WatchTarget::All) => {
            matches!(reason, EndReason::Unwatched | EndReason::Shutdown)
        }
        _ => false,
    };
    let gone = entry
        .events
        .send(Event {
            sub: watch,
            body: event.body,
        })
        .is_err();
    if last || gone {
        state.watches.remove(&watch);
        state.subs.remove(&event.sub);
    }
}
