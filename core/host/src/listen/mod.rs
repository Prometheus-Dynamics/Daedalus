//! The service listener: one host shared by every client that connects. Clients reach it over a
//! unix socket, over TCP on the loopback interface, or over stdio. Nothing listens on the network.
//!
//! Graphs and runs belong to the host, not to a connection. A client that disconnects leaves its
//! runs running, and another client can step them, read them and stop them. Only its watches end.
//!
//! A step never waits on a client. Answers and events are queued per session and written by that
//! session's own thread. A client that stops reading is disconnected once its queue is full, or
//! once a write to it has blocked for longer than the write timeout.

mod dispatch;
mod runs;
mod session;
mod stdio;
mod tcp;
#[cfg(unix)]
mod unix;
mod values;
mod watch;

use std::boxed::Box;
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::Path;
use std::string::String;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::vec::Vec;

use daedalus_backend::Backend;
use daedalus_graph::GraphBody;
use daedalus_planner::cache::PlanCache;
use daedalus_registry::Registry;
use daedalus_wire::Schema;
use daedalus_wire::service::{
    Capabilities, EndReason, ErrorCode, OpKind, PlanSummary, PluginInfo, RunId, ServiceError,
    ServiceHello, TraceRing,
};

pub use session::Channel;
pub use tcp::TcpAcceptor;
#[cfg(unix)]
pub use unix::UnixAcceptor;
pub use values::{FeedRefusal, WireCodec, WireCodecs};

use runs::RunEntry;
use session::Session;
use watch::Watchers;

/// The longest wait between two attempts to accept.
pub const ACCEPT_BACKOFF_CAP: Duration = Duration::from_secs(8);
/// The first wait after a failed accept.
pub(crate) const ACCEPT_BACKOFF_START: Duration = Duration::from_millis(50);
/// How many connections a host serves at once, unless its options say otherwise.
pub(crate) const DEFAULT_CONNECTION_LIMIT: usize = 64;

/// The wait after `failures` failed accepts in a row. It doubles and stops growing at 8 s.
pub fn accept_backoff(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    (ACCEPT_BACKOFF_START * (1 << doublings)).min(ACCEPT_BACKOFF_CAP)
}

/// What a listening host is told.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListenOptions {
    /// Most connections served at once. One more is told the host is busy, and closed.
    pub connections: usize,
    /// Most runs held at once. Zero is no limit.
    pub max_runs: u32,
    /// Most answers that may wait for one client to read. A client past this is disconnected.
    pub outbound_queue: usize,
    /// How long a write to a client may block before the client is disconnected.
    pub write_timeout: Duration,
    /// How many plans the cache keeps.
    pub plan_cache: usize,
    /// Most ticks a draining stop runs while it waits for the run to go idle.
    pub drain_ticks: u32,
}

impl Default for ListenOptions {
    fn default() -> ListenOptions {
        ListenOptions {
            connections: DEFAULT_CONNECTION_LIMIT,
            max_runs: 0,
            outbound_queue: 256,
            write_timeout: Duration::from_secs(10),
            plan_cache: 32,
            drain_ticks: 10_000,
        }
    }
}

/// Loads a plugin for a host. The daemon supplies one over the loader. A host without one
/// answers `load_plugin` with `unsupported`.
pub trait PluginLoader: Send {
    /// Loads the library at `path`, joins its nodes into `registry` and adds its backends.
    fn load(
        &mut self,
        path: &str,
        registry: &mut Registry,
        backends: &mut Vec<Arc<dyn Backend>>,
    ) -> Result<PluginInfo, String>;
}

/// Where connections come from. The unix and TCP transports implement it, and a test can too.
pub trait Acceptor: Send + 'static {
    /// Waits for the next connection.
    fn accept(&mut self) -> io::Result<Channel>;
    /// Something that makes a blocked `accept` return, by connecting to it.
    fn waker(&self) -> Box<dyn Fn() + Send + Sync>;
    /// The address, for logs.
    fn describe(&self) -> String;
}

/// What planning needs, and what it stores.
pub(crate) struct Engine {
    pub(crate) registry: Registry,
    pub(crate) backends: Vec<Arc<dyn Backend>>,
    pub(crate) plugins: Option<Box<dyn PluginLoader>>,
    pub(crate) loaded: Vec<String>,
    /// Stored graphs, by session and then by name.
    pub(crate) graphs: BTreeMap<String, BTreeMap<String, GraphBody>>,
    pub(crate) cache: PlanCache<PlanSummary>,
}

/// One host, shared by every session of every listener opened on it.
pub struct ServiceHost {
    pub(crate) engine: Mutex<Engine>,
    pub(crate) runs: Mutex<BTreeMap<RunId, Arc<RunEntry>>>,
    pub(crate) watchers: Mutex<Watchers>,
    pub(crate) trace: Mutex<TraceRing>,
    sessions: Mutex<BTreeMap<u64, Arc<Session>>>,
    wakers: Mutex<Vec<Box<dyn Fn() + Send + Sync>>>,
    next_run: AtomicU64,
    next_session: AtomicU64,
    stopping: AtomicBool,
    options: ListenOptions,
    pub(crate) codecs: WireCodecs,
    serves_plugins: bool,
    started: Instant,
}

/// Collects what a host is made of.
pub struct ServiceHostBuilder {
    registry: Registry,
    backends: Vec<Arc<dyn Backend>>,
    plugins: Option<Box<dyn PluginLoader>>,
    codecs: WireCodecs,
    options: ListenOptions,
}

impl ServiceHostBuilder {
    /// Adds a backend. Backends are tried in the order they are added, unless a request names a
    /// priority.
    pub fn backend(mut self, backend: Arc<dyn Backend>) -> ServiceHostBuilder {
        self.backends.push(backend);
        self
    }

    pub fn plugins(mut self, loader: Box<dyn PluginLoader>) -> ServiceHostBuilder {
        self.plugins = Some(loader);
        self
    }

    /// Replaces the value codecs. The default is every scalar.
    pub fn codecs(mut self, codecs: WireCodecs) -> ServiceHostBuilder {
        self.codecs = codecs;
        self
    }

    pub fn options(mut self, options: ListenOptions) -> ServiceHostBuilder {
        self.options = options;
        self
    }

    pub fn build(self) -> Arc<ServiceHost> {
        Arc::new(ServiceHost {
            serves_plugins: self.plugins.is_some(),
            engine: Mutex::new(Engine {
                registry: self.registry,
                backends: self.backends,
                plugins: self.plugins,
                loaded: Vec::new(),
                graphs: BTreeMap::new(),
                cache: PlanCache::new(self.options.plan_cache.max(1)),
            }),
            runs: Mutex::new(BTreeMap::new()),
            watchers: Mutex::new(Watchers::default()),
            trace: Mutex::new(TraceRing::new()),
            sessions: Mutex::new(BTreeMap::new()),
            wakers: Mutex::new(Vec::new()),
            next_run: AtomicU64::new(1),
            next_session: AtomicU64::new(1),
            stopping: AtomicBool::new(false),
            options: self.options,
            codecs: self.codecs,
            started: Instant::now(),
        })
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A dispatch that panicked is answered with an error, and the host goes on: what it held
    // is used as it was left.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An open listener. Dropping it does not close it: the host does, when it shuts down.
pub struct Listener {
    address: String,
    thread: Option<JoinHandle<()>>,
}

impl Listener {
    /// Where it listens, as `unix:<path>` or `tcp:<address>`.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Waits for the accept loop to end, which it does when the host shuts down.
    pub fn join(mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl ServiceHost {
    /// A host that plans against `registry`.
    pub fn builder(registry: Registry) -> ServiceHostBuilder {
        ServiceHostBuilder {
            registry,
            backends: Vec::new(),
            plugins: None,
            codecs: WireCodecs::scalars(),
            options: ListenOptions::default(),
        }
    }

    pub fn options(&self) -> &ListenOptions {
        &self.options
    }

    /// The hello this host sends: every op it answers, its codec keys and its run limit.
    pub fn hello(&self) -> ServiceHello {
        let ops = OpKind::ALL
            .into_iter()
            .filter(|op| self.serves_plugins || *op != OpKind::LoadPlugin)
            .collect();
        ServiceHello::server(Capabilities {
            ops,
            codecs: self.codecs.opaque_keys(),
            max_runs: self.options.max_runs,
        })
    }

    pub(crate) fn codec_schema(&self) -> Schema {
        self.hello().codecs()
    }

    pub(crate) fn next_session(&self) -> u64 {
        self.next_session.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn next_run(&self) -> RunId {
        self.next_run.fetch_add(1, Ordering::Relaxed)
    }

    /// Nanoseconds since the host was made: the clock runs measure backoff against.
    pub(crate) fn now_ns(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    pub(crate) fn session_opened(&self, session: &Arc<Session>) {
        lock(&self.sessions).insert(session.id, Arc::clone(session));
    }

    pub(crate) fn session_closed(&self, session: &Session) {
        lock(&self.sessions).remove(&session.id);
        lock(&self.watchers).drop_session(session.id);
    }

    /// How many clients are connected.
    pub fn connections(&self) -> usize {
        lock(&self.sessions).len()
    }

    /// How many runs the host holds.
    pub fn run_count(&self) -> usize {
        lock(&self.runs).len()
    }

    /// How many watches are subscribed.
    pub fn watch_count(&self) -> usize {
        lock(&self.watchers).len()
    }

    /// True once [`shutdown`](ServiceHost::shutdown) has been called.
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Listens on a unix socket at `path`.
    #[cfg(unix)]
    pub fn listen_unix(self: &Arc<Self>, path: impl AsRef<Path>) -> io::Result<Listener> {
        let acceptor = UnixAcceptor::bind(path.as_ref(), Some(self.options.write_timeout))?;
        Ok(self.listen_on(acceptor, std::thread::sleep))
    }

    /// Listens on a unix socket that is already bound, as socket activation hands one over.
    #[cfg(unix)]
    pub fn listen_unix_adopted(
        self: &Arc<Self>,
        listener: std::os::unix::net::UnixListener,
    ) -> Listener {
        let acceptor = UnixAcceptor::adopt(listener, Some(self.options.write_timeout));
        self.listen_on(acceptor, std::thread::sleep)
    }

    /// Listens on a loopback TCP address. Any other address is refused.
    pub fn listen_tcp(self: &Arc<Self>, addr: SocketAddr) -> io::Result<(Listener, SocketAddr)> {
        let acceptor = TcpAcceptor::bind(addr, Some(self.options.write_timeout))?;
        let bound = acceptor.local_addr();
        Ok((self.listen_on(acceptor, std::thread::sleep), bound))
    }

    /// Listens on any acceptor. `pause` is what the loop waits with after a failed accept: the
    /// wait doubles up to 8 s, and the loop never gives up.
    pub fn listen_on(
        self: &Arc<Self>,
        mut acceptor: impl Acceptor,
        pause: impl Fn(Duration) + Send + 'static,
    ) -> Listener {
        let address = acceptor.describe();
        lock(&self.wakers).push(acceptor.waker());
        let host = Arc::clone(self);
        let thread = std::thread::Builder::new()
            .name("daedalus-accept".into())
            .spawn(move || {
                let mut failures = 0u32;
                while !host.is_stopping() {
                    match acceptor.accept() {
                        Ok(channel) => {
                            failures = 0;
                            if host.is_stopping() {
                                (channel.close)();
                                break;
                            }
                            host.admit(channel);
                        }
                        Err(_) => {
                            failures = failures.saturating_add(1);
                            pause(accept_backoff(failures));
                        }
                    }
                }
            })
            .ok();
        Listener { address, thread }
    }

    /// Starts a session for a new connection, or turns it away at the connection limit.
    fn admit(self: &Arc<Self>, channel: Channel) {
        let refuse = (self.connections() >= self.options.connections).then(|| {
            ServiceError::new(
                ErrorCode::Busy,
                std::format!(
                    "the host serves {} connections at once",
                    self.options.connections
                ),
            )
        });
        let host = Arc::clone(self);
        let spawned = std::thread::Builder::new()
            .name("daedalus-session".into())
            .spawn(move || session::serve(&host, channel, refuse));
        // A thread that could not start drops the channel, which closes the connection.
        drop(spawned);
    }

    /// Stops the host: listeners stop accepting, every run is cancelled and dropped, every watch
    /// is told, and every session is closed once what it is owed is written. Returns how many
    /// runs were dropped.
    pub fn shutdown(&self) -> usize {
        self.stopping.store(true, Ordering::SeqCst);
        for wake in lock(&self.wakers).drain(..) {
            wake();
        }
        let runs = std::mem::take(&mut *lock(&self.runs));
        for entry in runs.values() {
            entry.cancel();
        }
        lock(&self.watchers).end_all(EndReason::Shutdown);
        let sessions = std::mem::take(&mut *lock(&self.sessions));
        for session in sessions.values() {
            session.finish();
        }
        // A run still inside a tick is dropped by whoever holds it, once that tick returns.
        runs.len()
    }
}
