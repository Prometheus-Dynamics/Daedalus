//! StreamGraph: a graph that runs continuously. The caller feeds input handles and reads output
//! handles, and a worker thread moves values in and out and ticks. Handles work on per-port
//! queues outside the graph, so a slow body never blocks a feed or a read.

mod input;
mod output;
pub(crate) mod worker;

use std::string::String;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use std::vec::Vec;

use daedalus_runner::TickReport;
use daedalus_value::Value;

pub use input::{GraphInput, InputStats, StreamFeed};
pub use output::{GraphOutput, OutputStats, OutputSubscription};
pub use worker::{StreamGraphWorker, StreamWorkerDiagnostics, StreamWorkerStopError, WorkerStop};

use crate::{FeedOutcome, Host, HostError, InboundSignal};
use input::InputPort;
use output::OutputPort;

/// The idle wait when a worker has nothing to do. A zero idle sleep normalizes to this.
pub const DEFAULT_STREAM_IDLE_SLEEP: Duration = Duration::from_millis(100);

fn normalize_idle_sleep(idle: Duration) -> Duration {
    if idle.is_zero() {
        DEFAULT_STREAM_IDLE_SLEEP
    } else {
        idle
    }
}

/// What a continuous worker does with a failed tick.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StreamErrorPolicy {
    /// Record the error and stop the worker.
    Stop,
    /// Record the error and keep running. A body panic is handled the same way.
    #[default]
    Continue,
}

/// A worker's settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamWorkerConfig {
    pub idle_sleep: Duration,
    pub on_error: StreamErrorPolicy,
}

impl StreamWorkerConfig {
    pub fn with_idle_sleep(mut self, idle_sleep: Duration) -> Self {
        self.idle_sleep = normalize_idle_sleep(idle_sleep);
        self
    }
}

impl Default for StreamWorkerConfig {
    fn default() -> Self {
        Self {
            idle_sleep: DEFAULT_STREAM_IDLE_SLEEP,
            on_error: StreamErrorPolicy::default(),
        }
    }
}

/// The graph's lifecycle.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StreamGraphState {
    #[default]
    Created,
    Running,
    Paused,
    Closed,
}

/// How a stream runs its ticks. Streams run one thread's ticks; adaptive selection isn't carried.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StreamExecutionMode {
    #[default]
    Serial,
}

/// One port's name and stats, for diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPortDiagnostics<S> {
    pub name: String,
    pub stats: S,
}

/// What a stream reports about itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamGraphDiagnostics {
    pub state: StreamGraphState,
    pub execution: StreamExecutionMode,
    pub config: StreamWorkerConfig,
    pub ticks: u64,
    pub last_error: Option<String>,
    pub inputs: Vec<StreamPortDiagnostics<InputStats>>,
    pub outputs: Vec<StreamPortDiagnostics<OutputStats>>,
}

/// Something a stream can drive: the host around a loaded graph.
pub trait StreamHost: Send + 'static {
    fn feed(&mut self, port: &str, value: Value) -> Result<FeedOutcome, HostError>;
    fn step(&mut self) -> Result<TickReport, HostError>;
    fn take_value(&mut self, port: &str) -> Result<Option<Value>, HostError>;
}

impl StreamHost for Host<'static> {
    fn feed(&mut self, port: &str, value: Value) -> Result<FeedOutcome, HostError> {
        Host::feed(self, port, value)
    }
    fn step(&mut self) -> Result<TickReport, HostError> {
        Host::step(self)
    }
    fn take_value(&mut self, port: &str) -> Result<Option<Value>, HostError> {
        Host::take_value(self, port)
    }
}

/// State shared between the handles, the graph and the worker.
pub(crate) struct Shared {
    pub(crate) inputs: Vec<(String, Arc<InputPort>)>,
    pub(crate) outputs: Vec<(String, Arc<OutputPort>)>,
    pub(crate) state: Mutex<Lifecycle>,
    pub(crate) wake: Condvar,
    /// Readiness for the drive loops: set by every feed, cleared when it is taken.
    pub(crate) signal: Arc<InboundSignal>,
}

/// The mutable part of the shared state.
#[derive(Debug, Default)]
pub(crate) struct Lifecycle {
    pub(crate) state: StreamGraphState,
    pub(crate) stop: bool,
    pub(crate) work: bool,
    pub(crate) ticks: u64,
    pub(crate) last_error: Option<String>,
}

impl Shared {
    pub(crate) fn lock(&self) -> MutexGuard<'_, Lifecycle> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Tells the worker there may be something to do.
    pub(crate) fn kick(&self) {
        self.lock().work = true;
        self.wake.notify_all();
    }

    fn set_state(&self, state: StreamGraphState) {
        self.lock().state = state;
        self.wake.notify_all();
    }
}

/// A graph and its stream ports.
pub struct StreamGraph<H> {
    pub(crate) host: Option<H>,
    pub(crate) shared: Arc<Shared>,
    worker: Option<StreamGraphWorker<H>>,
}

impl<H: StreamHost> StreamGraph<H> {
    /// A stream over `host` with these input and output ports.
    pub fn new(host: H, inputs: &[&str], outputs: &[&str]) -> StreamGraph<H> {
        let shared = Arc::new(Shared {
            inputs: inputs
                .iter()
                .map(|name| (String::from(*name), Arc::new(InputPort::new())))
                .collect(),
            outputs: outputs
                .iter()
                .map(|name| (String::from(*name), Arc::new(OutputPort::new())))
                .collect(),
            state: Mutex::new(Lifecycle::default()),
            wake: Condvar::new(),
            signal: Arc::new(InboundSignal::new()),
        });
        StreamGraph {
            host: Some(host),
            shared,
            worker: None,
        }
    }

    /// The input handle for `port`.
    pub fn input(&self, port: &str) -> Option<GraphInput> {
        let shared = Arc::clone(&self.shared);
        let port = self.shared.inputs.iter().find(|(name, _)| name == port)?;
        Some(GraphInput::new(Arc::clone(&port.1), shared))
    }

    /// The output handle for `port`.
    pub fn output(&self, port: &str) -> Option<GraphOutput> {
        let port = self.shared.outputs.iter().find(|(name, _)| name == port)?;
        Some(GraphOutput::new(Arc::clone(&port.1)))
    }

    pub fn state(&self) -> StreamGraphState {
        self.shared.lock().state
    }

    /// Starts a worker thread ticking continuously. The graph's host moves into it.
    pub fn spawn_continuous(
        &mut self,
        config: StreamWorkerConfig,
    ) -> Option<&StreamGraphWorker<H>> {
        let host = self.host.take()?;
        let config = config.with_idle_sleep(config.idle_sleep);
        self.shared.set_state(StreamGraphState::Running);
        self.worker = Some(StreamGraphWorker::spawn(
            host,
            Arc::clone(&self.shared),
            config,
        ));
        self.worker.as_ref()
    }

    /// Holds the worker between ticks. Feeds keep working and queue up.
    pub fn pause(&self) {
        self.shared.set_state(StreamGraphState::Paused);
    }

    /// Lets a paused worker run again.
    pub fn resume(&self) {
        self.shared.set_state(StreamGraphState::Running);
    }

    /// Closes every input, so further feeds are refused. Queued values are still processed.
    pub fn close(&self) {
        for (_, port) in &self.shared.inputs {
            port.close();
        }
        self.shared.signal.close();
        self.shared.set_state(StreamGraphState::Closed);
    }

    /// Runs what's queued on the caller's thread: moves inputs in, ticks once, moves outputs out.
    /// For a graph with no worker. Returns the tick report, or `None` when a worker owns the host.
    pub fn poll(&mut self) -> Option<Result<TickReport, HostError>> {
        let host = self.host.as_mut()?;
        worker::move_inputs(host, &self.shared);
        let result = host.step();
        worker::move_outputs(host, &self.shared);
        Some(result)
    }

    /// Stops the worker and returns its result. With no worker this is empty.
    pub fn stop(&mut self) -> Option<WorkerStop<H>> {
        self.worker.take().map(StreamGraphWorker::stop)
    }

    /// Stops the worker, waiting at most `timeout` for a body to finish.
    pub fn stop_timeout(
        &mut self,
        timeout: Duration,
    ) -> Option<Result<WorkerStop<H>, StreamWorkerStopError>> {
        self.worker
            .take()
            .map(|worker| worker.stop_timeout(timeout))
    }

    /// The worker's diagnostics, when one is running.
    pub fn worker(&self) -> Option<&StreamGraphWorker<H>> {
        self.worker.as_ref()
    }

    /// What the stream reports: lifecycle, execution mode, applied config, and per-port stats.
    pub fn diagnostics(&self) -> StreamGraphDiagnostics {
        let lifecycle = self.shared.lock();
        let (state, ticks, last_error) = (
            lifecycle.state,
            lifecycle.ticks,
            lifecycle.last_error.clone(),
        );
        drop(lifecycle);
        StreamGraphDiagnostics {
            state,
            execution: StreamExecutionMode::Serial,
            config: self
                .worker
                .as_ref()
                .map_or_else(StreamWorkerConfig::default, |w| w.config()),
            ticks,
            last_error,
            inputs: self
                .shared
                .inputs
                .iter()
                .map(|(name, port)| StreamPortDiagnostics {
                    name: name.clone(),
                    stats: port.stats(),
                })
                .collect(),
            outputs: self
                .shared
                .outputs
                .iter()
                .map(|(name, port)| StreamPortDiagnostics {
                    name: name.clone(),
                    stats: port.stats(),
                })
                .collect(),
        }
    }
}

impl<H> Drop for StreamGraph<H> {
    /// Requests a stop without waiting for a slow body. The worker finishes on its own.
    fn drop(&mut self) {
        self.shared.lock().stop = true;
        self.shared.wake.notify_all();
    }
}
