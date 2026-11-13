//! The continuous worker: a thread that moves values in, ticks, and moves values out. It holds the
//! host itself, so no lock is held while a body runs and feeds never wait on one.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::string::{String, ToString};
use std::sync::mpsc;
use std::sync::{Arc, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::HostError;
use crate::stream::{Shared, StreamErrorPolicy, StreamGraphState, StreamHost, StreamWorkerConfig};

/// Moves at most one queued value per port into the host, one tick's worth: a step consumes one
/// value per run. A value the host refused goes back to the front of its port.
pub(crate) fn move_inputs<H: StreamHost>(host: &mut H, shared: &Shared) -> bool {
    let mut moved = false;
    for (name, port) in &shared.inputs {
        if let Some(value) = port.pop() {
            match host.feed(name, copy_of(&value)) {
                Ok(_) => moved = true,
                Err(HostError::Full { .. }) => port.unpop(value),
                Err(_) => {}
            }
        }
    }
    moved
}

fn copy_of(value: &daedalus_value::Value) -> daedalus_value::Value {
    daedalus_value::Value {
        ty: value.ty,
        space: value.space,
        owner: value.owner.clone(),
    }
}

/// Moves everything the host's outputs hold to the output handles.
pub(crate) fn move_outputs<H: StreamHost>(host: &mut H, shared: &Shared) -> bool {
    let mut moved = false;
    for (name, port) in &shared.outputs {
        while let Ok(Some(value)) = host.take_value(name) {
            port.push(value);
            moved = true;
        }
    }
    moved
}

/// What a stopped worker hands back.
pub struct WorkerStop<H> {
    pub host: Option<H>,
    pub last_error: Option<String>,
}

/// Why a stop did not finish in time. The worker keeps going until its body returns.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamWorkerStopError {
    /// A body was still running when the timeout ended.
    SlowBody { waited: Duration },
}

impl fmt::Display for StreamWorkerStopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamWorkerStopError::SlowBody { waited } => {
                write!(f, "a body was still running after {waited:?}")
            }
        }
    }
}

impl std::error::Error for StreamWorkerStopError {}

/// A worker's own counters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamWorkerDiagnostics {
    pub ticks: u64,
    pub last_error: Option<String>,
}

/// The handle to a running worker. Dropping it requests a stop and returns at once.
pub struct StreamGraphWorker<H> {
    shared: Arc<Shared>,
    config: StreamWorkerConfig,
    thread: Option<JoinHandle<H>>,
    finished: mpsc::Receiver<()>,
}

impl<H: StreamHost> StreamGraphWorker<H> {
    pub(crate) fn spawn(host: H, shared: Arc<Shared>, config: StreamWorkerConfig) -> Self {
        let (done, finished) = mpsc::channel();
        let thread = {
            let shared = Arc::clone(&shared);
            thread::spawn(move || {
                let host = run(host, &shared, config);
                let _ = done.send(());
                host
            })
        };
        StreamGraphWorker {
            shared,
            config,
            thread: Some(thread),
            finished,
        }
    }

    pub fn config(&self) -> StreamWorkerConfig {
        self.config
    }

    /// The worker's state, for reports.
    pub fn diagnostics(&self) -> StreamWorkerDiagnostics {
        let lifecycle = self.shared.lock();
        StreamWorkerDiagnostics {
            ticks: lifecycle.ticks,
            last_error: lifecycle.last_error.clone(),
        }
    }

    fn request_stop(&self) {
        self.shared.lock().stop = true;
        self.shared.wake.notify_all();
    }

    /// Stops the worker and waits for it, however long the current body takes.
    pub fn stop(mut self) -> WorkerStop<H> {
        self.request_stop();
        self.join()
    }

    /// Stops the worker, waiting at most `timeout`. A slow body is reported, and the worker keeps
    /// going in the background until it returns. Nothing deadlocks and nothing is killed.
    pub fn stop_timeout(
        mut self,
        timeout: Duration,
    ) -> Result<WorkerStop<H>, StreamWorkerStopError> {
        self.request_stop();
        match self.finished.recv_timeout(timeout) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => Ok(self.join()),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err(StreamWorkerStopError::SlowBody { waited: timeout })
            }
        }
    }

    fn join(&mut self) -> WorkerStop<H> {
        let host = self.thread.take().and_then(|thread| thread.join().ok());
        let last_error = self.shared.lock().last_error.clone();
        WorkerStop { host, last_error }
    }
}

impl<H> Drop for StreamGraphWorker<H> {
    /// Requests a stop without waiting for a slow body.
    fn drop(&mut self) {
        if self.thread.is_some() {
            self.shared.lock().stop = true;
            self.shared.wake.notify_all();
        }
    }
}

fn run<H: StreamHost>(mut host: H, shared: &Shared, config: StreamWorkerConfig) -> H {
    loop {
        {
            let mut lifecycle = shared.lock();
            while lifecycle.state == StreamGraphState::Paused && !lifecycle.stop {
                lifecycle = shared
                    .wake
                    .wait(lifecycle)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            if lifecycle.stop {
                return host;
            }
            lifecycle.work = false;
        }
        let moved_in = move_inputs(&mut host, shared);
        let tick = catch_unwind(AssertUnwindSafe(|| host.step()));
        let moved_out = move_outputs(&mut host, shared);
        let mut lifecycle = shared.lock();
        lifecycle.ticks += 1;
        let failure: Option<String> = match tick {
            Ok(Ok(report)) => {
                let _ = report;
                None
            }
            Ok(Err(error)) => Some(error.to_string()),
            Err(payload) => Some(std::format!(
                "the tick panicked: {}",
                daedalus_abi::panic_message(&*payload)
            )),
        };
        let failed = failure.is_some();
        if failure.is_some() {
            lifecycle.last_error = failure;
        }
        if failed && config.on_error == StreamErrorPolicy::Stop {
            return host;
        }
        if lifecycle.stop {
            return host;
        }
        if !moved_in && !moved_out && !lifecycle.work && !failed {
            let (next, _) = shared
                .wake
                .wait_timeout_while(lifecycle, config.idle_sleep, |lifecycle| {
                    !lifecycle.work
                        && !lifecycle.stop
                        && lifecycle.state != StreamGraphState::Paused
                })
                .unwrap_or_else(PoisonError::into_inner);
            drop(next);
        }
    }
}
