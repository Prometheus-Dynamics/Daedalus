//! Continuous stream workers: a thread per graph (`threads` feature).

use crate::portable::Arc;
use crate::prelude::*;
use crate::sync::{Condvar, Mutex};
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use daedalus_core::platform::{Clock, Instant};
use std::thread::{self, JoinHandle};

use thiserror::Error;

use super::{
    STREAM_NO_PROGRESS_WARNING, SharedStreamGraph, StreamGraph, StreamGraphState,
    StreamWorkerConfig, normalize_idle_sleep,
};
use crate::executor::NodeHandler;
use crate::host_bridge::HostBridgeHandle;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamWorkerDiagnostics {
    pub stop_requested: bool,
    pub worker_finished: bool,
    pub shutdown_pending: bool,
    pub stop_requested_elapsed: Option<Duration>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum StreamWorkerStopError {
    #[error("stream worker did not stop within {timeout:?}")]
    Timeout { timeout: Duration },
}

#[must_use = "stream workers should be stopped explicitly with stop or stop_timeout"]
pub struct StreamGraphWorker {
    stop: Arc<AtomicBool>,
    stop_requested_at: Arc<Mutex<Option<Instant>>>,
    /// The graph's clock.
    clock: Clock,
    last_error: Arc<Mutex<Option<String>>>,
    done: Arc<WorkerDone>,
    wake: HostBridgeHandle,
    handle: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct WorkerDone {
    finished: Mutex<bool>,
    ready: Condvar,
}

impl WorkerDone {
    fn signal_finished(&self) {
        let mut finished = self.finished.lock();
        *finished = true;
        self.ready.notify_all();
    }

    fn wait_timeout(&self, timeout: Duration) -> bool {
        // Only reached with a worker thread, so the OS clock exists.
        let deadline = std::time::Instant::now() + timeout;
        let mut finished = self.finished.lock();
        while !*finished {
            if self.ready.wait_until(&mut finished, deadline).timed_out() {
                return *finished;
            }
        }
        true
    }
}

struct WorkerDoneGuard {
    done: Arc<WorkerDone>,
}

impl Drop for WorkerDoneGuard {
    fn drop(&mut self) {
        self.done.signal_finished();
    }
}

impl StreamGraphWorker {
    fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
        let mut requested_at = self.stop_requested_at.lock();
        requested_at.get_or_insert_with(|| self.clock.now());
        self.wake.wake_inbound_waiters();
    }

    /// Request worker shutdown and wait until the worker thread exits.
    ///
    /// Node handlers should be bounded and cooperative. If a handler blocks for a long time,
    /// `stop` can block until that handler returns; use [`Self::stop_timeout`] when callers need to
    /// observe a delayed shutdown without blocking indefinitely. Dropping the worker requests stop
    /// without waiting for a blocked handler.
    pub fn stop(mut self) -> Option<String> {
        self.request_stop();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.last_error()
    }

    /// Request worker shutdown and wait up to `timeout` for the worker thread to exit.
    ///
    /// On timeout, the worker remains owned by `self`; callers can inspect diagnostics and call
    /// this method again or call [`Self::stop`] once the in-flight handler has returned. This is
    /// the preferred shutdown API for release-facing hosts because it reports delayed handlers
    /// without detaching or killing the worker thread.
    pub fn stop_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<String>, StreamWorkerStopError> {
        self.request_stop();

        let Some(handle) = self.handle.as_ref() else {
            return Ok(self.last_error());
        };
        if !handle.is_finished() && !self.done.wait_timeout(timeout) {
            let diagnostics = self.diagnostics();
            tracing::warn!(
                target: "daedalus_runtime::stream",
                ?timeout,
                stop_requested_elapsed = ?diagnostics.stop_requested_elapsed,
                last_error = ?diagnostics.last_error,
                "stream worker stop timed out"
            );
            return Err(StreamWorkerStopError::Timeout { timeout });
        }

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        Ok(self.last_error())
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().clone()
    }

    pub fn diagnostics(&self) -> StreamWorkerDiagnostics {
        let stop_requested = self.stop.load(Ordering::Acquire);
        let worker_finished = self
            .handle
            .as_ref()
            .is_none_or(|handle| handle.is_finished());
        let stop_requested_elapsed = self
            .stop_requested_at
            .lock()
            .map(|requested_at| self.clock.elapsed(requested_at));
        StreamWorkerDiagnostics {
            stop_requested,
            worker_finished,
            shutdown_pending: stop_requested && !worker_finished,
            stop_requested_elapsed,
            last_error: self.last_error(),
        }
    }
}

impl Drop for StreamGraphWorker {
    fn drop(&mut self) {
        self.request_stop();
        if self
            .handle
            .as_ref()
            .is_some_and(|handle| handle.is_finished())
            && let Some(handle) = self.handle.take()
        {
            let _ = handle.join();
        } else if self.handle.is_some() {
            tracing::warn!(
                target: "daedalus_runtime::stream",
                stop_requested_elapsed = ?self
                    .stop_requested_at
                    .lock()
                    .map(|requested_at| self.clock.elapsed(requested_at)),
                "dropping stream worker before thread finished; call stop or stop_timeout to observe shutdown completion"
            );
        }
    }
}

impl<H> StreamGraph<H>
where
    H: NodeHandler + 'static,
{
    pub fn spawn_continuous(
        graph: SharedStreamGraph<H>,
        idle_sleep: Duration,
    ) -> StreamGraphWorker {
        Self::spawn_continuous_with_config(
            graph,
            StreamWorkerConfig {
                idle_sleep: normalize_idle_sleep(idle_sleep),
            },
        )
    }

    /// Run `graph` on a dedicated worker thread until stopped (`threads` feature; without it,
    /// drive the graph with [`StreamGraph::poll`] or [`StreamGraph::run_available`]).
    pub fn spawn_continuous_with_config(
        graph: SharedStreamGraph<H>,
        config: StreamWorkerConfig,
    ) -> StreamGraphWorker {
        let idle_sleep = normalize_idle_sleep(config.idle_sleep);
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let stop_requested_at = Arc::new(Mutex::new(None));
        let last_error = Arc::new(Mutex::new(None));
        let worker_error = last_error.clone();
        let done = Arc::new(WorkerDone::default());
        let worker_done = done.clone();
        let (wake, clock) = {
            let guard = graph.lock();
            (
                guard.bridges.ensure_handle(guard.host_alias.clone()),
                guard.clock.clone(),
            )
        };
        let worker_clock = clock.clone();
        let handle = thread::spawn(move || {
            let _done_guard = WorkerDoneGuard { done: worker_done };
            while !worker_stop.load(Ordering::Acquire) {
                let mut should_sleep = true;
                let mut pending_before = 0usize;
                let executor = {
                    let mut guard = graph.lock();
                    match guard.state {
                        StreamGraphState::Closed => break,
                        StreamGraphState::Running => {
                            let handle = guard.bridges.ensure_handle(guard.host_alias.clone());
                            pending_before = handle.pending_inbound();
                            if pending_before > 0 {
                                guard.current_execution_started_at = Some(worker_clock.now());
                                guard.executor.take()
                            } else {
                                None
                            }
                        }
                        StreamGraphState::Created | StreamGraphState::Paused => None,
                    }
                };
                if let Some(mut executor) = executor {
                    let result = executor.run_in_place();
                    let finished_at = worker_clock.now();
                    let mut guard = graph.lock();
                    if let Some(started) = guard.current_execution_started_at.take() {
                        guard.last_execution_duration = Some(finished_at.duration_since(started));
                    }
                    if guard.executor.is_none() {
                        guard.executor = Some(executor);
                    } else {
                        let message = "stream executor returned while another executor was present";
                        tracing::error!(
                            target: "daedalus_runtime::stream",
                            host_alias = %guard.host_alias,
                            "stream worker stopped after executor ownership violation"
                        );
                        *worker_error.lock() = Some(message.into());
                        guard.last_error = Some(message.into());
                        break;
                    }
                    match result {
                        Ok(telemetry) => {
                            guard.last_error = None;
                            guard.last_telemetry = Some(telemetry);
                            let pending_after = guard
                                .bridges
                                .ensure_handle(guard.host_alias.clone())
                                .pending_inbound();
                            should_sleep = pending_after == 0 || pending_after >= pending_before;
                            if pending_after >= pending_before && pending_after > 0 {
                                tracing::warn!(
                                    target: "daedalus_runtime::stream",
                                    host_alias = %guard.host_alias,
                                    pending_before,
                                    pending_after,
                                    "continuous stream tick made no host-inbound progress; waiting before retry"
                                );
                                if let Some(telemetry) = guard.last_telemetry.as_mut() {
                                    telemetry
                                        .warnings
                                        .push(STREAM_NO_PROGRESS_WARNING.to_string());
                                }
                            }
                        }
                        Err(err) => {
                            let error = err.to_string();
                            tracing::error!(
                                target: "daedalus_runtime::stream",
                                host_alias = %guard.host_alias,
                                error = %error,
                                "continuous stream tick failed"
                            );
                            *worker_error.lock() = Some(error.clone());
                            guard.last_error = Some(error);
                            break;
                        }
                    }
                }
                if should_sleep {
                    let handle = {
                        let guard = graph.lock();
                        if guard.state == StreamGraphState::Closed {
                            break;
                        }
                        guard.bridges.ensure_handle(guard.host_alias.clone())
                    };
                    let _ = handle.wait_inbound(Some(idle_sleep));
                }
            }
        });
        StreamGraphWorker {
            stop,
            stop_requested_at,
            clock,
            last_error,
            done,
            wake,
            handle: Some(handle),
        }
    }
}
