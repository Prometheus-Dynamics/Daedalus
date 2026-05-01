use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use thiserror::Error;

use super::{
    DEFAULT_STREAM_IDLE_SLEEP, STREAM_NO_PROGRESS_WARNING, SharedStreamGraph, StreamGraph,
    StreamGraphState,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamWorkerConfig {
    pub idle_sleep: Duration,
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
        }
    }
}

pub(super) fn normalize_idle_sleep(idle_sleep: Duration) -> Duration {
    if idle_sleep.is_zero() {
        DEFAULT_STREAM_IDLE_SLEEP
    } else {
        idle_sleep
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_idle_sleep_normalizes_to_default() {
        assert_eq!(
            StreamWorkerConfig::default()
                .with_idle_sleep(Duration::ZERO)
                .idle_sleep,
            DEFAULT_STREAM_IDLE_SLEEP
        );
        assert_eq!(
            normalize_idle_sleep(Duration::ZERO),
            DEFAULT_STREAM_IDLE_SLEEP
        );
    }
}

#[must_use = "stream workers should be stopped explicitly with stop or stop_timeout"]
pub struct StreamGraphWorker {
    stop: Arc<AtomicBool>,
    stop_requested_at: Arc<Mutex<Option<Instant>>>,
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
        let mut finished = self
            .finished
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *finished = true;
        self.ready.notify_all();
    }

    fn wait_timeout(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut finished = self
            .finished
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !*finished {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_finished, wait) = self
                .ready
                .wait_timeout(finished, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            finished = next_finished;
            if wait.timed_out() && !*finished {
                return false;
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
        let mut requested_at = self
            .stop_requested_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        requested_at.get_or_insert_with(Instant::now);
        self.wake.notify_waiters();
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
        self.last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
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
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .map(|requested_at| requested_at.elapsed());
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
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .map(|requested_at| requested_at.elapsed()),
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
        let wake = {
            let guard = graph
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.bridges.ensure_handle(guard.host_alias.clone())
        };
        let handle = thread::spawn(move || {
            let _done_guard = WorkerDoneGuard { done: worker_done };
            while !worker_stop.load(Ordering::Acquire) {
                let mut should_sleep = true;
                let mut pending_before = 0usize;
                let executor = {
                    let mut guard = graph
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    match guard.state {
                        StreamGraphState::Closed => break,
                        StreamGraphState::Running => {
                            let handle = guard.bridges.ensure_handle(guard.host_alias.clone());
                            pending_before = handle.pending_inbound();
                            if pending_before > 0 {
                                guard.current_execution_started_at = Some(Instant::now());
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
                    let finished_at = Instant::now();
                    let mut guard = graph
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
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
                        *worker_error
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                            Some(message.into());
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
                            *worker_error
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                Some(error.clone());
                            guard.last_error = Some(error);
                            break;
                        }
                    }
                }
                if should_sleep {
                    let handle = {
                        let guard = graph
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if guard.state == StreamGraphState::Closed {
                            break;
                        }
                        guard.bridges.ensure_handle(guard.host_alias.clone())
                    };
                    let _ = handle.wait_for_inbound(idle_sleep);
                }
            }
        });
        StreamGraphWorker {
            stop,
            stop_requested_at,
            last_error,
            done,
            wake,
            handle: Some(handle),
        }
    }
}
