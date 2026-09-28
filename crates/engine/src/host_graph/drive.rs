//! Event-driven host drive loops for [`HostGraph`].
//!
//! Instead of polling on a timer, a host waits for the bridge to signal new input, runs a graph
//! tick, drains outputs, and repeats. Combine with latest-only input policies
//! (`HostGraph::set_latest_input`) so bursty sources such as cameras replace stale values instead
//! of queueing them.
//!
//! The host-bridge lock is only held while checking/queueing payloads; it is never held while a
//! graph tick or the caller's output callback runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use daedalus_runtime::ExecutionTelemetry;
use daedalus_runtime::executor::NodeHandler;
use daedalus_runtime::host_bridge::{HostBridgeHandle, InboundWait, InboundWaiter};

use super::HostGraph;
use crate::error::EngineError;

/// Why a drive loop returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HostGraphDriveExit {
    /// [`HostGraphStopHandle::stop`] was called.
    Stopped,
    /// The host bridge was closed and all queued input was processed.
    Closed,
}

/// Outcome of one wait-then-tick step.
#[derive(Debug)]
pub struct HostGraphTurn {
    /// Why the wait finished.
    pub wait: InboundWait,
    /// Telemetry of the tick that ran, or `None` when no input was pending.
    pub telemetry: Option<ExecutionTelemetry>,
}

impl HostGraphTurn {
    pub fn ticked(&self) -> bool {
        self.telemetry.is_some()
    }
}

/// Cloneable, thread-safe stop signal for [`HostGraph::drive_blocking`] and [`HostGraph::drive`].
///
/// Stopping also wakes any inbound waiter on the bridge so the loop exits promptly.
#[derive(Clone)]
pub struct HostGraphStopHandle {
    stopped: Arc<AtomicBool>,
    host: HostBridgeHandle,
}

impl std::fmt::Debug for HostGraphStopHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostGraphStopHandle")
            .field("host_alias", &self.host.alias())
            .field("stopped", &self.is_stopped())
            .finish()
    }
}

impl HostGraphStopHandle {
    /// Request the drive loop to stop and wake it if it is waiting.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.host.wake_inbound_waiters();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    /// Clear a previous stop request so the handle can be reused for another drive loop.
    pub fn reset(&self) {
        self.stopped.store(false, Ordering::SeqCst);
    }
}

impl<H: NodeHandler + Send + Sync + 'static> HostGraph<H> {
    /// Create a new stop signal bound to this graph's host bridge.
    pub fn stop_handle(&self) -> HostGraphStopHandle {
        HostGraphStopHandle {
            stopped: Arc::new(AtomicBool::new(false)),
            host: self.host.clone(),
        }
    }

    /// Waiter for inbound host input; `.wait(timeout)` blocks, `.await` suspends.
    pub fn inbound_waiter(&self) -> InboundWaiter {
        self.host.inbound_waiter()
    }

    /// Block until host input is queued, the bridge closes, it is explicitly woken, or `timeout`
    /// elapses (`None` waits indefinitely).
    pub fn wait_for_input(&self, timeout: Option<Duration>) -> InboundWait {
        self.host.wait_inbound(timeout)
    }

    /// Wait for host input (see [`HostGraph::wait_for_input`]) and run one graph tick if any input
    /// is pending. Outputs produced by the tick are left on the bridge for the caller to drain.
    pub fn tick_on_input(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<HostGraphTurn, EngineError> {
        let wait = self.wait_for_input(timeout);
        let telemetry = self.tick_if_ready()?;
        Ok(HostGraphTurn { wait, telemetry })
    }

    /// Event-driven loop on the current thread: wait for input, tick, call `on_outputs`, repeat.
    ///
    /// `on_outputs` runs after every tick with `&self`, so it can drain outputs, inspect payloads,
    /// or push new inputs. One tick runs per turn; when more input is already pending the next
    /// wait returns immediately, so the graph runs until idle while outputs are handed back after
    /// each tick. Returns when `stop` is signalled, when the bridge is closed and drained, or with
    /// the first error from a tick or from `on_outputs`.
    pub fn drive_blocking<F>(
        &mut self,
        stop: &HostGraphStopHandle,
        mut on_outputs: F,
    ) -> Result<HostGraphDriveExit, EngineError>
    where
        F: FnMut(&Self, &HostGraphTurn) -> Result<(), EngineError>,
    {
        tracing::debug!(
            target: "daedalus_engine::host_graph",
            host_alias = self.host.alias(),
            "host graph drive loop started"
        );
        loop {
            // Create the waiter before checking the stop flag so a concurrent `stop()` (which
            // wakes waiters) cannot slip in between the check and the wait.
            let waiter = self.host.inbound_waiter();
            if stop.is_stopped() {
                return Ok(self.drive_exit(HostGraphDriveExit::Stopped));
            }
            let wait = waiter.wait(None);
            if let Some(exit) = self.drive_turn(wait, stop, &mut on_outputs)? {
                return Ok(exit);
            }
        }
    }

    /// Async variant of [`HostGraph::drive_blocking`], usable from any executor.
    ///
    /// Waiting never blocks an executor thread. Graph ticks run inline on the polling task, so on a
    /// multi-threaded runtime prefer a dedicated task (or `drive_blocking` on a blocking thread)
    /// for CPU-heavy graphs.
    pub async fn drive<F>(
        &mut self,
        stop: &HostGraphStopHandle,
        mut on_outputs: F,
    ) -> Result<HostGraphDriveExit, EngineError>
    where
        F: FnMut(&Self, &HostGraphTurn) -> Result<(), EngineError>,
    {
        tracing::debug!(
            target: "daedalus_engine::host_graph",
            host_alias = self.host.alias(),
            "async host graph drive loop started"
        );
        loop {
            let waiter = self.host.inbound_waiter();
            if stop.is_stopped() {
                return Ok(self.drive_exit(HostGraphDriveExit::Stopped));
            }
            let wait = waiter.await;
            if let Some(exit) = self.drive_turn(wait, stop, &mut on_outputs)? {
                return Ok(exit);
            }
        }
    }

    fn drive_turn<F>(
        &mut self,
        wait: InboundWait,
        stop: &HostGraphStopHandle,
        on_outputs: &mut F,
    ) -> Result<Option<HostGraphDriveExit>, EngineError>
    where
        F: FnMut(&Self, &HostGraphTurn) -> Result<(), EngineError>,
    {
        if stop.is_stopped() {
            return Ok(Some(self.drive_exit(HostGraphDriveExit::Stopped)));
        }
        match wait {
            InboundWait::Ready => {
                let telemetry = self.tick_if_ready()?;
                let turn = HostGraphTurn { wait, telemetry };
                if turn.ticked() {
                    tracing::trace!(
                        target: "daedalus_engine::host_graph",
                        host_alias = self.host.alias(),
                        "host graph drive tick complete"
                    );
                    on_outputs(self, &turn)?;
                }
                Ok(None)
            }
            InboundWait::Closed => Ok(Some(self.drive_exit(HostGraphDriveExit::Closed))),
            InboundWait::Woken | InboundWait::TimedOut => Ok(None),
        }
    }

    fn drive_exit(&self, exit: HostGraphDriveExit) -> HostGraphDriveExit {
        tracing::debug!(
            target: "daedalus_engine::host_graph",
            host_alias = self.host.alias(),
            ?exit,
            "host graph drive loop finished"
        );
        exit
    }
}
