//! Drive loops over a stream graph on the caller's thread: tick until idle, tick when input is
//! ready, and block until stopped. A tick runs with no lock held, because the graph owns its host
//! here and feeds go through the input handles.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use daedalus_runner::TickReport;

use crate::HostError;
use crate::stream::{StreamGraph, StreamHost, worker};
use crate::{InboundSignal, InboundWait};

/// Stops a drive loop from any thread. `stop` sets the flag and wakes the waiter. `reset` clears
/// it so the handle can be used again.
#[derive(Clone)]
pub struct StopHandle {
    stopped: Arc<AtomicBool>,
    signal: Arc<InboundSignal>,
}

impl StopHandle {
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.signal.stop();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub fn reset(&self) {
        self.stopped.store(false, Ordering::Release);
        self.signal.reset();
    }
}

impl<H: StreamHost> StreamGraph<H> {
    /// The stop handle for this graph's drive loops.
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            stopped: Arc::new(AtomicBool::new(false)),
            signal: Arc::clone(&self.shared.signal),
        }
    }

    /// The readiness signal, for waiters.
    pub fn inbound_signal(&self) -> Arc<InboundSignal> {
        Arc::clone(&self.shared.signal)
    }

    /// Waits up to `timeout` for input, a close, or a stop.
    pub fn wait_for_input(&self, timeout: Option<Duration>) -> InboundWait {
        self.shared.signal.wait(timeout)
    }

    /// The pollable descriptor for this graph's input.
    #[cfg(target_os = "linux")]
    pub fn inbound_fd(&self) -> std::io::Result<crate::inbound_fd::InboundFd> {
        crate::inbound_fd::InboundFd::new(Arc::clone(&self.shared.signal))
    }

    /// One tick's work: moves one value per port in, ticks, moves outputs out.
    pub(crate) fn tick_once(&mut self) -> Option<Result<TickReport, HostError>> {
        let host = self.host.as_mut()?;
        worker::move_inputs(host, &self.shared);
        let report = host.step();
        worker::move_outputs(host, &self.shared);
        Some(report)
    }

    /// Ticks until a tick finds nothing to do and no input is waiting. Returns the ticks run.
    pub fn tick_until_idle(&mut self) -> Result<u64, HostError> {
        let mut ticks = 0;
        while let Some(report) = self.tick_once() {
            let report = report?;
            ticks += 1;
            if report.idle && !self.has_queued_input() {
                break;
            }
        }
        self.shared.signal.clear_pending();
        Ok(ticks)
    }

    fn has_queued_input(&self) -> bool {
        self.shared.inputs.iter().any(|(_, port)| port.has_items())
    }

    /// Ticks if input is pending, then says so again when more is still queued. `None` means no
    /// tick ran, so a `poll` loop can go back to waiting.
    pub fn tick_ready(&mut self) -> Result<Option<TickReport>, HostError> {
        if !self.shared.signal.is_pending() {
            return Ok(None);
        }
        self.shared.signal.clear_pending();
        let report = self.tick_once().transpose()?;
        if self.has_queued_input() {
            self.shared.signal.notify();
        }
        Ok(report)
    }

    /// Waits for input and ticks until the handle stops it, the inputs close with nothing left, or
    /// the graph's host is gone. `on_output` runs after each tick with no lock held.
    pub fn drive_blocking(
        &mut self,
        stop: &StopHandle,
        mut on_output: impl FnMut(&StreamGraph<H>),
    ) -> Result<(), HostError> {
        loop {
            if stop.is_stopped() {
                return Ok(());
            }
            match self.wait_for_input(None) {
                InboundWait::Stopped => return Ok(()),
                InboundWait::Closed => {
                    if !self.has_queued_input() {
                        return Ok(());
                    }
                }
                InboundWait::Ready | InboundWait::TimedOut => {}
            }
            self.tick_ready()?;
            on_output(self);
        }
    }
}
