//! Drive loops for graphs fed by a multi-camera feeder
//! ([`MultiCamera`](daedalus_runtime::host_bridge::multicam::MultiCamera)): like
//! [`HostGraph::drive_blocking`] and [`HostGraph::tick_ready`], plus the feeder's deadlines (a
//! synchronized group's timeout, an independent camera's `max_age`), so a stalled camera resolves
//! on time even when no other frame arrives.

use daedalus_runtime::executor::NodeHandler;
#[cfg(feature = "threads")]
use daedalus_runtime::host_bridge::InboundWait;
use daedalus_runtime::host_bridge::multicam::CameraFeed;

use super::HostGraph;
#[cfg(feature = "threads")]
use super::{HostGraphDriveExit, HostGraphStopHandle, HostGraphTurn};
use crate::error::EngineError;

impl<H: NodeHandler + Send + Sync + 'static> HostGraph<H> {
    /// [`Self::drive_blocking`] for a graph fed by `cameras` (built on this graph's bridge):
    /// every wait ends by the feeder's next deadline at the latest, and due deadlines are expired
    /// before each wait, so a timed-out group ticks (or is dropped) without waiting for another
    /// frame.
    #[cfg(feature = "threads")]
    pub fn drive_cameras_blocking<F>(
        &mut self,
        stop: &HostGraphStopHandle,
        cameras: &impl CameraFeed,
        mut on_outputs: F,
    ) -> Result<HostGraphDriveExit, EngineError>
    where
        F: FnMut(&Self, &HostGraphTurn) -> Result<(), EngineError>,
    {
        loop {
            // Waiter first (see `drive_blocking`); expiring may queue a group, which it then sees.
            let waiter = self.host.inbound_waiter();
            if stop.is_stopped() {
                return Ok(HostGraphDriveExit::Stopped);
            }
            cameras.expire();
            let wait = waiter.wait(cameras.poll_timeout());
            if stop.is_stopped() {
                return Ok(HostGraphDriveExit::Stopped);
            }
            match wait {
                InboundWait::Ready => {
                    let telemetry = self.tick_if_ready()?;
                    let turn = HostGraphTurn { wait, telemetry };
                    if turn.ticked() {
                        on_outputs(self, &turn)?;
                    }
                }
                InboundWait::Closed => return Ok(HostGraphDriveExit::Closed),
                InboundWait::Woken | InboundWait::TimedOut => {}
            }
        }
    }

    /// [`Self::tick_ready`] for a graph fed by `cameras`: expire due deadlines first. Poll the
    /// inbound fd with [`CameraFeed::poll_timeout`] as the timeout and call this whenever `poll`
    /// returns, readable or timed out.
    #[cfg(all(feature = "std", target_os = "linux"))]
    pub fn tick_ready_cameras(
        &mut self,
        cameras: &impl CameraFeed,
    ) -> Result<Option<daedalus_runtime::ExecutionTelemetry>, EngineError> {
        cameras.expire();
        self.tick_ready()
    }
}
