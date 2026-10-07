//! Independent cameras: each camera's latest frame held, ticks per arrival.

use crate::portable::Arc;
use crate::prelude::*;
use core::time::Duration;

use daedalus_core::platform::{Clock, Instant};
use daedalus_transport::{FeedOutcome, Payload};

use crate::handles::PortId;
use crate::sync::Mutex;

use super::super::HostBridgeHandle;
use super::{CameraFeed, CameraSet, MultiCameraStats};

/// Parameters of [`IndependentCameras`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndependentConfig {
    /// Cameras whose arrivals trigger ticks; `None` means every camera.
    pub trigger: Option<CameraSet>,
    /// Clear a camera's held frame once it is this old (bridge clock, since its arrival), so a
    /// stalled camera's consumers see no frame instead of an ever older one. `None` holds forever.
    pub max_age: Option<Duration>,
}

/// Independent-latest multi-camera feeder ([`super::MultiCamera::independent`]); clones share it.
///
/// Every camera port is held (`HostBridgeHandle::set_held_input`): it keeps only its latest frame
/// and every tick re-delivers it as an `Arc` clone, so a tick sees every camera's current frame
/// and nothing queues up. Trigger cameras' ports are also triggering
/// (`HostBridgeHandle::set_triggering_held_input`): an arrival leaves the port pending until a
/// tick takes it, so a burst of arrivals triggers one tick on the newest frame, never a stale one.
/// Arrivals of other cameras only replace their held frame.
#[derive(Clone)]
pub struct IndependentCameras {
    inner: Arc<Inner>,
}

struct Inner {
    host: HostBridgeHandle,
    clock: Clock,
    ports: Vec<PortId>,
    max_age: Option<Duration>,
    state: Mutex<State>,
}

struct State {
    /// Arrival of each camera's held frame.
    arrived: Vec<Option<Instant>>,
    stats: MultiCameraStats,
}

impl IndependentCameras {
    pub(super) fn new(
        host: &HostBridgeHandle,
        ports: Vec<PortId>,
        config: IndependentConfig,
    ) -> Self {
        let trigger = config
            .trigger
            .unwrap_or_else(|| CameraSet::all(ports.len()));
        for (camera, port) in ports.iter().enumerate() {
            if trigger.contains(camera) {
                host.set_triggering_held_input(port.clone());
            } else {
                host.set_held_input(port.clone());
            }
        }
        Self {
            inner: Arc::new(Inner {
                host: host.clone(),
                clock: host.clock(),
                max_age: config.max_age,
                state: Mutex::new(State {
                    arrived: vec![None; ports.len()],
                    stats: MultiCameraStats::default(),
                }),
                ports,
            }),
        }
    }

    pub fn camera_count(&self) -> usize {
        self.inner.ports.len()
    }

    /// Replace `camera`'s held frame (and trigger a tick for a trigger camera). The outcome is
    /// the bridge's (`Replaced` when a frame was held); `None` for an unknown camera.
    pub fn push(&self, camera: usize, payload: Payload) -> Option<FeedOutcome> {
        let inner = &*self.inner;
        let port = inner.ports.get(camera)?;
        let mut state = inner.state.lock();
        let outcome = inner.host.feed_payload(port.clone(), payload);
        if matches!(
            outcome,
            FeedOutcome::Accepted { .. } | FeedOutcome::Replaced { .. }
        ) {
            state.arrived[camera] = Some(inner.clock.now());
            state.stats.frames += 1;
        }
        Some(outcome)
    }

    /// Cameras holding a frame right now.
    pub fn present(&self) -> CameraSet {
        let state = self.inner.state.lock();
        (0..state.arrived.len())
            .filter(|&camera| state.arrived[camera].is_some())
            .collect()
    }

    /// When `camera`'s held frame arrived (bridge clock), `None` when it holds none.
    pub fn arrived(&self, camera: usize) -> Option<Instant> {
        self.inner
            .state
            .lock()
            .arrived
            .get(camera)
            .copied()
            .flatten()
    }

    /// Drop `camera`'s held frame; its consumers see none until its next arrival.
    pub fn clear(&self, camera: usize) {
        let inner = &*self.inner;
        let mut state = inner.state.lock();
        if let Some(port) = inner.ports.get(camera) {
            inner.host.clear_input(port);
            state.arrived[camera] = None;
        }
    }

    pub fn stats(&self) -> MultiCameraStats {
        self.inner.state.lock().stats
    }
}

impl CameraFeed for IndependentCameras {
    fn host(&self) -> &HostBridgeHandle {
        &self.inner.host
    }

    fn clock(&self) -> &Clock {
        &self.inner.clock
    }

    fn next_deadline(&self) -> Option<Instant> {
        let max_age = self.inner.max_age?;
        let state = self.inner.state.lock();
        state.arrived.iter().flatten().min().map(|at| *at + max_age)
    }

    fn expire(&self) -> bool {
        let inner = &*self.inner;
        let Some(max_age) = inner.max_age else {
            return false;
        };
        let now = inner.clock.now();
        let mut state = inner.state.lock();
        let mut expired = false;
        for (camera, port) in inner.ports.iter().enumerate() {
            if state.arrived[camera].is_some_and(|at| at + max_age <= now) {
                inner.host.clear_input(port);
                state.arrived[camera] = None;
                state.stats.expired_frames += 1;
                expired = true;
            }
        }
        expired
    }
}
