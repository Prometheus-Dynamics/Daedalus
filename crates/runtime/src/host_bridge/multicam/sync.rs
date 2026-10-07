//! Synchronized cameras: frames grouped by stamp into atomic ticks.

use crate::portable::Arc;
use crate::prelude::*;
use core::time::Duration;

use daedalus_core::platform::{Clock, Instant};
use daedalus_transport::Payload;

use crate::handles::PortId;
use crate::sync::Mutex;

use super::super::{Direction, HostBridgeBuffers, HostBridgeHandle, PortKey, enqueue_locked};
use super::super::{io_timing, wait};
use super::{
    CameraFeed, CameraGroup, CameraPush, CameraSet, MultiCameraStats, StampFn, frame_timestamp,
};

/// What to do with a group some camera did not complete in time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum PartialPolicy {
    /// Discard the incomplete group's frames; no tick sees them.
    #[default]
    Drop,
    /// Commit the frames that arrived; missing cameras' ports get nothing, so their consumers'
    /// optional inputs are `None` (required inputs skip the node that tick).
    TickPartial,
    /// Commit the frames that arrived plus each missing camera's last committed frame again (an
    /// `Arc` clone); a camera that never delivered stays absent as with
    /// [`Self::TickPartial`]. Keeps one extra frame per camera alive.
    HoldLast,
}

/// Grouping parameters of [`SynchronizedCameras`].
#[derive(Clone, Copy, Debug)]
pub struct SyncConfig {
    /// Largest stamp spread within one group, in stamp units (ns for timestamps). Keep it well
    /// under the frame period (half of it at most), or one group swallows the next.
    pub window: u64,
    /// How long an incomplete group waits for its missing cameras, on the bridge clock from the
    /// arrival of its earliest frame, before [`Self::partial`] resolves it.
    pub timeout: Duration,
    pub partial: PartialPolicy,
    /// Frames buffered per camera (preallocated). A frame arriving at a full buffer first
    /// resolves the oldest group under [`Self::partial`] (counted as an overflow).
    pub max_buffered: usize,
    /// Reads a frame's stamp; [`frame_timestamp`] by default. Pushes with
    /// [`SynchronizedCameras::push_stamped`] skip it.
    pub stamp: StampFn,
}

impl SyncConfig {
    /// Group by capture timestamp within `window`, wait up to `timeout`, drop incomplete groups,
    /// buffer four frames per camera.
    pub fn new(window: Duration, timeout: Duration) -> Self {
        Self {
            window: u64::try_from(window.as_nanos()).unwrap_or(u64::MAX),
            timeout,
            partial: PartialPolicy::Drop,
            max_buffered: 4,
            stamp: frame_timestamp,
        }
    }
}

/// Synchronized multi-camera feeder ([`super::MultiCamera::synchronized`]); clones share it.
///
/// Camera threads [`push`](Self::push) frames (thread-safe, never blocks on the graph). Each
/// frame's stamp, plus its camera's offset ([`Self::set_offset`], for clock skew), goes into
/// that camera's sorted buffer, so out-of-order arrival within and across cameras is fine.
/// Grouping anchors on the oldest buffered frame: every camera whose oldest frame lies within
/// [`SyncConfig::window`] of it contributes that frame. Then:
///
/// - **Complete**: every camera contributed. The group is committed to the camera ports under
///   one bridge lock (like a batch), so a tick sees it whole or not at all.
/// - **Incomplete** and decided: a missing camera already buffered a later frame (it skipped this
///   one), or the [`SyncConfig::timeout`] elapsed (a stalled camera), or a camera's buffer is
///   full. [`SyncConfig::partial`] resolves it.
/// - Otherwise the group waits; [`CameraFeed::next_deadline`] says until when.
///
/// Committing replaces whatever the camera ports still queue from the previous group, so a slow
/// graph always ticks on the latest group and queues never grow (`superseded_groups`). Frames
/// stamped at or before a resolved group's window are dropped as late. Payloads move from the
/// camera to the bridge without copies; buffering, grouping and committing allocate nothing.
#[derive(Clone)]
pub struct SynchronizedCameras {
    inner: Arc<Inner>,
}

struct Inner {
    host: HostBridgeHandle,
    clock: Clock,
    config: SyncConfig,
    state: Mutex<State>,
}

struct Slot {
    stamp: u64,
    arrived: Instant,
    payload: Payload,
}

struct Camera {
    port: PortId,
    offset: i64,
    /// Buffered frames, sorted by stamp; capacity `max_buffered`, never grown.
    ring: Vec<Slot>,
    /// Last committed frame ([`PartialPolicy::HoldLast`] only).
    last: Option<Payload>,
}

struct State {
    cameras: Vec<Camera>,
    /// Frames stamped at or before this belong to resolved groups.
    watermark: Option<u64>,
    deadline: Option<Instant>,
    stats: MultiCameraStats,
}

/// The oldest group: who contributes, its anchor stamp, and its earliest arrival.
struct Pending {
    cameras: CameraSet,
    stamp: u64,
    arrived: Instant,
    /// Every missing camera already buffered a later frame.
    decided: bool,
}

impl SynchronizedCameras {
    pub(super) fn new(host: &HostBridgeHandle, ports: Vec<PortId>, config: SyncConfig) -> Self {
        assert!(
            config.max_buffered > 0,
            "SyncConfig::max_buffered must be > 0"
        );
        let cameras = ports
            .into_iter()
            .map(|port| Camera {
                port,
                offset: 0,
                ring: Vec::with_capacity(config.max_buffered),
                last: None,
            })
            .collect();
        Self {
            inner: Arc::new(Inner {
                host: host.clone(),
                clock: host.clock(),
                config,
                state: Mutex::new(State {
                    cameras,
                    watermark: None,
                    deadline: None,
                    stats: MultiCameraStats::default(),
                }),
            }),
        }
    }

    pub fn camera_count(&self) -> usize {
        self.inner.state.lock().cameras.len()
    }

    pub fn config(&self) -> SyncConfig {
        self.inner.config
    }

    /// Push `camera`'s frame, stamped by [`SyncConfig::stamp`].
    pub fn push(&self, camera: usize, payload: Payload) -> CameraPush {
        match (self.inner.config.stamp)(&payload) {
            Some(stamp) => self.push_stamped(camera, payload, stamp),
            None => {
                self.inner.state.lock().stats.unstamped_frames += 1;
                CameraPush::Unstamped
            }
        }
    }

    /// Push `camera`'s frame with a stamp the caller read (same units as the window).
    pub fn push_stamped(&self, camera: usize, payload: Payload, stamp: u64) -> CameraPush {
        let _scope = io_timing::host_alloc_scope();
        let inner = &*self.inner;
        let mut state = inner.state.lock();
        let Some(port) = state.cameras.get(camera).map(|camera| &camera.port) else {
            return CameraPush::UnknownCamera;
        };
        {
            let bridge = inner.host.shared.buffers.lock();
            if bridge.closed
                || bridge
                    .inbound
                    .get(port.as_str())
                    .is_some_and(|port| port.closed)
            {
                return CameraPush::Closed;
            }
            if let Err(error) = bridge.types.check_payload(&payload) {
                return CameraPush::Rejected(Box::new(error));
            }
        }
        let stamp = stamp.saturating_add_signed(state.cameras[camera].offset);
        let is_late = |state: &State| state.watermark.is_some_and(|mark| stamp <= mark);
        if is_late(&state) {
            state.stats.late_frames += 1;
            return CameraPush::Late;
        }
        let now = inner.clock.now();
        let mut resolved = false;
        while state.cameras[camera].ring.len() == inner.config.max_buffered {
            let pending = state
                .pending(inner.config.window)
                .expect("a full buffer has a pending group");
            state.stats.overflows += 1;
            inner.resolve(&mut state, pending);
            resolved = true;
        }
        // Making room can resolve the window an out-of-order frame belongs to.
        if is_late(&state) {
            state.stats.late_frames += 1;
            return CameraPush::Late;
        }
        state.stats.frames += 1;
        let ring = &mut state.cameras[camera].ring;
        let at = ring.partition_point(|slot| slot.stamp <= stamp);
        ring.insert(
            at,
            Slot {
                stamp,
                arrived: now,
                payload,
            },
        );
        resolved |= inner.settle(&mut state, now);
        if resolved {
            CameraPush::Resolved
        } else {
            CameraPush::Buffered
        }
    }

    /// Set `camera`'s clock offset in stamp units, added to every later stamp of it (e.g. the
    /// measured skew of its clock against camera 0). Ignored for an unknown camera.
    pub fn set_offset(&self, camera: usize, offset: i64) {
        if let Some(camera) = self.inner.state.lock().cameras.get_mut(camera) {
            camera.offset = offset;
        }
    }

    /// Drop every buffered frame (and [`PartialPolicy::HoldLast`]'s last frames).
    pub fn clear(&self) {
        let mut state = self.inner.state.lock();
        for camera in &mut state.cameras {
            camera.ring.clear();
            camera.last = None;
        }
        state.deadline = None;
    }

    pub fn stats(&self) -> MultiCameraStats {
        self.inner.state.lock().stats
    }

    /// Frames buffered per camera right now.
    pub fn buffered(&self, camera: usize) -> usize {
        self.inner
            .state
            .lock()
            .cameras
            .get(camera)
            .map_or(0, |camera| camera.ring.len())
    }
}

impl CameraFeed for SynchronizedCameras {
    fn host(&self) -> &HostBridgeHandle {
        &self.inner.host
    }

    fn clock(&self) -> &Clock {
        &self.inner.clock
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.inner.state.lock().deadline
    }

    fn expire(&self) -> bool {
        let _scope = io_timing::host_alloc_scope();
        let now = self.inner.clock.now();
        let mut state = self.inner.state.lock();
        if state.deadline.is_none_or(|deadline| deadline > now) {
            return false;
        }
        self.inner.settle(&mut state, now)
    }
}

impl State {
    /// The group anchored on the oldest buffered frame, if any frame is buffered.
    fn pending(&self, window: u64) -> Option<Pending> {
        let stamp = self
            .cameras
            .iter()
            .filter_map(|camera| camera.ring.first())
            .map(|slot| slot.stamp)
            .min()?;
        let mut cameras = CameraSet::EMPTY;
        let mut arrived: Option<Instant> = None;
        let mut decided = true;
        for (index, camera) in self.cameras.iter().enumerate() {
            match camera.ring.first() {
                Some(slot) if slot.stamp - stamp <= window => {
                    cameras = cameras.with(index);
                    arrived = Some(arrived.map_or(slot.arrived, |at| at.min(slot.arrived)));
                }
                Some(_) => {}
                None => decided = false,
            }
        }
        Some(Pending {
            cameras,
            stamp,
            arrived: arrived.expect("the anchor frame is in its group"),
            decided,
        })
    }
}

impl Inner {
    /// Resolve every group that is complete or decided; record when the next one times out.
    fn settle(&self, state: &mut State, now: Instant) -> bool {
        let mut resolved = false;
        state.deadline = None;
        while let Some(pending) = state.pending(self.config.window) {
            let complete = pending.cameras.len() == state.cameras.len();
            let deadline = pending.arrived + self.config.timeout;
            if !complete && !pending.decided {
                if deadline > now {
                    state.deadline = Some(deadline);
                    break;
                }
                state.stats.timeouts += 1;
            }
            self.resolve(state, pending);
            resolved = true;
        }
        resolved
    }

    /// Commit or drop `pending` (the oldest group) and advance the watermark past it.
    fn resolve(&self, state: &mut State, pending: Pending) {
        let complete = pending.cameras.len() == state.cameras.len();
        let watermark = pending.stamp.saturating_add(self.config.window);
        state.watermark = Some(state.watermark.map_or(watermark, |w| w.max(watermark)));
        if !complete && self.config.partial == PartialPolicy::Drop {
            for index in pending.cameras.iter() {
                state.cameras[index].ring.remove(0);
            }
            state.stats.dropped_groups += 1;
            state.stats.dropped_frames += pending.cameras.len() as u64;
        } else {
            self.commit(state, &pending, complete);
        }
        let late = state.watermark.unwrap_or(0);
        for camera in &mut state.cameras {
            let stale = camera.ring.partition_point(|slot| slot.stamp <= late);
            if stale > 0 {
                camera.ring.drain(..stale);
                state.stats.dropped_frames += stale as u64;
            }
        }
    }

    /// Move the group's frames into the camera ports under one bridge lock, replacing what the
    /// previous group left unconsumed, and wake the drive loop once.
    fn commit(&self, state: &mut State, pending: &Pending, complete: bool) {
        let hold_last = self.config.partial == PartialPolicy::HoldLast;
        let mut group = CameraGroup {
            cameras: pending.cameras,
            stamp: pending.stamp,
            complete,
            ..CameraGroup::default()
        };
        let mut bridge = self.host.shared.buffers.lock();
        let buffers: &mut HostBridgeBuffers = &mut bridge;
        let mut superseded = false;
        for (index, camera) in state.cameras.iter_mut().enumerate() {
            let payload = if pending.cameras.contains(index) {
                let slot = camera.ring.remove(0);
                group.skew = group.skew.max(slot.stamp - pending.stamp);
                Some(slot.payload)
            } else if hold_last && camera.last.is_some() {
                group.reused = group.reused.with(index);
                camera.last.clone()
            } else {
                None
            };
            let port = buffers.inbound.port(camera.port.clone());
            if port.held.is_none() && !port.queue.is_empty() {
                port.queue.clear();
                superseded = true;
            }
            let Some(payload) = payload else { continue };
            if hold_last {
                camera.last = Some(payload.clone());
            }
            enqueue_locked(
                buffers,
                Direction::Inbound,
                self.host.alias.as_str(),
                PortKey::Id(camera.port.clone()),
                payload,
            );
        }
        let stats = &mut state.stats;
        if complete {
            stats.complete_groups += 1;
        } else {
            stats.partial_groups += 1;
        }
        stats.superseded_groups += u64::from(superseded);
        stats.max_skew = stats.max_skew.max(group.skew);
        stats.last_group = Some(group);
        wait::wake_inbound(&self.host.shared, bridge);
    }
}
