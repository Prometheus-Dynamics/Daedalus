//! One graph fed by several cameras.
//!
//! Two feeders sit between camera threads and a host bridge's camera ports, both built on the
//! bridge's own machinery (one-lock commits, held ports, inbound wakeups) and both allocation-free
//! and copy-free per frame once constructed:
//!
//! - [`MultiCamera::synchronized`] groups frames whose stamps (capture timestamps, or sequence
//!   numbers) fall within a window into one tick, committed atomically, with a timeout and a
//!   [`PartialPolicy`] for sets a camera did not complete ([`SynchronizedCameras`]).
//! - [`MultiCamera::independent`] keeps each camera's latest frame held and ticks per arrival of
//!   the trigger cameras, so every tick sees every camera's current frame
//!   ([`IndependentCameras`]).
//!
//! Both implement [`CameraFeed`]: a drive loop services their deadlines (the synchronized
//! timeout, the independent `max_age`) by calling [`CameraFeed::expire`] when
//! [`CameraFeed::poll_timeout`] elapses (`HostGraph::drive_cameras_blocking` and
//! `HostGraph::tick_ready_cameras` in `daedalus-engine` do this).

use crate::prelude::*;
use core::time::Duration;

use daedalus_core::platform::{Clock, Instant};
use daedalus_transport::{FrameInterface, FrameSource, FrameView, Payload, TypeKeyError};

use crate::handles::PortId;

use super::HostBridgeHandle;

mod independent;
mod sync;
#[cfg(test)]
mod tests;

pub use independent::{IndependentCameras, IndependentConfig};
pub use sync::{PartialPolicy, SyncConfig, SynchronizedCameras};

/// Most cameras one feeder serves (the width of [`CameraSet`]).
pub const MAX_CAMERAS: usize = 64;

/// Reads a payload's stamp: a capture timestamp (ns) or a sequence number. A plain function
/// pointer, so feeders store and call it without allocating.
pub type StampFn = fn(&Payload) -> Option<u64>;

fn frame_view<R>(payload: &Payload, read: impl FnOnce(&FrameView<'_>) -> R) -> Option<R> {
    let view = payload.foreign_borrow()?.view::<FrameInterface>().ok()?;
    Some(read(&view))
}

/// `timestamp_ns` of a `daedalus:frame` payload (a foreign handle or a
/// `Payload::provide_foreign` payload), read from metadata only: no CPU access to the planes.
pub fn frame_timestamp(payload: &Payload) -> Option<u64> {
    frame_view(payload, |frame| frame.timestamp_ns())
}

/// `sequence` of a `daedalus:frame` payload (see [`frame_timestamp`]).
pub fn frame_sequence(payload: &Payload) -> Option<u64> {
    frame_view(payload, |frame| frame.sequence())
}

/// [`FrameSource::timestamp_ns`] of a payload holding the owner frame type `O` (what hosts
/// usually push), else [`frame_timestamp`]. Use as `source_timestamp::<MyFrame>`.
pub fn source_timestamp<O: FrameSource>(payload: &Payload) -> Option<u64> {
    payload
        .get_ref::<O>()
        .map(O::timestamp_ns)
        .or_else(|| frame_timestamp(payload))
}

/// [`FrameSource::sequence`] of an owner frame `O`, else [`frame_sequence`].
pub fn source_sequence<O: FrameSource>(payload: &Payload) -> Option<u64> {
    payload
        .get_ref::<O>()
        .map(O::sequence)
        .or_else(|| frame_sequence(payload))
}

/// A set of camera indices (`0..64`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct CameraSet(u64);

impl CameraSet {
    pub const EMPTY: Self = Self(0);

    /// Cameras `0..count`.
    pub const fn all(count: usize) -> Self {
        if count >= MAX_CAMERAS {
            Self(u64::MAX)
        } else {
            Self((1 << count) - 1)
        }
    }

    pub const fn only(camera: usize) -> Self {
        Self::EMPTY.with(camera)
    }

    pub const fn with(self, camera: usize) -> Self {
        Self(self.0 | 1 << camera)
    }

    pub const fn contains(self, camera: usize) -> bool {
        camera < MAX_CAMERAS && self.0 & 1 << camera != 0
    }

    pub const fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Camera indices in ascending order.
    pub fn iter(self) -> impl Iterator<Item = usize> {
        (0..MAX_CAMERAS).filter(move |&camera| self.contains(camera))
    }
}

impl FromIterator<usize> for CameraSet {
    fn from_iter<I: IntoIterator<Item = usize>>(cameras: I) -> Self {
        cameras.into_iter().fold(Self::EMPTY, Self::with)
    }
}

/// One synchronized group as committed to the bridge (what the next tick sees).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CameraGroup {
    /// Cameras whose frame of this group was committed.
    pub cameras: CameraSet,
    /// Cameras that missed the group and got their last frame again ([`PartialPolicy::HoldLast`]).
    pub reused: CameraSet,
    /// Offset-corrected stamp of the group's oldest frame.
    pub stamp: u64,
    /// Largest minus smallest offset-corrected stamp among [`Self::cameras`].
    pub skew: u64,
    /// Every camera contributed a frame of this group.
    pub complete: bool,
}

/// Counters of one multi-camera feeder ([`SynchronizedCameras::stats`],
/// [`IndependentCameras::stats`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MultiCameraStats {
    /// Frames accepted from cameras.
    pub frames: u64,
    /// Complete groups committed.
    pub complete_groups: u64,
    /// Incomplete groups committed ([`PartialPolicy::TickPartial`], [`PartialPolicy::HoldLast`]).
    pub partial_groups: u64,
    /// Incomplete groups discarded ([`PartialPolicy::Drop`]).
    pub dropped_groups: u64,
    /// Buffered frames discarded: those of dropped groups, and those a resolved group's window
    /// covered without taking them (a camera's second frame within one window).
    pub dropped_frames: u64,
    /// Incomplete groups resolved because their timeout elapsed.
    pub timeouts: u64,
    /// Incomplete groups resolved early because a camera's buffer was full.
    pub overflows: u64,
    /// Committed groups replaced by a newer one before a tick took them.
    pub superseded_groups: u64,
    /// Frames refused on arrival because they are stamped at or before an already resolved
    /// group's window.
    pub late_frames: u64,
    /// Frames the stamp function could not read, refused.
    pub unstamped_frames: u64,
    /// Held frames cleared because they outlived `max_age` ([`IndependentConfig::max_age`]).
    pub expired_frames: u64,
    /// Largest skew of a committed group.
    pub max_skew: u64,
    /// The latest committed group.
    pub last_group: Option<CameraGroup>,
}

/// What a synchronized camera push did ([`SynchronizedCameras::push`]).
#[derive(Debug)]
pub enum CameraPush {
    /// Buffered until its group completes or times out.
    Buffered,
    /// Buffered, and at least one group was resolved (committed or dropped by policy).
    Resolved,
    /// Stamped at or before an already resolved group: dropped.
    Late,
    /// The stamp function read no stamp: refused.
    Unstamped,
    /// No camera with this index.
    UnknownCamera,
    /// The bridge or the camera's port is closed: refused.
    Closed,
    /// The payload fails the bridge's type check (see `HostBridgeHandle::feed_payload`).
    Rejected(Box<TypeKeyError>),
}

/// Host-side deadlines of a multi-camera feeder that a drive loop must service.
pub trait CameraFeed {
    /// The bridge the feeder commits to.
    fn host(&self) -> &HostBridgeHandle;
    /// The clock deadlines are read on (the bridge clock when the feeder was built).
    fn clock(&self) -> &Clock;
    /// When [`Self::expire`] next has work, if ever.
    fn next_deadline(&self) -> Option<Instant>;
    /// Resolve whatever is due now; `true` when input was committed or cleared.
    fn expire(&self) -> bool;
    /// Time until [`Self::next_deadline`] (zero when due): the timeout for `poll(2)` or a wait.
    fn poll_timeout(&self) -> Option<Duration> {
        let deadline = self.next_deadline()?;
        Some(deadline.saturating_duration_since(self.clock().now()))
    }
}

/// Constructors of the multi-camera feeders.
pub struct MultiCamera;

impl MultiCamera {
    /// Group frames of `ports` (camera `i` feeds `ports[i]`) by stamp into atomic ticks; see
    /// [`SynchronizedCameras`].
    ///
    /// # Panics
    /// With no ports, more than [`MAX_CAMERAS`], or `config.max_buffered == 0`.
    pub fn synchronized<P: Into<PortId>>(
        host: &HostBridgeHandle,
        ports: impl IntoIterator<Item = P>,
        config: SyncConfig,
    ) -> SynchronizedCameras {
        SynchronizedCameras::new(host, ports_of(ports), config)
    }

    /// Hold each camera's latest frame on `ports` and tick per arrival; see
    /// [`IndependentCameras`].
    ///
    /// # Panics
    /// With no ports or more than [`MAX_CAMERAS`].
    pub fn independent<P: Into<PortId>>(
        host: &HostBridgeHandle,
        ports: impl IntoIterator<Item = P>,
        config: IndependentConfig,
    ) -> IndependentCameras {
        IndependentCameras::new(host, ports_of(ports), config)
    }
}

fn ports_of<P: Into<PortId>>(ports: impl IntoIterator<Item = P>) -> Vec<PortId> {
    let ports: Vec<PortId> = ports.into_iter().map(Into::into).collect();
    assert!(
        (1..=MAX_CAMERAS).contains(&ports.len()),
        "a multi-camera feeder serves 1..={MAX_CAMERAS} cameras, got {}",
        ports.len()
    );
    ports
}
