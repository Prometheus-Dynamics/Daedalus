use crate::prelude::*;

use crate::portable::Arc;
#[cfg(feature = "std")]
use crate::sync::Condvar;
use crate::sync::Mutex;

use daedalus_core::platform::Clock;
use daedalus_transport::{
    CorrelationId, DropReason, FeedOutcome, FreshnessPolicy, Payload, PolicyValidationError,
    PressurePolicy, TypeKey, TypeKeyError, validate_stream_policy,
};

use crate::handles::{HostAlias, PortId};
use crate::type_index::TypeIndex;

mod events;
mod inspect;
mod io_timing;
mod manager;
mod policy;
mod ports;
mod serializers;
mod types;
mod wait;
use events::{
    EventLog, EventSubject, outcome_drop_reason, record_drop_reason, record_host_event,
    replacement_reason,
};
pub use inspect::{PayloadInspection, PayloadSummary, inspect_payload, serialize_payload_value};
pub use io_timing::HostIoTime;
pub use manager::{HostBridgeManager, bridge_handler};
use policy::freshness_drop_reason;
use ports::{PortDirection, PortEntry, PortKey, PortState};
#[cfg(feature = "plugins")]
pub(crate) use serializers::for_each_builtin_primitive;
pub use serializers::{
    ValueSerializer, ValueSerializerMap, new_value_serializer_map, primitive_value_serializer_map,
    register_primitive_value_serializers_in, register_value_serializer_in,
};
pub use types::{
    HostBridgeConfig, HostBridgeDropStats, HostBridgeEvent, HostBridgeEventKind, HostBridgePayload,
    HostBridgeStats, HostPortStats,
};
pub use wait::{InboundWait, InboundWaiter};

/// Default retained event limit when host bridge event recording is enabled.
pub const DEFAULT_HOST_BRIDGE_EVENT_LIMIT: usize = 1024;
/// Host bridge event recording is off by default; enable it for diagnostics.
pub const DEFAULT_HOST_BRIDGE_EVENT_RECORDING: bool = false;

/// Metadata key attached to host-bridge descriptors to mark them for runtime wiring.
pub const HOST_BRIDGE_META_KEY: &str = daedalus_core::metadata::HOST_BRIDGE_META_KEY;
/// Canonical registry id for the host-bridge node.
pub const HOST_BRIDGE_ID: &str = "io.host_bridge";

/// State guarded by the bridge's single buffer lock.
pub(super) struct HostBridgeBuffers {
    /// Host → graph ports.
    pub(in crate::host_bridge) inbound: PortDirection,
    /// Graph → host ports.
    pub(in crate::host_bridge) outbound: PortDirection,
    pub(super) closed: bool,
    pub(super) stats: HostBridgeStats,
    pub(in crate::host_bridge) events: EventLog,
    /// Bumped by `wake_inbound_waiters` so waiters can tell explicit wakeups from spurious ones.
    pub(super) wake_epoch: u64,
    pub(super) next_waker_id: u64,
    /// Async inbound waiters, keyed by waiter id. Woken outside the lock.
    pub(super) inbound_wakers: Vec<(u64, core::task::Waker)>,
    /// Resolves typed pushes and checks fed payloads (see [`HostBridgeManager::set_type_index`]).
    pub(super) types: TypeIndex,
    /// Stamps payloads built by pushes and events, and ages payloads for
    /// `FreshnessPolicy::MaxAge` (see [`HostBridgeManager::set_clock`]).
    pub(super) clock: Clock,
}

pub(super) struct HostBridgeShared {
    pub(super) buffers: Mutex<HostBridgeBuffers>,
    pub(super) io_timing: io_timing::HostIoTiming,
    /// Wakes blocking waits (`threads`); kept with `std` so the layout follows the lock backend.
    #[cfg(feature = "std")]
    pub(super) ready: Condvar,
}

impl HostBridgeShared {
    pub(super) fn new(buffers: HostBridgeBuffers) -> Self {
        Self {
            buffers: Mutex::new(buffers),
            io_timing: io_timing::HostIoTiming::default(),
            #[cfg(feature = "std")]
            ready: Condvar::new(),
        }
    }

    /// Wake blocking waiters after a change to `buffers`.
    #[inline]
    pub(super) fn notify_all(&self) {
        #[cfg(feature = "std")]
        self.ready.notify_all();
    }
}

impl Default for HostBridgeShared {
    fn default() -> Self {
        Self::new(HostBridgeBuffers::default())
    }
}

impl Default for HostBridgeBuffers {
    fn default() -> Self {
        Self {
            inbound: PortDirection::default(),
            outbound: PortDirection::default(),
            closed: false,
            stats: HostBridgeStats::default(),
            events: EventLog::new(
                DEFAULT_HOST_BRIDGE_EVENT_RECORDING,
                Some(DEFAULT_HOST_BRIDGE_EVENT_LIMIT),
            ),
            wake_epoch: 0,
            next_waker_id: 0,
            inbound_wakers: Vec::new(),
            types: TypeIndex::default(),
            clock: Clock::default(),
        }
    }
}

impl HostBridgeBuffers {
    fn from_config(config: &HostBridgeConfig) -> Self {
        let mut buffers = Self::default();
        buffers.apply_config(config);
        buffers
    }

    /// Apply direction defaults and event settings; per-port overrides are kept.
    fn apply_config(&mut self, config: &HostBridgeConfig) {
        self.inbound
            .set_default_policy(&config.default_input_policy);
        self.outbound
            .set_default_policy(&config.default_output_policy);
        self.events.limit = config.event_limit;
        self.events.set_enabled(config.event_recording);
        self.events.trim();
    }

    fn ports_mut(&mut self, direction: Direction) -> &mut PortDirection {
        match direction {
            Direction::Inbound => &mut self.inbound,
            Direction::Outbound => &mut self.outbound,
        }
    }
}

/// Which side of the bridge a payload is queued on; selects stats counters and event kinds.
#[derive(Clone, Copy)]
enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    fn enqueue_kind(self, outcome: &FeedOutcome) -> HostBridgeEventKind {
        match (self, outcome) {
            (Direction::Inbound, FeedOutcome::Accepted { .. }) => HostBridgeEventKind::SourceFeed,
            (Direction::Inbound, FeedOutcome::Replaced { .. }) => {
                HostBridgeEventKind::SourceReplace
            }
            (Direction::Inbound, _) => HostBridgeEventKind::SourceDrop,
            (Direction::Outbound, FeedOutcome::Accepted { .. } | FeedOutcome::Replaced { .. }) => {
                HostBridgeEventKind::OutputEnqueue
            }
            (Direction::Outbound, _) => HostBridgeEventKind::OutputDrop,
        }
    }

    fn drop_kind(self) -> HostBridgeEventKind {
        match self {
            Direction::Inbound => HostBridgeEventKind::SourceDrop,
            Direction::Outbound => HostBridgeEventKind::OutputDrop,
        }
    }

    fn count_drop(self, stats: &mut HostBridgeStats, reason: Option<DropReason>) {
        let (dropped, reasons) = match self {
            Direction::Inbound => (&mut stats.inbound_dropped, &mut stats.inbound_drop_reasons),
            Direction::Outbound => (
                &mut stats.outbound_dropped,
                &mut stats.outbound_drop_reasons,
            ),
        };
        *dropped = dropped.saturating_add(1);
        if let Some(reason) = reason {
            record_drop_reason(reasons, reason);
        }
    }

    fn count_replaced(self, stats: &mut HostBridgeStats, reason: Option<DropReason>) {
        let (replaced, reasons) = match self {
            Direction::Inbound => {
                stats.inbound_accepted = stats.inbound_accepted.saturating_add(1);
                (&mut stats.inbound_replaced, &mut stats.inbound_drop_reasons)
            }
            Direction::Outbound => (
                &mut stats.outbound_replaced,
                &mut stats.outbound_drop_reasons,
            ),
        };
        *replaced = replaced.saturating_add(1);
        if let Some(reason) = reason {
            record_drop_reason(reasons, reason);
        }
    }

    fn count_accepted(self, stats: &mut HostBridgeStats) {
        if let Direction::Inbound = self {
            stats.inbound_accepted = stats.inbound_accepted.saturating_add(1);
        }
    }
}

/// Apply freshness and pressure policy for one payload on one port, update stats, and record
/// diagnostics. Performs a single port-state lookup.
fn enqueue_locked(
    buffers: &mut HostBridgeBuffers,
    direction: Direction,
    alias: &str,
    port: PortKey<'_>,
    payload: Payload,
) -> FeedOutcome {
    let HostBridgeBuffers {
        inbound,
        outbound,
        closed,
        stats,
        events,
        clock,
        ..
    } = buffers;
    let ports = match direction {
        Direction::Inbound => inbound,
        Direction::Outbound => outbound,
    };
    let PortEntry {
        state,
        default_pressure,
        default_freshness,
    } = ports.entry(port);

    if matches!(direction, Direction::Inbound) && (*closed || state.closed) {
        direction.count_drop(stats, Some(DropReason::Closed));
        state.stats.dropped = state.stats.dropped.saturating_add(1);
        let outcome = FeedOutcome::Dropped {
            correlation_id: payload.correlation_id(),
            reason: DropReason::Closed,
        };
        record_host_event(
            events,
            clock,
            alias,
            state.id.as_str(),
            EventSubject::from(&payload),
            direction.drop_kind(),
            Some(outcome.clone()),
            Some(DropReason::Closed),
        );
        return outcome;
    }

    let freshness = state.freshness.as_ref().unwrap_or(default_freshness);
    if let Some(reason) = freshness_drop_reason(&mut state.marks, &payload, freshness, clock) {
        direction.count_drop(stats, Some(reason.clone()));
        state.stats.dropped = state.stats.dropped.saturating_add(1);
        let outcome = FeedOutcome::Dropped {
            correlation_id: payload.correlation_id(),
            reason: reason.clone(),
        };
        record_host_event(
            events,
            clock,
            alias,
            state.id.as_str(),
            EventSubject::from(&payload),
            direction.drop_kind(),
            Some(outcome.clone()),
            Some(reason),
        );
        return outcome;
    }

    let pressure = state.pressure.as_ref().unwrap_or(default_pressure);
    let replacement = replacement_reason(pressure);
    // Capture only the event identity; cloning the payload would make a unique payload shared.
    let subject: Option<(CorrelationId, TypeKey)> = events
        .enabled
        .then(|| (payload.correlation_id(), payload.type_key().clone()));
    let incoming = payload.correlation_id();
    let outcome = FeedOutcome::from_push(
        state.queue.push(
            pressure,
            HostBridgePayload {
                port: state.id.clone(),
                payload,
            },
        ),
        incoming,
        |old| old.payload.correlation_id(),
    );
    state.stats.record_enqueue(&outcome);
    let reason = match outcome {
        FeedOutcome::Accepted { .. } => {
            direction.count_accepted(stats);
            None
        }
        FeedOutcome::Replaced { .. } => {
            direction.count_replaced(stats, replacement.clone());
            replacement
        }
        FeedOutcome::Dropped { .. }
        | FeedOutcome::Backpressured
        | FeedOutcome::Closed
        | FeedOutcome::Rejected(_) => {
            let reason = outcome_drop_reason(&outcome);
            direction.count_drop(stats, reason.clone());
            reason
        }
    };
    if let Some((correlation_id, type_key)) = subject.as_ref() {
        record_host_event(
            events,
            clock,
            alias,
            state.id.as_str(),
            EventSubject {
                correlation_id: *correlation_id,
                type_key,
            },
            direction.enqueue_kind(&outcome),
            Some(outcome.clone()),
            reason,
        );
    }
    outcome
}

fn is_enqueued(outcome: &FeedOutcome) -> bool {
    matches!(
        outcome,
        FeedOutcome::Accepted { .. } | FeedOutcome::Replaced { .. }
    )
}

/// Host-side handle to one host bridge.
///
/// Port arguments follow one convention: methods that write or configure a port (`push*`,
/// `feed_payload`, `set_*_policy`, `close_input`) take `impl Into<PortId>`, so passing a
/// pre-built [`PortId`] never allocates. Methods that read or look up a port (`try_pop*`,
/// `drain*`, `recv_payload_timeout`, `is_input_closed`) take `impl AsRef<str>` and look the port
/// up without allocating.
#[derive(Clone)]
pub struct HostBridgeHandle {
    alias: HostAlias,
    shared: Arc<HostBridgeShared>,
}

impl HostBridgeHandle {
    pub(super) fn new(alias: HostAlias, shared: Arc<HostBridgeShared>) -> Self {
        Self { alias, shared }
    }

    pub fn alias(&self) -> &str {
        self.alias.as_str()
    }

    pub fn set_input_policy(
        &self,
        port: impl Into<PortId>,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.set_port_policy(Direction::Inbound, port.into(), pressure, freshness)
    }

    pub fn set_output_policy(
        &self,
        port: impl Into<PortId>,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.set_port_policy(Direction::Outbound, port.into(), pressure, freshness)
    }

    fn set_port_policy(
        &self,
        direction: Direction,
        port: PortId,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        validate_stream_policy(&pressure, &freshness)?;
        let mut guard = self.shared.buffers.lock();
        let state = guard.ports_mut(direction).port(port);
        state.pressure = Some(pressure);
        state.freshness = Some(freshness);
        Ok(())
    }

    pub fn set_default_input_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.set_default_policy(Direction::Inbound, pressure, freshness)
    }

    pub fn set_default_output_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.set_default_policy(Direction::Outbound, pressure, freshness)
    }

    fn set_default_policy(
        &self,
        direction: Direction,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        validate_stream_policy(&pressure, &freshness)?;
        self.shared
            .buffers
            .lock()
            .ports_mut(direction)
            .set_defaults(pressure, freshness);
        Ok(())
    }

    /// Enable or disable retained diagnostic events. Disabling clears retained events.
    pub fn set_event_recording(&self, enabled: bool) {
        self.shared.buffers.lock().events.set_enabled(enabled);
    }

    pub fn set_event_limit(&self, limit: Option<usize>) {
        self.shared.buffers.lock().events.set_limit(limit);
    }

    pub fn apply_config(&self, config: &HostBridgeConfig) -> Result<(), PolicyValidationError> {
        config.validate()?;
        self.shared.buffers.lock().apply_config(config);
        Ok(())
    }

    /// Feed a payload into an inbound port, applying the port's freshness and pressure policy.
    ///
    /// A payload whose key the graph's registry maps to another Rust type than the payload
    /// holds is refused with [`FeedOutcome::Rejected`] (see [`TypeIndex::check_payload`]).
    ///
    /// The payload keeps its lineage: `FreshnessPolicy::MaxAge` ages it on the bridge clock
    /// ([`Self::clock`]), so with a custom clock build it with [`Payload::stamp`]. The `push*`
    /// methods stamp the payloads they build.
    pub fn feed_payload(&self, port: impl Into<PortId>, payload: Payload) -> FeedOutcome {
        self.feed_with(port.into(), |_, _| Ok(payload))
    }

    /// Feed `value` under the key the graph's registry gives `T`
    /// ([`HostBridgeManager::set_type_index`]); a type the registry has no single key for is
    /// refused with [`FeedOutcome::Rejected`] naming the fixes.
    pub fn push<T>(&self, port: impl Into<PortId>, value: T) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        self.feed_with(port.into(), |types, clock| {
            types
                .key_of::<T>()
                .map(|key| Payload::owned(key, value).stamp(clock))
        })
    }

    /// The type index this bridge resolves typed pushes through.
    pub fn type_index(&self) -> TypeIndex {
        self.shared.buffers.lock().types.clone()
    }

    /// The bridge clock ([`HostBridgeManager::set_clock`]).
    pub fn clock(&self) -> Clock {
        self.shared.buffers.lock().clock.clone()
    }

    fn feed_with(
        &self,
        port: PortId,
        payload: impl FnOnce(&TypeIndex, &Clock) -> Result<Payload, TypeKeyError>,
    ) -> FeedOutcome {
        let _timer = self.shared.io_timing.push_timer();
        let mut guard = self.shared.buffers.lock();
        let payload = match payload(&guard.types, &guard.clock)
            .and_then(|payload| guard.types.check_payload(&payload).map(|()| payload))
        {
            Ok(payload) => payload,
            Err(error) => return FeedOutcome::Rejected(Box::new(error)),
        };
        let outcome = enqueue_locked(
            &mut guard,
            Direction::Inbound,
            self.alias.as_str(),
            PortKey::Id(port),
            payload,
        );
        if is_enqueued(&outcome) {
            self.shared.notify_all();
            let wakers = wait::take_inbound_wakers(&mut guard);
            drop(guard);
            wait::wake_all(wakers);
        }
        outcome
    }

    pub fn push_as<T>(
        &self,
        port: impl Into<PortId>,
        type_key: impl Into<TypeKey>,
        value: T,
    ) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        let payload = Payload::owned(type_key, value);
        self.feed_with(port.into(), |_, clock| Ok(payload.stamp(clock)))
    }

    pub fn push_arc_as<T>(
        &self,
        port: impl Into<PortId>,
        type_key: impl Into<TypeKey>,
        value: Arc<T>,
    ) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        let payload = Payload::shared(type_key, value);
        self.feed_with(port.into(), |_, clock| Ok(payload.stamp(clock)))
    }

    pub fn try_pop_payload(&self, port: impl AsRef<str>) -> Option<Payload> {
        let _timer = self.shared.io_timing.take_timer();
        self.pop_payload(port.as_ref())
    }

    fn pop_payload(&self, port: &str) -> Option<Payload> {
        let mut guard = self.shared.buffers.lock();
        pop_outbound_locked(&mut guard, self.alias.as_str(), port)
    }

    /// Pop an outbound payload from `port`, blocking up to `timeout` for one (`threads`).
    #[cfg(feature = "threads")]
    pub fn recv_payload_timeout(
        &self,
        port: impl AsRef<str>,
        timeout: core::time::Duration,
    ) -> Option<Payload> {
        let port = port.as_ref();
        let deadline = std::time::Instant::now() + timeout;
        let mut guard = self.shared.buffers.lock();
        loop {
            if let Some(payload) = pop_outbound_locked(&mut guard, self.alias.as_str(), port) {
                return Some(payload);
            }
            if guard.closed {
                return None;
            }
            if self
                .shared
                .ready
                .wait_until(&mut guard, deadline)
                .timed_out()
            {
                return pop_outbound_locked(&mut guard, self.alias.as_str(), port);
            }
        }
    }

    pub fn close(&self) {
        let mut guard = self.shared.buffers.lock();
        guard.closed = true;
        guard.stats.closed = true;
        self.shared.notify_all();
        let wakers = wait::take_inbound_wakers(&mut guard);
        drop(guard);
        wait::wake_all(wakers);
    }

    /// Close one inbound port: queued input is discarded and later feeds are dropped as closed.
    pub fn close_input(&self, port: impl Into<PortId>) {
        let mut guard = self.shared.buffers.lock();
        let state = guard.inbound.port(port.into());
        state.closed = true;
        state.queue.clear();
        self.shared.notify_all();
    }

    pub fn is_input_closed(&self, port: impl AsRef<str>) -> bool {
        let guard = self.shared.buffers.lock();
        guard.closed
            || guard
                .inbound
                .get(port.as_ref())
                .is_some_and(|state| state.closed)
    }

    /// Counters for one inbound (host → graph) port; `None` if the port was never used.
    pub fn input_port_stats(&self, port: impl AsRef<str>) -> Option<HostPortStats> {
        let guard = self.shared.buffers.lock();
        guard.inbound.get(port.as_ref()).map(PortState::stats)
    }

    /// Counters for one outbound (graph → host) port; `None` if the port was never used.
    pub fn output_port_stats(&self, port: impl AsRef<str>) -> Option<HostPortStats> {
        let guard = self.shared.buffers.lock();
        guard.outbound.get(port.as_ref()).map(PortState::stats)
    }

    pub fn stats(&self) -> HostBridgeStats {
        let guard = self.shared.buffers.lock();
        let mut stats = guard.stats.clone();
        stats.closed = guard.closed;
        stats
    }

    pub fn config_snapshot(&self) -> HostBridgeConfig {
        let guard = self.shared.buffers.lock();
        HostBridgeConfig {
            default_input_policy: guard.inbound.default_policy(),
            default_output_policy: guard.outbound.default_policy(),
            event_recording: guard.events.enabled,
            event_limit: guard.events.limit,
        }
    }

    pub fn events(&self) -> Vec<HostBridgeEvent> {
        let guard = self.shared.buffers.lock();
        guard.events.events.iter().cloned().collect()
    }

    pub fn pending_inbound(&self) -> usize {
        self.shared.buffers.lock().inbound.pending()
    }

    pub fn pending_outbound(&self) -> usize {
        self.shared.buffers.lock().outbound.pending()
    }

    pub fn has_pending_inbound(&self) -> bool {
        has_pending_inbound_locked(&self.shared.buffers.lock())
    }

    pub fn try_pop<T>(&self, port: impl AsRef<str>) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let _timer = self.shared.io_timing.take_timer();
        self.pop_payload(port.as_ref())
            .and_then(|payload| payload.get_ref::<T>().cloned())
    }

    pub fn try_pop_owned<T>(&self, port: impl AsRef<str>) -> Result<Option<T>, Box<Payload>>
    where
        T: Send + Sync + 'static,
    {
        let _timer = self.shared.io_timing.take_timer();
        let Some(payload) = self.pop_payload(port.as_ref()) else {
            return Ok(None);
        };
        payload.try_into_owned::<T>().map(Some)
    }

    /// Queue a graph output for the host. Allocates a `PortId` only the first time a port is seen.
    pub(crate) fn push_outbound_ref(&self, port: &str, payload: Payload) {
        let mut guard = self.shared.buffers.lock();
        let outcome = enqueue_locked(
            &mut guard,
            Direction::Outbound,
            self.alias.as_str(),
            PortKey::Name(port),
            payload,
        );
        if is_enqueued(&outcome) {
            self.shared.notify_all();
        }
    }

    /// Move every queued inbound payload into `out` (oldest first per port). Reuse `out` across
    /// calls to keep draining allocation-free.
    pub fn take_inbound_into(&self, out: &mut Vec<HostBridgePayload>) {
        let mut guard = self.shared.buffers.lock();
        for state in guard.inbound.ports.values_mut() {
            state.drain_into(|entry| out.push(entry));
        }
    }

    /// Hand every queued payload of one inbound port to `sink` while the bridge lock is held.
    /// `sink` must not call back into this bridge.
    pub(crate) fn drain_inbound_port(&self, port: &str, sink: impl FnMut(HostBridgePayload)) {
        if let Some(state) = self.shared.buffers.lock().inbound.get_mut(port) {
            state.drain_into(sink);
        }
    }

    pub fn try_pop_arc<T>(&self, port: impl AsRef<str>) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let _timer = self.shared.io_timing.take_timer();
        self.pop_payload(port.as_ref())
            .and_then(|payload| payload.get_arc::<T>())
    }

    pub fn drain_payloads(&self, port: impl AsRef<str>) -> Vec<Payload> {
        let _timer = self.shared.io_timing.take_timer();
        self.drain_port(port.as_ref())
    }

    fn drain_port(&self, port: &str) -> Vec<Payload> {
        let mut guard = self.shared.buffers.lock();
        let buffers = &mut *guard;
        let mut payloads = Vec::new();
        if let Some(state) = buffers.outbound.get_mut(port) {
            state.drain_into(|entry| payloads.push(entry.payload));
        }
        buffers.stats.outbound_delivered = buffers
            .stats
            .outbound_delivered
            .saturating_add(payloads.len() as u64);
        for payload in &payloads {
            record_host_event(
                &mut buffers.events,
                &buffers.clock,
                self.alias.as_str(),
                port,
                EventSubject::from(payload),
                HostBridgeEventKind::OutputDeliver,
                None,
                None,
            );
        }
        payloads
    }

    pub fn drain<T>(&self, port: impl AsRef<str>) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let _timer = self.shared.io_timing.take_timer();
        self.drain_port(port.as_ref())
            .into_iter()
            .filter_map(|payload| payload.get_ref::<T>().cloned())
            .collect()
    }

    pub fn drain_arcs<T>(&self, port: impl AsRef<str>) -> Vec<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let _timer = self.shared.io_timing.take_timer();
        self.drain_port(port.as_ref())
            .into_iter()
            .filter_map(|payload| payload.get_arc::<T>())
            .collect()
    }
}

fn has_pending_inbound_locked(guard: &HostBridgeBuffers) -> bool {
    guard.inbound.has_pending()
}

fn pop_outbound_locked(guard: &mut HostBridgeBuffers, alias: &str, port: &str) -> Option<Payload> {
    let payload = guard
        .outbound
        .get_mut(port)
        .and_then(PortState::pop_front)
        .map(|entry| entry.payload)?;
    guard.stats.outbound_delivered = guard.stats.outbound_delivered.saturating_add(1);
    record_host_event(
        &mut guard.events,
        &guard.clock,
        alias,
        port,
        EventSubject::from(&payload),
        HostBridgeEventKind::OutputDeliver,
        None,
        None,
    );
    Some(payload)
}
