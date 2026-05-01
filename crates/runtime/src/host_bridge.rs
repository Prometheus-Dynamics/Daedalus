use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use daedalus_transport::{
    DropReason, FeedOutcome, FreshnessPolicy, Payload, PolicyValidationError, PressurePolicy,
    TypeKey, validate_stream_policy,
};
use smallvec::SmallVec;

use crate::handles::{HostAlias, PortId};

mod events;
mod manager;
mod policy;
mod serializers;
mod types;
use events::{
    outcome_drop_reason, record_drop_reason, record_host_event, replacement_reason,
    trim_host_events,
};
pub use manager::{HostBridgeManager, bridge_handler};
use policy::{apply_host_pressure, freshness_drop_reason};
pub use serializers::{
    ValueSerializer, ValueSerializerMap, new_value_serializer_map, register_value_serializer_in,
    value_serializer_map,
};
pub use types::{
    HostBridgeConfig, HostBridgeDropStats, HostBridgeEvent, HostBridgeEventKind, HostBridgePayload,
    HostBridgeStats,
};

pub const DEFAULT_HOST_BRIDGE_EVENT_LIMIT: usize = 1024;

/// Metadata key attached to host-bridge descriptors to mark them for runtime wiring.
pub const HOST_BRIDGE_META_KEY: &str = daedalus_core::metadata::HOST_BRIDGE_META_KEY;
/// Canonical registry id for the host-bridge node.
pub const HOST_BRIDGE_ID: &str = "io.host_bridge";

pub(super) struct HostBridgeBuffers {
    pub(super) inbound: HashMap<PortId, VecDeque<HostBridgePayload>>,
    pub(super) outbound: HashMap<PortId, VecDeque<HostBridgePayload>>,
    pub(super) default_input_pressure: PressurePolicy,
    pub(super) default_input_freshness: FreshnessPolicy,
    pub(super) default_output_pressure: PressurePolicy,
    pub(super) default_output_freshness: FreshnessPolicy,
    pub(super) input_pressure: HashMap<PortId, PressurePolicy>,
    pub(super) input_freshness: HashMap<PortId, FreshnessPolicy>,
    pub(super) output_pressure: HashMap<PortId, PressurePolicy>,
    pub(super) output_freshness: HashMap<PortId, FreshnessPolicy>,
    pub(super) latest_sequence: HashMap<PortId, u64>,
    pub(super) latest_timestamp: HashMap<PortId, u64>,
    pub(super) closed_inputs: HashSet<PortId>,
    pub(super) closed: bool,
    pub(super) events_enabled: bool,
    pub(super) event_limit: Option<usize>,
    pub(super) stats: HostBridgeStats,
    pub(super) events: VecDeque<HostBridgeEvent>,
}

pub(super) struct HostBridgeShared {
    pub(super) buffers: Mutex<HostBridgeBuffers>,
    pub(super) ready: Condvar,
}

impl Default for HostBridgeShared {
    fn default() -> Self {
        Self {
            buffers: Mutex::new(HostBridgeBuffers::default()),
            ready: Condvar::new(),
        }
    }
}

impl Default for HostBridgeBuffers {
    fn default() -> Self {
        Self {
            inbound: HashMap::new(),
            outbound: HashMap::new(),
            default_input_pressure: PressurePolicy::default(),
            default_input_freshness: FreshnessPolicy::default(),
            default_output_pressure: PressurePolicy::default(),
            default_output_freshness: FreshnessPolicy::default(),
            input_pressure: HashMap::new(),
            input_freshness: HashMap::new(),
            output_pressure: HashMap::new(),
            output_freshness: HashMap::new(),
            latest_sequence: HashMap::new(),
            latest_timestamp: HashMap::new(),
            closed_inputs: HashSet::new(),
            closed: false,
            events_enabled: true,
            event_limit: Some(DEFAULT_HOST_BRIDGE_EVENT_LIMIT),
            stats: HostBridgeStats::default(),
            events: VecDeque::new(),
        }
    }
}

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
        port: impl Into<String>,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        validate_stream_policy(&pressure, &freshness)?;
        let port = port.into();
        let mut guard = lock_host_buffers(&self.shared);
        let port = PortId::new(port);
        guard.input_pressure.insert(port.clone(), pressure);
        guard.input_freshness.insert(port, freshness);
        Ok(())
    }

    pub fn set_output_policy(
        &self,
        port: impl Into<String>,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        validate_stream_policy(&pressure, &freshness)?;
        let port = port.into();
        let mut guard = lock_host_buffers(&self.shared);
        let port = PortId::new(port);
        guard.output_pressure.insert(port.clone(), pressure);
        guard.output_freshness.insert(port, freshness);
        Ok(())
    }

    pub fn set_default_input_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        validate_stream_policy(&pressure, &freshness)?;
        let mut guard = lock_host_buffers(&self.shared);
        guard.default_input_pressure = pressure;
        guard.default_input_freshness = freshness;
        Ok(())
    }

    pub fn set_default_output_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        validate_stream_policy(&pressure, &freshness)?;
        let mut guard = lock_host_buffers(&self.shared);
        guard.default_output_pressure = pressure;
        guard.default_output_freshness = freshness;
        Ok(())
    }

    pub fn set_event_recording(&self, enabled: bool) {
        let mut guard = lock_host_buffers(&self.shared);
        guard.events_enabled = enabled;
        if !enabled {
            guard.events.clear();
        }
    }

    pub fn set_event_limit(&self, limit: Option<usize>) {
        let mut guard = lock_host_buffers(&self.shared);
        guard.event_limit = limit;
        trim_host_events(&mut guard);
    }

    pub fn apply_config(&self, config: &HostBridgeConfig) -> Result<(), PolicyValidationError> {
        validate_stream_policy(
            &config.default_input_policy.pressure,
            &config.default_input_policy.freshness,
        )?;
        validate_stream_policy(
            &config.default_output_policy.pressure,
            &config.default_output_policy.freshness,
        )?;

        let mut guard = lock_host_buffers(&self.shared);
        guard.default_input_pressure = config.default_input_policy.pressure.clone();
        guard.default_input_freshness = config.default_input_policy.freshness.clone();
        guard.default_output_pressure = config.default_output_policy.pressure.clone();
        guard.default_output_freshness = config.default_output_policy.freshness.clone();
        guard.events_enabled = config.event_recording;
        guard.event_limit = config.event_limit;
        if !config.event_recording {
            guard.events.clear();
        } else {
            trim_host_events(&mut guard);
        }
        Ok(())
    }

    pub fn feed_payload(&self, port: impl Into<String>, payload: Payload) -> FeedOutcome {
        let port = port.into();
        self.feed_payload_ref(&port, payload)
    }

    pub fn feed_payload_ref(&self, port: &str, payload: Payload) -> FeedOutcome {
        let mut guard = lock_host_buffers(&self.shared);
        let port_id = PortId::from(port);
        if guard.closed || guard.closed_inputs.contains(&port_id) {
            guard.stats.inbound_dropped = guard.stats.inbound_dropped.saturating_add(1);
            record_drop_reason(&mut guard.stats.inbound_drop_reasons, DropReason::Closed);
            let outcome = FeedOutcome::Dropped {
                correlation_id: payload.correlation_id(),
                reason: DropReason::Closed,
            };
            record_host_event(
                &mut guard,
                self.alias.as_str(),
                port,
                &payload,
                HostBridgeEventKind::SourceDrop,
                Some(outcome.clone()),
                Some(DropReason::Closed),
            );
            return outcome;
        }
        let freshness = guard
            .input_freshness
            .get(port)
            .cloned()
            .unwrap_or_else(|| guard.default_input_freshness.clone());
        if let Some(reason) = freshness_drop_reason(&mut guard, port, &payload, &freshness) {
            guard.stats.inbound_dropped = guard.stats.inbound_dropped.saturating_add(1);
            record_drop_reason(&mut guard.stats.inbound_drop_reasons, reason.clone());
            let outcome = FeedOutcome::Dropped {
                correlation_id: payload.correlation_id(),
                reason: reason.clone(),
            };
            record_host_event(
                &mut guard,
                self.alias.as_str(),
                port,
                &payload,
                HostBridgeEventKind::SourceDrop,
                Some(outcome.clone()),
                Some(reason),
            );
            return outcome;
        }
        let pressure = guard
            .input_pressure
            .get(port)
            .cloned()
            .unwrap_or_else(|| guard.default_input_pressure.clone());
        let event_payload = guard.events_enabled.then(|| payload.clone());
        let queue = guard.inbound.entry(port_id.clone()).or_default();
        let outcome = apply_host_pressure(
            &pressure,
            queue,
            HostBridgePayload {
                port: port_id,
                payload,
            },
        );
        match outcome {
            FeedOutcome::Accepted { .. } => {
                guard.stats.inbound_accepted = guard.stats.inbound_accepted.saturating_add(1);
            }
            FeedOutcome::Replaced { .. } => {
                guard.stats.inbound_accepted = guard.stats.inbound_accepted.saturating_add(1);
                guard.stats.inbound_replaced = guard.stats.inbound_replaced.saturating_add(1);
                if let Some(reason) = replacement_reason(&pressure) {
                    record_drop_reason(&mut guard.stats.inbound_drop_reasons, reason);
                }
            }
            FeedOutcome::Dropped { .. } | FeedOutcome::Backpressured | FeedOutcome::Closed => {
                guard.stats.inbound_dropped = guard.stats.inbound_dropped.saturating_add(1);
                if let Some(reason) = outcome_drop_reason(&outcome) {
                    record_drop_reason(&mut guard.stats.inbound_drop_reasons, reason);
                }
            }
        }
        let kind = match outcome {
            FeedOutcome::Accepted { .. } => HostBridgeEventKind::SourceFeed,
            FeedOutcome::Replaced { .. } => HostBridgeEventKind::SourceReplace,
            FeedOutcome::Dropped { .. } | FeedOutcome::Backpressured | FeedOutcome::Closed => {
                HostBridgeEventKind::SourceDrop
            }
        };
        if let Some(event_payload) = event_payload.as_ref() {
            let reason = match outcome {
                FeedOutcome::Replaced { .. } => replacement_reason(&pressure),
                _ => outcome_drop_reason(&outcome),
            };
            record_host_event(
                &mut guard,
                self.alias.as_str(),
                port,
                event_payload,
                kind,
                Some(outcome.clone()),
                reason,
            );
        }
        if matches!(
            outcome,
            FeedOutcome::Accepted { .. } | FeedOutcome::Replaced { .. }
        ) {
            self.shared.ready.notify_all();
        }
        outcome
    }

    pub fn push_payload(&self, port: impl Into<String>, payload: Payload) -> FeedOutcome {
        self.feed_payload(port, payload)
    }

    pub fn push<T>(&self, port: impl Into<String>, value: T) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        let type_key =
            crate::transport::typeexpr_transport_key(&daedalus_data::typing::type_expr::<T>())
                .unwrap_or_else(|_| TypeKey::new(std::any::type_name::<T>()));
        self.push_payload(port, Payload::owned(type_key, value))
    }

    pub fn push_as<T>(
        &self,
        port: impl Into<String>,
        type_key: impl Into<TypeKey>,
        value: T,
    ) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        self.push_payload(port, Payload::owned(type_key, value))
    }

    pub fn push_arc_as<T>(
        &self,
        port: impl Into<String>,
        type_key: impl Into<TypeKey>,
        value: Arc<T>,
    ) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        self.push_payload(port, Payload::shared(type_key, value))
    }

    pub fn push_any<T>(&self, port: impl Into<String>, value: T) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        self.push(port, value)
    }

    pub fn try_pop_payload(&self, port: impl AsRef<str>) -> Option<Payload> {
        let mut guard = lock_host_buffers(&self.shared);
        pop_outbound_locked(&mut guard, self.alias.as_str(), port.as_ref())
    }

    pub fn recv_payload_timeout(
        &self,
        port: impl AsRef<str>,
        timeout: Duration,
    ) -> Option<Payload> {
        let port = port.as_ref();
        let deadline = Instant::now() + timeout;
        let mut guard = lock_host_buffers(&self.shared);
        loop {
            if let Some(payload) = pop_outbound_locked(&mut guard, self.alias.as_str(), port) {
                return Some(payload);
            }
            if guard.closed {
                return None;
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_guard, wait) = self
                .shared
                .ready
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard = next_guard;
            if wait.timed_out() {
                return pop_outbound_locked(&mut guard, self.alias.as_str(), port);
            }
        }
    }

    pub(crate) fn wait_for_inbound(&self, timeout: Duration) -> bool {
        let guard = lock_host_buffers(&self.shared);
        if has_pending_inbound_locked(&guard) || guard.closed {
            return true;
        }
        if timeout.is_zero() {
            return false;
        }
        let (guard, _) = self
            .shared
            .ready
            .wait_timeout(guard, timeout)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        has_pending_inbound_locked(&guard) || guard.closed
    }

    pub(crate) fn notify_waiters(&self) {
        self.shared.ready.notify_all();
    }

    pub fn close(&self) {
        let mut guard = lock_host_buffers(&self.shared);
        guard.closed = true;
        guard.stats.closed = true;
        self.shared.ready.notify_all();
    }

    pub fn close_input(&self, port: impl Into<String>) {
        let mut guard = lock_host_buffers(&self.shared);
        let port = PortId::new(port);
        guard.closed_inputs.insert(port.clone());
        guard.inbound.remove(&port);
        self.shared.ready.notify_all();
    }

    pub fn is_input_closed(&self, port: impl AsRef<str>) -> bool {
        let guard = lock_host_buffers(&self.shared);
        guard.closed || guard.closed_inputs.contains(&PortId::from(port.as_ref()))
    }

    pub fn stats(&self) -> HostBridgeStats {
        let guard = lock_host_buffers(&self.shared);
        let mut stats = guard.stats.clone();
        stats.closed = guard.closed;
        stats
    }

    pub fn config_snapshot(&self) -> HostBridgeConfig {
        let guard = lock_host_buffers(&self.shared);
        HostBridgeConfig {
            default_input_policy: crate::plan::RuntimeEdgePolicy {
                pressure: guard.default_input_pressure.clone(),
                freshness: guard.default_input_freshness.clone(),
            },
            default_output_policy: crate::plan::RuntimeEdgePolicy {
                pressure: guard.default_output_pressure.clone(),
                freshness: guard.default_output_freshness.clone(),
            },
            event_recording: guard.events_enabled,
            event_limit: guard.event_limit,
        }
    }

    pub fn events(&self) -> Vec<HostBridgeEvent> {
        let guard = lock_host_buffers(&self.shared);
        guard.events.iter().cloned().collect()
    }

    pub fn pending_inbound(&self) -> usize {
        let guard = lock_host_buffers(&self.shared);
        guard.inbound.values().map(VecDeque::len).sum()
    }

    pub fn pending_outbound(&self) -> usize {
        let guard = lock_host_buffers(&self.shared);
        guard.outbound.values().map(VecDeque::len).sum()
    }

    pub fn has_pending_inbound(&self) -> bool {
        self.pending_inbound() > 0
    }

    pub fn try_pop<T>(&self, port: impl AsRef<str>) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.try_pop_payload(port)
            .and_then(|payload| payload.get_ref::<T>().cloned())
    }

    pub fn try_pop_owned<T>(&self, port: impl AsRef<str>) -> Result<Option<T>, Box<Payload>>
    where
        T: Send + Sync + 'static,
    {
        let Some(payload) = self.try_pop_payload(port) else {
            return Ok(None);
        };
        payload.try_into_owned::<T>().map(Some)
    }

    pub(crate) fn push_outbound_ref(&self, port: &str, payload: Payload) {
        let mut guard = lock_host_buffers(&self.shared);
        let freshness = guard
            .output_freshness
            .get(port)
            .cloned()
            .unwrap_or_else(|| guard.default_output_freshness.clone());
        if let Some(reason) = freshness_drop_reason(&mut guard, port, &payload, &freshness) {
            guard.stats.outbound_dropped = guard.stats.outbound_dropped.saturating_add(1);
            record_drop_reason(&mut guard.stats.outbound_drop_reasons, reason.clone());
            let outcome = FeedOutcome::Dropped {
                correlation_id: payload.correlation_id(),
                reason: reason.clone(),
            };
            record_host_event(
                &mut guard,
                self.alias.as_str(),
                port,
                &payload,
                HostBridgeEventKind::OutputDrop,
                Some(outcome),
                Some(reason),
            );
            return;
        }
        let pressure = guard
            .output_pressure
            .get(port)
            .cloned()
            .unwrap_or_else(|| guard.default_output_pressure.clone());
        let event_payload = guard.events_enabled.then(|| payload.clone());
        let port_id = PortId::from(port);
        let queue = guard.outbound.entry(port_id.clone()).or_default();
        let outcome = apply_host_pressure(
            &pressure,
            queue,
            HostBridgePayload {
                port: port_id,
                payload,
            },
        );
        match outcome {
            FeedOutcome::Accepted { .. } => {
                if let Some(event_payload) = event_payload.as_ref() {
                    record_host_event(
                        &mut guard,
                        self.alias.as_str(),
                        port,
                        event_payload,
                        HostBridgeEventKind::OutputEnqueue,
                        Some(outcome.clone()),
                        None,
                    );
                }
            }
            FeedOutcome::Replaced { .. } => {
                guard.stats.outbound_replaced = guard.stats.outbound_replaced.saturating_add(1);
                if let Some(reason) = replacement_reason(&pressure) {
                    record_drop_reason(&mut guard.stats.outbound_drop_reasons, reason);
                }
                if let Some(event_payload) = event_payload.as_ref() {
                    record_host_event(
                        &mut guard,
                        self.alias.as_str(),
                        port,
                        event_payload,
                        HostBridgeEventKind::OutputEnqueue,
                        Some(outcome.clone()),
                        replacement_reason(&pressure),
                    );
                }
            }
            FeedOutcome::Dropped { .. } | FeedOutcome::Backpressured | FeedOutcome::Closed => {
                guard.stats.outbound_dropped = guard.stats.outbound_dropped.saturating_add(1);
                let reason = outcome_drop_reason(&outcome);
                if let Some(reason) = reason.clone() {
                    record_drop_reason(&mut guard.stats.outbound_drop_reasons, reason);
                }
                if let Some(event_payload) = event_payload.as_ref() {
                    record_host_event(
                        &mut guard,
                        self.alias.as_str(),
                        port,
                        event_payload,
                        HostBridgeEventKind::OutputDrop,
                        Some(outcome.clone()),
                        reason,
                    );
                }
            }
        }
        if matches!(
            outcome,
            FeedOutcome::Accepted { .. } | FeedOutcome::Replaced { .. }
        ) {
            self.shared.ready.notify_all();
        }
    }

    pub(crate) fn take_inbound_small(&self) -> SmallVec<[HostBridgePayload; 4]> {
        let mut guard = lock_host_buffers(&self.shared);
        guard
            .inbound
            .values_mut()
            .flat_map(|queue| queue.drain(..))
            .collect()
    }

    pub(crate) fn take_inbound_for_ports_small(
        &self,
        ports: &[PortId],
    ) -> SmallVec<[HostBridgePayload; 4]> {
        let mut guard = lock_host_buffers(&self.shared);
        let mut payloads = SmallVec::new();
        for port in ports {
            if let Some(queue) = guard.inbound.get_mut(port) {
                payloads.extend(queue.drain(..));
            }
        }
        payloads
    }

    pub fn try_pop_arc<T>(&self, port: impl AsRef<str>) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.try_pop_payload(port)
            .and_then(|payload| payload.get_arc::<T>())
    }

    pub fn drain_payloads(&self, port: impl AsRef<str>) -> Vec<Payload> {
        let mut guard = lock_host_buffers(&self.shared);
        let payloads: Vec<Payload> = guard
            .outbound
            .get_mut(port.as_ref())
            .map(|queue| queue.drain(..).map(|entry| entry.payload).collect())
            .unwrap_or_default();
        guard.stats.outbound_delivered = guard
            .stats
            .outbound_delivered
            .saturating_add(payloads.len() as u64);
        for payload in &payloads {
            record_host_event(
                &mut guard,
                self.alias.as_str(),
                port.as_ref(),
                payload,
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
        self.drain_payloads(port)
            .into_iter()
            .filter_map(|payload| payload.get_ref::<T>().cloned())
            .collect()
    }

    pub fn drain_arcs<T>(&self, port: impl AsRef<str>) -> Vec<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.drain_payloads(port)
            .into_iter()
            .filter_map(|payload| payload.get_arc::<T>())
            .collect()
    }
}

fn has_pending_inbound_locked(guard: &HostBridgeBuffers) -> bool {
    guard.inbound.values().any(|queue| !queue.is_empty())
}

fn pop_outbound_locked(guard: &mut HostBridgeBuffers, alias: &str, port: &str) -> Option<Payload> {
    let payload = guard
        .outbound
        .get_mut(port)
        .and_then(VecDeque::pop_front)
        .map(|entry| entry.payload);
    if payload.is_some() {
        guard.stats.outbound_delivered = guard.stats.outbound_delivered.saturating_add(1);
    }
    if let Some(payload) = payload.as_ref() {
        record_host_event(
            guard,
            alias,
            port,
            payload,
            HostBridgeEventKind::OutputDeliver,
            None,
            None,
        );
    }
    payload
}

pub(super) fn lock_host_buffers(shared: &HostBridgeShared) -> MutexGuard<'_, HostBridgeBuffers> {
    shared
        .buffers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) fn lock_host_map(
    inner: &Mutex<HashMap<HostAlias, Arc<HostBridgeShared>>>,
) -> MutexGuard<'_, HashMap<HostAlias, Arc<HostBridgeShared>>> {
    inner
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(in crate::host_bridge) fn lock_host_defaults(
    defaults: &Mutex<manager::HostBridgeDefaults>,
) -> MutexGuard<'_, manager::HostBridgeDefaults> {
    defaults
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
