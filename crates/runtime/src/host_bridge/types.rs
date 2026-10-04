use crate::prelude::*;
use daedalus_core::platform::Instant;

use daedalus_transport::{
    CorrelationId, DropReason, FeedOutcome, Payload, PolicyValidationError, TypeKey,
    validate_stream_policy,
};

use crate::handles::PortId;
use crate::plan::RuntimeEdgePolicy;

use super::{DEFAULT_HOST_BRIDGE_EVENT_LIMIT, DEFAULT_HOST_BRIDGE_EVENT_RECORDING};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostBridgeConfig {
    pub default_input_policy: RuntimeEdgePolicy,
    pub default_output_policy: RuntimeEdgePolicy,
    /// Whether host bridge feed/drop/deliver events are retained for runtime diagnostics.
    ///
    /// Off by default (`DEFAULT_HOST_BRIDGE_EVENT_RECORDING`): recording allocates one event per
    /// push and delivery. Enable it while debugging dropped or missing host payloads. Stats and
    /// `tracing` pressure warnings are always available.
    pub event_recording: bool,
    /// Maximum retained event snapshots per host bridge handle.
    ///
    /// The default is `Some(DEFAULT_HOST_BRIDGE_EVENT_LIMIT)`. Use `Some(n)` to retain only the
    /// most recent `n` events, `Some(0)` to retain none, or `None` for unbounded retention.
    pub event_limit: Option<usize>,
}

impl HostBridgeConfig {
    /// Check both direction default policies with [`validate_stream_policy`].
    pub fn validate(&self) -> Result<(), PolicyValidationError> {
        for policy in [&self.default_input_policy, &self.default_output_policy] {
            validate_stream_policy(&policy.pressure, &policy.freshness)?;
        }
        Ok(())
    }

    pub fn with_default_input_policy(mut self, policy: RuntimeEdgePolicy) -> Self {
        self.default_input_policy = policy;
        self
    }

    pub fn with_default_output_policy(mut self, policy: RuntimeEdgePolicy) -> Self {
        self.default_output_policy = policy;
        self
    }

    /// Enable or disable diagnostic host bridge event retention.
    ///
    /// Disabling recording keeps queue behavior and stats intact but makes `events()` snapshots
    /// empty for existing and future handles once the config is applied.
    pub fn with_event_recording(mut self, enabled: bool) -> Self {
        self.event_recording = enabled;
        self
    }

    /// Set the retained diagnostic event limit.
    ///
    /// `Some(n)` keeps the latest `n` events, `Some(0)` disables retention without changing
    /// `event_recording`, and `None` keeps all events until the caller changes the limit or drops
    /// the handle.
    pub fn with_event_limit(mut self, limit: Option<usize>) -> Self {
        self.event_limit = limit;
        self
    }
}

impl Default for HostBridgeConfig {
    fn default() -> Self {
        Self {
            default_input_policy: RuntimeEdgePolicy::bounded(1),
            default_output_policy: RuntimeEdgePolicy::bounded(1),
            event_recording: DEFAULT_HOST_BRIDGE_EVENT_RECORDING,
            event_limit: Some(DEFAULT_HOST_BRIDGE_EVENT_LIMIT),
        }
    }
}

#[derive(Clone)]
pub struct HostBridgePayload {
    pub port: PortId,
    pub payload: Payload,
}

/// Counters for one host port, from [`HostBridgeHandle::input_port_stats`] or
/// [`HostBridgeHandle::output_port_stats`].
///
/// [`HostBridgeHandle::input_port_stats`]: super::HostBridgeHandle::input_port_stats
/// [`HostBridgeHandle::output_port_stats`]: super::HostBridgeHandle::output_port_stats
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostPortStats {
    /// Payloads queued on the port, including ones that replaced a queued value.
    pub accepted: u64,
    /// Queued payloads replaced by a newer one before they were taken.
    pub replaced: u64,
    /// Payloads rejected by freshness, pressure, or a closed port.
    pub dropped: u64,
    /// Payloads taken off the port: by the graph for inputs, by the host for outputs.
    pub delivered: u64,
    /// Payloads currently queued.
    pub pending: usize,
}

impl HostPortStats {
    pub(super) fn record_enqueue(&mut self, outcome: &FeedOutcome) {
        let counter = match outcome {
            FeedOutcome::Accepted { .. } => &mut self.accepted,
            FeedOutcome::Replaced { .. } => {
                self.accepted = self.accepted.saturating_add(1);
                &mut self.replaced
            }
            FeedOutcome::Dropped { .. }
            | FeedOutcome::Backpressured
            | FeedOutcome::Closed
            | FeedOutcome::Rejected(_) => &mut self.dropped,
        };
        *counter = counter.saturating_add(1);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostBridgeStats {
    pub inbound_accepted: u64,
    pub inbound_replaced: u64,
    pub inbound_dropped: u64,
    pub inbound_drop_reasons: HostBridgeDropStats,
    pub outbound_delivered: u64,
    pub outbound_replaced: u64,
    pub outbound_dropped: u64,
    pub outbound_drop_reasons: HostBridgeDropStats,
    pub closed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostBridgeDropStats {
    pub backpressure: u64,
    pub drop_newest: u64,
    pub drop_oldest: u64,
    pub latest_only_replace: u64,
    pub max_age: u64,
    pub max_lag: u64,
    pub closed: u64,
    pub error_on_full: u64,
}

impl HostBridgeDropStats {
    pub fn count(&self, reason: DropReason) -> u64 {
        match reason {
            DropReason::Backpressure => self.backpressure,
            DropReason::DropNewest => self.drop_newest,
            DropReason::DropOldest => self.drop_oldest,
            DropReason::LatestOnlyReplace => self.latest_only_replace,
            DropReason::MaxAge => self.max_age,
            DropReason::MaxLag => self.max_lag,
            DropReason::Closed => self.closed,
            DropReason::ErrorOnFull => self.error_on_full,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostBridgeEventKind {
    SourceFeed,
    SourceDrop,
    SourceReplace,
    OutputEnqueue,
    OutputDrop,
    OutputDeliver,
}

#[derive(Clone, Debug)]
pub struct HostBridgeEvent {
    /// When the event was recorded, on the bridge clock
    /// ([`HostBridgeManager::set_clock`](crate::HostBridgeManager::set_clock)).
    pub at: Instant,
    pub alias: String,
    pub port: String,
    pub correlation_id: CorrelationId,
    pub kind: HostBridgeEventKind,
    pub type_key: TypeKey,
    pub outcome: Option<FeedOutcome>,
    pub reason: Option<DropReason>,
}
