use std::time::Instant;

use daedalus_transport::{CorrelationId, DropReason, FeedOutcome, Payload, TypeKey};

use crate::handles::PortId;
use crate::plan::RuntimeEdgePolicy;

use super::DEFAULT_HOST_BRIDGE_EVENT_LIMIT;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostBridgeConfig {
    pub default_input_policy: RuntimeEdgePolicy,
    pub default_output_policy: RuntimeEdgePolicy,
    /// Whether host bridge feed/drop/deliver events are retained for runtime diagnostics.
    pub event_recording: bool,
    /// Maximum retained event snapshots per host bridge handle.
    ///
    /// The default is `Some(DEFAULT_HOST_BRIDGE_EVENT_LIMIT)`. Use `Some(n)` to retain only the
    /// most recent `n` events, `Some(0)` to retain none, or `None` for unbounded retention.
    pub event_limit: Option<usize>,
}

impl HostBridgeConfig {
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
            event_recording: true,
            event_limit: Some(DEFAULT_HOST_BRIDGE_EVENT_LIMIT),
        }
    }
}

#[derive(Clone)]
pub struct HostBridgePayload {
    pub port: PortId,
    pub payload: Payload,
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
    /// Monotonic timestamp captured when the host bridge event was recorded.
    pub at: Instant,
    pub alias: String,
    pub port: String,
    pub correlation_id: CorrelationId,
    pub kind: HostBridgeEventKind,
    pub type_key: TypeKey,
    pub outcome: Option<FeedOutcome>,
    pub reason: Option<DropReason>,
}
