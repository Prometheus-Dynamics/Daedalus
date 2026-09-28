use std::collections::VecDeque;
use std::time::Instant;

use daedalus_transport::{
    CorrelationId, DropReason, FeedOutcome, OverflowPolicy, Payload, PressurePolicy, TypeKey,
};

use super::{HostBridgeDropStats, HostBridgeEvent, HostBridgeEventKind};

/// Retained diagnostic events for one host bridge.
pub(super) struct EventLog {
    pub(super) enabled: bool,
    pub(super) limit: Option<usize>,
    pub(super) events: VecDeque<HostBridgeEvent>,
}

impl EventLog {
    pub(super) fn new(enabled: bool, limit: Option<usize>) -> Self {
        Self {
            enabled,
            limit,
            events: VecDeque::new(),
        }
    }

    pub(super) fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        if !enabled {
            self.events.clear();
        }
    }

    pub(super) fn set_limit(&mut self, limit: Option<usize>) {
        self.limit = limit;
        self.trim();
    }

    pub(super) fn trim(&mut self) {
        if let Some(limit) = self.limit {
            while self.events.len() > limit {
                self.events.pop_front();
            }
        }
    }
}

/// Identity of the payload an event refers to. Borrowed, so recording never clones the payload
/// (a clone would make a queued unique payload shared).
#[derive(Clone, Copy)]
pub(super) struct EventSubject<'a> {
    pub(super) correlation_id: CorrelationId,
    pub(super) type_key: &'a TypeKey,
}

impl<'a> From<&'a Payload> for EventSubject<'a> {
    fn from(payload: &'a Payload) -> Self {
        Self {
            correlation_id: payload.correlation_id(),
            type_key: payload.type_key(),
        }
    }
}

pub(super) fn record_host_event(
    log: &mut EventLog,
    alias: &str,
    port: &str,
    subject: EventSubject<'_>,
    kind: HostBridgeEventKind,
    outcome: Option<FeedOutcome>,
    reason: Option<DropReason>,
) {
    if matches!(
        kind,
        HostBridgeEventKind::SourceDrop
            | HostBridgeEventKind::OutputDrop
            | HostBridgeEventKind::SourceReplace
    ) || reason.is_some()
    {
        tracing::warn!(
            target: "daedalus_runtime::host_bridge",
            alias,
            port,
            kind = ?kind,
            reason = ?reason,
            outcome = ?outcome,
            payload_type = %subject.type_key,
            correlation_id = subject.correlation_id,
            "host bridge payload pressure event",
        );
    } else {
        tracing::trace!(
            target: "daedalus_runtime::host_bridge",
            alias,
            port,
            kind = ?kind,
            outcome = ?outcome,
            payload_type = %subject.type_key,
            correlation_id = subject.correlation_id,
            "host bridge payload event",
        );
    }

    if !log.enabled {
        return;
    }
    match log.limit {
        Some(0) => {
            log.events.clear();
            return;
        }
        Some(limit) => {
            while log.events.len() >= limit {
                log.events.pop_front();
            }
        }
        None => {}
    }
    log.events.push_back(HostBridgeEvent {
        at: Instant::now(),
        alias: alias.to_string(),
        port: port.to_string(),
        correlation_id: subject.correlation_id,
        kind,
        type_key: subject.type_key.clone(),
        outcome,
        reason,
    });
}

pub(super) fn record_drop_reason(stats: &mut HostBridgeDropStats, reason: DropReason) {
    match reason {
        DropReason::Backpressure => stats.backpressure = stats.backpressure.saturating_add(1),
        DropReason::DropNewest => stats.drop_newest = stats.drop_newest.saturating_add(1),
        DropReason::DropOldest => stats.drop_oldest = stats.drop_oldest.saturating_add(1),
        DropReason::LatestOnlyReplace => {
            stats.latest_only_replace = stats.latest_only_replace.saturating_add(1);
        }
        DropReason::MaxAge => stats.max_age = stats.max_age.saturating_add(1),
        DropReason::MaxLag => stats.max_lag = stats.max_lag.saturating_add(1),
        DropReason::Closed => stats.closed = stats.closed.saturating_add(1),
        DropReason::ErrorOnFull => stats.error_on_full = stats.error_on_full.saturating_add(1),
    }
}

pub(super) fn outcome_drop_reason(outcome: &FeedOutcome) -> Option<DropReason> {
    match outcome {
        FeedOutcome::Dropped { reason, .. } => Some(reason.clone()),
        FeedOutcome::Backpressured => Some(DropReason::Backpressure),
        FeedOutcome::Closed => Some(DropReason::Closed),
        _ => None,
    }
}

pub(super) fn replacement_reason(pressure: &PressurePolicy) -> Option<DropReason> {
    match pressure {
        PressurePolicy::LatestOnly => Some(DropReason::LatestOnlyReplace),
        PressurePolicy::DropOldest => Some(DropReason::DropOldest),
        PressurePolicy::Bounded {
            overflow: OverflowPolicy::DropOldest,
            ..
        } => Some(DropReason::DropOldest),
        _ => None,
    }
}
