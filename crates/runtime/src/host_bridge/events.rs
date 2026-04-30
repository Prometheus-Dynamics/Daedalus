use std::time::Instant;

use daedalus_transport::{DropReason, FeedOutcome, OverflowPolicy, Payload, PressurePolicy};

use super::{HostBridgeBuffers, HostBridgeDropStats, HostBridgeEvent, HostBridgeEventKind};

pub(super) fn record_host_event(
    guard: &mut HostBridgeBuffers,
    alias: &str,
    port: &str,
    payload: &Payload,
    kind: HostBridgeEventKind,
    outcome: Option<FeedOutcome>,
    reason: Option<DropReason>,
) {
    let target = "daedalus_runtime::host_bridge";
    if matches!(
        kind,
        HostBridgeEventKind::SourceDrop
            | HostBridgeEventKind::OutputDrop
            | HostBridgeEventKind::SourceReplace
    ) || reason.is_some()
    {
        tracing::warn!(
            target,
            alias,
            port,
            kind = ?kind,
            reason = ?reason,
            outcome = ?outcome,
            payload_type = %payload.type_key(),
            correlation_id = payload.correlation_id(),
            "host bridge payload pressure event",
        );
    } else {
        tracing::trace!(
            target,
            alias,
            port,
            kind = ?kind,
            outcome = ?outcome,
            payload_type = %payload.type_key(),
            correlation_id = payload.correlation_id(),
            "host bridge payload event",
        );
    }

    if !guard.events_enabled {
        return;
    }
    let Some(limit) = guard.event_limit else {
        guard.events.push_back(HostBridgeEvent {
            at: Instant::now(),
            alias: alias.to_string(),
            port: port.to_string(),
            correlation_id: payload.correlation_id(),
            kind,
            type_key: payload.type_key().clone(),
            outcome,
            reason,
        });
        return;
    };
    if limit == 0 {
        guard.events.clear();
        return;
    }
    while guard.events.len() >= limit {
        guard.events.pop_front();
    }
    guard.events.push_back(HostBridgeEvent {
        at: Instant::now(),
        alias: alias.to_string(),
        port: port.to_string(),
        correlation_id: payload.correlation_id(),
        kind,
        type_key: payload.type_key().clone(),
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

pub(super) fn trim_host_events(guard: &mut HostBridgeBuffers) {
    if let Some(limit) = guard.event_limit {
        while guard.events.len() > limit {
            guard.events.pop_front();
        }
    }
}
