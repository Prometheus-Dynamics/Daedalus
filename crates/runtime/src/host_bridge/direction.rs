//! Which side of the host bridge a payload is queued on, and the counters and event kinds each
//! side updates.

use daedalus_transport::{DropReason, FeedOutcome};

use super::events::record_drop_reason;
use super::{HostBridgeEventKind, HostBridgeStats};

/// Which side of the bridge a payload is queued on; selects stats counters and event kinds.
#[derive(Clone, Copy)]
pub(super) enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    pub(super) fn enqueue_kind(self, outcome: &FeedOutcome) -> HostBridgeEventKind {
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

    pub(super) fn drop_kind(self) -> HostBridgeEventKind {
        match self {
            Direction::Inbound => HostBridgeEventKind::SourceDrop,
            Direction::Outbound => HostBridgeEventKind::OutputDrop,
        }
    }

    pub(super) fn count_drop(self, stats: &mut HostBridgeStats, reason: Option<DropReason>) {
        let (dropped, reasons) = match self {
            Direction::Inbound => (&mut stats.inbound_dropped, &mut stats.inbound_drop_reasons),
            Direction::Outbound => (
                &mut stats.outbound_dropped,
                &mut stats.outbound_drop_reasons,
            ),
        };
        *dropped = dropped.saturating_add(1);
        if let Some(reason) = reason {
            record_drop_reason(reasons, reason, 1);
        }
    }

    /// Count one accepted push that evicted `dropped` queued payloads: each eviction is a
    /// replacement, and each counts toward the drop reason when one applies.
    pub(super) fn count_replaced(
        self,
        stats: &mut HostBridgeStats,
        dropped: u64,
        reason: Option<DropReason>,
    ) {
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
        *replaced = replaced.saturating_add(dropped);
        if let Some(reason) = reason {
            record_drop_reason(reasons, reason, dropped);
        }
    }

    pub(super) fn count_accepted(self, stats: &mut HostBridgeStats) {
        if let Direction::Inbound = self {
            stats.inbound_accepted = stats.inbound_accepted.saturating_add(1);
        }
    }
}
