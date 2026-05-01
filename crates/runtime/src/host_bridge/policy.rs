use std::collections::VecDeque;

use daedalus_transport::{
    DropReason, FeedOutcome, FreshnessPolicy, OverflowPolicy, Payload, PressurePolicy,
};

use crate::handles::PortId;

use super::{HostBridgeBuffers, HostBridgePayload};

pub(super) fn apply_host_pressure(
    policy: &PressurePolicy,
    queue: &mut VecDeque<HostBridgePayload>,
    payload: HostBridgePayload,
) -> FeedOutcome {
    let incoming_id = payload.payload.correlation_id();
    match policy {
        PressurePolicy::LatestOnly => {
            let old = queue.pop_back().map(|old| old.payload.correlation_id());
            queue.clear();
            queue.push_back(payload);
            if let Some(old) = old {
                FeedOutcome::Replaced {
                    old,
                    new: incoming_id,
                }
            } else {
                FeedOutcome::Accepted {
                    correlation_id: incoming_id,
                }
            }
        }
        PressurePolicy::DropNewest => {
            if queue.is_empty() {
                queue.push_back(payload);
                FeedOutcome::Accepted {
                    correlation_id: incoming_id,
                }
            } else {
                FeedOutcome::Dropped {
                    correlation_id: incoming_id,
                    reason: DropReason::DropNewest,
                }
            }
        }
        PressurePolicy::DropOldest => {
            let old = queue.pop_front().map(|old| old.payload.correlation_id());
            queue.push_back(payload);
            if let Some(old) = old {
                FeedOutcome::Replaced {
                    old,
                    new: incoming_id,
                }
            } else {
                FeedOutcome::Accepted {
                    correlation_id: incoming_id,
                }
            }
        }
        PressurePolicy::Bounded { capacity, overflow } => {
            let capacity = (*capacity).max(1);
            if queue.len() < capacity {
                queue.push_back(payload);
                return FeedOutcome::Accepted {
                    correlation_id: incoming_id,
                };
            }
            match overflow {
                OverflowPolicy::DropIncoming => FeedOutcome::Dropped {
                    correlation_id: incoming_id,
                    reason: DropReason::DropNewest,
                },
                OverflowPolicy::DropOldest => {
                    let old = queue.pop_front().map(|old| old.payload.correlation_id());
                    queue.push_back(payload);
                    if let Some(old) = old {
                        FeedOutcome::Replaced {
                            old,
                            new: incoming_id,
                        }
                    } else {
                        FeedOutcome::Accepted {
                            correlation_id: incoming_id,
                        }
                    }
                }
                OverflowPolicy::Backpressure => FeedOutcome::Backpressured,
                OverflowPolicy::Error => FeedOutcome::Dropped {
                    correlation_id: incoming_id,
                    reason: DropReason::ErrorOnFull,
                },
            }
        }
        PressurePolicy::ErrorOnFull => {
            if queue.is_empty() {
                queue.push_back(payload);
                FeedOutcome::Accepted {
                    correlation_id: incoming_id,
                }
            } else {
                FeedOutcome::Dropped {
                    correlation_id: incoming_id,
                    reason: DropReason::ErrorOnFull,
                }
            }
        }
        PressurePolicy::BufferAll => {
            queue.push_back(payload);
            FeedOutcome::Accepted {
                correlation_id: incoming_id,
            }
        }
        PressurePolicy::Coalesce { .. } => {
            let old = queue.pop_back().map(|old| old.payload.correlation_id());
            queue.push_back(payload);
            if let Some(old) = old {
                FeedOutcome::Replaced {
                    old,
                    new: incoming_id,
                }
            } else {
                FeedOutcome::Accepted {
                    correlation_id: incoming_id,
                }
            }
        }
    }
}

pub(super) fn freshness_drop_reason(
    guard: &mut HostBridgeBuffers,
    port: &str,
    payload: &Payload,
    freshness: &FreshnessPolicy,
) -> Option<DropReason> {
    match freshness {
        FreshnessPolicy::PreserveAll => None,
        FreshnessPolicy::MaxAge(max_age) => {
            (payload.lineage().created_at.elapsed() > *max_age).then_some(DropReason::MaxAge)
        }
        FreshnessPolicy::LatestBySequence => {
            let sequence = payload.lineage().sequence?;
            let latest = guard.latest_sequence.entry(PortId::from(port)).or_insert(0);
            if sequence < *latest {
                Some(DropReason::MaxLag)
            } else {
                *latest = sequence;
                None
            }
        }
        FreshnessPolicy::LatestByTimestamp => {
            let timestamp = payload.lineage().source_timestamp?;
            let latest = guard
                .latest_timestamp
                .entry(PortId::from(port))
                .or_insert(0);
            if timestamp < *latest {
                Some(DropReason::MaxAge)
            } else {
                *latest = timestamp;
                None
            }
        }
        FreshnessPolicy::MaxLag { frames } => {
            let sequence = payload.lineage().sequence?;
            let latest = guard
                .latest_sequence
                .entry(PortId::from(port))
                .or_insert(sequence);
            if sequence > *latest {
                *latest = sequence;
                return None;
            }
            latest
                .saturating_sub(sequence)
                .gt(frames)
                .then_some(DropReason::MaxLag)
        }
    }
}
