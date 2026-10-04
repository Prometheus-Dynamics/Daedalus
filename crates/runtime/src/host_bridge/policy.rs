use daedalus_core::platform::Clock;
use daedalus_transport::{DropReason, FreshnessPolicy, Payload};

use super::ports::FreshnessMarks;

pub(super) fn freshness_drop_reason(
    marks: &mut FreshnessMarks,
    payload: &Payload,
    freshness: &FreshnessPolicy,
    clock: &Clock,
) -> Option<DropReason> {
    match freshness {
        FreshnessPolicy::PreserveAll => None,
        FreshnessPolicy::MaxAge(max_age) => {
            (payload.lineage().age(clock) > *max_age).then_some(DropReason::MaxAge)
        }
        FreshnessPolicy::LatestBySequence => {
            let sequence = payload.lineage().sequence?;
            let latest = marks.latest_sequence.get_or_insert(0);
            if sequence < *latest {
                Some(DropReason::MaxLag)
            } else {
                *latest = sequence;
                None
            }
        }
        FreshnessPolicy::LatestByTimestamp => {
            let timestamp = payload.lineage().source_timestamp?;
            let latest = marks.latest_timestamp.get_or_insert(0);
            if timestamp < *latest {
                Some(DropReason::MaxAge)
            } else {
                *latest = timestamp;
                None
            }
        }
        FreshnessPolicy::MaxLag { frames } => {
            let sequence = payload.lineage().sequence?;
            let latest = marks.latest_sequence.get_or_insert(sequence);
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
