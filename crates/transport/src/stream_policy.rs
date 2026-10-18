use alloc::boxed::Box;
use alloc::collections::VecDeque;
use core::time::Duration;

use daedalus_core::platform::Instant;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CorrelationId, TypeKeyError};

/// Queue pressure policy used by sources, edges, and host outputs.
///
/// These policies are non-blocking. A producer either enqueues, replaces, drops,
/// or receives [`FeedOutcome::Backpressured`] when the selected policy cannot
/// accept more payloads immediately.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PressurePolicy {
    BufferAll,
    DropNewest,
    DropOldest,
    LatestOnly,
    Bounded {
        capacity: usize,
        overflow: OverflowPolicy,
    },
    /// Single-slot coalescing; see [`CoalesceStrategy`] for how `window` applies.
    Coalesce {
        window: Duration,
        strategy: CoalesceStrategy,
    },
    ErrorOnFull,
}

impl Default for PressurePolicy {
    fn default() -> Self {
        Self::Bounded {
            capacity: 1,
            overflow: OverflowPolicy::DropOldest,
        }
    }
}

/// Overflow behavior for bounded pressure policies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    DropIncoming,
    DropOldest,
    Backpressure,
    Error,
}

/// Coalescing behavior for high-rate input streams.
///
/// `Coalesce` keeps a single slot: the value a consumer would see next. `window` bounds how long
/// the value that opened a coalescing group keeps the slot:
///
/// - `KeepNewest`: every arrival replaces the held value (the outcome is `Replaced`), so the
///   newest value always wins and `window` has no effect.
/// - `KeepOldest`: the value that opened the group is kept. Arrivals within `window` of when it
///   was stored are rejected with [`DropReason::DropNewest`]. Once `window` has elapsed, the next
///   arrival replaces the held value and opens a new group. A zero window replaces on every
///   arrival.
///
/// Windows are read from [`Instant::now`]. Without an OS clock (`no_std` without an injected
/// clock) they never elapse, so `KeepOldest` keeps its first value until it is popped.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoalesceStrategy {
    KeepNewest,
    KeepOldest,
}

/// Freshness policy used to decide whether queued payloads are still worth executing.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessPolicy {
    #[default]
    PreserveAll,
    LatestBySequence,
    LatestByTimestamp,
    MaxAge(Duration),
    MaxLag {
        frames: u64,
    },
}

/// Reason a feed/drop decision was made.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropReason {
    Backpressure,
    DropNewest,
    DropOldest,
    LatestOnlyReplace,
    MaxAge,
    MaxLag,
    Closed,
    ErrorOnFull,
}

/// Result of feeding a payload into a continuous graph input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedOutcome {
    Accepted {
        correlation_id: CorrelationId,
    },
    /// The payload was queued and `dropped` previously queued payloads were evicted. `old` is
    /// the newest evicted payload; `dropped` counts every evicted payload, including `old`.
    Replaced {
        old: CorrelationId,
        new: CorrelationId,
        dropped: usize,
    },
    Dropped {
        correlation_id: CorrelationId,
        reason: DropReason,
    },
    Backpressured,
    Closed,
    /// Refused before queueing: the payload's Rust type does not match the type registered for
    /// its key, or a typed push found no key for its Rust type.
    Rejected(Box<TypeKeyError>),
}

impl FeedOutcome {
    /// Translate a [`PolicyQueue::push`] result for an incoming payload with id `incoming`.
    /// `id_of` extracts the correlation id of an evicted item.
    pub fn from_push<T>(
        outcome: PushOutcome<T>,
        incoming: CorrelationId,
        id_of: impl FnOnce(&T) -> CorrelationId,
    ) -> Self {
        match outcome {
            PushOutcome::Accepted => Self::Accepted {
                correlation_id: incoming,
            },
            PushOutcome::Replaced { old, dropped } => Self::Replaced {
                old: id_of(&old),
                new: incoming,
                dropped,
            },
            PushOutcome::Rejected(_, DropReason::Backpressure) => Self::Backpressured,
            PushOutcome::Rejected(_, reason) => Self::Dropped {
                correlation_id: incoming,
                reason,
            },
        }
    }
}

impl PressurePolicy {
    /// Capacity of a `Bounded` policy (at least one).
    pub fn bounded_capacity(&self) -> Option<usize> {
        match self {
            Self::Bounded { capacity, .. } => Some((*capacity).max(1)),
            _ => None,
        }
    }

    /// Whether the policy keeps at most one item by replacing the queued one: `LatestOnly`,
    /// `DropOldest`, `Coalesce`, and `Bounded { capacity: 1, overflow: DropOldest }`.
    pub fn is_single_slot(&self) -> bool {
        match self {
            Self::LatestOnly | Self::DropOldest | Self::Coalesce { .. } => true,
            Self::Bounded { capacity, overflow } => {
                *capacity <= 1 && matches!(overflow, OverflowPolicy::DropOldest)
            }
            Self::BufferAll | Self::DropNewest | Self::ErrorOnFull => false,
        }
    }
}

/// Result of [`PolicyQueue::push`].
#[derive(Debug, PartialEq, Eq)]
pub enum PushOutcome<T> {
    /// The item was queued without displacing anything.
    Accepted,
    /// The item was queued and previously queued items were evicted. `old` is the newest evicted
    /// item; `dropped` counts every evicted item (at least one), including `old`. The others are
    /// discarded without being returned, so this count is their only report.
    Replaced { old: T, dropped: usize },
    /// The item was not queued. `DropReason::Backpressure` means the caller may retry later.
    Rejected(T, DropReason),
}

impl<T> PushOutcome<T> {
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }

    /// Items this push removed from the queue or refused, counting every dropped item.
    pub fn dropped(&self) -> usize {
        match self {
            Self::Accepted => 0,
            Self::Replaced { dropped, .. } => *dropped,
            Self::Rejected(..) => 1,
        }
    }
}

fn replaced<T>(old: Option<T>, dropped: usize) -> PushOutcome<T> {
    match old {
        Some(old) => PushOutcome::Replaced { old, dropped },
        None => PushOutcome::Accepted,
    }
}

/// Non-blocking queue that applies a [`PressurePolicy`] on every push.
///
/// Single-slot policies (see [`PressurePolicy::is_single_slot`]) store at most one item in place;
/// every other policy uses a FIFO deque, preallocated to the bounded capacity. The storage adapts
/// when the policy changes; a FIFO holding more than one item stays a FIFO until the policy has
/// trimmed it, so switching never loses queued items.
#[derive(Debug)]
pub struct PolicyQueue<T> {
    storage: Storage<T>,
    capacity: Option<usize>,
    /// When the value in the `Coalesce` slot opened its `KeepOldest` window.
    opened: Option<Instant>,
}

#[derive(Debug)]
enum Storage<T> {
    Slot(Option<T>),
    Fifo(VecDeque<T>),
}

impl<T> Default for PolicyQueue<T> {
    fn default() -> Self {
        Self {
            storage: Storage::Slot(None),
            capacity: None,
            opened: None,
        }
    }
}

impl<T> PolicyQueue<T> {
    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::Slot(slot) => usize::from(slot.is_some()),
            Storage::Fifo(queue) => queue.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.storage {
            Storage::Slot(slot) => slot.is_none(),
            Storage::Fifo(queue) => queue.is_empty(),
        }
    }

    /// Bounded capacity of the policy last applied, if any.
    pub fn capacity(&self) -> Option<usize> {
        self.capacity
    }

    /// Whether a bounded policy's capacity is reached.
    pub fn is_full(&self) -> bool {
        self.capacity.is_some_and(|capacity| self.len() >= capacity)
    }

    pub fn pop_front(&mut self) -> Option<T> {
        match &mut self.storage {
            Storage::Slot(slot) => slot.take(),
            Storage::Fifo(queue) => queue.pop_front(),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.storage {
            Storage::Slot(slot) => *slot = None,
            Storage::Fifo(queue) => queue.clear(),
        }
    }

    /// Queued items, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        let (slot, fifo) = match &self.storage {
            Storage::Slot(slot) => (slot.as_ref(), None),
            Storage::Fifo(queue) => (None, Some(queue.iter())),
        };
        slot.into_iter().chain(fifo.into_iter().flatten())
    }

    /// Move every queued item, oldest first, into `sink`.
    pub fn drain_into(&mut self, mut sink: impl FnMut(T)) {
        match &mut self.storage {
            Storage::Slot(slot) => {
                if let Some(item) = slot.take() {
                    sink(item);
                }
            }
            Storage::Fifo(queue) => queue.drain(..).for_each(sink),
        }
    }

    /// Adapt storage and capacity to `pressure` without queueing anything.
    #[inline]
    pub fn set_policy(&mut self, pressure: &PressurePolicy) {
        let single = match pressure {
            PressurePolicy::Bounded { capacity, overflow } => {
                let capacity = (*capacity).max(1);
                self.capacity = Some(capacity);
                capacity == 1 && matches!(overflow, OverflowPolicy::DropOldest)
            }
            _ => {
                self.capacity = None;
                pressure.is_single_slot()
            }
        };
        match &mut self.storage {
            Storage::Fifo(queue) if single && queue.len() <= 1 => {
                self.storage = Storage::Slot(queue.pop_front());
            }
            Storage::Slot(slot) if !single => {
                let mut queue = VecDeque::with_capacity(self.capacity.unwrap_or(1));
                queue.extend(slot.take());
                self.storage = Storage::Fifo(queue);
            }
            _ => {}
        }
    }

    /// Apply `pressure` to an incoming item.
    #[inline]
    pub fn push(&mut self, pressure: &PressurePolicy, item: T) -> PushOutcome<T> {
        self.set_policy(pressure);
        if let PressurePolicy::Coalesce { window, strategy } = pressure {
            return self.push_coalesce(*window, strategy, item);
        }
        let queue = match &mut self.storage {
            Storage::Slot(slot) => return replaced(slot.replace(item), 1),
            Storage::Fifo(queue) => queue,
        };
        let full = self
            .capacity
            .is_some_and(|capacity| queue.len() >= capacity);
        match pressure {
            PressurePolicy::LatestOnly => {
                let dropped = queue.len();
                let old = queue.pop_back();
                queue.clear();
                queue.push_back(item);
                replaced(old, dropped)
            }
            PressurePolicy::DropNewest if !queue.is_empty() => {
                PushOutcome::Rejected(item, DropReason::DropNewest)
            }
            PressurePolicy::ErrorOnFull if !queue.is_empty() => {
                PushOutcome::Rejected(item, DropReason::ErrorOnFull)
            }
            PressurePolicy::Bounded { overflow, .. } if full => match overflow {
                OverflowPolicy::DropOldest => evict_oldest(queue, item),
                OverflowPolicy::DropIncoming => PushOutcome::Rejected(item, DropReason::DropNewest),
                OverflowPolicy::Backpressure => {
                    PushOutcome::Rejected(item, DropReason::Backpressure)
                }
                OverflowPolicy::Error => PushOutcome::Rejected(item, DropReason::ErrorOnFull),
            },
            PressurePolicy::DropOldest => evict_oldest(queue, item),
            _ => {
                queue.push_back(item);
                PushOutcome::Accepted
            }
        }
    }

    /// `Coalesce` push; the window rules are on [`CoalesceStrategy`]. A FIFO backlog left by a
    /// buffering policy is replaced wholesale, as `LatestOnly` does.
    fn push_coalesce(
        &mut self,
        window: Duration,
        strategy: &CoalesceStrategy,
        item: T,
    ) -> PushOutcome<T> {
        let keep_oldest = matches!(strategy, CoalesceStrategy::KeepOldest);
        match &mut self.storage {
            Storage::Fifo(queue) => {
                let dropped = queue.len();
                let old = queue.pop_back();
                queue.clear();
                queue.push_back(item);
                if keep_oldest {
                    self.opened = Some(Instant::now());
                }
                replaced(old, dropped)
            }
            Storage::Slot(slot) => {
                if slot.is_none() {
                    if keep_oldest {
                        self.opened = Some(Instant::now());
                    }
                    *slot = Some(item);
                    return PushOutcome::Accepted;
                }
                if keep_oldest {
                    let now = Instant::now();
                    let opened = *self.opened.get_or_insert(now);
                    if now - opened < window {
                        return PushOutcome::Rejected(item, DropReason::DropNewest);
                    }
                    self.opened = Some(now);
                }
                replaced(slot.replace(item), 1)
            }
        }
    }
}

fn evict_oldest<T>(queue: &mut VecDeque<T>, item: T) -> PushOutcome<T> {
    let old = queue.pop_front();
    queue.push_back(item);
    replaced(old, 1)
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum PolicyValidationError {
    #[error("PreserveAll requires bounded capacity or buffer-all pressure")]
    UnboundedPreserveAll,
}

pub fn validate_stream_policy(
    pressure: &PressurePolicy,
    freshness: &FreshnessPolicy,
) -> Result<(), PolicyValidationError> {
    if matches!(freshness, FreshnessPolicy::PreserveAll)
        && !matches!(
            pressure,
            PressurePolicy::Bounded { .. } | PressurePolicy::BufferAll
        )
    {
        return Err(PolicyValidationError::UnboundedPreserveAll);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounded(capacity: usize, overflow: OverflowPolicy) -> PressurePolicy {
        PressurePolicy::Bounded { capacity, overflow }
    }

    fn items(queue: &PolicyQueue<u64>) -> Vec<u64> {
        queue.iter().copied().collect()
    }

    #[test]
    fn single_slot_replaces_in_place() {
        let mut queue = PolicyQueue::default();
        let policy = PressurePolicy::LatestOnly;
        assert_eq!(queue.push(&policy, 1), PushOutcome::Accepted);
        assert_eq!(
            queue.push(&policy, 2),
            PushOutcome::Replaced { old: 1, dropped: 1 }
        );
        assert!(matches!(queue.storage, Storage::Slot(Some(2))));
        assert_eq!(queue.pop_front(), Some(2));
        assert!(queue.is_empty());
    }

    #[test]
    fn policy_switch_keeps_queued_items() {
        let mut queue = PolicyQueue::default();
        queue.push(&PressurePolicy::BufferAll, 1);
        queue.push(&PressurePolicy::BufferAll, 2);
        assert_eq!(queue.len(), 2);
        // A replace policy trims the FIFO first, then later pushes use the slot.
        queue.push(&PressurePolicy::LatestOnly, 3);
        assert_eq!(queue.len(), 1);
        queue.push(&PressurePolicy::LatestOnly, 4);
        assert!(matches!(queue.storage, Storage::Slot(Some(4))));
        queue.push(&PressurePolicy::BufferAll, 5);
        assert!(matches!(queue.storage, Storage::Fifo(_)));
        let mut drained = Vec::new();
        queue.drain_into(|item| drained.push(item));
        assert_eq!(drained, vec![4, 5]);
    }

    #[test]
    fn bounded_overflow_policies() {
        let mut queue = PolicyQueue::default();
        let drop_oldest = bounded(2, OverflowPolicy::DropOldest);
        queue.push(&drop_oldest, 1);
        queue.push(&drop_oldest, 2);
        assert!(queue.is_full());
        assert_eq!(
            queue.push(&drop_oldest, 3),
            PushOutcome::Replaced { old: 1, dropped: 1 }
        );
        assert_eq!(items(&queue), vec![2, 3]);

        for (overflow, reason) in [
            (OverflowPolicy::DropIncoming, DropReason::DropNewest),
            (OverflowPolicy::Backpressure, DropReason::Backpressure),
            (OverflowPolicy::Error, DropReason::ErrorOnFull),
        ] {
            assert_eq!(
                queue.push(&bounded(2, overflow), 9),
                PushOutcome::Rejected(9, reason)
            );
            assert_eq!(items(&queue), vec![2, 3]);
        }
        queue.clear();
        assert_eq!(queue.capacity(), Some(2));
    }

    #[test]
    fn drop_newest_and_error_on_full_reject_when_occupied() {
        for (policy, reason) in [
            (PressurePolicy::DropNewest, DropReason::DropNewest),
            (PressurePolicy::ErrorOnFull, DropReason::ErrorOnFull),
        ] {
            let mut queue = PolicyQueue::default();
            assert_eq!(queue.push(&policy, 1), PushOutcome::Accepted);
            assert_eq!(queue.push(&policy, 2), PushOutcome::Rejected(2, reason));
            assert_eq!(items(&queue), vec![1]);
        }
    }

    fn coalesce(window: Duration, strategy: CoalesceStrategy) -> PressurePolicy {
        PressurePolicy::Coalesce { window, strategy }
    }

    #[test]
    fn coalesce_keep_newest_replaces_every_arrival() {
        let mut queue = PolicyQueue::default();
        let policy = coalesce(Duration::from_secs(3600), CoalesceStrategy::KeepNewest);
        assert_eq!(queue.push(&policy, 1), PushOutcome::Accepted);
        assert_eq!(
            queue.push(&policy, 2),
            PushOutcome::Replaced { old: 1, dropped: 1 }
        );
        assert_eq!(
            queue.push(&policy, 3),
            PushOutcome::Replaced { old: 2, dropped: 1 }
        );
        assert_eq!(items(&queue), vec![3]);
    }

    #[test]
    fn coalesce_keep_oldest_rejects_arrivals_inside_window() {
        let mut queue = PolicyQueue::default();
        let policy = coalesce(Duration::from_secs(3600), CoalesceStrategy::KeepOldest);
        assert_eq!(queue.push(&policy, 1), PushOutcome::Accepted);
        let outcome = queue.push(&policy, 2);
        assert_eq!(outcome, PushOutcome::Rejected(2, DropReason::DropNewest));
        assert_eq!(outcome.dropped(), 1);
        assert_eq!(items(&queue), vec![1]);
    }

    #[test]
    fn coalesce_keep_oldest_zero_window_replaces_after_window() {
        let mut queue = PolicyQueue::default();
        let policy = coalesce(Duration::ZERO, CoalesceStrategy::KeepOldest);
        assert_eq!(queue.push(&policy, 1), PushOutcome::Accepted);
        assert_eq!(
            queue.push(&policy, 2),
            PushOutcome::Replaced { old: 1, dropped: 1 }
        );
        assert_eq!(items(&queue), vec![2]);
    }

    #[test]
    fn coalesce_clears_fifo_backlog_and_counts_every_dropped_item() {
        let mut queue = PolicyQueue::default();
        for value in 1..=3 {
            queue.push(&PressurePolicy::BufferAll, value);
        }
        let policy = coalesce(Duration::from_secs(3600), CoalesceStrategy::KeepOldest);
        assert_eq!(
            queue.push(&policy, 4),
            PushOutcome::Replaced { old: 3, dropped: 3 }
        );
        assert_eq!(items(&queue), vec![4]);
        // The window opened by the backlog's replacement still coalesces.
        assert_eq!(
            queue.push(&policy, 5),
            PushOutcome::Rejected(5, DropReason::DropNewest)
        );
        assert_eq!(items(&queue), vec![4]);
    }

    #[test]
    fn latest_only_backlog_reports_every_dropped_item() {
        let mut queue = PolicyQueue::default();
        for value in 1..=3 {
            queue.push(&PressurePolicy::BufferAll, value);
        }
        let outcome = queue.push(&PressurePolicy::LatestOnly, 4);
        assert_eq!(outcome, PushOutcome::Replaced { old: 3, dropped: 3 });
        assert_eq!(outcome.dropped(), 3);
        assert_eq!(items(&queue), vec![4]);
    }

    #[test]
    fn feed_outcome_from_push() {
        let id = |value: &u64| *value;
        assert_eq!(
            FeedOutcome::from_push(PushOutcome::Replaced { old: 7, dropped: 2 }, 8, id),
            FeedOutcome::Replaced {
                old: 7,
                new: 8,
                dropped: 2,
            }
        );
        assert_eq!(
            FeedOutcome::from_push(PushOutcome::Rejected(8, DropReason::Backpressure), 8, id),
            FeedOutcome::Backpressured
        );
    }
}
