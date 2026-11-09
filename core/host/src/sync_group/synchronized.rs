//! Values grouped by stamp into whole ticks.

#![cfg(feature = "std")]

use std::collections::VecDeque;
use std::vec::Vec;

use super::{Group, GroupSink, InputSet, SyncStats};

/// What to do with a group that an input didn't finish in time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PartialPolicy {
    /// Discard what arrived. No tick sees it.
    #[default]
    Drop,
    /// Deliver what arrived. The missing inputs get nothing.
    TickPartial,
    /// Deliver what arrived, and each missing input's last delivered value again.
    HoldLast,
}

/// How values are grouped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncConfig {
    /// The largest stamp spread within one group.
    pub window: u64,
    /// How long an incomplete group waits, from the arrival of its first value.
    pub timeout_ns: u64,
    pub partial: PartialPolicy,
    /// Values buffered per input. Allocated once. A full buffer resolves the oldest group.
    pub max_buffered: usize,
}

/// What became of a pushed value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Push {
    /// It waits for its group.
    Buffered,
    /// At least one group resolved because of it.
    Resolved,
    /// Its group was already resolved, so it was dropped.
    Late,
    UnknownInput,
}

struct Entry<V> {
    stamp: u64,
    arrived: u64,
    value: V,
}

struct Input<V> {
    offset: i64,
    /// Sorted by stamp, and never grown past `max_buffered`.
    ring: VecDeque<Entry<V>>,
    last: Option<V>,
}

/// The oldest group: who is in it, its first stamp and first arrival.
struct Pending {
    inputs: InputSet,
    stamp: u64,
    arrived: u64,
    /// Every missing input already has a later value, so it can't complete.
    decided: bool,
}

/// Groups values by stamp. Grouping anchors on the oldest buffered value: every input whose oldest
/// value lies within the window of it contributes that value.
pub struct Synchronized<V> {
    inputs: Vec<Input<V>>,
    config: SyncConfig,
    /// Stamps at or before this belong to resolved groups.
    watermark: Option<u64>,
    deadline: Option<u64>,
    stats: SyncStats,
}

impl<V: Clone> Synchronized<V> {
    pub fn new(count: usize, config: SyncConfig) -> Synchronized<V> {
        let config = SyncConfig {
            max_buffered: config.max_buffered.max(1),
            ..config
        };
        let inputs = (0..count.min(super::MAX_INPUTS))
            .map(|_| Input {
                offset: 0,
                ring: VecDeque::with_capacity(config.max_buffered),
                last: None,
            })
            .collect();
        Synchronized {
            inputs,
            config,
            watermark: None,
            deadline: None,
            stats: SyncStats::default(),
        }
    }

    /// Adds `offset` to every later stamp of `input`, for a clock that runs ahead or behind.
    pub fn set_offset(&mut self, input: usize, offset: i64) {
        if let Some(input) = self.inputs.get_mut(input) {
            input.offset = offset;
        }
    }

    pub fn stats(&self) -> SyncStats {
        self.stats
    }

    pub fn buffered(&self, input: usize) -> usize {
        self.inputs.get(input).map_or(0, |input| input.ring.len())
    }

    /// When an incomplete group times out, if one is waiting.
    pub fn next_deadline(&self) -> Option<u64> {
        self.deadline
    }

    /// Gives up every buffered value and each input's last value.
    pub fn clear(&mut self) {
        for input in &mut self.inputs {
            input.ring.clear();
            input.last = None;
        }
        self.deadline = None;
    }

    fn is_late(&self, stamp: u64) -> bool {
        self.watermark.is_some_and(|mark| stamp <= mark)
    }

    /// Buffers a value with its stamp, and resolves whatever that makes possible.
    pub fn push(
        &mut self,
        input: usize,
        value: V,
        stamp: u64,
        now: u64,
        sink: &mut impl GroupSink<V>,
    ) -> Push {
        let Some(at) = self.inputs.get(input) else {
            return Push::UnknownInput;
        };
        let stamp = stamp.saturating_add_signed(at.offset);
        if self.is_late(stamp) {
            self.stats.late_values += 1;
            return Push::Late;
        }
        let mut resolved = false;
        while self.inputs[input].ring.len() == self.config.max_buffered {
            let Some(pending) = self.pending() else { break };
            self.stats.overflows += 1;
            self.resolve(pending, sink);
            resolved = true;
        }
        // Making room can resolve the window an out-of-order value belongs to.
        if self.is_late(stamp) {
            self.stats.late_values += 1;
            return Push::Late;
        }
        self.stats.values += 1;
        let ring = &mut self.inputs[input].ring;
        let at = ring.partition_point(|entry| entry.stamp <= stamp);
        ring.insert(
            at,
            Entry {
                stamp,
                arrived: now,
                value,
            },
        );
        resolved |= self.settle(now, sink);
        if resolved {
            Push::Resolved
        } else {
            Push::Buffered
        }
    }

    /// Resolves a group whose timeout has come. Returns true if anything resolved.
    pub fn expire(&mut self, now: u64, sink: &mut impl GroupSink<V>) -> bool {
        if self.deadline.is_none_or(|deadline| deadline > now) {
            return false;
        }
        self.settle(now, sink)
    }

    fn pending(&self) -> Option<Pending> {
        let stamp = self
            .inputs
            .iter()
            .filter_map(|input| input.ring.front())
            .map(|entry| entry.stamp)
            .min()?;
        let mut inputs = InputSet::EMPTY;
        let mut arrived = u64::MAX;
        let mut decided = true;
        for (index, input) in self.inputs.iter().enumerate() {
            match input.ring.front() {
                Some(entry) if entry.stamp - stamp <= self.config.window => {
                    inputs = inputs.with(index);
                    arrived = arrived.min(entry.arrived);
                }
                Some(_) => {}
                None => decided = false,
            }
        }
        Some(Pending {
            inputs,
            stamp,
            arrived,
            decided,
        })
    }

    /// Resolves every group that is complete or decided, and notes when the next one times out.
    fn settle(&mut self, now: u64, sink: &mut impl GroupSink<V>) -> bool {
        let mut resolved = false;
        self.deadline = None;
        while let Some(pending) = self.pending() {
            let complete = pending.inputs.len() == self.inputs.len();
            let deadline = pending.arrived.saturating_add(self.config.timeout_ns);
            if !complete && !pending.decided {
                if deadline > now {
                    self.deadline = Some(deadline);
                    break;
                }
                self.stats.timeouts += 1;
            }
            self.resolve(pending, sink);
            resolved = true;
        }
        resolved
    }

    /// Delivers or drops the oldest group and moves the watermark past it.
    fn resolve(&mut self, pending: Pending, sink: &mut impl GroupSink<V>) {
        let complete = pending.inputs.len() == self.inputs.len();
        let watermark = pending.stamp.saturating_add(self.config.window);
        self.watermark = Some(self.watermark.map_or(watermark, |mark| mark.max(watermark)));
        if !complete && self.config.partial == PartialPolicy::Drop {
            for index in pending.inputs.iter() {
                self.inputs[index].ring.pop_front();
            }
            self.stats.dropped_groups += 1;
            self.stats.dropped_values += pending.inputs.len() as u64;
        } else {
            self.commit(&pending, complete, sink);
        }
        let late = watermark.max(self.watermark.unwrap_or(0));
        for input in &mut self.inputs {
            let stale = input.ring.partition_point(|entry| entry.stamp <= late);
            if stale > 0 {
                input.ring.drain(..stale);
                self.stats.dropped_values += stale as u64;
            }
        }
    }

    /// Hands the group over whole, replacing what the previous group left unread, so a slow graph
    /// always ticks on the latest group and queues don't grow.
    fn commit(&mut self, pending: &Pending, complete: bool, sink: &mut impl GroupSink<V>) {
        let hold_last = self.config.partial == PartialPolicy::HoldLast;
        let mut group = Group {
            inputs: pending.inputs,
            stamp: pending.stamp,
            complete,
            ..Group::default()
        };
        let mut superseded = false;
        for (index, input) in self.inputs.iter_mut().enumerate() {
            let value = if pending.inputs.contains(index) {
                let Some(entry) = input.ring.pop_front() else {
                    continue;
                };
                group.skew = group.skew.max(entry.stamp - pending.stamp);
                Some(entry.value)
            } else if hold_last && input.last.is_some() {
                group.reused = group.reused.with(index);
                input.last.clone()
            } else {
                None
            };
            superseded |= sink.clear(index);
            let Some(value) = value else { continue };
            if hold_last {
                input.last = Some(value.clone());
            }
            sink.deliver(index, value);
        }
        if complete {
            self.stats.complete_groups += 1;
        } else {
            self.stats.partial_groups += 1;
        }
        self.stats.superseded_groups += u64::from(superseded);
        self.stats.max_skew = self.stats.max_skew.max(group.skew);
        self.stats.last_group = Some(group);
    }
}
