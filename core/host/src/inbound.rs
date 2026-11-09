//! Per-port policies on the host's inputs: the same pressure and freshness rules as a graph edge.
//! A host sets them when it builds the graph and may change them between ticks. Sizing happens
//! once, when the first policy is set, so feeds never allocate.

use daedalus_value::{Decision, DropReason, Freshness, Pressure, Value};

use crate::io::{Host, HostError};
use crate::label::Label;

/// A port's rules.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PortPolicy {
    pub pressure: Pressure,
    pub freshness: Freshness,
}

/// How many values a port gave up, by reason.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Drops {
    pub drop_newest: u64,
    pub drop_oldest: u64,
    pub latest_only: u64,
    pub coalesced: u64,
    pub max_age: u64,
    pub max_lag: u64,
    pub out_of_order: u64,
}

impl Drops {
    pub(crate) fn count(&mut self, reason: DropReason) {
        match reason {
            DropReason::DropNewest => self.drop_newest += 1,
            DropReason::DropOldest => self.drop_oldest += 1,
            DropReason::LatestOnlyReplace => self.latest_only += 1,
            DropReason::Coalesced => self.coalesced += 1,
            DropReason::MaxAge => self.max_age += 1,
            DropReason::MaxLag => self.max_lag += 1,
            DropReason::OutOfOrder => self.out_of_order += 1,
        }
    }

    /// Every drop, whatever the reason.
    pub fn total(&self) -> u64 {
        self.drop_newest
            + self.drop_oldest
            + self.latest_only
            + self.coalesced
            + self.max_age
            + self.max_lag
            + self.out_of_order
    }
}

/// One port's state.
#[derive(Clone, Debug, Default)]
pub(crate) struct Inbound {
    pub(crate) policy: PortPolicy,
    pub(crate) drops: Drops,
    /// When each queued value arrived, as a ring: `pushed % len` is the next spot.
    pub(crate) arrivals: std::vec::Vec<u64>,
    pub(crate) pushed: u64,
    pub(crate) newest_ns: u64,
    pub(crate) newest_mark: Option<u64>,
    pub(crate) opened_ns: u64,
}

/// What a policy said about an arriving value.
pub(crate) enum Admit {
    /// Store it. The reason is why an older value gave way, if one did.
    Store(Option<DropReason>),
    Discard(DropReason),
}

impl Host<'_> {
    /// Sets the pressure and freshness rules of the input port `name`. Allowed between ticks.
    pub fn set_port_policy(&mut self, name: &str, policy: PortPolicy) -> Result<(), HostError> {
        self.input(name)?;
        let count = self.ports.len();
        if self.inbound.is_empty() {
            self.inbound.resize(count, Inbound::default());
        }
        let slot_cap = self.queue_cap(name)?;
        let index = self.ports.index_of(name).unwrap_or(0);
        let entry = &mut self.inbound[index];
        entry.policy = policy;
        entry.arrivals.resize(slot_cap as usize, 0);
        Ok(())
    }

    /// The policy of the port `name`.
    pub fn port_policy(&self, name: &str) -> Result<PortPolicy, HostError> {
        self.input(name)?;
        let index = self.ports.index_of(name).unwrap_or(0);
        Ok(self
            .inbound
            .get(index)
            .map(|entry| entry.policy)
            .unwrap_or_default())
    }

    /// How many values the port `name` has given up.
    pub fn port_drops(&self, name: &str) -> Result<Drops, HostError> {
        self.input(name)?;
        let index = self.ports.index_of(name).unwrap_or(0);
        Ok(self
            .inbound
            .get(index)
            .map(|entry| entry.drops)
            .unwrap_or_default())
    }

    /// Stores `value` in `name` with a sequence number or timestamp, for the latest-by freshness
    /// rules. A mark that doesn't go up is dropped.
    pub fn feed_marked(
        &mut self,
        name: &str,
        value: Value,
        mark: u64,
    ) -> Result<crate::FeedOutcome, HostError> {
        self.input(name)?;
        let index = self.ports.index_of(name).unwrap_or(0);
        if let Some(entry) = self.inbound.get_mut(index) {
            if !entry.policy.freshness.in_order(mark, entry.newest_mark) {
                entry.drops.count(DropReason::OutOfOrder);
                return Ok(crate::FeedOutcome {
                    woke: 0,
                    dropped: Some(DropReason::OutOfOrder),
                });
            }
            entry.newest_mark = Some(mark);
        }
        self.feed(name, value)
    }

    fn queue_cap(&self, name: &str) -> Result<u32, HostError> {
        let spec = self.input(name)?;
        Ok(self.runner.queue(spec.slot).map_or(0, |queue| queue.cap))
    }

    /// Applies the port's rules to an arriving value. A refusal comes back as `Full`.
    pub(crate) fn admit(&mut self, name: &str, slot: u32) -> Result<Admit, HostError> {
        let index = self.ports.index_of(name).unwrap_or(0);
        let Some(entry) = self.inbound.get_mut(index) else {
            return Ok(Admit::Store(None));
        };
        let (len, cap) = self
            .runner
            .queue(slot)
            .map_or((0, 0), |queue| (queue.len, queue.cap));
        let now = self.now_ns;
        let decision = entry.policy.pressure.decide(len, cap, entry.opened_ns, now);
        match decision {
            Decision::Store => {
                if len == 0 {
                    entry.opened_ns = now;
                }
                Ok(Admit::Store(None))
            }
            Decision::Evict(reason) => {
                entry.drops.count(reason);
                entry.opened_ns = now;
                self.runner.drop_oldest(slot).map_err(HostError::Run)?;
                Ok(Admit::Store(Some(reason)))
            }
            Decision::Discard(reason) => {
                entry.drops.count(reason);
                Ok(Admit::Discard(reason))
            }
            Decision::Refuse => Err(HostError::Full {
                port: Label::new(name),
            }),
        }
    }

    /// Records when a stored value arrived.
    pub(crate) fn note_arrival(&mut self, name: &str) {
        let index = self.ports.index_of(name).unwrap_or(0);
        let now = self.now_ns;
        if let Some(entry) = self.inbound.get_mut(index)
            && !entry.arrivals.is_empty()
        {
            let at = (entry.pushed % entry.arrivals.len() as u64) as usize;
            entry.arrivals[at] = now;
            entry.pushed += 1;
            entry.newest_ns = now;
        }
    }
}
