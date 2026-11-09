//! Pressure and freshness rules for a slot ring. Nothing here allocates or reads a clock: time
//! comes from the host, in nanoseconds.

use core::time::Duration;

use crate::{Push, Ring, Value};

pub const EDGE_PRESSURE_POLICY_KEY: &str = "daedalus.edge.pressure";
pub const EDGE_CAPACITY_KEY: &str = "daedalus.edge.capacity";
pub const EDGE_FRESHNESS_POLICY_KEY: &str = "daedalus.edge.freshness";
pub const EDGE_OVERFLOW_POLICY_KEY: &str = "daedalus.edge.overflow";
pub const EDGE_PRESSURE_FIFO: &str = "fifo";
pub const EDGE_PRESSURE_LATEST_ONLY: &str = "latest_only";
pub const EDGE_PRESSURE_NEWEST_WINS: &str = "newest_wins";
pub const EDGE_PRESSURE_BOUNDED: &str = "bounded";
pub const EDGE_PRESSURE_DROP_NEWEST: &str = "drop_newest";
pub const EDGE_PRESSURE_DROP_OLDEST: &str = "drop_oldest";
pub const EDGE_PRESSURE_ERROR_ON_FULL: &str = "error_on_full";
pub const EDGE_PRESSURE_COALESCE: &str = "coalesce";
pub const EDGE_FRESHNESS_PRESERVE_ALL: &str = "preserve_all";
pub const EDGE_FRESHNESS_LATEST_BY_SEQUENCE: &str = "latest_by_sequence";
pub const EDGE_FRESHNESS_LATEST_BY_TIMESTAMP: &str = "latest_by_timestamp";
pub const EDGE_FRESHNESS_MAX_AGE: &str = "max_age";
pub const EDGE_FRESHNESS_MAX_LAG: &str = "max_lag";

/// Which value of a coalescing window survives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Keep {
    /// The value that opened the window stays until the window ends.
    #[default]
    Oldest,
    /// Each arrival replaces the held value.
    Newest,
}

/// What a bounded queue does at capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Overflow {
    DropOldest,
    DropNewest,
    ErrorOnFull,
    Coalesce { keep: Keep, window_ns: u64 },
}

/// How a slot takes values under pressure.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Pressure {
    /// Queue up to the plan's capacity. Full is an error.
    #[default]
    BufferAll,
    /// One value, the newest.
    LatestOnly,
    /// Up to `cap`, and the rule at capacity is `overflow`.
    Bounded { cap: u32, overflow: Overflow },
}

/// Why a value was given up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    DropNewest,
    DropOldest,
    LatestOnlyReplace,
    Coalesced,
    MaxAge,
    MaxLag,
    OutOfOrder,
}

/// What a push must do, given the queue's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// There is room.
    Store,
    /// Remove the oldest value first, then store.
    Evict(DropReason),
    /// Give up the incoming value.
    Discard(DropReason),
    /// Refuse the push: the caller reports a full queue.
    Refuse,
}

impl Pressure {
    /// The most values this policy queues, given the slot's capacity.
    pub const fn capacity(&self, slot_cap: u32) -> u32 {
        match self {
            Pressure::BufferAll => slot_cap,
            Pressure::LatestOnly => 1,
            Pressure::Bounded { cap, .. } => {
                if *cap == 0 {
                    1
                } else if *cap < slot_cap {
                    *cap
                } else {
                    slot_cap
                }
            }
        }
    }

    /// Decides one push. `opened_ns` is when the value now in a coalescing window was stored.
    pub const fn decide(&self, len: u32, slot_cap: u32, opened_ns: u64, now_ns: u64) -> Decision {
        if len < self.capacity(slot_cap) {
            return Decision::Store;
        }
        match self {
            Pressure::BufferAll => Decision::Refuse,
            Pressure::LatestOnly => Decision::Evict(DropReason::LatestOnlyReplace),
            Pressure::Bounded { overflow, .. } => match overflow {
                Overflow::DropOldest => Decision::Evict(DropReason::DropOldest),
                Overflow::DropNewest => Decision::Discard(DropReason::DropNewest),
                Overflow::ErrorOnFull => Decision::Refuse,
                Overflow::Coalesce {
                    keep: Keep::Newest, ..
                } => Decision::Evict(DropReason::Coalesced),
                Overflow::Coalesce {
                    keep: Keep::Oldest,
                    window_ns,
                } => {
                    if now_ns.saturating_sub(opened_ns) < *window_ns {
                        Decision::Discard(DropReason::Coalesced)
                    } else {
                        Decision::Evict(DropReason::Coalesced)
                    }
                }
            },
        }
    }

    /// Reads the policy from a graph document's metadata strings.
    pub fn from_meta(
        pressure: &str,
        capacity: Option<u32>,
        overflow: Option<&str>,
    ) -> Option<Pressure> {
        let bounded = |overflow| Pressure::Bounded {
            cap: capacity.unwrap_or(1),
            overflow,
        };
        Some(match pressure {
            EDGE_PRESSURE_FIFO => Pressure::BufferAll,
            EDGE_PRESSURE_LATEST_ONLY | EDGE_PRESSURE_NEWEST_WINS => Pressure::LatestOnly,
            EDGE_PRESSURE_DROP_NEWEST => bounded(Overflow::DropNewest),
            EDGE_PRESSURE_DROP_OLDEST => bounded(Overflow::DropOldest),
            EDGE_PRESSURE_ERROR_ON_FULL => bounded(Overflow::ErrorOnFull),
            EDGE_PRESSURE_COALESCE => bounded(Overflow::Coalesce {
                keep: Keep::Newest,
                window_ns: 0,
            }),
            EDGE_PRESSURE_BOUNDED => bounded(match overflow? {
                "drop_oldest" => Overflow::DropOldest,
                "drop_newest" | "drop_incoming" => Overflow::DropNewest,
                "error_on_full" | "error" | "backpressure" => Overflow::ErrorOnFull,
                _ => return None,
            }),
            _ => return None,
        })
    }

    /// Pushes `value` under this policy. `opened_ns` is kept with the ring and updated here.
    pub fn push(
        &self,
        ring: &mut Ring<'_>,
        value: Value,
        opened_ns: &mut u64,
        now_ns: u64,
    ) -> Pushed {
        let decision = self.decide(
            ring.len() as u32,
            ring.capacity() as u32,
            *opened_ns,
            now_ns,
        );
        match decision {
            Decision::Store => {
                if ring.is_empty() {
                    *opened_ns = now_ns;
                }
                Pushed {
                    push: ring.push(value),
                    reason: None,
                }
            }
            Decision::Evict(reason) => {
                let old = ring.pop();
                *opened_ns = now_ns;
                match ring.push(value) {
                    Push::Stored => {}
                    other => {
                        return Pushed {
                            push: other,
                            reason: Some(reason),
                        };
                    }
                }
                Pushed {
                    push: old.map_or(Push::Stored, Push::Dropped),
                    reason: Some(reason),
                }
            }
            Decision::Discard(reason) => Pushed {
                push: Push::Dropped(value),
                reason: Some(reason),
            },
            Decision::Refuse => Pushed {
                push: Push::Full(value),
                reason: None,
            },
        }
    }
}

/// What a policy push did, and why a value was given up, when one was.
#[derive(Debug)]
pub struct Pushed {
    pub push: Push,
    pub reason: Option<DropReason>,
}

/// Whether queued or arriving values are still worth running.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Freshness {
    #[default]
    PreserveAll,
    /// An arrival whose sequence does not go up is dropped.
    LatestBySequence,
    /// An arrival whose timestamp does not go up is dropped.
    LatestByTimestamp,
    /// A value older than this is dropped.
    MaxAge(Duration),
    /// A value further than this behind the newest arrival is dropped.
    MaxLag(Duration),
}

impl Freshness {
    /// Whether a value that arrived at `arrived_ns` should be dropped, at time `now_ns`, with the
    /// newest arrival at `newest_ns`.
    pub fn expired(&self, arrived_ns: u64, now_ns: u64, newest_ns: u64) -> Option<DropReason> {
        match self {
            Freshness::MaxAge(age) => (u128::from(now_ns.saturating_sub(arrived_ns))
                > age.as_nanos())
            .then_some(DropReason::MaxAge),
            Freshness::MaxLag(lag) => (u128::from(newest_ns.saturating_sub(arrived_ns))
                > lag.as_nanos())
            .then_some(DropReason::MaxLag),
            _ => None,
        }
    }

    /// Whether an arrival marked `mark` is in order after the newest mark seen, for the latest-by
    /// policies. Every other policy accepts anything.
    pub fn in_order(&self, mark: u64, newest: Option<u64>) -> bool {
        match self {
            Freshness::LatestBySequence | Freshness::LatestByTimestamp => {
                newest.is_none_or(|seen| mark > seen)
            }
            _ => true,
        }
    }

    /// Reads the policy from a graph document's metadata strings. `age` is in nanoseconds.
    pub fn from_meta(name: &str, age: Option<u64>) -> Option<Freshness> {
        Some(match name {
            EDGE_FRESHNESS_PRESERVE_ALL => Freshness::PreserveAll,
            EDGE_FRESHNESS_LATEST_BY_SEQUENCE => Freshness::LatestBySequence,
            EDGE_FRESHNESS_LATEST_BY_TIMESTAMP => Freshness::LatestByTimestamp,
            EDGE_FRESHNESS_MAX_AGE => Freshness::MaxAge(Duration::from_nanos(age?)),
            EDGE_FRESHNESS_MAX_LAG => Freshness::MaxLag(Duration::from_nanos(age?)),
            _ => return None,
        })
    }
}
