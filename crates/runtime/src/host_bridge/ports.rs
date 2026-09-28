//! Per-port host bridge state.
//!
//! Every host port keeps its queue, policy overrides, freshness watermarks, and close flag in one
//! [`PortState`], so a push resolves everything it needs with a single map lookup. State lives
//! under the bridge's single buffer lock (see `docs/host-bridge-lock-granularity.md`).

use std::collections::{HashMap, VecDeque};

use daedalus_transport::{FeedOutcome, FreshnessPolicy, OverflowPolicy, PressurePolicy};

use crate::handles::PortId;

use super::HostBridgePayload;
use super::policy::apply_host_pressure;

/// Queue storage for one host port.
///
/// Replace-style policies with an effective capacity of one (`LatestOnly`, `DropOldest`,
/// `Coalesce`, and `Bounded { capacity: 1, overflow: DropOldest }`, which is the default) use a
/// single slot that is overwritten in place. Every other policy uses a FIFO deque.
pub(super) enum PortQueue {
    Slot(Option<HostBridgePayload>),
    Fifo(VecDeque<HostBridgePayload>),
}

impl Default for PortQueue {
    fn default() -> Self {
        Self::Slot(None)
    }
}

/// Whether `policy` keeps at most one value by replacing the queued one.
pub(super) fn is_single_slot(policy: &PressurePolicy) -> bool {
    match policy {
        PressurePolicy::LatestOnly
        | PressurePolicy::DropOldest
        | PressurePolicy::Coalesce { .. } => true,
        PressurePolicy::Bounded { capacity, overflow } => {
            *capacity <= 1 && matches!(overflow, OverflowPolicy::DropOldest)
        }
        PressurePolicy::BufferAll | PressurePolicy::DropNewest | PressurePolicy::ErrorOnFull => {
            false
        }
    }
}

impl PortQueue {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Slot(slot) => usize::from(slot.is_some()),
            Self::Fifo(queue) => queue.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Slot(slot) => slot.is_none(),
            Self::Fifo(queue) => queue.is_empty(),
        }
    }

    pub(super) fn pop_front(&mut self) -> Option<HostBridgePayload> {
        match self {
            Self::Slot(slot) => slot.take(),
            Self::Fifo(queue) => queue.pop_front(),
        }
    }

    pub(super) fn clear(&mut self) {
        match self {
            Self::Slot(slot) => *slot = None,
            Self::Fifo(queue) => queue.clear(),
        }
    }

    /// Move every queued payload, oldest first, into `sink`.
    pub(super) fn drain_into(&mut self, mut sink: impl FnMut(HostBridgePayload)) {
        match self {
            Self::Slot(slot) => {
                if let Some(entry) = slot.take() {
                    sink(entry);
                }
            }
            Self::Fifo(queue) => queue.drain(..).for_each(sink),
        }
    }

    /// Apply `pressure` to an incoming payload. Outcomes match the FIFO policy semantics.
    pub(super) fn push(
        &mut self,
        pressure: &PressurePolicy,
        payload: HostBridgePayload,
    ) -> FeedOutcome {
        self.adapt_to(pressure);
        match self {
            Self::Slot(slot) => {
                let new = payload.payload.correlation_id();
                match slot.replace(payload) {
                    Some(old) => FeedOutcome::Replaced {
                        old: old.payload.correlation_id(),
                        new,
                    },
                    None => FeedOutcome::Accepted {
                        correlation_id: new,
                    },
                }
            }
            Self::Fifo(queue) => apply_host_pressure(pressure, queue, payload),
        }
    }

    /// Switch storage when the effective policy changed. A FIFO holding more than one value stays
    /// a FIFO until the policy has trimmed it, so no queued payload is lost by the switch.
    fn adapt_to(&mut self, pressure: &PressurePolicy) {
        let single = is_single_slot(pressure);
        match self {
            Self::Fifo(queue) if single && queue.len() <= 1 => {
                *self = Self::Slot(queue.pop_front());
            }
            Self::Slot(slot) if !single => {
                *self = Self::Fifo(slot.take().into_iter().collect());
            }
            _ => {}
        }
    }
}

/// Freshness watermarks tracked per port.
#[derive(Default)]
pub(super) struct FreshnessMarks {
    pub(super) latest_sequence: Option<u64>,
    pub(super) latest_timestamp: Option<u64>,
}

/// Queue, policy overrides, freshness watermarks, and close flag for one host port.
pub(super) struct PortState {
    pub(super) id: PortId,
    pub(super) queue: PortQueue,
    /// Per-port pressure override; `None` uses the direction default.
    pub(super) pressure: Option<PressurePolicy>,
    /// Per-port freshness override; `None` uses the direction default.
    pub(super) freshness: Option<FreshnessPolicy>,
    pub(super) marks: FreshnessMarks,
    /// Set by `close_input`; only meaningful for inbound ports.
    pub(super) closed: bool,
}

impl PortState {
    fn new(id: PortId) -> Self {
        Self {
            id,
            queue: PortQueue::default(),
            pressure: None,
            freshness: None,
            marks: FreshnessMarks::default(),
            closed: false,
        }
    }
}

/// How a caller names a port: an owned id (no allocation on first use) or a borrowed name
/// (allocates a `PortId` only the first time the port is seen).
pub(super) enum PortKey<'a> {
    Id(PortId),
    Name(&'a str),
}

/// A port's state plus the direction defaults it falls back to.
pub(super) struct PortEntry<'a> {
    pub(super) state: &'a mut PortState,
    pub(super) default_pressure: &'a PressurePolicy,
    pub(super) default_freshness: &'a FreshnessPolicy,
}

/// All ports of one direction (host → graph inbound, or graph → host outbound).
#[derive(Default)]
pub(super) struct PortDirection {
    pub(super) ports: HashMap<PortId, PortState>,
    pub(super) default_pressure: PressurePolicy,
    pub(super) default_freshness: FreshnessPolicy,
}

impl PortDirection {
    pub(super) fn with_defaults(pressure: PressurePolicy, freshness: FreshnessPolicy) -> Self {
        Self {
            ports: HashMap::new(),
            default_pressure: pressure,
            default_freshness: freshness,
        }
    }

    /// Get or create a port's state together with the direction defaults.
    pub(super) fn entry(&mut self, key: PortKey<'_>) -> PortEntry<'_> {
        let Self {
            ports,
            default_pressure,
            default_freshness,
        } = self;
        let state = match key {
            PortKey::Id(id) => ports
                .entry(id)
                .or_insert_with_key(|id| PortState::new(id.clone())),
            PortKey::Name(name) => {
                if !ports.contains_key(name) {
                    let id = PortId::from(name);
                    ports.insert(id.clone(), PortState::new(id));
                }
                ports
                    .get_mut(name)
                    .expect("host bridge port state was just ensured")
            }
        };
        PortEntry {
            state,
            default_pressure,
            default_freshness,
        }
    }

    /// Get or create a port's state.
    pub(super) fn port(&mut self, id: PortId) -> &mut PortState {
        self.entry(PortKey::Id(id)).state
    }

    pub(super) fn get(&self, port: &str) -> Option<&PortState> {
        self.ports.get(port)
    }

    pub(super) fn get_mut(&mut self, port: &str) -> Option<&mut PortState> {
        self.ports.get_mut(port)
    }

    pub(super) fn pending(&self) -> usize {
        self.ports.values().map(|state| state.queue.len()).sum()
    }

    pub(super) fn has_pending(&self) -> bool {
        self.ports.values().any(|state| !state.queue.is_empty())
    }

    pub(super) fn set_defaults(&mut self, pressure: PressurePolicy, freshness: FreshnessPolicy) {
        self.default_pressure = pressure;
        self.default_freshness = freshness;
    }
}

#[cfg(test)]
mod tests {
    use daedalus_transport::Payload;

    use super::*;

    fn entry(value: u32) -> HostBridgePayload {
        HostBridgePayload {
            port: PortId::from("in"),
            payload: Payload::owned("demo:u32", value),
        }
    }

    #[test]
    fn single_slot_replaces_in_place() {
        let mut queue = PortQueue::default();
        let policy = PressurePolicy::LatestOnly;
        assert!(matches!(
            queue.push(&policy, entry(1)),
            FeedOutcome::Accepted { .. }
        ));
        assert!(matches!(
            queue.push(&policy, entry(2)),
            FeedOutcome::Replaced { .. }
        ));
        assert!(matches!(queue, PortQueue::Slot(Some(_))));
        let popped = queue.pop_front().expect("latest value");
        assert_eq!(popped.payload.get_ref::<u32>(), Some(&2));
        assert!(queue.is_empty());
    }

    #[test]
    fn policy_switch_keeps_queued_values() {
        let mut queue = PortQueue::default();
        queue.push(&PressurePolicy::BufferAll, entry(1));
        queue.push(&PressurePolicy::BufferAll, entry(2));
        assert_eq!(queue.len(), 2);
        // A replace policy trims the FIFO first, then later pushes use the slot.
        queue.push(&PressurePolicy::LatestOnly, entry(3));
        assert_eq!(queue.len(), 1);
        queue.push(&PressurePolicy::LatestOnly, entry(4));
        assert!(matches!(queue, PortQueue::Slot(Some(_))));
        queue.push(&PressurePolicy::BufferAll, entry(5));
        assert!(matches!(queue, PortQueue::Fifo(_)));
        let mut values = Vec::new();
        queue.drain_into(|entry| values.push(*entry.payload.get_ref::<u32>().unwrap()));
        assert_eq!(values, vec![4, 5]);
    }
}
