//! Per-port host bridge state.
//!
//! Every host port keeps its queue, policy overrides, freshness watermarks, and close flag in one
//! [`PortState`], so a push resolves everything it needs with a single map lookup. State lives
//! under the bridge's single buffer lock (see `docs/host-bridge-lock-granularity.md`).

use crate::collections::FastHashMap as HashMap;

use daedalus_transport::{FreshnessPolicy, Payload, PolicyQueue, PressurePolicy};

use crate::handles::PortId;
use crate::plan::RuntimeEdgePolicy;

use super::{HostBridgePayload, HostPortStats};

/// Freshness watermarks tracked per port.
#[derive(Default)]
pub(super) struct FreshnessMarks {
    pub(super) latest_sequence: Option<u64>,
    pub(super) latest_timestamp: Option<u64>,
}

/// Queue, policy overrides, freshness watermarks, and close flag for one host port.
pub(super) struct PortState {
    pub(super) id: PortId,
    pub(super) queue: PolicyQueue<HostBridgePayload>,
    /// Per-port pressure override; `None` uses the direction default.
    pub(super) pressure: Option<PressurePolicy>,
    /// Per-port freshness override; `None` uses the direction default.
    pub(super) freshness: Option<FreshnessPolicy>,
    pub(super) marks: FreshnessMarks,
    /// Set by `close_input`; only meaningful for inbound ports.
    pub(super) closed: bool,
    /// `Some` for a held inbound port (`set_held_input`): its current value, re-delivered to
    /// every tick until replaced or cleared. Held ports never queue.
    pub(super) held: Option<Option<Payload>>,
    /// Lifetime counters; `pending` is filled in from the queue when snapshotted.
    pub(super) stats: HostPortStats,
}

impl PortState {
    fn new(id: PortId) -> Self {
        Self {
            id,
            queue: PolicyQueue::default(),
            pressure: None,
            freshness: None,
            marks: FreshnessMarks::default(),
            closed: false,
            held: None,
            stats: HostPortStats::default(),
        }
    }

    /// Take the oldest queued payload, counting it as delivered.
    pub(super) fn pop_front(&mut self) -> Option<HostBridgePayload> {
        let entry = self.queue.pop_front()?;
        self.stats.delivered = self.stats.delivered.saturating_add(1);
        Some(entry)
    }

    /// Move every queued payload, oldest first, into `sink`, counting each as delivered.
    pub(super) fn drain_into(&mut self, mut sink: impl FnMut(HostBridgePayload)) {
        let delivered = &mut self.stats.delivered;
        self.queue.drain_into(|entry| {
            *delivered = delivered.saturating_add(1);
            sink(entry);
        });
    }

    /// For a held port, a clone of its current value (`Some(None)` when it has none), counted as
    /// delivered; `None` for a queued port.
    pub(super) fn take_held(&mut self) -> Option<Option<Payload>> {
        let held = self.held.as_ref()?.clone();
        if held.is_some() {
            self.stats.delivered = self.stats.delivered.saturating_add(1);
        }
        Some(held)
    }

    /// Drop queued payloads and a held port's value; the port keeps its mode.
    pub(super) fn discard_input(&mut self) {
        self.queue.clear();
        if let Some(held) = self.held.as_mut() {
            *held = None;
        }
    }

    pub(super) fn stats(&self) -> HostPortStats {
        HostPortStats {
            pending: self.queue.len(),
            ..self.stats.clone()
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
                    let id = PortId::new(name);
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

    pub(super) fn set_default_policy(&mut self, policy: &RuntimeEdgePolicy) {
        self.set_defaults(policy.pressure.clone(), policy.freshness.clone());
    }

    pub(super) fn default_policy(&self) -> RuntimeEdgePolicy {
        RuntimeEdgePolicy {
            pressure: self.default_pressure.clone(),
            freshness: self.default_freshness.clone(),
        }
    }
}
