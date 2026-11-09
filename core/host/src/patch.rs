//! Changing const values between ticks. The table is copy-on-write: a patch publishes a new table
//! with one entry replaced, and a reader keeps whatever it already took. Each entry has its own
//! version, so a body that decodes through a `ConfigCache` decodes only the entry that moved. A
//! patch never reloads the graph.

#![cfg(feature = "std")]

use std::fmt;
use std::string::String;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::vec::Vec;

use daedalus_graph::Lit;
use daedalus_planner::Planned;

use crate::load::{LoadedHost, node_entries};

/// A const value and the version it arrived at. The version moves only when the value does.
#[derive(Clone, Debug, PartialEq)]
pub struct ConstValue {
    pub value: Arc<Lit>,
    pub version: u32,
}

#[derive(Clone, Debug)]
struct Entry {
    node: String,
    port: String,
    current: ConstValue,
    /// What the plan said, so a reload can tell whether the plan moved it.
    planned: Arc<Lit>,
}

#[derive(Debug, Default)]
struct Shared {
    entries: RwLock<Arc<Vec<Entry>>>,
    ticking: AtomicBool,
}

/// The const values of a running graph. Clone it to give a step body a way to read them.
#[derive(Clone, Debug, Default)]
pub struct ConstTable {
    shared: Arc<Shared>,
}

/// Why a patch was refused. The table is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PatchError {
    /// A tick is running. Patch between ticks.
    TickRunning,
    /// The graph has no such const input.
    NoSuchConst { node: String, port: String },
}

impl fmt::Display for PatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatchError::TickRunning => write!(f, "a tick is running"),
            PatchError::NoSuchConst { node, port } => write!(f, "no const `{node}.{port}`"),
        }
    }
}

impl std::error::Error for PatchError {}

/// What a patch did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PatchOutcome {
    Changed {
        version: u32,
    },
    /// The value was already that, so nothing needs decoding again.
    Unchanged,
}

fn position(entries: &[Entry], node: &str, port: &str) -> Result<usize, usize> {
    entries.binary_search_by(|entry| (entry.node.as_str(), entry.port.as_str()).cmp(&(node, port)))
}

impl ConstTable {
    pub fn new() -> ConstTable {
        ConstTable::default()
    }

    fn snapshot(&self) -> Arc<Vec<Entry>> {
        let entries = self.shared.entries.read();
        Arc::clone(&entries.unwrap_or_else(PoisonError::into_inner))
    }

    fn publish(&self, entries: Vec<Entry>) {
        let slot = self.shared.entries.write();
        *slot.unwrap_or_else(PoisonError::into_inner) = Arc::new(entries);
    }

    /// The current value of `node.port`.
    pub fn get(&self, node: &str, port: &str) -> Option<ConstValue> {
        let entries = self.snapshot();
        let at = position(&entries, node, port).ok()?;
        Some(entries[at].current.clone())
    }

    /// How many consts the graph has.
    pub fn len(&self) -> usize {
        self.snapshot().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Replaces one value. Refused while a tick runs.
    pub fn patch(&self, node: &str, port: &str, value: Lit) -> Result<PatchOutcome, PatchError> {
        if self.shared.ticking.load(Ordering::Acquire) {
            return Err(PatchError::TickRunning);
        }
        let entries = self.snapshot();
        let Ok(at) = position(&entries, node, port) else {
            return Err(PatchError::NoSuchConst {
                node: String::from(node),
                port: String::from(port),
            });
        };
        if *entries[at].current.value == value {
            return Ok(PatchOutcome::Unchanged);
        }
        let version = entries[at].current.version.wrapping_add(1);
        let mut next = Vec::clone(&entries);
        next[at].current = ConstValue {
            value: Arc::new(value),
            version,
        };
        self.publish(next);
        Ok(PatchOutcome::Changed { version })
    }

    pub(crate) fn set_ticking(&self, ticking: bool) {
        self.shared.ticking.store(ticking, Ordering::Release);
    }

    /// Takes the plan's consts. An entry the plan didn't move keeps its patched value.
    pub(crate) fn seed(&self, planned: &Planned) {
        let previous = self.snapshot();
        let keys = node_entries(planned);
        let mut next = Vec::new();
        for (node, entry) in planned.graph.nodes.iter().zip(&keys) {
            for (port, value) in &node.consts {
                let old = position(&previous, &entry.key, port)
                    .ok()
                    .map(|at| &previous[at]);
                let current = match old {
                    Some(old) if *old.planned == *value => old.current.clone(),
                    _ => ConstValue {
                        value: Arc::new(value.clone()),
                        version: old.map_or(0, |old| old.current.version.wrapping_add(1)),
                    },
                };
                next.push(Entry {
                    node: entry.key.clone(),
                    port: port.clone(),
                    current,
                    planned: Arc::new(value.clone()),
                });
            }
        }
        next.sort_by(|a, b| (&a.node, &a.port).cmp(&(&b.node, &b.port)));
        self.publish(next);
    }
}

impl LoadedHost {
    /// The table a step body reads its consts from.
    pub fn consts(&self) -> &ConstTable {
        &self.consts
    }

    /// Changes one const between ticks. The next tick sees it, and no other node is touched.
    pub fn patch_const(
        &mut self,
        node: &str,
        port: &str,
        value: Lit,
    ) -> Result<PatchOutcome, PatchError> {
        self.consts.patch(node, port, value)
    }
}
