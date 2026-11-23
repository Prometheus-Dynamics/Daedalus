//! Changing const values between ticks. A patch offers the value to the step that reads the
//! constant, which decodes it there and then: one that does not fit is refused and nothing
//! changes. The table is the record of what each constant is now. It is copy-on-write: a patch
//! publishes a new table with one entry replaced, and a reader keeps whatever it already took.
//! Each entry has its own version, so a body that reads the table itself and decodes through a
//! `ConfigCache` decodes only the entry that moved. A patch never reloads the graph.

#![cfg(feature = "std")]

use std::fmt;
use std::string::String;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::vec::Vec;

use daedalus_backend::NotTaken;
use daedalus_graph::Lit;
use daedalus_made::Bound;
use daedalus_types::NodeIndex;

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
    bound: Arc<Lit>,
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
    /// No step takes a later value for this constant: its backend fixed it when the graph was
    /// planned. Changing it takes a replan.
    NotRead { node: String, port: String },
    /// The step that reads the constant refused the value, in the words a plan is refused in.
    Refused {
        node: String,
        port: String,
        why: String,
    },
}

impl fmt::Display for PatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatchError::TickRunning => write!(f, "a tick is running"),
            PatchError::NoSuchConst { node, port } => write!(f, "no const `{node}.{port}`"),
            PatchError::NotRead { node, port } => write!(
                f,
                "no step reads const `{node}.{port}` while the graph runs: its backend fixed it \
                 when the graph was planned, so changing it takes a replan"
            ),
            PatchError::Refused { node, port, why } => {
                write!(f, "const `{node}.{port}` was refused: {why}")
            }
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

    /// Whether a patch of `node.port` to `value` would change it. Refused while a tick runs,
    /// and for a const the graph does not have.
    pub(crate) fn would_change(
        &self,
        node: &str,
        port: &str,
        value: &Lit,
    ) -> Result<bool, PatchError> {
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
        Ok(*entries[at].current.value != *value)
    }

    /// The entries a patch moved away from what the plan says, as (node, port, value).
    pub(crate) fn patched(&self) -> Vec<(String, String, Arc<Lit>)> {
        let entries = self.snapshot();
        let moved = entries
            .iter()
            .filter(|entry| *entry.current.value != *entry.bound);
        moved
            .map(|entry| {
                let value = Arc::clone(&entry.current.value);
                (entry.node.clone(), entry.port.clone(), value)
            })
            .collect()
    }

    /// Puts an entry back to what the plan says.
    pub(crate) fn put_back(&self, node: &str, port: &str) {
        let entries = self.snapshot();
        let Ok(at) = position(&entries, node, port) else {
            return;
        };
        let mut next = Vec::clone(&entries);
        next[at].current = ConstValue {
            value: Arc::clone(&next[at].bound),
            version: next[at].current.version.wrapping_add(1),
        };
        self.publish(next);
    }

    /// Replaces one value in the table. Refused while a tick runs. This is the table alone: a
    /// body that reads the table sees it, and [`LoadedHost::patch_const`] is what reaches a step.
    pub fn patch(&self, node: &str, port: &str, value: Lit) -> Result<PatchOutcome, PatchError> {
        if !self.would_change(node, port, &value)? {
            return Ok(PatchOutcome::Unchanged);
        }
        let entries = self.snapshot();
        let Ok(at) = position(&entries, node, port) else {
            return Ok(PatchOutcome::Unchanged);
        };
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
    pub(crate) fn seed(&self, bound: &Bound) {
        let previous = self.snapshot();
        let keys = node_entries(bound);
        let mut next = Vec::new();
        for (node, entry) in bound.facts.nodes.iter().zip(&keys) {
            for (port, value) in &node.consts {
                let old = position(&previous, &entry.key, port)
                    .ok()
                    .map(|at| &previous[at]);
                let current = match old {
                    Some(old) if *old.bound == *value => old.current.clone(),
                    _ => ConstValue {
                        value: Arc::new(value.clone()),
                        version: old.map_or(0, |old| old.current.version.wrapping_add(1)),
                    },
                };
                next.push(Entry {
                    node: entry.key.clone(),
                    port: port.clone(),
                    current,
                    bound: Arc::new(value.clone()),
                });
            }
        }
        next.sort_by(|a, b| (&a.node, &a.port).cmp(&(&b.node, &b.port)));
        self.publish(next);
    }
}

impl LoadedHost {
    /// The record of what each const is now. A step body given this table when the plan was
    /// compiled reads its consts from it.
    pub fn consts(&self) -> &ConstTable {
        &self.consts
    }

    /// Offers a value to the step that reads `node.port`, which decodes it as it would when
    /// the plan is compiled.
    fn offer(&self, node: &str, port: &str, value: &Lit) -> Result<(), NotTaken> {
        let Some(index) = self.nodes.iter().position(|entry| entry.key == node) else {
            return Err(NotTaken::NoReader);
        };
        let index = NodeIndex(index as u32);
        for patcher in &self.patchers {
            match patcher.offer(index, port, value) {
                Err(NotTaken::NoReader) => {}
                taken => return taken,
            }
        }
        Err(NotTaken::NoReader)
    }

    /// Has the runner tell the step that runs `node` that a value is waiting for it.
    fn mark_consts(&mut self, node: &str) {
        let covering = self.covers.iter().position(|nodes| nodes.contains(&node));
        if let Some(step) = covering {
            self.host.runner.mark_consts(step as u32);
        }
    }

    /// Changes one const between ticks, without a replan. The step that reads it decodes the
    /// value here, so one that does not fit is refused in the words a plan is refused in and the
    /// old value stays. The runner marks the step's next call and the step swaps the new value in
    /// before it runs: the next tick sees it, the node keeps its state, and no other node is
    /// touched. A tick with nothing patched does nothing for this.
    ///
    /// A const no step takes a later value for is refused by name
    /// ([`PatchError::NotRead`]): `#[node]` bodies on the CPU backend take one for every const
    /// input and for `config`, which is one const and is patched whole. Anything the planner
    /// used the const for was settled when the graph was planned, so a change that would move
    /// the plan (a type, an edge, a choice made from the value) takes a replan.
    ///
    /// Decoding keeps memory for the life of the process when the value has text, lists,
    /// structs or enum names in it: numbers and bools keep nothing; a text keeps its bytes, a
    /// list 32 bytes an element, a struct 48 bytes a field and its field names, an enum its
    /// name. A struct of 8 numeric fields given as an input constant keeps about 500 bytes for
    /// each patch that changes it. A config's own fields are not kept, only what a field's
    /// value holds, so a config of numbers keeps nothing. `daedalus_cpu::literal_bytes_kept` is
    /// the running count. A value that changes often belongs on a host input, which keeps
    /// nothing.
    pub fn patch_const(
        &mut self,
        node: &str,
        port: &str,
        value: Lit,
    ) -> Result<PatchOutcome, PatchError> {
        if !self.consts.would_change(node, port, &value)? {
            return Ok(PatchOutcome::Unchanged);
        }
        let named = |node: &str, port: &str| (String::from(node), String::from(port));
        match self.offer(node, port, &value) {
            Ok(()) => self.mark_consts(node),
            // Bodies that hold the table read it for themselves.
            Err(NotTaken::NoReader) if self.consts_given => {}
            Err(NotTaken::NoReader) => {
                let (node, port) = named(node, port);
                return Err(PatchError::NotRead { node, port });
            }
            Err(NotTaken::Refused(why)) => {
                let (node, port) = named(node, port);
                return Err(PatchError::Refused { node, port, why });
            }
        }
        self.consts.patch(node, port, value)
    }

    /// After a reload: a step made again holds what the plan says, so each const a patch had
    /// moved is offered to it. One the new step won't take goes back to the plan's value.
    pub(crate) fn offer_patched(&mut self) {
        // A step carried across may have a value waiting that the old runner had marked it for.
        for step in 0..self.covers.len() {
            self.host.runner.mark_consts(step as u32);
        }
        for (node, port, value) in self.consts.patched() {
            match self.offer(&node, &port, &value) {
                Ok(()) => {}
                Err(NotTaken::NoReader) if self.consts_given => {}
                Err(_) => self.consts.put_back(&node, &port),
            }
        }
    }
}
