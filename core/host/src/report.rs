//! What a reload did: kept, fresh, added and removed nodes, and what went with the removed ports.

#![cfg(feature = "std")]

use std::string::String;
use std::vec::Vec;

/// The most names a report's list holds. The counts are always exact.
pub const REPORT_NAME_LIMIT: usize = 32;

/// A list of names that keeps its length bounded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BoundedNames {
    names: Vec<String>,
    total: usize,
}

impl BoundedNames {
    pub(crate) fn push(&mut self, name: &str) {
        self.total += 1;
        if self.names.len() < REPORT_NAME_LIMIT {
            self.names.push(String::from(name));
        }
    }

    /// How many there were, even when the list stopped at the limit.
    pub fn total(&self) -> usize {
        self.total
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn contains(&self, name: &str) -> bool {
        self.names.iter().any(|known| known == name)
    }
}

/// What a reload did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReloadReport {
    /// Same body: state carried.
    pub carried: BoundedNames,
    /// Same node, new body: state started fresh.
    pub fresh: BoundedNames,
    /// New nodes.
    pub added: BoundedNames,
    /// Removed nodes, whose state was dropped.
    pub removed: BoundedNames,
    /// Host ports that are gone.
    pub removed_ports: BoundedNames,
    /// Values dropped with removed ports.
    pub dropped_values: u32,
}
