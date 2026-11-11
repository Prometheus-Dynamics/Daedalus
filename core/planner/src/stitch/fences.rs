//! Which units a reader in another region has to wait for.

use std::collections::{BTreeMap, BTreeSet};

use super::UnitId;

/// The units that may be pending, by the value they write.
#[derive(Default)]
pub(super) struct Fences {
    pending_writer: BTreeMap<u32, UnitId>,
    waited: BTreeSet<UnitId>,
}

impl Fences {
    pub(crate) fn record_write(&mut self, value: u32, unit: UnitId) {
        self.pending_writer.insert(value, unit);
    }

    /// The unit to wait for before `region` reads `value`, the first time only.
    pub(crate) fn wait_for(&mut self, value: u32, region: u32) -> Option<UnitId> {
        let unit = *self.pending_writer.get(&value)?;
        (unit.region != region && self.waited.insert(unit)).then_some(unit)
    }
}
