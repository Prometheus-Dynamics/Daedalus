//! Per-index slots that keep their storage across runs.

use crate::prelude::*;

/// Slots indexed by node or edge index. [`Self::clear`] empties every slot without freeing the
/// vector, so a recycled telemetry records into the same storage each tick (a `BTreeMap` frees
/// its nodes when they empty). Equality compares the present slots only, so a cleared table
/// equals a new one.
#[derive(Clone, Debug, Default)]
pub(crate) struct DenseSlots<T> {
    slots: Vec<Option<T>>,
}

impl<T> DenseSlots<T> {
    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        self.slots.get(index)?.as_ref()
    }

    /// The slot at `index`, growing the table to reach it.
    fn slot_mut(&mut self, index: usize) -> &mut Option<T> {
        if index >= self.slots.len() {
            self.slots.resize_with(index + 1, || None);
        }
        &mut self.slots[index]
    }

    pub(crate) fn set(&mut self, index: usize, value: T) {
        *self.slot_mut(index) = Some(value);
    }

    /// The value at `index`, inserted with `T::default()` if missing.
    pub(crate) fn entry(&mut self, index: usize) -> &mut T
    where
        T: Default,
    {
        self.slot_mut(index).get_or_insert_with(T::default)
    }

    pub(crate) fn take(&mut self, index: usize) {
        if let Some(slot) = self.slots.get_mut(index) {
            *slot = None;
        }
    }

    /// Empty every slot, keeping the storage.
    pub(crate) fn clear(&mut self) {
        self.slots.fill_with(|| None);
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.slots.iter().all(Option::is_none)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (usize, &T)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| Some((idx, slot.as_ref()?)))
    }
}

impl<T: PartialEq> PartialEq for DenseSlots<T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<T: Eq> Eq for DenseSlots<T> {}
