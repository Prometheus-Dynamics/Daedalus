//! Numbers the slots: a value takes the lowest free one, and may give it back after its last reader.

use std::collections::{BTreeMap, BTreeSet};

use super::layout::Draft;

/// A value in a space.
pub(super) type Live = (u32, usize);

pub(super) struct Numbered {
    pub of: BTreeMap<Live, u32>,
    pub count: u32,
    pub readers: BTreeMap<Live, u32>,
}

/// Walks the draft once. `at_start` are written before anything runs, and `at_end` are read after
/// everything has. With `share`, a slot goes back after its last reader. Steps must not share: a
/// slot is a queue there, and a value that waited for the next tick is still in it.
pub(super) fn number(draft: &[Draft], at_start: &[Live], at_end: &[Live], share: bool) -> Numbered {
    let mut last = BTreeMap::new();
    let mut readers: BTreeMap<Live, u32> = BTreeMap::new();
    for (at, item) in draft.iter().enumerate() {
        for live in item.reads() {
            last.insert(live, at);
            *readers.entry(live).or_default() += 1;
        }
    }
    for live in at_end {
        last.insert(*live, usize::MAX);
        *readers.entry(*live).or_default() += 1;
    }
    let mut of = BTreeMap::new();
    let mut free = BTreeSet::new();
    let mut count = 0;
    let mut take = |live: Live, of: &mut BTreeMap<Live, u32>, free: &mut BTreeSet<u32>| {
        of.entry(live).or_insert_with(|| {
            free.pop_first().unwrap_or_else(|| {
                count += 1;
                count - 1
            })
        });
    };
    for live in at_start {
        take(*live, &mut of, &mut free);
    }
    for (at, item) in draft.iter().enumerate() {
        for live in item.writes() {
            take(live, &mut of, &mut free);
        }
        for live in item.reads().into_iter().chain(item.writes()) {
            if share
                && last.get(&live).copied().unwrap_or(at) == at
                && let Some(&slot) = of.get(&live)
            {
                free.insert(slot);
            }
        }
    }
    Numbered { of, count, readers }
}
