//! Hand-overs of ranges of memory that something else keeps mapped, with each region's holders
//! counted.
//!
//! A [`Regions`] is a set of regions of memory that stay where they are while it lives: the
//! buffers of a shared memory pool, say. [`HeldRegions::adopt`] makes a [`SharedBytes`] of a
//! range of one region, with no copy; while any hand-over of a region is held, the region is held,
//! and [`HeldRegions::free_region`] names only regions no one holds. The regions outlive the
//! `HeldRegions` that made them for as long as a hand-over of them is held.
//!
//! The records are pooled: one is made the first time no free one is left, and reused after, so
//! a steady run makes none.

#![cfg(feature = "std")]

use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::{BytesC, HandedRecord, SharedBytes};

/// Regions of memory that stay at the same address, unchanged by anyone but their writer, for as
/// long as `self` lives. A region read through [`Regions::region`] is handed over as it is.
pub trait Regions: Send + Sync + 'static {
    /// Region `index`, or None when there is no such region.
    fn region(&self, index: usize) -> Option<&[u8]>;
}

/// The regions of a [`Regions`], and who holds them.
pub struct HeldRegions<R: Regions> {
    regions: R,
    /// For each region, how many records hold a range of it.
    holds: Box<[AtomicUsize]>,
    /// Records no one holds, ready to be filled again.
    free: Mutex<Vec<NonNull<HandedRecord>>>,
    /// Every record made, to free them when the last one is let go.
    made: Mutex<Vec<NonNull<HandedRecord>>>,
}

// SAFETY: the records are touched only through the two locks, or by the holder of a count, and
// the bytes they point at are the regions', which `R: Sync` lets any thread read.
unsafe impl<R: Regions> Send for HeldRegions<R> {}
// SAFETY: as above.
unsafe impl<R: Regions> Sync for HeldRegions<R> {}

impl<R: Regions> HeldRegions<R> {
    /// Regions `0 .. count` of `regions`.
    pub fn new(regions: R, count: usize) -> Arc<HeldRegions<R>> {
        Arc::new(HeldRegions {
            regions,
            holds: (0..count).map(|_| AtomicUsize::new(0)).collect(),
            free: Mutex::new(Vec::new()),
            made: Mutex::new(Vec::new()),
        })
    }

    /// How many regions there are.
    pub fn count(&self) -> usize {
        self.holds.len()
    }

    /// A region no one holds, if there is one.
    pub fn free_region(&self) -> Option<usize> {
        self.holds
            .iter()
            .position(|holds| holds.load(Ordering::Acquire) == 0)
    }

    /// How many records were made: one for each hand-over held at once, at most.
    pub fn records(&self) -> usize {
        self.made
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// The bytes `offset .. offset + len` of region `region`, as a hand-over. None when there is
    /// no such region or the range is not inside it.
    pub fn adopt(
        self: &Arc<HeldRegions<R>>,
        region: usize,
        offset: usize,
        len: usize,
    ) -> Option<SharedBytes> {
        let holds = self.holds.get(region)?;
        let bytes = self
            .regions
            .region(region)?
            .get(offset..offset.checked_add(len)?)?;
        let reused = self
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let record = reused.unwrap_or_else(|| self.make());
        holds.fetch_add(1, Ordering::AcqRel);
        // SAFETY: the record came off the free list or was just made, so nothing else touches it.
        let fields = unsafe { &mut *record.as_ptr() };
        fields.refs.store(1, Ordering::Relaxed);
        fields.bytes = BytesC {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
            data: region as *mut (),
            release: None,
        };
        fields.keep = Arc::into_raw(Arc::clone(self)).cast();
        fields.keep_drop = Some(drop_keep::<R>);
        fields.recycle = Some(recycle::<R>);
        fields.home = Arc::as_ptr(self).cast();
        // SAFETY: the record holds one count, which moves into the result. Its bytes are inside
        // a region, which stays put while `regions` lives; the record's `keep` holds it alive
        // until the last count is let go.
        Some(unsafe { SharedBytes::from_record(record) })
    }

    /// A new record, kept in `made` so the last holder frees it.
    fn make(&self) -> NonNull<HandedRecord> {
        let record = NonNull::from(Box::leak(Box::new(HandedRecord {
            refs: AtomicUsize::new(0),
            bytes: BytesC {
                ptr: std::ptr::null(),
                len: 0,
                data: std::ptr::null_mut(),
                release: None,
            },
            keep: std::ptr::null(),
            keep_drop: None,
            recycle: None,
            home: std::ptr::null(),
        })));
        let mut made = self.made.lock().unwrap_or_else(PoisonError::into_inner);
        made.push(record);
        // The free list can then take every record back without growing.
        let room = made.len();
        drop(made);
        let mut free = self.free.lock().unwrap_or_else(PoisonError::into_inner);
        let more = room.saturating_sub(free.len());
        free.reserve(more);
        record
    }
}

impl<R: Regions> Drop for HeldRegions<R> {
    fn drop(&mut self) {
        let made = std::mem::take(self.made.get_mut().unwrap_or_else(PoisonError::into_inner));
        for record in made {
            // SAFETY: each record came from `Box::leak` in `make`, and no one holds it: every
            // held record keeps this value alive, so it is dropped only when none is held.
            drop(unsafe { Box::from_raw(record.as_ptr()) });
        }
    }
}

/// The last count on a record was let go: its region is held once less, and the record goes back
/// on the free list.
///
/// # Safety
/// `record` must be one `HeldRegions::<R>::adopt` filled, released once, with nothing else
/// holding it.
unsafe extern "C" fn recycle<R: Regions>(record: *mut HandedRecord) {
    // SAFETY: `home` is the `HeldRegions`, which the record's `keep` holds alive until after
    // this call (`handed_release` reads `keep` before it recycles and drops it after).
    let held = unsafe { &*(*record).home.cast::<HeldRegions<R>>() };
    // SAFETY: as above, the record is live, and `data` is its region's index.
    let region = unsafe { (*record).bytes.data } as usize;
    if let Some(holds) = held.holds.get(region) {
        holds.fetch_sub(1, Ordering::AcqRel);
    }
    let Some(record) = NonNull::new(record) else {
        return;
    };
    held.free
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(record);
}

/// Drops the count on the `HeldRegions` a record kept.
///
/// # Safety
/// `keep` must be what `HeldRegions::<R>::adopt` stored, dropped once.
unsafe extern "C" fn drop_keep<R: Regions>(keep: *const ()) {
    // SAFETY: as the caller promises, `keep` came from `Arc::into_raw` on a `HeldRegions<R>`.
    drop(unsafe { Arc::from_raw(keep.cast::<HeldRegions<R>>()) });
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Boxes(Vec<Box<[u8]>>);

    impl Regions for Boxes {
        fn region(&self, index: usize) -> Option<&[u8]> {
            self.0.get(index).map(|region| &region[..])
        }
    }

    fn held() -> Arc<HeldRegions<Boxes>> {
        let regions = (0..2)
            .map(|at| vec![at as u8; 64].into_boxed_slice())
            .collect();
        HeldRegions::new(Boxes(regions), 2)
    }

    #[test]
    fn a_region_is_held_while_a_range_of_it_is_and_free_after() {
        let held = held();
        assert_eq!(held.free_region(), Some(0));
        let value = held.adopt(0, 0, 16).expect("in range");
        assert_eq!(value.bytes(), &[0; 16]);
        assert_eq!(held.free_region(), Some(1), "region 0 is held");
        let again = value.clone();
        drop(value);
        assert_eq!(held.free_region(), Some(1), "a clone still holds it");
        drop(again);
        assert_eq!(held.free_region(), Some(0), "let go, it is free");
    }

    #[test]
    fn a_range_outside_a_region_is_refused() {
        let held = held();
        assert!(held.adopt(2, 0, 16).is_none(), "no such region");
        assert!(held.adopt(1, 56, 16).is_none(), "runs off the end");
        assert!(held.adopt(1, usize::MAX, 2).is_none(), "overflows");
    }

    #[test]
    fn a_range_outlives_the_regions_it_came_from_and_records_are_reused() {
        let held = held();
        let value = held.adopt(1, 8, 8).expect("in range");
        let weak = Arc::downgrade(&held);
        drop(held);
        assert_eq!(value.bytes(), &[1; 8], "the region is still there");
        drop(value);
        assert!(weak.upgrade().is_none(), "the last hand-over let it go");

        let held = self::held();
        drop(held.adopt(0, 0, 8).expect("in range"));
        let second = held.adopt(0, 0, 8).expect("in range");
        assert_eq!(held.records(), 1, "the record was reused");
        drop(second);
    }
}
