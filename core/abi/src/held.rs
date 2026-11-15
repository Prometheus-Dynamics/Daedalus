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
//!
//! Regions that can be written ([`WritableRegions`]) are also lent out a range at a time, to be
//! written before they are handed over: [`HeldRegions::lease`] gives a [`Lease`] of room in one
//! region, which holds the region as a hand-over does, and becomes one when it is sealed. A lease
//! starts at the front of a region nothing holds, so a value still read while the next is written
//! leaves the next to another region; when every other region is held, leases follow one another
//! in the one region leases are taken in. A region a writer elsewhere is told to write
//! in is claimed ([`HeldRegions::claim`]), and is never the region leases are taken in.

#![cfg(feature = "std")]

use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::{BytesC, HandedRecord, SharedBytes};

mod lease;

use lease::{Counts, Lending};
pub use lease::{Lease, LeaseCounts, Lend};

/// Regions of memory that stay at the same address, unchanged by anyone but their writer, for as
/// long as `self` lives, wherever `self` is moved. A region read through [`Regions::region`] is
/// handed over as it is.
pub trait Regions: Send + Sync + 'static {
    /// Region `index`, or None when there is no such region.
    fn region(&self, index: usize) -> Option<&[u8]>;
}

/// [`Regions`] this process may write, through [`HeldRegions::lease`].
pub trait WritableRegions: Regions {
    /// Region `index` to write, or None when there is no such region. The same memory as
    /// [`Regions::region`] gives.
    fn region_mut(&mut self, index: usize) -> Option<&mut [u8]>;
}

/// Where a region starts and how long it is, taken once when the regions are held.
#[derive(Clone, Copy)]
struct Base {
    ptr: NonNull<u8>,
    len: usize,
}

/// The regions of a [`Regions`], and who holds them.
pub struct HeldRegions<R: Regions> {
    /// Kept for its memory, which `bases` points into.
    #[allow(dead_code)]
    regions: R,
    bases: Box<[Base]>,
    /// The regions can be written, so they can be leased.
    writable: bool,
    lending: Mutex<Lending>,
    counts: Counts,
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
        let bases = (0..count)
            .map(|at| {
                let region = regions.region(at).unwrap_or_default();
                Base {
                    ptr: NonNull::from(region).cast(),
                    len: region.len(),
                }
            })
            .collect();
        HeldRegions::with_bases(regions, bases, false)
    }

    fn with_bases(regions: R, bases: Box<[Base]>, writable: bool) -> Arc<HeldRegions<R>> {
        Arc::new(HeldRegions {
            holds: bases.iter().map(|_| AtomicUsize::new(0)).collect(),
            regions,
            bases,
            writable,
            lending: Mutex::new(Lending::default()),
            counts: Counts::default(),
            free: Mutex::new(Vec::new()),
            made: Mutex::new(Vec::new()),
        })
    }

    /// The bytes `offset .. offset + len` of region `region`, if they are inside it.
    fn range(&self, region: usize, offset: usize, len: usize) -> Option<&[u8]> {
        let base = self.bases.get(region)?;
        if offset.checked_add(len)? > base.len {
            return None;
        }
        if len == 0 {
            return Some(&[]);
        }
        // No lease of these bytes is written while they are read: a range is adopted only once
        // written (`Lease::seal`), or from a claimed region, which leases are never taken in.
        let start = base.ptr.as_ptr();
        // SAFETY: the range is inside the region, which stays mapped and in place while `self`
        // lives (the `Regions` contract), and the slice borrows `self`.
        Some(unsafe { core::slice::from_raw_parts(start.add(offset), len) })
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

    /// Claims a region no one holds and leases are not taken in, for a writer elsewhere to write
    /// in: it is held until [`HeldRegions::unclaim`]. None when every region is held.
    pub fn claim(&self) -> Option<usize> {
        let lending = self.lending.lock().unwrap_or_else(PoisonError::into_inner);
        let at = (0..self.holds.len()).find(|&at| {
            Some(at) != lending.region && self.holds[at].load(Ordering::Acquire) == 0
        })?;
        self.holds[at].fetch_add(1, Ordering::AcqRel);
        Some(at)
    }

    /// Lets go of a region [`HeldRegions::claim`] gave.
    pub fn unclaim(&self, region: usize) {
        if let Some(holds) = self.holds.get(region) {
            holds.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// How many holders region `region` has: hand-overs, leases and a claim.
    pub fn holders(&self, region: usize) -> usize {
        self.holds
            .get(region)
            .map_or(0, |holds| holds.load(Ordering::Acquire))
    }

    /// Where `bytes` are, when they are a hand-over of these regions: the region and the offset
    /// in it.
    pub fn locate(self: &Arc<HeldRegions<R>>, bytes: &SharedBytes) -> Option<(usize, usize)> {
        // SAFETY: `bytes` holds a count, so its record is live.
        let fields = unsafe { bytes.record().as_ref() };
        if fields.home != Arc::as_ptr(self).cast() {
            return None;
        }
        let region = fields.bytes.data as usize;
        let base = self.bases.get(region)?;
        let offset = (fields.bytes.ptr as usize).checked_sub(base.ptr.as_ptr() as usize)?;
        (offset.checked_add(fields.bytes.len)? <= base.len).then_some((region, offset))
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
        let bytes = self.range(region, offset, len)?;
        let record = self.record();
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

    /// A free record, or a new one when none is free.
    fn record(&self) -> NonNull<HandedRecord> {
        let reused = self
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        reused.unwrap_or_else(|| self.make())
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
