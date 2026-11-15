//! Room in a region, lent to be written and then handed over.

#![cfg(feature = "std")]

use core::ptr::NonNull;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError};

use super::{Base, HeldRegions, WritableRegions, drop_keep, recycle};
use crate::{BytesC, HandedRecord, SharedBytes, handed_release};

/// Where a lease's items start: a cache line from the region's start, so no value shares one with
/// another, and an array of numbers is aligned for any of them when the region is.
const ALIGN: usize = 64;

/// The first offset at or after `cursor` that puts the byte `items_at` of a value on an [`ALIGN`]
/// boundary.
fn aligned(cursor: usize, items_at: usize) -> Option<usize> {
    let skew = items_at % ALIGN;
    Some(cursor.checked_add(skew)?.checked_next_multiple_of(ALIGN)? - skew)
}

/// Which region leases are taken in, and how far it is taken.
#[derive(Default)]
pub(super) struct Lending {
    pub(super) region: Option<usize>,
    pub(super) cursor: usize,
}

#[derive(Default)]
pub(super) struct Counts {
    leased: AtomicUsize,
    sealed: AtomicUsize,
    abandoned: AtomicUsize,
}

/// How many leases were taken, sealed into hand-overs, and let go unsealed. Every lease is one or
/// the other once it is gone, so `leased == sealed + abandoned + live leases`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeaseCounts {
    pub leased: usize,
    pub sealed: usize,
    pub abandoned: usize,
}

/// Something that lends room to write a value in, which a reader elsewhere then reads where it
/// was written.
pub trait Lend: Send + Sync {
    /// Room for `len` bytes, with the byte `items_at` on an aligned address. None when nothing is
    /// lent right now, or no room is free.
    fn lease(&self, len: usize, items_at: usize) -> Option<Lease>;
    /// The smallest value worth writing in lent room: anything smaller travels with its run.
    fn threshold(&self) -> usize;
}

/// Room in one region, held for its writer until it is sealed into a hand-over or let go. The
/// region is held while the lease is, as by a hand-over, and no other lease or hand-over covers
/// the room, so its writer alone touches those bytes.
pub struct Lease {
    /// A record already filled for this room, holding one count, with no bytes yet.
    record: NonNull<HandedRecord>,
    ptr: NonNull<u8>,
    cap: usize,
    region: usize,
    start: usize,
    grow: unsafe fn(home: *const (), region: usize, end: usize, new_end: usize) -> bool,
    settle: unsafe fn(home: *const (), sealed: bool),
}

// SAFETY: a lease is the one owner of its room and of one count on its record, which is atomic;
// the regions it points into may be written from any thread (`Regions: Send + Sync`).
unsafe impl Send for Lease {}
// SAFETY: a shared lease gives only reads of its fields.
unsafe impl Sync for Lease {}

impl Lease {
    /// How many bytes the room holds.
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// The region the room is in, and its offset there.
    pub fn place(&self) -> (usize, usize) {
        (self.region, self.start)
    }

    /// The room, to write.
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        if self.cap == 0 {
            return &mut [];
        }
        // SAFETY: the room is `cap` bytes inside a region that stays mapped while the record's
        // `keep` holds it, which this lease's count keeps alive. No other lease or hand-over covers
        // these bytes (`HeldRegions::lease`), and the slice borrows `self` mutably.
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.cap) }
    }

    /// Makes the room `cap` bytes long, in place. Refused, leaving it as it was, when another
    /// lease was taken after this one or the region ends first.
    pub fn grow(&mut self, cap: usize) -> bool {
        if cap <= self.cap {
            return true;
        }
        let (Some(end), Some(new_end)) = (
            self.start.checked_add(self.cap),
            self.start.checked_add(cap),
        ) else {
            return false;
        };
        // SAFETY: `home` is the record's `HeldRegions`, which its `keep` holds alive.
        let grown = unsafe { (self.grow)(self.record.as_ref().home, self.region, end, new_end) };
        if grown {
            self.cap = cap;
        }
        grown
    }

    /// Hands over the first `len` bytes written, with no copy. None, and the lease let go, when
    /// `len` is more than the room.
    pub fn seal(self, len: usize) -> Option<SharedBytes> {
        if len > self.cap {
            return None;
        }
        let this = core::mem::ManuallyDrop::new(self);
        // SAFETY: the lease owns the record's one count, so nothing else touches it until it is
        // handed over here.
        let fields = unsafe { &mut *this.record.as_ptr() };
        fields.bytes.len = len;
        // SAFETY: `home` is live while the record is, as in `grow`.
        unsafe { (this.settle)(fields.home, true) };
        // SAFETY: the count moves into the result, which `this` no longer drops.
        Some(unsafe { SharedBytes::from_record(this.record) })
    }
}

impl Drop for Lease {
    /// Let go unsealed, after a failed write: the room is given back with the region.
    fn drop(&mut self) {
        // SAFETY: `home` is live while the record is, as in `grow`.
        unsafe { (self.settle)(self.record.as_ref().home, false) };
        // SAFETY: the lease owns one count on the live record, and gives it back once.
        unsafe { handed_release(self.record.as_ptr()) };
    }
}

impl<R: WritableRegions> HeldRegions<R> {
    /// Regions `0 .. count` of `regions`, which can be leased as well as handed over.
    pub fn writable(mut regions: R, count: usize) -> Arc<HeldRegions<R>> {
        let bases = (0..count)
            .map(|at| {
                let region = regions.region_mut(at).unwrap_or_default();
                Base {
                    len: region.len(),
                    ptr: NonNull::from(region).cast(),
                }
            })
            .collect();
        HeldRegions::with_bases(regions, bases, true)
    }

    /// Room for `len` bytes with the byte `items_at` of them on a 64-byte boundary from the
    /// region's start: from the start of the region leases are taken in once nothing holds it, or
    /// of another region nothing holds, or else after the last lease in the region leases are
    /// taken in. None when no region has the room.
    pub fn lease(self: &Arc<HeldRegions<R>>, len: usize, items_at: usize) -> Option<Lease> {
        if !self.writable {
            return None;
        }
        let mut lending = self.lending.lock().unwrap_or_else(PoisonError::into_inner);
        let fits = |region: usize, cursor: usize| -> Option<usize> {
            let start = aligned(cursor, items_at)?;
            (start.checked_add(len)? <= self.bases[region].len).then_some(start)
        };
        let idle = |region: usize| self.holds[region].load(Ordering::Acquire) == 0;
        // From the front of a region nothing holds: the one leases are taken in when it is idle,
        // else another, so a value still read while the next is written sits in one region and
        // the next in another, and both stay in memory already touched. After the last lease
        // only when every other region is held.
        let current = lending.region;
        let fresh = current
            .into_iter()
            .chain((0..self.holds.len()).filter(|&at| Some(at) != current))
            .find_map(|at| Some((at, fits(at, 0).filter(|_| idle(at))?)));
        let (region, start) = match fresh {
            Some(found) => found,
            None => {
                let region = current?;
                (region, fits(region, lending.cursor)?)
            }
        };
        lending.region = Some(region);
        lending.cursor = start + len;
        self.holds[region].fetch_add(1, Ordering::AcqRel);
        drop(lending);
        let record = self.record();
        let base = self.bases[region].ptr.as_ptr();
        // SAFETY: `start + len` is inside the region (`fits`).
        let ptr = unsafe { NonNull::new_unchecked(base.add(start)) };
        // SAFETY: the record came off the free list or was just made, so nothing else touches it.
        let fields = unsafe { &mut *record.as_ptr() };
        fields.refs.store(1, Ordering::Relaxed);
        fields.bytes = BytesC {
            ptr: ptr.as_ptr(),
            len: 0,
            data: region as *mut (),
            release: None,
        };
        fields.keep = Arc::into_raw(Arc::clone(self)).cast();
        fields.keep_drop = Some(drop_keep::<R>);
        fields.recycle = Some(recycle::<R>);
        fields.home = Arc::as_ptr(self).cast();
        self.counts.leased.fetch_add(1, Ordering::Relaxed);
        Some(Lease {
            record,
            ptr,
            cap: len,
            region,
            start,
            grow: grow_in::<R>,
            settle: settle_in::<R>,
        })
    }

    /// How many leases were taken, sealed and let go.
    pub fn lease_counts(&self) -> LeaseCounts {
        LeaseCounts {
            leased: self.counts.leased.load(Ordering::Relaxed),
            sealed: self.counts.sealed.load(Ordering::Relaxed),
            abandoned: self.counts.abandoned.load(Ordering::Relaxed),
        }
    }
}

/// Moves the end of the last lease in `region` from `end` to `new_end`, if it is the last.
///
/// # Safety
/// `home` must be a live `HeldRegions<R>`.
unsafe fn grow_in<R: WritableRegions>(
    home: *const (),
    region: usize,
    end: usize,
    new_end: usize,
) -> bool {
    // SAFETY: as the caller promises.
    let held = unsafe { &*home.cast::<HeldRegions<R>>() };
    let mut lending = held.lending.lock().unwrap_or_else(PoisonError::into_inner);
    let last = lending.region == Some(region) && lending.cursor == end;
    if !last || held.bases.get(region).is_none_or(|base| new_end > base.len) {
        return false;
    }
    lending.cursor = new_end;
    true
}

/// Counts a lease sealed or let go.
///
/// # Safety
/// `home` must be a live `HeldRegions<R>`.
unsafe fn settle_in<R: WritableRegions>(home: *const (), sealed: bool) {
    // SAFETY: as the caller promises.
    let held = unsafe { &*home.cast::<HeldRegions<R>>() };
    let count = if sealed {
        &held.counts.sealed
    } else {
        &held.counts.abandoned
    };
    count.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
mod tests;
