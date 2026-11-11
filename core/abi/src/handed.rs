//! Bytes a plugin handed over, held by reference count. A cell that holds them has owner
//! `OwnerKind::Handed`: its `data` and `len` are the bytes, and its `cap` is the address of the
//! [`HandedRecord`] that holds one count on them. The last release calls the plugin's release.

use core::sync::atomic::AtomicUsize;
#[cfg(target_has_atomic = "ptr")]
use core::sync::atomic::{Ordering, fence};

use core::fmt;
use core::ptr::NonNull;

use crate::BytesC;

/// The cell owner code of a hand-over: `OwnerKind::Handed` in daedalus-value. A test there keeps
/// the two equal.
pub const OWNER_HANDED: u32 = 6;

/// Set in `reserved` by a plugin on an output cell that is a hand-over the loader has not yet
/// turned into a record. The loader reads the bytes from `StepCx::handover[cap]`, then clears the
/// flag by writing the cell as a record.
pub const HANDOVER_PENDING: u32 = 1;

/// One hand-over, counted. `refs` is how many cells and values hold it. The bytes stay valid until
/// the last release, which calls `bytes.release` once, then `recycle`, then `keep_drop`.
#[repr(C)]
pub struct HandedRecord {
    pub refs: AtomicUsize,
    /// The plugin's bytes and its release.
    pub bytes: BytesC,
    /// What the record keeps alive for as long as it lives: the plugin's generation, for example.
    /// Dropped by `keep_drop`, after the bytes are released.
    pub keep: *const (),
    pub keep_drop: Option<unsafe extern "C" fn(keep: *const ())>,
    /// Returns the record to the place it came from, or frees it. Called once, last but `keep_drop`.
    pub recycle: Option<unsafe extern "C" fn(record: *mut HandedRecord)>,
    /// The place the record came from, for `recycle` to use. Opaque to everything else.
    pub home: *const (),
}

/// Takes one more count on `record`.
///
/// # Safety
/// `record` must be live: at least one count is held on it.
#[cfg(target_has_atomic = "ptr")]
pub unsafe fn handed_retain(record: *const HandedRecord) {
    // SAFETY: as the caller promises, `record` is live, so its counter is valid.
    unsafe { (*record).refs.fetch_add(1, Ordering::Relaxed) };
}

/// Takes one more count on `record`. A target without atomic counts has no hand-over: no plugin
/// loads there, so no record exists to count.
///
/// # Safety
/// `record` must be live: at least one count is held on it.
#[cfg(not(target_has_atomic = "ptr"))]
pub unsafe fn handed_retain(_record: *const HandedRecord) {
    panic!("a hand-over needs atomic counts, which this target does not have");
}

/// Gives back one count on `record`. The last one releases the bytes, recycles the record, and
/// drops what it kept.
///
/// # Safety
/// `record` must be live, and the caller must own the count it gives back.
#[cfg(target_has_atomic = "ptr")]
pub unsafe fn handed_release(record: *mut HandedRecord) {
    // SAFETY: as the caller promises, `record` is live.
    if unsafe { (*record).refs.fetch_sub(1, Ordering::Release) } != 1 {
        return;
    }
    fence(Ordering::Acquire);
    // SAFETY: this was the last count, so nothing else reads or writes the record now, and the
    // fields it reads were set before the record was shared.
    let fields = unsafe { &*record };
    if let Some(release) = fields.bytes.release {
        // SAFETY: the plugin handed `data` out with this release, and this is its one call.
        unsafe { release(fields.bytes.data) };
    }
    if let Some(recycle) = fields.recycle {
        // SAFETY: the record is no longer shared, and `recycle` takes it over.
        unsafe { recycle(record) };
    }
    if let Some(keep_drop) = fields.keep_drop {
        // SAFETY: `keep` came from the same record, and this is its one drop.
        unsafe { keep_drop(fields.keep) };
    }
}

/// Gives back one count on `record`. A target without atomic counts has no hand-over, as
/// `handed_retain` says.
///
/// # Safety
/// `record` must be live, and the caller must own the count it gives back.
#[cfg(not(target_has_atomic = "ptr"))]
pub unsafe fn handed_release(_record: *mut HandedRecord) {
    panic!("a hand-over needs atomic counts, which this target does not have");
}

/// One count on a hand-over. Clone and drop take and give back counts; moving it moves the count.
/// It reads as a byte slice.
pub struct SharedBytes {
    record: NonNull<HandedRecord>,
}

// SAFETY: the bytes are immutable, and the record's count is atomic. Any thread may read the bytes,
// and any thread may give back a count, as the plugin's contract allows.
unsafe impl Send for SharedBytes {}
// SAFETY: as above: shared reads of immutable bytes are safe from any thread.
unsafe impl Sync for SharedBytes {}

impl SharedBytes {
    /// Takes over one count on `record`.
    ///
    /// # Safety
    /// `record` must be live, and the count the caller holds on it moves into the result.
    pub unsafe fn from_record(record: NonNull<HandedRecord>) -> SharedBytes {
        SharedBytes { record }
    }

    /// Gives the count up, as the raw record. The caller then owns it.
    pub fn into_record(self) -> NonNull<HandedRecord> {
        let record = self.record;
        core::mem::forget(self);
        record
    }

    /// The plugin's bytes.
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: this value holds a count, so the record is live, and its bytes are valid.
        let fields = unsafe { self.record.as_ref() };
        if fields.bytes.len == 0 {
            return &[];
        }
        // SAFETY: the plugin's bytes are `len` valid bytes until the last release, and this count
        // keeps that release from running.
        unsafe { core::slice::from_raw_parts(fields.bytes.ptr, fields.bytes.len) }
    }
}

impl Clone for SharedBytes {
    fn clone(&self) -> SharedBytes {
        // SAFETY: this value holds a count, so the record is live.
        unsafe { handed_retain(self.record.as_ptr()) };
        SharedBytes {
            record: self.record,
        }
    }
}

impl Drop for SharedBytes {
    fn drop(&mut self) {
        // SAFETY: this value owns one count, and gives it back once.
        unsafe { handed_release(self.record.as_ptr()) };
    }
}

impl core::ops::Deref for SharedBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

impl fmt::Debug for SharedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedBytes")
            .field("len", &self.bytes().len())
            .finish()
    }
}

#[cfg(feature = "alloc")]
impl SharedBytes {
    /// Bytes a host made itself, as a handle with one count. The vector is freed with the last count.
    pub fn from_vec(bytes: alloc::vec::Vec<u8>) -> SharedBytes {
        let len = bytes.len();
        let owner: &'static mut alloc::vec::Vec<u8> =
            alloc::boxed::Box::leak(alloc::boxed::Box::new(bytes));
        let ptr = owner.as_ptr();
        let record: &'static mut HandedRecord =
            alloc::boxed::Box::leak(alloc::boxed::Box::new(HandedRecord {
                refs: AtomicUsize::new(1),
                bytes: BytesC {
                    ptr,
                    len,
                    data: core::ptr::from_mut(owner).cast(),
                    release: Some(free_vec),
                },
                keep: core::ptr::null(),
                keep_drop: None,
                recycle: Some(free_record),
                home: core::ptr::null(),
            }));
        // SAFETY: the record is new, and its one count moves into the handle.
        unsafe { SharedBytes::from_record(NonNull::from(record)) }
    }
}

/// Frees the vector `SharedBytes::from_vec` leaked.
///
/// # Safety
/// `data` must be the vector `from_vec` leaked, freed once.
#[cfg(feature = "alloc")]
unsafe extern "C" fn free_vec(data: *mut ()) {
    // SAFETY: as the caller promises, `data` is that leaked vector.
    drop(unsafe { alloc::boxed::Box::from_raw(data.cast::<alloc::vec::Vec<u8>>()) });
}

/// Frees the record `SharedBytes::from_vec` leaked, once its last count is gone.
///
/// # Safety
/// `record` must be the record `from_vec` leaked, freed once.
#[cfg(feature = "alloc")]
unsafe extern "C" fn free_record(record: *mut HandedRecord) {
    // SAFETY: as the caller promises, `record` is that leaked record.
    drop(unsafe { alloc::boxed::Box::from_raw(record) });
}
