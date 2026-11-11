//! Plugin bytes as the host holds them. The bytes are the plugin's own, held by one count on a
//! [`HandedRecord`]: the record comes from a pool made when the plugin's backend loads, so a tick
//! that hands bytes over allocates nothing. When the pool is empty, the host copies the bytes into
//! a record of its own, and that copy is counted.
//!
//! The record keeps the plugin's generation open for as long as it lives. Its last release calls
//! the plugin's release, returns the record to its pool (or frees it), and drops the generation.

use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use daedalus_abi::{BytesC, HandedRecord, NO_BYTES, SharedBytes};

use crate::generation::Generation;

/// Bytes the host has copied, in total: the copies it made of plugin bytes when a pool was empty,
/// and the copies `Plugin::share` makes.
static COPIED: AtomicUsize = AtomicUsize::new(0);
/// Records released in total: one for each hand-over whose last count went, whatever the origin.
static RELEASED: AtomicUsize = AtomicUsize::new(0);

/// How many bytes the host has copied from plugins, in total.
pub fn bytes_copied() -> usize {
    COPIED.load(Ordering::Relaxed)
}

/// How many hand-over records have been released, in total. A plugin's release runs once for each.
pub fn bytes_released() -> usize {
    RELEASED.load(Ordering::Relaxed)
}

/// What the records of one plugin backend share: its generation, and the pool of records.
pub(crate) struct Holder {
    /// Held so the library stays open while any record of this holder lives.
    _generation: Arc<Generation>,
    /// Records not in use, ready for the next hand-over.
    free: Mutex<Vec<NonNull<HandedRecord>>>,
    /// Every pooled record, freed with the holder. A record keeps the holder alive, so none is in
    /// use when it drops.
    pooled: Vec<NonNull<HandedRecord>>,
}

// SAFETY: the records are only touched through the free list's lock, or by the one holder of a
// count, and the bytes they point at are immutable.
unsafe impl Send for Holder {}
// SAFETY: as above.
unsafe impl Sync for Holder {}

impl Holder {
    /// A holder for `generation` with `slots` records in its pool, made now.
    pub(crate) fn new(generation: Arc<Generation>, slots: usize) -> Arc<Holder> {
        let mut pooled = Vec::with_capacity(slots);
        for _ in 0..slots {
            let record = Box::new(HandedRecord {
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
            });
            pooled.push(NonNull::from(Box::leak(record)));
        }
        Arc::new(Holder {
            _generation: generation,
            free: Mutex::new(pooled.clone()),
            pooled,
        })
    }

    /// A record holding `bytes` with one count, from the pool, or `None` when the pool is empty.
    pub(crate) fn take(self: &Arc<Holder>, bytes: BytesC) -> Option<NonNull<HandedRecord>> {
        let record = self.free.lock().unwrap_or_else(|e| e.into_inner()).pop()?;
        self.fill(
            record,
            bytes,
            Some(Arc::as_ptr(self) as *const ()),
            recycle_pooled,
        );
        Some(record)
    }

    /// A record holding a copy of `bytes`, made now, with one count. Counted as a copy.
    pub(crate) fn copy(self: &Arc<Holder>, bytes: &[u8]) -> NonNull<HandedRecord> {
        COPIED.fetch_add(bytes.len(), Ordering::Relaxed);
        let copy: Box<[u8]> = bytes.into();
        let ptr = copy.as_ptr();
        let len = copy.len();
        let owner = Box::into_raw(Box::new(copy)).cast::<u8>();
        let record = Box::new(HandedRecord {
            refs: AtomicUsize::new(0),
            bytes: BytesC {
                ptr,
                len,
                data: owner.cast(),
                release: Some(free_copy),
            },
            keep: std::ptr::null(),
            keep_drop: None,
            recycle: None,
            home: std::ptr::null(),
        });
        let record = NonNull::from(Box::leak(record));
        self.fill(record, NO_BYTES, None, free_record);
        record
    }

    /// A record holding the plugin's own bytes, with one count, and no copy. Used when the pool has
    /// no record to spare; the record is freed rather than pooled.
    pub(crate) fn wrap(self: &Arc<Holder>, bytes: BytesC) -> NonNull<HandedRecord> {
        let record = Box::new(HandedRecord {
            refs: AtomicUsize::new(0),
            bytes,
            keep: std::ptr::null(),
            keep_drop: None,
            recycle: None,
            home: std::ptr::null(),
        });
        let record = NonNull::from(Box::leak(record));
        self.fill(record, NO_BYTES, None, free_record);
        record
    }

    /// Sets a record's fields for one hand-over: its bytes, one count, and what it keeps.
    fn fill(
        self: &Arc<Holder>,
        record: NonNull<HandedRecord>,
        bytes: BytesC,
        home: Option<*const ()>,
        recycle: unsafe extern "C" fn(*mut HandedRecord),
    ) {
        // SAFETY: the record is out of the pool, or was just made, so nothing else touches it.
        let fields = unsafe { &mut *record.as_ptr() };
        if home.is_some() {
            fields.bytes = bytes;
        }
        fields.refs.store(1, Ordering::Relaxed);
        fields.keep = Arc::into_raw(Arc::clone(self)).cast();
        fields.keep_drop = Some(drop_holder);
        fields.recycle = Some(recycle);
        fields.home = home.unwrap_or(std::ptr::null());
    }

    /// The handle a record's one count makes.
    pub(crate) fn handle(record: NonNull<HandedRecord>) -> SharedBytes {
        // SAFETY: the record was filled with one count for the caller, which moves into the result.
        unsafe { SharedBytes::from_record(record) }
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        for record in self.pooled.drain(..) {
            // SAFETY: each pooled record came from `Box::into_raw` in `new`, and no value holds it:
            // every value keeps the holder alive, so the holder is dropped only when none is left.
            drop(unsafe { Box::from_raw(record.as_ptr()) });
        }
    }
}

/// Takes the holder's count back for a pooled record, and returns it to the free list.
///
/// # Safety
/// `record` must be a pooled record of the holder `home` points at, released once, with nothing
/// else holding it.
unsafe extern "C" fn recycle_pooled(record: *mut HandedRecord) {
    // SAFETY: `home` is the holder, which the record's `keep` holds alive until after this call.
    let home = unsafe { &*(*record).home.cast::<Holder>() };
    let record = NonNull::new(record).expect("a record is never null");
    home.free
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(record);
}

/// Frees a record a host copy made, after its bytes were freed.
///
/// # Safety
/// `record` must be a record `Holder::copy` made, released once.
unsafe extern "C" fn free_record(record: *mut HandedRecord) {
    // SAFETY: as the caller promises, `record` is a `Box` from `Holder::copy`.
    drop(unsafe { Box::from_raw(record) });
}

/// Frees the bytes of a host copy.
///
/// # Safety
/// `data` must be the box `Holder::copy` made for these bytes, freed once.
unsafe extern "C" fn free_copy(data: *mut ()) {
    // SAFETY: as the caller promises, `data` is the `Box<Box<[u8]>>` `Holder::copy` leaked.
    drop(unsafe { Box::from_raw(data.cast::<Box<[u8]>>()) });
}

/// Drops the keep of a record: its holder, and so its generation, once no record holds them.
///
/// # Safety
/// `keep` must be an `Arc<Holder>` from `Arc::into_raw`, dropped once.
unsafe extern "C" fn drop_holder(keep: *const ()) {
    RELEASED.fetch_add(1, Ordering::Relaxed);
    // SAFETY: as the caller promises, `keep` came from `Arc::into_raw` on a holder.
    drop(unsafe { Arc::from_raw(keep.cast::<Holder>()) });
}
