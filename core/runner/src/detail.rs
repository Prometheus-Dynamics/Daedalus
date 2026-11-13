//! Somewhere for steps to write the longer account of a failure, for a host that has a heap.
//! Each step has its own place, bounded, and only the thread running a step writes to it, so
//! its lock is never waited on. The host reads between ticks.

#![cfg(feature = "alloc")]

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::ffi::c_void;
use core::sync::atomic::{AtomicBool, Ordering};

use daedalus_abi::DetailSink;

use crate::Runner;

/// What one step has written since the host last looked.
#[derive(Default)]
struct Pending {
    text: String,
    cut: bool,
}

/// Where the steps of one loaded plan write what they give [`StepCx::detail`].
///
/// [`StepCx::detail`]: daedalus_abi::StepCx::detail
pub struct DetailStore {
    cap: usize,
    /// Whether any step has written since the host last asked.
    any: AtomicBool,
    steps: Vec<spin::Mutex<Pending>>,
}

impl DetailStore {
    /// A store for `steps` steps, keeping at most `cap` bytes for each.
    pub fn new(steps: usize, cap: usize) -> Arc<DetailStore> {
        Arc::new(DetailStore {
            cap,
            any: AtomicBool::new(false),
            steps: (0..steps).map(|_| spin::Mutex::default()).collect(),
        })
    }

    /// Whether any step has written since this was last asked.
    pub fn written(&self) -> bool {
        self.any.load(Ordering::Acquire) && self.any.swap(false, Ordering::AcqRel)
    }

    /// Takes what `step` has written, and whether it was more than the store keeps. `None` when
    /// it wrote nothing.
    pub fn take(&self, step: usize) -> Option<(String, bool)> {
        let mut pending = self.steps.get(step)?.lock();
        if pending.text.is_empty() {
            return None;
        }
        let cut = core::mem::take(&mut pending.cut);
        Some((core::mem::take(&mut pending.text), cut))
    }

    /// Drops what every step has written, keeping the memory for the next time.
    pub fn clear(&self) {
        for cell in &self.steps {
            let mut pending = cell.lock();
            pending.text.clear();
            pending.cut = false;
        }
    }

    fn write(&self, step: u32, bytes: &[u8]) {
        let Some(cell) = self.steps.get(step as usize) else {
            return;
        };
        let piece = String::from_utf8_lossy(bytes);
        let mut pending = cell.lock();
        let room = self.cap.saturating_sub(pending.text.len());
        let mut end = piece.len().min(room);
        while !piece.is_char_boundary(end) {
            end -= 1;
        }
        pending.text.push_str(&piece[..end]);
        pending.cut |= end < piece.len();
        self.any.store(true, Ordering::Release);
    }
}

extern "C" fn write(context: *mut c_void, step: u32, text: *const u8, len: usize) {
    if context.is_null() || text.is_null() || len == 0 {
        return;
    }
    // SAFETY: `context` is the store `keep_details` gave the runner, which the runner holds for
    // as long as it hands this function out. `text` is `len` bytes the caller holds for this
    // call.
    let (store, bytes) = unsafe {
        (
            &*context.cast::<DetailStore>(),
            core::slice::from_raw_parts(text, len),
        )
    };
    store.write(step, bytes);
}

impl Runner<'_> {
    /// Has every step this runner calls write its detail into `store`, or nowhere with `None`.
    /// The runner holds the store, so it is there for as long as a step can write to it.
    pub fn keep_details(&mut self, store: Option<Arc<DetailStore>>) {
        let sink = match &store {
            Some(store) => DetailSink {
                context: Arc::as_ptr(store).cast_mut().cast(),
                write: Some(write),
            },
            None => DetailSink::NONE,
        };
        // SAFETY: the store lives in `self.details` until the runner is dropped or the next
        // call here, which sets another sink first. `write` locks per step, so any thread may
        // call it.
        unsafe { self.set_detail(sink) };
        self.details = store;
    }
}
