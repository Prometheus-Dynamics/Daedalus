//! A shared `u32` that works on every target.

#[cfg(target_has_atomic = "32")]
mod imp {
    use core::sync::atomic::{AtomicU32, Ordering};

    #[repr(transparent)]
    pub struct Word(AtomicU32);

    impl Word {
        pub const fn new(value: u32) -> Word {
            Word(AtomicU32::new(value))
        }

        #[inline]
        pub fn get(&self) -> u32 {
            self.0.load(Ordering::Relaxed)
        }

        #[inline]
        pub fn set(&self, value: u32) {
            self.0.store(value, Ordering::Relaxed);
        }

        /// Sets the word to `new` if it holds `current`, and says whether it did.
        #[inline]
        pub fn replace_if(&self, current: u32, new: u32) -> bool {
            self.0
                .compare_exchange(current, new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        }
    }
}

// Chips with no atomic operations are single-core, so a volatile word is enough.
#[cfg(not(target_has_atomic = "32"))]
mod imp {
    use core::cell::UnsafeCell;

    #[repr(transparent)]
    pub struct Word(UnsafeCell<u32>);

    // SAFETY: targets without 32-bit atomics are single-core, and every access is one volatile
    // read or write of an aligned word, so no access can tear or be observed partway.
    unsafe impl Sync for Word {}

    impl Word {
        pub const fn new(value: u32) -> Word {
            Word(UnsafeCell::new(value))
        }

        #[inline]
        pub fn get(&self) -> u32 {
            // SAFETY: the pointer comes from a live `&self`, is aligned, and is valid for reads.
            unsafe { self.0.get().read_volatile() }
        }

        #[inline]
        pub fn set(&self, value: u32) {
            // SAFETY: the pointer comes from a live `&self`, is aligned, and is valid for writes.
            unsafe { self.0.get().write_volatile(value) }
        }

        /// Sets the word to `new` if it holds `current`, and says whether it did.
        #[inline]
        pub fn replace_if(&self, current: u32, new: u32) -> bool {
            let held = self.get() == current;
            if held {
                self.set(new);
            }
            held
        }
    }
}

/// One `u32` shared between a host and a running step: a cancel word or a progress cell.
pub use imp::Word;
