//! What is known of whether a device imports a dma-buf.
//!
//! A device says it can by its extensions, and that isn't enough: a driver may refuse every
//! dma-buf it didn't make, or one whose fd only allows reading. So the answer comes from trying.
//! A process that can open a dma-heap tries when the device opens. One that can't (the heaps are
//! root's on a board, and a consumer there isn't root) has nothing to try with, and its answer is
//! unknown until it is handed a real dma-buf, or its host says.

use std::sync::atomic::{AtomicU8, Ordering};

const UNKNOWN: u8 = 0;
const YES: u8 = 1;
const NO: u8 = 2;

/// The answer for each kind of fd: one that can be written through, and one that only allows
/// reading.
pub(crate) struct ImportState([AtomicU8; 2]);

const fn code(answer: Option<bool>) -> u8 {
    match answer {
        None => UNKNOWN,
        Some(true) => YES,
        Some(false) => NO,
    }
}

impl ImportState {
    pub(crate) const fn new(writable: Option<bool>, read_only: Option<bool>) -> ImportState {
        ImportState([
            AtomicU8::new(code(writable)),
            AtomicU8::new(code(read_only)),
        ])
    }

    /// Whether the device imports a dma-buf of this kind, or `None` while nothing has tried.
    pub(crate) fn get(&self, read_only: bool) -> Option<bool> {
        match self.0[usize::from(read_only)].load(Ordering::Acquire) {
            YES => Some(true),
            NO => Some(false),
            _ => None,
        }
    }

    /// Records what a try found, or what the host said.
    pub(crate) fn set(&self, read_only: bool, imports: bool) {
        self.0[usize::from(read_only)].store(code(Some(imports)), Ordering::Release);
    }

    /// Whether an import of this kind is worth planning: it works, or nothing has tried.
    #[cfg(any(target_os = "linux", test))]
    pub(crate) fn may(&self, read_only: bool) -> bool {
        self.get(read_only) != Some(false)
    }

    /// The two answers as one number, for a plan's key: a plan made while an answer was unknown
    /// is made again once it is known.
    pub(crate) fn stamp(&self) -> u8 {
        self.0[0].load(Ordering::Acquire) | (self.0[1].load(Ordering::Acquire) << 2)
    }
}

#[cfg(test)]
mod tests {
    use super::ImportState;

    #[test]
    fn an_answer_is_unknown_until_set_and_each_kind_has_its_own() {
        let state = ImportState::new(None, None);
        assert_eq!((state.get(false), state.get(true)), (None, None));
        assert!(
            state.may(false) && state.may(true),
            "unknown is worth a try"
        );
        let before = state.stamp();
        state.set(false, true);
        assert_eq!((state.get(false), state.get(true)), (Some(true), None));
        assert_ne!(
            state.stamp(),
            before,
            "the plan's key moves with the answer"
        );
        state.set(true, false);
        assert_eq!(state.get(true), Some(false));
        assert!(state.may(false) && !state.may(true));
        let known = ImportState::new(Some(false), Some(false));
        assert!(!known.may(false) && !known.may(true));
    }
}
