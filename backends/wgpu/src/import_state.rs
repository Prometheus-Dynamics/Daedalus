//! What is known of whether a device imports a dma-buf.
//!
//! A device says it can by its extensions, and that isn't enough: a driver may refuse every
//! dma-buf it didn't make, or one whose fd only allows reading. So the answer comes from trying.
//! A process that can open a dma-heap tries when the device opens. One that can't (the heaps are
//! root's on a board, and a consumer there isn't root) has nothing to try with, and its answer is
//! unknown until it is handed a real dma-buf, or its host says.

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};

/// Why a device that opened couldn't be tried with a dma-buf of this process's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoTrial {
    /// There is no dma-heap here: the kernel has none, or this process isn't shown it.
    HeapMissing,
    /// The dma-heap is there and this user may not open it. It is usually root's.
    HeapDenied,
    /// The dma-heap couldn't be opened for another reason, with this `errno`.
    HeapError(i32),
    /// The host asked that nothing be tried ([`Options::no_heap_access`](crate::Options)).
    NotAsked,
}

/// Why a device copies a dma-buf's bytes when it could have had the buffer in place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhyCopied {
    /// The device has no way to import a dma-buf: its driver lacks the extensions, or this
    /// isn't Linux.
    DeviceCant,
    /// The device was tried with one, or the host said, and it doesn't take this kind.
    Refused,
    /// The host turned imports off ([`Options::no_dma_heap`](crate::Options)).
    TurnedOff,
}

/// What a device does with a value that is a dma-buf, and why. A host prints it to say what to
/// expect, and what a person could change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DmaBufStatus {
    /// No device is open yet.
    NotOpen,
    /// The device imports the buffer in place. Nothing is copied.
    Imports,
    /// The value's bytes are copied to the device.
    Copies(WhyCopied),
    /// Nothing has tried yet. The first dma-buf this process is handed decides.
    Undecided(NoTrial),
}

impl fmt::Display for DmaBufStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DmaBufStatus::NotOpen => f.write_str("no device is open yet, so nothing is known"),
            DmaBufStatus::Imports => {
                f.write_str("the device imports a dma-buf in place: nothing is copied")
            }
            DmaBufStatus::Copies(why) => {
                f.write_str("a dma-buf's bytes are copied to the device, because ")?;
                f.write_str(match why {
                    WhyCopied::DeviceCant => {
                        "the device has no dma-buf import. Nothing here can change that: \
                         another driver or device is what would"
                    }
                    WhyCopied::Refused => {
                        "the device refused one of this kind when it was tried, or the host \
                         said it would"
                    }
                    WhyCopied::TurnedOff => "the host turned dma-buf imports off",
                })
            }
            DmaBufStatus::Undecided(why) => {
                f.write_str("not known yet, because ")?;
                f.write_str(match why {
                    NoTrial::HeapMissing => {
                        "there is no dma-heap under /dev/dma_heap for this process to try \
                         the device with"
                    }
                    NoTrial::HeapDenied => {
                        "this user may not open the dma-heap under /dev/dma_heap, which is \
                         usually root's, so the device couldn't be tried"
                    }
                    NoTrial::HeapError(_) => {
                        "the dma-heap under /dev/dma_heap couldn't be opened, so the device \
                         couldn't be tried"
                    }
                    NoTrial::NotAsked => "the host asked that nothing be tried",
                })?;
                f.write_str(
                    ". That is normal, and nothing fails: the first dma-buf this process is \
                     handed decides. If the device takes it, it and every later one are used \
                     in place. If not, that value and every later one are copied. A host that \
                     knows the answer, or has a buffer to try with, can settle it first",
                )
            }
        }
    }
}

const UNKNOWN: u8 = 0;
const YES: u8 = 1;
const NO: u8 = 2;

/// What happened when the device opened, for [`DmaBufStatus`] to say why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// Off Linux no device is ever tried: the answer is always that it copies.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) enum Untried {
    /// The device was tried with a buffer of this process's own.
    Tried,
    /// Nothing was tried, and the answer was left open.
    NoTrial(NoTrial),
    /// Nothing was tried, since the answer was already no.
    Copies(WhyCopied),
}

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
