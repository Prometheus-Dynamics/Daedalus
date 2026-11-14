//! Descriptors sent once per buffer.
//!
//! A value by descriptor (a dma-buf) goes to a worker as the descriptor itself, the first time its
//! buffer is seen, and by a slot number after that. Both sides key the buffer by its identity, the
//! device and inode behind the descriptor, so a buffer that comes back as another descriptor is
//! still found.
//!
//! The host keeps a duplicate of each descriptor it has sent, so the buffer can't be freed and its
//! identity given to another buffer while the worker still has it under that slot. At most
//! [`SLOTS`] buffers are kept: when a new one comes and all are taken, the one used longest ago is
//! let go, and the worker is told to let go of it in the same note. The worker maps each buffer
//! once, read-only, when it arrives, and unmaps it when told to forget it.

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

use daedalus_dma_sys::{Access, DmaBuf, Identity, Lent};

use crate::Brackets;
use crate::linux::ShmError;

/// The most buffers a worker keeps by descriptor, and the host keeps open for it. One frame can
/// carry this many descriptors, so a run can bring them all at once.
pub const SLOTS: usize = shm_fdpass::MAX_FDS;

struct Kept {
    id: Identity,
    fd: OwnedFd,
    used: u64,
}

/// The host's side: which buffer each slot holds.
#[derive(Default)]
pub(crate) struct FdCache {
    slots: Vec<Option<Kept>>,
    tick: u64,
    /// The slots of descriptors to send with the next note, and to have the worker forget.
    pub(crate) attach: Vec<u32>,
    pub(crate) forget: Vec<u32>,
}

impl FdCache {
    pub(crate) fn new() -> FdCache {
        FdCache {
            slots: (0..SLOTS).map(|_| None).collect(),
            tick: 0,
            attach: Vec::with_capacity(SLOTS),
            forget: Vec::with_capacity(SLOTS),
        }
    }

    /// Starts a run's note: nothing to attach or forget yet.
    pub(crate) fn begin(&mut self) {
        self.attach.clear();
        self.forget.clear();
    }

    /// The slot of the buffer behind `fd`, adding it to the note's attachments the first time.
    pub(crate) fn slot(&mut self, fd: BorrowedFd<'_>) -> Result<u32, ShmError> {
        let id = Identity::of(fd).map_err(|error| ShmError(format!("fstat: {error:?}")))?;
        self.tick += 1;
        if let Some(at) = self
            .slots
            .iter()
            .position(|kept| kept.as_ref().is_some_and(|kept| kept.id == id))
        {
            if let Some(kept) = self.slots[at].as_mut() {
                kept.used = self.tick;
            }
            return Ok(at as u32);
        }
        // A buffer this run already attached can't be let go for another in the same note.
        let at = match self.slots.iter().position(Option::is_none) {
            Some(at) => at,
            None => {
                let oldest = self
                    .slots
                    .iter()
                    .enumerate()
                    .filter(|(at, _)| !self.attach.contains(&(*at as u32)))
                    .min_by_key(|(_, kept)| kept.as_ref().map_or(0, |kept| kept.used))
                    .map(|(at, _)| at)
                    .ok_or_else(|| ShmError(format!("a run brings more than {SLOTS} buffers")))?;
                self.forget.push(oldest as u32);
                oldest
            }
        };
        let fd = fd
            .try_clone_to_owned()
            .map_err(|error| ShmError(format!("dup: {error}")))?;
        self.slots[at] = Some(Kept {
            id,
            fd,
            used: self.tick,
        });
        self.attach.push(at as u32);
        Ok(at as u32)
    }

    /// The descriptors to attach to the note, in the order of `attach`.
    pub(crate) fn attached(&self) -> impl Iterator<Item = BorrowedFd<'_>> {
        self.attach.iter().filter_map(|at| {
            self.slots[*at as usize]
                .as_ref()
                .map(|kept| kept.fd.as_fd())
        })
    }

    /// How many buffers are kept.
    pub(crate) fn kept(&self) -> usize {
        self.slots.iter().filter(|kept| kept.is_some()).count()
    }
}

/// One buffer a worker mapped.
pub(crate) struct Mapped {
    buf: DmaBuf,
    bytes: Lent,
    /// Whether the buffer takes the kernel's CPU access brackets. A dma-buf does; a memfd, which
    /// has no device behind it, refuses them and needs none.
    bracketed: bool,
}

/// The worker's side: what each slot maps.
#[derive(Default)]
pub(crate) struct FdTable {
    slots: Vec<Option<Mapped>>,
    pub(crate) brackets: Brackets,
}

impl FdTable {
    pub(crate) fn new() -> FdTable {
        FdTable {
            slots: (0..SLOTS).map(|_| None).collect(),
            brackets: Brackets::default(),
        }
    }

    /// Lets go of the buffer in `slot`.
    pub(crate) fn forget(&mut self, slot: u32) {
        if let Some(kept) = self.slots.get_mut(slot as usize) {
            *kept = None;
        }
    }

    /// Maps the buffer `fd` as `slot`, read-only.
    pub(crate) fn attach(&mut self, slot: u32, fd: OwnedFd) -> Result<(), ShmError> {
        let place = self
            .slots
            .get_mut(slot as usize)
            .ok_or_else(|| ShmError(format!("slot {slot} is past the last")))?;
        let buf = DmaBuf::from_fd(fd);
        let mapping = buf
            .as_ref()
            .map_read_only()
            .map_err(|error| ShmError(format!("mapping a buffer: {error:?}")))?;
        let bytes = std::sync::Arc::new(mapping).lend();
        let bracketed = match buf.as_ref().begin_cpu_access(Access::Read) {
            Ok(()) => {
                let _ = buf.as_ref().end_cpu_access(Access::Read);
                true
            }
            Err(error) if error.is_unsupported() => false,
            Err(error) => return Err(ShmError(format!("a buffer's sync call: {error:?}"))),
        };
        self.brackets.attached += 1;
        *place = Some(Mapped {
            buf,
            bytes,
            bracketed,
        });
        Ok(())
    }

    /// The bytes `offset .. offset + len` of the buffer in `slot`.
    pub(crate) fn bytes(&self, slot: u32, offset: u64, len: u64) -> Option<&[u8]> {
        let mapped = self.slots.get(slot as usize)?.as_ref()?;
        let start = usize::try_from(offset).ok()?;
        let end = start.checked_add(usize::try_from(len).ok()?)?;
        mapped.bytes.get(start..end)
    }

    /// Begins the host's reads of the buffer in `slot`, before a node runs.
    pub(crate) fn begin(&mut self, slot: u32) -> Result<(), ShmError> {
        let Some(mapped) = self.slots.get(slot as usize).and_then(Option::as_ref) else {
            return Err(ShmError(format!("no buffer in slot {slot}")));
        };
        if !mapped.bracketed {
            self.brackets.unbracketed += 1;
            return Ok(());
        }
        mapped
            .buf
            .as_ref()
            .begin_cpu_access(Access::Read)
            .map_err(|error| ShmError(format!("beginning a read: {error:?}")))?;
        self.brackets.begun += 1;
        Ok(())
    }

    /// Ends the reads [`FdTable::begin`] began, after the node ran.
    pub(crate) fn end(&mut self, slot: u32) {
        let Some(mapped) = self.slots.get(slot as usize).and_then(Option::as_ref) else {
            return;
        };
        if mapped.bracketed && mapped.buf.as_ref().end_cpu_access(Access::Read).is_ok() {
            self.brackets.ended += 1;
        }
    }
}
