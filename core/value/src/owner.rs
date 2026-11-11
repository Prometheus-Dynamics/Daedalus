//! Who holds a value's bytes.

use daedalus_abi::Handle;
use daedalus_types::SlotIndex;

use crate::InlineBytes;

#[derive(Clone, Debug)]
pub enum Owner {
    /// Bytes that live forever: a constant.
    Static(&'static [u8]),
    /// Bytes in the value itself.
    Inline(InlineBytes<16>),
    /// Another slot holds it.
    Slot(SlotIndex),
    /// A host-owned object. The handle is data here, and releasing it is the host's job.
    Host(Handle),
    #[cfg(feature = "alloc")]
    Shared(alloc::sync::Arc<[u8]>),
    #[cfg(feature = "alloc")]
    Typed(alloc::sync::Arc<dyn core::any::Any + Send + Sync>),
    /// Bytes a plugin handed over, held by one count. No allocation to hold or to take.
    Handed(crate::SharedBytes),
}

/// The `owner` field of a raw [`daedalus_abi::Slot`].
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerKind {
    Static = 0,
    Inline = 1,
    Slot = 2,
    Host = 3,
    Shared = 4,
    Typed = 5,
    /// Bytes a plugin handed over: one count on a `HandedRecord`.
    Handed = 6,
}

impl Owner {
    pub const fn kind(&self) -> OwnerKind {
        match self {
            Owner::Static(_) => OwnerKind::Static,
            Owner::Inline(_) => OwnerKind::Inline,
            Owner::Slot(_) => OwnerKind::Slot,
            Owner::Host(_) => OwnerKind::Host,
            #[cfg(feature = "alloc")]
            Owner::Shared(_) => OwnerKind::Shared,
            #[cfg(feature = "alloc")]
            Owner::Typed(_) => OwnerKind::Typed,
            Owner::Handed(_) => OwnerKind::Handed,
        }
    }
}
