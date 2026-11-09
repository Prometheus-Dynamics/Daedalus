//! A type, a memory space and an owner.

use daedalus_abi::{Slot, SlotState};
use daedalus_types::{SpaceIndex, TypeIndex};

use crate::Owner;

#[derive(Debug)]
pub struct Value {
    pub ty: TypeIndex,
    pub space: SpaceIndex,
    pub owner: Owner,
}

impl Value {
    /// The bytes, for owners made of bytes.
    pub fn bytes(&self) -> Option<&[u8]> {
        match &self.owner {
            Owner::Static(bytes) => Some(bytes),
            Owner::Inline(inline) => Some(inline.as_slice()),
            #[cfg(feature = "alloc")]
            Owner::Shared(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// The Rust value, if it is a `T`. A value from a separately built copy of the same type has
    /// a different `TypeId`, so it is refused.
    #[cfg(feature = "std")]
    pub fn typed<T: 'static>(&self) -> Option<&T> {
        match &self.owner {
            Owner::Typed(value) => (**value).downcast_ref(),
            _ => None,
        }
    }

    /// A second holder of this value: an inline value is copied, an `Arc` is shared.
    pub fn clone_inline(&self) -> Value {
        Value {
            ty: self.ty,
            space: self.space,
            owner: self.owner.clone(),
        }
    }

    /// The raw description of this value, for code on the far side of the step interface. The
    /// pointer is valid for as long as this value stays where it is and isn't changed.
    pub fn raw(&self, version: u32) -> Slot {
        let (data, len) = match &self.owner {
            #[cfg(feature = "alloc")]
            Owner::Typed(value) => (alloc::sync::Arc::as_ptr(value).cast::<u8>().cast_mut(), 0),
            _ => self.bytes().map_or((core::ptr::null_mut(), 0), |b| {
                (b.as_ptr().cast_mut(), b.len())
            }),
        };
        Slot {
            state: SlotState::Full as u32,
            space: u32::from(self.space.0),
            type_index: self.ty.0,
            owner: self.owner.kind() as u32,
            version,
            reserved: 0,
            data,
            len,
            cap: len,
        }
    }
}
