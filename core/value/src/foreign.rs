//! Values owned by code outside this build. The mechanism is in `daedalus-abi`, which may use
//! `unsafe`. This adds holding one in a [`Value`], and names no kind of value.

#![cfg(feature = "std")]

use alloc::sync::Arc;

pub use daedalus_abi::{
    ForeignHandle, ForeignInterface, ForeignInterfaceInfo, ForeignInterfaceMismatch, ForeignOwner,
    ForeignRef, ProvideForeign, foreign_layout_hash,
};
use daedalus_types::{SpaceIndex, TypeIndex};

use crate::{Owner, Value};

impl Value {
    /// A value owned through `handle`. Cloning the value shares it, and the last holder to let go
    /// releases the handle, once.
    pub fn foreign(ty: TypeIndex, space: SpaceIndex, handle: ForeignHandle) -> Value {
        Value {
            ty,
            space,
            owner: Owner::Typed(Arc::new(handle)),
        }
    }

    /// The handle, if this value is owned that way.
    pub fn foreign_handle(&self) -> Option<&ForeignHandle> {
        self.typed()
    }
}
