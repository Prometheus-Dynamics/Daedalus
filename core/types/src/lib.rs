//! Type keys, type expressions, ids and errors. Daedalus compares these and reads nothing else.
//!
//! Everything is buildable in a `const` and needs no heap. The `alloc` feature adds keys whose
//! text arrives at run time.

#![no_std]
#![forbid(unsafe_code)]

#[cfg(feature = "alloc")]
extern crate alloc;

mod error;
mod expr;
mod hash;
mod id;
mod key;
mod literal;
mod scalar;
mod typed;

pub use error::{Code, Error};
pub use expr::{Field, TypeExpr, Variant};
pub use hash::{fnv1a64, stable_id128};
pub use id::{
    BackendIndex, EdgeIndex, NodeId, NodeIndex, PortIndex, PortName, SlotIndex, SpaceIndex,
    TypeIndex,
};
pub use key::TypeKey;
pub use literal::{FromLiteral, Literal};
pub use scalar::Scalar;
pub use typed::Typed;
