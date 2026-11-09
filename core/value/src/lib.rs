//! A value is a type, a memory space and an owner. The core stores and passes these and reads
//! nothing else, so a value's meaning belongs to whoever wrote the nodes.
//!
//! Handing a value over allocates nothing: the owner sits in the slot, and a value with one
//! consumer moves without touching a reference count.

#![no_std]
#![forbid(unsafe_code)]

#[cfg(feature = "alloc")]
extern crate alloc;

mod access;
mod inline;
mod owner;
mod pool;
#[cfg(feature = "alloc")]
mod recycle;
mod ring;
mod value;

pub use access::Access;
pub use inline::InlineBytes;
pub use owner::{Owner, OwnerKind};
pub use pool::FixedPool;
#[cfg(feature = "alloc")]
pub use recycle::{PayloadSlot, RecyclePool, Reusable};
pub use ring::{Push, Ring};
pub use value::Value;
