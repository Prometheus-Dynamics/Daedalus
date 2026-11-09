//! The id is the node. A node's definition has two parts: what the graph sees, and how it runs.
//! Each `#[node]` emits one constant record holding both, and the registry joins records that
//! share an id. The records need no heap, so they can live in statics on any target. The registry
//! itself is built when a host starts.

#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

#[cfg(feature = "std")]
mod error;
#[cfg(feature = "std")]
mod offer;
#[cfg(feature = "std")]
mod owned;
mod record;
#[cfg(feature = "std")]
mod registry;
#[cfg(feature = "std")]
mod types;

#[cfg(feature = "std")]
pub use error::RegError;
#[cfg(feature = "std")]
pub use offer::OfferData;
#[cfg(feature = "std")]
pub use owned::{OwnedBody, OwnedDecl, OwnedPort};
pub use record::{
    BodyRecord, ConfigDecl, Conversion, FanInDecl, FieldRule, Fire, NoCode, NodeDecl, NodeRecord,
    PortDecl, SourceAt, StateDecl, TypeRef,
};
#[cfg(feature = "std")]
pub use registry::{ConversionEntry, Entry, Registry};
#[cfg(feature = "std")]
pub use types::{TypeRecord, TypeTable};

/// What the macros' generated code names. Not part of the interface.
#[doc(hidden)]
pub mod __private;
