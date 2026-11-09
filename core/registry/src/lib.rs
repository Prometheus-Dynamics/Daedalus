//! The id is the node. A node's definition has two parts: what the graph sees, and how it runs.
//! Each `#[node]` emits one constant record holding both, and the registry joins records that
//! share an id. The records need no heap, so they can live in statics on any target. The registry
//! itself is built when a host starts.

#![forbid(unsafe_code)]

mod error;
mod offer;
mod owned;
mod record;
mod registry;
mod types;

pub use error::RegError;
pub use offer::OfferData;
pub use owned::{OwnedBody, OwnedDecl, OwnedPort};
pub use record::{
    BodyRecord, ConfigDecl, Conversion, FanInDecl, Fire, NoCode, NodeDecl, NodeRecord, PortDecl,
    SourceAt, StateDecl, TypeRef,
};
pub use registry::{ConversionEntry, Entry, Registry};
pub use types::{TypeRecord, TypeTable};
