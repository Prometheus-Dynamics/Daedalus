//! The graph model. [`GraphDef`] is a constant with no heap, for chips and generated code. With
//! the `alloc` feature, [`GraphDoc`] reads and writes the JSON format. A graph names node ids and
//! at most a forced backend, and knows nothing about what either one is.

#![no_std]
#![forbid(unsafe_code)]

#[cfg(feature = "alloc")]
extern crate alloc;

mod def;
#[cfg(feature = "alloc")]
mod doc;

pub use def::{
    EdgeDef, EdgePolicy, GraphDef, GroupDef, NodeDef, NodeKind, PortRef, Rate, StagePort,
};
#[cfg(feature = "schema")]
pub use doc::json_schema;
#[cfg(feature = "alloc")]
pub use doc::{
    DocError, EdgeDoc, EdgePolicyDoc, FORMAT, GraphBody, GraphDoc, GraphPatch, GroupDoc, Lit,
    LitField, NodeDoc, NodeKindDoc, NodeSelector, PatchOp, PortRefDoc, RateDoc, Requirement,
    SCHEMA_VERSION, StageConstDoc, StagePortDoc,
};
