//! The constant records a `#[node]` emits. Records are only ever read.

use core::any::Any;

use daedalus_graph::GroupDef;
use daedalus_types::{Literal, TypeExpr, TypeKey};
use daedalus_value::Access;

/// `file:line`, written by the macro.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceAt(pub &'static str);

/// When a node runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Fire {
    /// When any input has a new value.
    Any,
    /// When every input has a new value.
    #[default]
    All,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeRef {
    Key(TypeKey),
    Expr(TypeExpr),
    /// Not in the attribute: the body supplies it.
    Body,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PortDecl {
    pub name: &'static str,
    pub ty: TypeRef,
    pub access: Access,
    /// An `Option` input.
    pub optional: bool,
    /// An `Option` output.
    pub conditional: bool,
    pub default: Option<Literal>,
    pub label: &'static str,
    pub unit: &'static str,
    pub group: &'static str,
    pub advanced: bool,
    pub description: &'static str,
    /// Where a config field's value comes from, by name. Empty for an ordinary port.
    pub source: &'static str,
}

impl PortDecl {
    /// A port with every optional field empty.
    pub const fn new(name: &'static str, ty: TypeRef) -> PortDecl {
        PortDecl {
            name,
            ty,
            access: Access::Read,
            optional: false,
            conditional: false,
            default: None,
            label: "",
            unit: "",
            group: "",
            advanced: false,
            description: "",
            source: "",
        }
    }
}

/// Limits on a config field. Recorded here and enforced when a graph is planned.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FieldRule {
    pub field: &'static str,
    pub min: Option<Literal>,
    pub max: Option<Literal>,
    pub odd: bool,
    pub policy: &'static str,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConfigDecl {
    pub name: &'static str,
    pub ty: TypeRef,
    pub fields: &'static [PortDecl],
    pub rules: &'static [FieldRule],
}

/// Indexed fan-in ports: `prefix0`, `prefix1`, and so on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FanInDecl {
    pub prefix: &'static str,
    pub start: u32,
    pub ty: TypeRef,
}

/// Marks a one-in, one-out node as a conversion between its two types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Conversion {
    pub cost: u32,
    pub lossy: bool,
}

/// What the graph sees. One per id, shared by every body.
#[derive(Clone, Debug)]
pub struct NodeDecl {
    pub id: &'static str,
    pub at: SourceAt,
    pub summary: &'static str,
    pub description: &'static str,
    pub inputs: &'static [PortDecl],
    pub outputs: &'static [PortDecl],
    pub config: &'static [ConfigDecl],
    pub fanin: &'static [FanInDecl],
    pub fire: Fire,
    pub reusable: bool,
    /// Identical upstream nodes may be shared between graphs in one host.
    pub shareable: bool,
    pub conversion: Option<Conversion>,
    /// A node made of other nodes.
    pub group: Option<&'static GroupDef>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateDecl {
    pub size: usize,
    pub align: usize,
    pub name: &'static str,
}

/// For a body with no code object of its own, held entirely in `text`.
#[derive(Clone, Copy, Debug)]
pub struct NoCode;

/// How a node runs on one backend.
#[derive(Clone, Debug)]
pub struct BodyRecord {
    pub backend: &'static str,
    pub at: SourceAt,
    /// The backend's own requirements, as text it reads in `can_take`.
    pub needs: &'static [&'static str],
    pub state: Option<StateDecl>,
    /// Port types this body supplies, for ports declared `TypeRef::Body`.
    pub ports: &'static [(&'static str, TypeRef)],
    /// Port to memory space, overriding the backend's own space. Almost nothing needs it.
    pub memory: &'static [(&'static str, &'static str)],
    /// The backend's own payload.
    pub code: &'static (dyn Any + Send + Sync),
    /// Source, for bodies written as text.
    pub text: &'static str,
}

impl BodyRecord {
    /// Adds what the node's attribute says about this body, which the backend's own macro
    /// doesn't see.
    pub const fn with_node(
        mut self,
        state: Option<StateDecl>,
        memory: &'static [(&'static str, &'static str)],
    ) -> BodyRecord {
        self.state = state;
        self.memory = memory;
        self
    }
}

#[derive(Clone, Debug)]
pub struct NodeRecord {
    pub decl: &'static NodeDecl,
    pub body: BodyRecord,
}

/// How expensive a memory handler is, cheapest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransferClass {
    Handoff,
    Import,
    Copy,
}

/// A memory handler a `#[transfer]` function declares, listed by hand beside the nodes.
#[derive(Clone, Debug)]
pub struct TransferRecord {
    pub id: &'static str,
    pub at: SourceAt,
    /// The type it reads.
    pub key: TypeKey,
    /// The type it produces.
    pub output: TypeKey,
    pub from: &'static str,
    pub to: &'static str,
    pub class: TransferClass,
    pub body: BodyRecord,
}
