//! What a backend is asked about.

use daedalus_types::{NodeIndex, TypeIndex};
use daedalus_value::Access;

/// Which of a node's bodies, in the registry's order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BodyIndex(pub u32);

/// A node a backend says it could run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeRef {
    pub node: NodeIndex,
    pub body: BodyIndex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortInfo<'a> {
    pub name: &'a str,
    pub ty: TypeIndex,
    pub access: Access,
    pub optional: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OfferNode<'a> {
    pub node: NodeIndex,
    pub body: BodyIndex,
    pub inputs: &'a [PortInfo<'a>],
    pub outputs: &'a [PortInfo<'a>],
    /// What this body asked of the backend, as the backend's own text.
    pub needs: &'a [&'a str],
}

/// Only the nodes that have a body for this backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Offer<'a> {
    pub nodes: &'a [OfferNode<'a>],
}
