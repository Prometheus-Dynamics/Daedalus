//! Where values live, and how they move.

use std::borrow::Cow;

use daedalus_abi::Step;
use daedalus_types::TypeKey;

use crate::{CompileCx, Refusal};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpaceKind {
    Host,
    Device,
    Shared,
}

/// A place a value can be. Spaces are named, so one backend's transfer can name another's space.
/// The planner turns the names into indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemorySpace {
    pub id: Cow<'static, str>,
    pub kind: SpaceKind,
    /// Whether this backend's nodes can read their inputs straight from here.
    pub readable: bool,
}

/// How expensive a transfer is, cheapest first. The planner takes the cheapest path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransferCost {
    /// Ownership changes hands, and no work happens.
    Handoff,
    /// The destination gets at the same memory. Often cached per buffer.
    Import,
    /// The bytes are copied.
    Copy,
}

/// One way to move a value from one memory space to another.
#[derive(Clone, Debug)]
pub struct Transfer {
    pub id: &'static str,
    /// The type this moves. `None` moves any type.
    pub key: Option<TypeKey>,
    pub from: Cow<'static, str>,
    pub to: Cow<'static, str>,
    pub cost: TransferCost,
    /// Makes the step that does the move.
    pub make: fn(&CompileCx<'_>) -> Result<Step, Refusal>,
}

/// The cheapest transfer from `from` to `to` for `key`. A transfer for exactly this type beats
/// an any-type one, then the lowest cost, then the lowest id.
pub fn find_transfer<'a>(
    table: &'a [Transfer],
    key: &TypeKey,
    from: &str,
    to: &str,
) -> Option<&'a Transfer> {
    table
        .iter()
        .filter(|t| t.from == from && t.to == to)
        .filter(|t| t.key.as_ref().is_none_or(|k| k == key))
        .min_by_key(|t| (t.key.is_none(), t.cost, t.id))
}
