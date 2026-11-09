//! The host's named ports. The plan supplies them; each one names a slot, a type, and the steps
//! to wake when a value arrives.

use daedalus_types::{SpaceIndex, TypeIndex, TypeKey};

/// Which way a port carries values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    /// The host feeds it, and the graph reads it.
    Input,
    /// The graph writes it, and the host takes from it.
    Output,
}

/// One host port as the plan describes it.
#[derive(Clone, Copy, Debug)]
pub struct PortSpec<'a> {
    pub name: &'a str,
    pub dir: Dir,
    /// The slot the port reads or writes.
    pub slot: u32,
    /// The type index a value must carry to pass this port.
    pub ty: TypeIndex,
    pub space: SpaceIndex,
    /// The key the type index stands for, for error messages.
    pub key: &'a TypeKey,
    /// Consumer steps woken when a value is fed. Empty for outputs.
    pub wake: &'a [u32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortsError {
    /// Names must be strictly increasing, so lookup can binary-search them.
    NotSorted { at: usize },
}

/// Ports sorted by name. Lookup is a binary search over that order.
#[derive(Clone, Copy, Debug)]
pub struct Ports<'a> {
    specs: &'a [PortSpec<'a>],
}

impl<'a> Ports<'a> {
    /// Checks that `specs` is sorted by name with no repeats.
    pub fn new(specs: &'a [PortSpec<'a>]) -> Result<Ports<'a>, PortsError> {
        for at in 1..specs.len() {
            if specs[at - 1].name >= specs[at].name {
                return Err(PortsError::NotSorted { at });
            }
        }
        Ok(Ports { specs })
    }

    pub fn find(&self, name: &str) -> Option<&'a PortSpec<'a>> {
        self.specs
            .binary_search_by(|spec| spec.name.cmp(name))
            .ok()
            .map(|at| &self.specs[at])
    }
}
