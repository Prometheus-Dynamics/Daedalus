//! The bodies a firmware links. A plan names them by position, and their interfaces hash to the
//! number a plan has to match.

use daedalus_registry::{Fire, NodeDecl, NodeRecord, TypeRef};
use daedalus_runner::ArenaBody;
use daedalus_types::fnv1a64;

use crate::body::CpuBody;

/// The 32-bit name of a port's type or a host port.
pub const fn name_hash(text: &str) -> u32 {
    fnv1a64(text.as_bytes()) as u32
}

pub(super) fn type_hash(ty: &TypeRef) -> Option<u32> {
    match ty {
        TypeRef::Key(key) => Some(name_hash(key.as_str())),
        _ => None,
    }
}

const fn type_text(ty: &TypeRef) -> &'static str {
    match ty {
        TypeRef::Key(key) => match key.literal_text() {
            Some(text) => text,
            None => "?",
        },
        _ => "?",
    }
}

const fn mix(hash: u64, text: &str) -> u64 {
    (hash ^ fnv1a64(text.as_bytes())).wrapping_mul(0x0100_0000_01b3)
}

const fn hash_ports(mut hash: u64, ports: &[daedalus_registry::PortDecl]) -> u64 {
    let mut at = 0;
    while at < ports.len() {
        hash = mix(hash, ports[at].name);
        hash = mix(hash, type_text(&ports[at].ty));
        hash = mix(hash, if ports[at].optional { "opt" } else { "req" });
        at += 1;
    }
    hash
}

const fn hash_of(nodes: &[&NodeRecord]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    let mut at = 0;
    while at < nodes.len() {
        let decl = nodes[at].decl;
        hash = mix(hash, decl.id);
        hash = mix(
            hash,
            if matches!(decl.fire, Fire::All) {
                "all"
            } else {
                "any"
            },
        );
        hash = hash_ports(mix(hash, "in"), decl.inputs);
        hash = hash_ports(mix(hash, "out"), decl.outputs);
        let mut fan = 0;
        while fan < decl.fanin.len() {
            hash = mix(hash, decl.fanin[fan].prefix);
            hash = mix(hash, type_text(&decl.fanin[fan].ty));
            fan += 1;
        }
        at += 1;
    }
    hash
}

/// A firmware's bodies, in the order plans name them.
#[derive(Clone, Copy, Debug)]
pub struct Library {
    /// Covers each node's id, port names, types and which inputs are optional, and its fire mode.
    /// Not its code, so a firmware can fix a body without invalidating plans.
    pub hash: u64,
    pub nodes: &'static [&'static NodeRecord],
}

impl Library {
    /// # Panics
    ///
    /// At compile time, if there are more than [`super::MAX_NODES`] nodes.
    pub const fn new(nodes: &'static [&'static NodeRecord]) -> Library {
        assert!(nodes.len() <= super::MAX_NODES);
        Library {
            hash: hash_of(nodes),
            nodes,
        }
    }

    /// Entry `index`, if a plan can run it: its declaration and how to run it.
    pub(super) fn entry(&self, index: usize) -> Option<(&'static NodeDecl, ArenaBody)> {
        let record = *self.nodes.get(index)?;
        let body = record.body.code.downcast_ref::<CpuBody>()?;
        Some((record.decl, body.arena?))
    }

    #[cfg(feature = "std")]
    pub(super) fn position(&self, id: &str) -> Option<usize> {
        self.nodes.iter().position(|node| node.decl.id == id)
    }
}
