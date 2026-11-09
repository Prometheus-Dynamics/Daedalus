//! The bodies a firmware links. A plan names them by position, and their interfaces hash to the
//! number a plan has to match.

use core::any::Any;

use daedalus_registry::{Fire, NodeRecord, PortDecl, TypeRef};
use daedalus_runner::ArenaBody;
use daedalus_types::fnv1a64;

use super::MAX_PORTS;
use crate::body::CpuBody;

/// The 32-bit name of a port's type or a host port.
pub const fn name_hash(text: &str) -> u32 {
    fnv1a64(text.as_bytes()) as u32
}

#[cfg(feature = "std")]
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

const fn hash_ports(mut hash: u64, ports: &[PortDecl]) -> u64 {
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

/// What the runner needs to know about a node. No text, so a firmware doesn't keep any.
#[derive(Clone, Copy, Debug)]
pub struct Entry {
    code: &'static (dyn Any + Send + Sync),
    pub(super) inputs: u8,
    pub(super) outputs: u8,
    /// Bit `n` is set when input `n` must have a value.
    pub(super) required: u8,
    pub(super) fire_all: bool,
    fits: bool,
    input_types: [u32; MAX_PORTS],
    fan_type: u32,
    output_types: [u32; MAX_PORTS],
}

/// A port's type name, or zero if it has none a plan can name.
const fn type_name(ty: &TypeRef) -> u32 {
    match ty {
        TypeRef::Key(key) => match key.literal_text() {
            Some(text) => name_hash(text),
            None => 0,
        },
        _ => 0,
    }
}

const fn port_types(ports: &[PortDecl]) -> [u32; MAX_PORTS] {
    let mut types = [0; MAX_PORTS];
    let mut at = 0;
    while at < ports.len() && at < MAX_PORTS {
        types[at] = type_name(&ports[at].ty);
        at += 1;
    }
    types
}

const fn required_mask(ports: &[PortDecl]) -> u8 {
    let (mut mask, mut at) = (0, 0);
    while at < ports.len() && at < MAX_PORTS {
        if !ports[at].optional {
            mask |= 1 << at;
        }
        at += 1;
    }
    mask
}

impl Entry {
    const UNUSED: Entry = Entry {
        code: &(),
        inputs: 0,
        outputs: 0,
        required: 0,
        fire_all: false,
        fits: false,
        input_types: [0; MAX_PORTS],
        fan_type: 0,
        output_types: [0; MAX_PORTS],
    };

    const fn of(record: &'static NodeRecord) -> Entry {
        let decl = record.decl;
        Entry {
            code: record.body.code,
            inputs: decl.inputs.len() as u8,
            outputs: decl.outputs.len() as u8,
            required: required_mask(decl.inputs),
            fire_all: matches!(decl.fire, Fire::All),
            fits: decl.inputs.len() <= MAX_PORTS && decl.outputs.len() <= MAX_PORTS,
            input_types: port_types(decl.inputs),
            fan_type: if decl.fanin.is_empty() {
                0
            } else {
                type_name(&decl.fanin[0].ty)
            },
            output_types: port_types(decl.outputs),
        }
    }

    /// The type name and value size of input `port`.
    pub(super) fn input(&self, body: &ArenaBody, port: usize) -> Option<(u32, u16)> {
        let named = |hash: u32| (hash != 0).then_some(hash);
        if port < usize::from(self.inputs) {
            return Some((named(self.input_types[port])?, *body.inputs.get(port)?));
        }
        Some((named(self.fan_type)?, body.fan?))
    }

    /// The type name of output `port`.
    pub(super) fn output(&self, port: usize) -> Option<u32> {
        (port < usize::from(self.outputs))
            .then(|| self.output_types[port])
            .filter(|hash| *hash != 0)
    }
}

/// A firmware's bodies, in the order plans name them.
#[derive(Debug)]
pub struct Library<T: ?Sized = [Entry]> {
    /// Covers each node's id, port names, types and which inputs are optional, and its fire mode.
    /// Not its code, so a firmware can fix a body without invalidating plans.
    pub hash: u64,
    /// The records, for planning. A firmware has no use for them, and their text would stay in
    /// its flash.
    #[cfg(feature = "std")]
    pub nodes: &'static [&'static NodeRecord],
    entries: T,
}

impl<const N: usize> Library<[Entry; N]> {
    /// # Panics
    ///
    /// At compile time, if there are more than [`super::MAX_NODES`] nodes.
    pub const fn new(nodes: &'static [&'static NodeRecord; N]) -> Library<[Entry; N]> {
        assert!(N <= super::MAX_NODES);
        let mut entries = [Entry::UNUSED; N];
        let mut at = 0;
        while at < N {
            entries[at] = Entry::of(nodes[at]);
            at += 1;
        }
        Library {
            hash: hash_of(nodes),
            #[cfg(feature = "std")]
            nodes,
            entries,
        }
    }
}

impl Library {
    /// Entry `index`, if a plan can run it, and how to run it.
    pub(super) fn entry(&self, index: usize) -> Option<(&Entry, ArenaBody)> {
        let entry = self.entries.get(index).filter(|entry| entry.fits)?;
        let body = entry.code.downcast_ref::<CpuBody>()?;
        Some((entry, body.arena?))
    }

    #[cfg(feature = "std")]
    pub(super) fn position(&self, id: &str) -> Option<usize> {
        self.nodes.iter().position(|node| node.decl.id == id)
    }
}
