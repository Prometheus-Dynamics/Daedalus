//! The const inputs a node's calls share, with a name index built once when the list is made. A
//! by-name lookup costs one hash and a short probe at any port count, not a scan of the list.

use core::ops::Deref;

use crate::io::NodePort;
use crate::prelude::*;

/// Open-addressing table over the port names, FNV-1a hashed. A slot holds `index + 1` (0 is
/// empty), so a lookup compares the name at that index and never stores a copy of it.
#[derive(Clone)]
enum NameIndex {
    Table {
        slots: Vec<u32>,
        mask: usize,
    },
    /// Some name repeats. `find` scans, so the first match wins, as a scan always has.
    Scan,
}

/// The const inputs of one node, in declaration order, with their name index.
#[derive(Clone)]
pub struct NodeConstInputs {
    ports: Vec<NodePort>,
    index: NameIndex,
}

impl NodeConstInputs {
    /// The const list `ports` (declaration order), indexed by name.
    pub fn new(ports: Vec<NodePort>) -> Self {
        let index = NameIndex::build(&ports);
        Self { ports, index }
    }

    /// Edit the list in place (a patch adds, removes or replaces ports); the index is rebuilt after.
    pub(crate) fn edit(&mut self, change: impl FnOnce(&mut Vec<NodePort>)) {
        change(&mut self.ports);
        self.index = NameIndex::build(&self.ports);
    }

    /// The index of the first const named `name`, if any (its taken state is the caller's).
    pub(crate) fn find(&self, name: &str) -> Option<usize> {
        match &self.index {
            NameIndex::Table { slots, mask } => {
                let mut at = fnv1a(name.as_bytes()) as usize & mask;
                loop {
                    let slot = slots[at];
                    if slot == 0 {
                        return None;
                    }
                    let idx = slot as usize - 1;
                    if self.ports[idx].0.as_str() == name {
                        return Some(idx);
                    }
                    at = (at + 1) & mask;
                }
            }
            NameIndex::Scan => self
                .ports
                .iter()
                .position(|(port, _)| port.as_str() == name),
        }
    }
}

impl From<Vec<NodePort>> for NodeConstInputs {
    fn from(ports: Vec<NodePort>) -> Self {
        Self::new(ports)
    }
}

impl Deref for NodeConstInputs {
    type Target = [NodePort];

    fn deref(&self) -> &[NodePort] {
        &self.ports
    }
}

/// Lists this short are scanned: at most this many compares, cheaper than hashing the name.
const SCAN_UP_TO: usize = 8;

impl NameIndex {
    fn build(ports: &[NodePort]) -> Self {
        if ports.len() <= SCAN_UP_TO {
            return Self::Scan;
        }
        let cap = (ports.len() * 2).next_power_of_two().max(8);
        let mask = cap - 1;
        let mut slots: Vec<u32> = (0..cap).map(|_| 0).collect();
        for (idx, (port, _)) in ports.iter().enumerate() {
            let name = port.as_str();
            let mut at = fnv1a(name.as_bytes()) as usize & mask;
            while slots[at] != 0 {
                if ports[slots[at] as usize - 1].0.as_str() == name {
                    return Self::Scan;
                }
                at = (at + 1) & mask;
            }
            slots[at] = idx as u32 + 1;
        }
        Self::Table { slots, mask }
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// A set of static names (a node's config ports), built once: membership costs one hash and a
/// probe, not a scan of the names. `ConfigCache` checks each edge input against it.
#[derive(Clone)]
pub(crate) struct NameSet {
    names: &'static [&'static str],
    slots: Vec<u32>,
    mask: usize,
}

impl NameSet {
    pub(crate) fn new(names: &'static [&'static str]) -> Self {
        let cap = (names.len() * 2).next_power_of_two().max(8);
        let mask = cap - 1;
        let mut slots: Vec<u32> = (0..cap).map(|_| 0).collect();
        for (idx, name) in names.iter().enumerate() {
            let mut at = fnv1a(name.as_bytes()) as usize & mask;
            while slots[at] != 0 {
                at = (at + 1) & mask;
            }
            slots[at] = idx as u32 + 1;
        }
        Self { names, slots, mask }
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        let mut at = fnv1a(name.as_bytes()) as usize & self.mask;
        loop {
            let slot = self.slots[at];
            if slot == 0 {
                return false;
            }
            if self.names[slot as usize - 1] == name {
                return true;
            }
            at = (at + 1) & self.mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::CorrelatedPayload;
    use crate::portable::Arc;
    use daedalus_transport::Payload;

    fn port(name: &str, value: i64) -> NodePort {
        let payload = Payload::owned("value", daedalus_data::model::Value::Int(value));
        (
            crate::handles::PortId::new(name),
            CorrelatedPayload::from_edge(payload),
        )
    }

    #[test]
    fn find_returns_the_index_of_each_distinct_name() {
        let names: Vec<String> = (0..60).map(|i| format!("port_{i}")).collect();
        let list = NodeConstInputs::new(
            names
                .iter()
                .enumerate()
                .map(|(i, n)| port(n, i as i64))
                .collect(),
        );
        for (i, name) in names.iter().enumerate() {
            assert_eq!(list.find(name), Some(i));
        }
        assert_eq!(list.find("missing"), None);
        assert_eq!(list.find(""), None);
    }

    #[test]
    fn a_repeated_name_resolves_to_its_first_entry() {
        let list = NodeConstInputs::new(vec![port("a", 1), port("b", 2), port("a", 3)]);
        assert_eq!(list.find("a"), Some(0));
        assert_eq!(list.find("b"), Some(1));
    }

    #[test]
    fn edit_rebuilds_the_index() {
        let mut list = NodeConstInputs::new(vec![port("a", 1)]);
        list.edit(|ports| {
            ports.push(port("b", 2));
            ports.remove(0);
        });
        assert_eq!(list.find("a"), None);
        assert_eq!(list.find("b"), Some(0));
        assert_eq!(list.len(), 1);
    }

    fn consts(count: usize) -> Arc<NodeConstInputs> {
        let names: Vec<String> = (0..count).map(|i| format!("p{i}")).collect();
        let ports = names
            .iter()
            .enumerate()
            .map(|(i, n)| port(n, i as i64))
            .collect();
        Arc::new(NodeConstInputs::new(ports))
    }

    #[test]
    fn the_last_shared_const_is_found_and_taken_at_the_cap() {
        let list = consts(64);
        let mut io = crate::io::NodeIo::from_inputs(std::iter::empty())
            .with_shared_consts(Some(list.clone()));
        assert!(
            io.shared_consts().is_some(),
            "64 consts are shared, not copied"
        );
        assert!(io.get_payload("p63").is_some());
        assert!(io.take_input_payload("p63").is_some());
        assert!(io.get_payload("p63").is_none(), "taken once");
        assert!(io.get_payload("p0").is_some(), "the others stay");
    }

    #[test]
    fn a_list_over_the_cap_spills_into_the_inputs() {
        let list = consts(70);
        let mut io = crate::io::NodeIo::from_inputs(std::iter::empty())
            .with_shared_consts(Some(list.clone()));
        assert!(
            io.shared_consts().is_none(),
            "over the cap the consts are copies in the inputs"
        );
        assert_eq!(io.inputs().count(), 70);
        assert!(io.get_payload("p0").is_some());
        assert!(io.get_payload("p69").is_some(), "the last const is found");
        assert!(io.take_input_payload("p69").is_some());
        assert!(io.get_payload("p69").is_none(), "taken once");
        assert_eq!(io.inputs().count(), 69);
    }

    #[test]
    fn a_name_set_answers_membership_for_each_static_name() {
        static NAMES: [&str; 5] = ["c0", "c1", "frame", "c3", "level"];
        let set = NameSet::new(&NAMES);
        for name in NAMES {
            assert!(set.contains(name), "{name} is in the set");
        }
        assert!(!set.contains("c2"));
        assert!(!set.contains(""));
        assert!(!set.contains("c"));
        let empty = NameSet::new(&[]);
        assert!(!empty.contains("c0"));
    }
}
