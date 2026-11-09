//! A host and a chip read the same node records, so their declarations agree by construction.
//! This checks they do, and that state starts from the same value everywhere.

use daedalus_chip_shared::{Edges, LIBRARY, Lowpass, NODES, lowpass};
use daedalus_cpu::BACKEND_ID;
use daedalus_registry::{NodeDecl, PortDecl, Registry};

/// A port as a plan sees it.
fn port(port: &PortDecl) -> String {
    format!(
        "{} {:?} optional {} conditional {}",
        port.name, port.ty, port.optional, port.conditional
    )
}

/// What a planner reads from a declaration, on any target.
fn contract(decl: &NodeDecl) -> (String, Vec<String>, Vec<String>, String) {
    (
        decl.id.to_string(),
        decl.inputs.iter().map(port).collect(),
        decl.outputs.iter().map(port).collect(),
        format!("{:?}", decl.fire),
    )
}

#[test]
fn the_host_registry_and_the_chip_library_hold_the_same_records() {
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    assert_eq!(LIBRARY.nodes.len(), NODES.len());
    for (record, listed) in NODES.iter().zip(LIBRARY.nodes) {
        assert_eq!(contract(record.decl), contract(listed.decl));
        let entry = registry.entry(record.decl.id).expect(record.decl.id);
        assert_eq!(contract(record.decl), contract(entry.decl));
        assert!(entry.body(BACKEND_ID).is_some(), "{}", record.decl.id);
    }
}

#[test]
fn declarations_are_not_vacuous() {
    let clamp = NODES[2].decl;
    assert_eq!(clamp.id, "shared.clamp");
    assert!(clamp.inputs[1].optional, "ceiling is optional");
    let rising = NODES[4].decl;
    assert!(rising.outputs[0].conditional, "count is conditional");
}

#[test]
fn state_starts_from_the_same_value() {
    // `Default` is what every form starts a node's state from.
    let mut state = Lowpass::default();
    assert_eq!(state, Lowpass::default());
    assert_eq!(lowpass(1.0, 0.5, &mut state), 1.0);
    assert_ne!(state, Lowpass::default());
    assert_eq!(Edges::default(), Edges::default());
}
