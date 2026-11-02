//! The shared graph in compiled mode, natively: the generated `Graph` (the code the device runs)
//! matches the shared node functions called by hand.

mod common;

use common::{Reference, signal};

/// The generated plan; the tests use part of it.
#[allow(dead_code)]
mod graph {
    include!("../generated/graph.rs");
}

#[test]
fn compiled_graph_matches_the_node_functions() {
    let mut graph = graph::Graph::new();
    let mut reference = Reference::new();
    let mut rises = 0;
    for raw in signal() {
        graph.push_sample(raw).unwrap();
        graph.tick().unwrap();
        let expected = reference.step(raw);
        assert_eq!(graph.pop_level(), Some(expected.level));
        assert_eq!(graph.pop_active(), Some(expected.active));
        assert_eq!(graph.pop_rises(), expected.rises);
        rises += usize::from(expected.rises.is_some());
    }
    assert_eq!(rises, 2);
}
