//! The generated module matches the node functions.

mod common;

#[allow(dead_code)]
mod generated {
    include!("../generated/graph.rs");
}

use common::{Reference, signal};

#[test]
fn compiled_graph_matches_the_node_functions() {
    let mut graph = generated::Graph::new();
    let mut reference = Reference::default();
    let mut rises = 0;
    for raw in signal() {
        graph.push_sample(raw).unwrap();
        graph.tick().unwrap();
        let expected = reference.step(raw);
        assert_eq!(
            (graph.pop_level(), graph.pop_active(), graph.pop_rises()),
            expected
        );
        rises += usize::from(expected.2.is_some());
    }
    assert_eq!(rises, 2);
}
