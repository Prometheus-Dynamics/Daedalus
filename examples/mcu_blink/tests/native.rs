//! Runs the compiled blink plan natively (the same code the firmware runs) and checks its
//! outputs against the node functions called by hand, and that pushes, ticks and pops never
//! allocate.

mod common;

use common::{Reference, allocations, signal};
use daedalus_mcu_blink::Graph;
use daedalus_mcu_blink::graph::{EDGES, NODE_IDS};

#[test]
fn plan_follows_the_graph_document() {
    assert_eq!(
        NODE_IDS,
        ["scale", "lowpass", "threshold", "blink", "rising"]
    );
    assert_eq!(EDGES.len(), 8);
    // Frozen parameters: constants are literals and the graph has no parameter table.
    let source = include_str!(concat!(env!("OUT_DIR"), "/graph.rs"));
    assert!(!source.contains("Tunable") && source.contains("0.25_f32"));
}

#[test]
fn graph_matches_the_node_functions_without_allocating() {
    let mut graph = Graph::new();
    let mut reference = Reference::a();
    let before = allocations();
    let (mut rises, mut led_on) = (0, 0);
    for raw in signal() {
        graph.push_sample(raw).unwrap();
        graph.tick().unwrap();
        let (level, led, rose) = reference.step(raw);
        assert_eq!(graph.pop_level(), Some(level));
        assert_eq!(graph.pop_led(), Some(led));
        assert_eq!(graph.pop_rises(), rose);
        rises += usize::from(rose.is_some());
        led_on += usize::from(led);
    }
    assert_eq!(
        allocations(),
        before,
        "pushes, ticks and pops allocate nothing"
    );
    assert_eq!(rises, 2);
    assert!(led_on > 0);
}

#[test]
fn edge_policies_shape_the_queues() {
    let mut graph = Graph::new();
    // `sample` is latest-only: the second push replaces the first, which primes the filter.
    graph.push_sample(4000).unwrap();
    graph.push_sample(0).unwrap();
    graph.tick().unwrap();
    assert_eq!(graph.pop_level(), Some(0.0));

    // `rises` is bounded to 8 (drop oldest): toggle 10 times without popping it.
    for _ in 0..10 {
        for raw in [4000, 0] {
            for _ in 0..8 {
                graph.push_sample(raw).unwrap();
                graph.tick().unwrap();
            }
        }
    }
    let kept: Vec<u32> = std::iter::from_fn(|| graph.pop_rises()).collect();
    assert_eq!(kept, (3..=10).collect::<Vec<_>>());
    // With no new sample, nothing runs: the filter output stays where it was.
    let _ = graph.pop_level();
    graph.tick().unwrap();
    assert_eq!(graph.pop_level(), None);
}
