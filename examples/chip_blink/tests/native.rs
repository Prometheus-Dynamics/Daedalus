//! Runs the compiled graph natively, the same code a firmware runs, against the node functions
//! called by hand.

mod common;

use common::{Reference, signal};
use daedalus_abi::counting::counted;
use daedalus_chip_blink::Graph;
use daedalus_chip_blink::graph::{EDGES, NODE_IDS};

#[test]
fn plan_follows_the_graph_document() {
    assert_eq!(
        NODE_IDS,
        [
            "blink.scale",
            "blink.lowpass",
            "blink.threshold",
            "blink.rising",
            "blink.blink"
        ]
    );
    assert_eq!(EDGES.len(), 7);
    // Unmarked constants are literals, and there's no parameter table.
    let source = include_str!("../generated/graph.rs");
    assert!(!source.contains("Tunable") && source.contains("0.25"));
}

#[test]
fn graph_matches_the_node_functions_without_allocating() {
    let mut graph = Graph::new();
    let mut reference = Reference::a();
    let (mut rises, mut lit) = (0, 0);
    let ((), allocated) = counted(|| {
        for (now, raw) in signal().enumerate() {
            let (level, led, rose) = reference.step(raw);
            graph.push_sample(raw).unwrap();
            graph.tick_at(now as u64).unwrap();
            assert_eq!(graph.pop_level(), level);
            assert_eq!(graph.pop_led(), led);
            assert_eq!(graph.pop_rises(), rose);
            rises += usize::from(rose.is_some());
            lit += usize::from(led == Some(true));
        }
    });
    assert_eq!(allocated, 0, "pushes, ticks and pops allocate nothing");
    assert_eq!(rises, 2);
    assert!(lit > 0);
}

#[test]
fn edge_policies_shape_the_queues() {
    let mut graph = Graph::new();
    // `sample` keeps only the latest: the second push replaces the first, which primes the filter.
    graph.push_sample(4000).unwrap();
    graph.push_sample(0).unwrap();
    graph.tick().unwrap();
    assert_eq!(graph.pop_level(), Some(0.0));

    // `rises` holds 8 and drops the oldest: toggle 10 times without popping it.
    for _ in 0..10 {
        for raw in [4000, 0] {
            for _ in 0..8 {
                graph.push_sample(raw).unwrap();
                graph.tick().unwrap();
                let _ = (graph.pop_level(), graph.pop_led());
            }
        }
    }
    let kept: Vec<u32> = std::iter::from_fn(|| graph.pop_rises()).collect();
    assert_eq!(kept, (3..=10).collect::<Vec<_>>());

    // With no new sample, nothing runs.
    let _ = graph.pop_level();
    graph.tick().unwrap();
    assert_eq!(graph.pop_level(), None);
}
