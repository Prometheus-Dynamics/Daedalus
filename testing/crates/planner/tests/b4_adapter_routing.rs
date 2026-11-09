//! A planning run on a large registry must still find every conversion.

use daedalus_graph::GraphBody;
use daedalus_planner::{Config, solve};
use daedalus_planner_tests::{decl, edge, key, node};
use daedalus_registry::Registry;

const TYPES: usize = 300;
const NOISE_PER_TYPE: usize = 10;
const STAGES: usize = 1000;

fn type_key(i: usize) -> String {
    format!("bench:t{i}")
}

fn large_registry() -> Registry {
    let mut registry = Registry::new();
    let mut seed: u64 = 0x5eed;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as usize
    };
    for i in 0..TYPES {
        decl(&format!("c.chain_{i:04}"))
            .input("in", key(&type_key(i)))
            .output("out", key(&type_key((i + 1) % TYPES)))
            .conversion(1, false)
            .body("sim")
            .add(&mut registry);
        for k in 0..NOISE_PER_TYPE {
            let to = next() % TYPES;
            decl(&format!("c.noise_{i:04}_{k:02}"))
                .input("in", key(&type_key(i)))
                .output("out", key(&type_key(to)))
                .conversion(1000, false)
                .body("sim")
                .add(&mut registry);
        }
    }
    for k in 0..STAGES {
        decl(&format!("b.stage{k:04}"))
            .input("in", key(&type_key((7 * k) % TYPES)))
            .output("out", key(&type_key((7 * k + 1) % TYPES)))
            .body("sim")
            .add(&mut registry);
    }
    registry
}

#[test]
fn adapter_routing_scale_converts_every_edge() {
    let registry = large_registry();
    let body = GraphBody {
        nodes: (0..STAGES)
            .map(|k| node(&format!("b.stage{k:04}")))
            .collect(),
        edges: (0..STAGES - 1)
            .map(|k| edge(k as u32, "out", k as u32 + 1, "in"))
            .collect(),
        groups: Vec::new(),
    };
    let graph = solve(&body, &registry, &Config::default()).unwrap();
    // Stage k makes 7k+1 and the next stage takes 7k+7: six chain steps, each cost 1.
    assert_eq!(graph.nodes.len(), STAGES + (STAGES - 1) * 6);
    assert_eq!(graph.edges.len(), (STAGES - 1) * 7);
}
