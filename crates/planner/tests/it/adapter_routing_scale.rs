//! Edge adapter routing on a large registry: a planning run must still find every conversion, and
//! `cargo test -p daedalus-planner -- --ignored --nocapture adapter_routing_scale` times it.

use daedalus_planner::{
    DiagnosticCode, Edge, Graph, NodeInstance, PlannerConfig, PlannerInput, build_plan,
};
use daedalus_registry::capability::{AdapterDecl, CapabilityRegistry, NodeDecl, PortDecl};
use daedalus_transport::AdaptCost;

const TYPES: usize = 300;
const NOISE_ADAPTERS_PER_TYPE: usize = 10;
const STAGES: usize = 1000;

fn type_key(i: usize) -> String {
    format!("bench:t{i}")
}

fn stage_in_type(k: usize) -> usize {
    (7 * k) % TYPES
}

fn stage_out_type(k: usize) -> usize {
    (7 * k + 1) % TYPES
}

/// A view chain through every type (so any pair is reachable) plus random materializing shortcuts.
fn large_registry() -> CapabilityRegistry {
    let mut reg = CapabilityRegistry::new();
    let mut seed: u64 = 0x5eed;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as usize
    };
    for i in 0..TYPES {
        reg.register_adapter(
            AdapterDecl::new(
                format!("chain_{i:04}"),
                type_key(i),
                type_key((i + 1) % TYPES),
            )
            .cost(AdaptCost::view()),
        )
        .unwrap();
        for k in 0..NOISE_ADAPTERS_PER_TYPE {
            let to = next() % TYPES;
            reg.register_adapter(
                AdapterDecl::new(format!("noise_{i:04}_{k:02}"), type_key(i), type_key(to))
                    .cost(AdaptCost::materialize()),
            )
            .unwrap();
        }
    }
    for k in 0..STAGES {
        reg.register_node(
            NodeDecl::new(format!("bench.stage{k:04}"))
                .input(PortDecl::new("in", type_key(stage_in_type(k))))
                .output(PortDecl::new("out", type_key(stage_out_type(k)))),
        )
        .unwrap();
    }
    reg
}

/// `STAGES` nodes in a row; each edge needs a conversion from `out` of stage k to `in` of k+1.
fn chained_graph() -> Graph {
    let mut graph = Graph::default();
    for k in 0..STAGES {
        graph
            .nodes
            .push(NodeInstance::new(format!("bench.stage{k:04}")));
    }
    for k in 0..STAGES - 1 {
        graph.edges.push(Edge::new(k, "out", k + 1, "in"));
    }
    graph
}

#[test]
fn adapter_routing_scale_converts_every_edge() {
    let config = PlannerConfig {
        transport_capabilities: Some(large_registry()),
        enable_lints: false,
        ..PlannerConfig::default()
    };
    let out = build_plan(
        PlannerInput {
            graph: chained_graph(),
        },
        config,
    );
    let rejected: Vec<_> = out
        .diagnostics
        .iter()
        .filter(|diag| {
            matches!(
                diag.code,
                DiagnosticCode::ConverterMissing | DiagnosticCode::TypeMismatch
            )
        })
        .map(|diag| diag.message.clone())
        .collect();
    assert!(rejected.is_empty(), "unconverted edges: {rejected:?}");
}

#[test]
#[ignore = "timing: run with --ignored --nocapture"]
#[allow(clippy::print_stdout)]
fn adapter_routing_scale_timing() {
    let reg = large_registry();
    const RUNS: u32 = 5;
    let started = std::time::Instant::now();
    for _ in 0..RUNS {
        let config = PlannerConfig {
            transport_capabilities: Some(reg.clone()),
            enable_lints: false,
            ..PlannerConfig::default()
        };
        let out = build_plan(
            PlannerInput {
                graph: chained_graph(),
            },
            config,
        );
        std::hint::black_box(out.diagnostics.len());
    }
    let per_run = started.elapsed() / RUNS;
    println!(
        "adapter_routing_scale: {TYPES} types, {} adapters, {STAGES} nodes, {} edges: {per_run:?} per build_plan",
        reg.adapters().values().count(),
        STAGES - 1
    );
}
