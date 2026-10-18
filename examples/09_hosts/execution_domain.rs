//! Execution domain: one shared preprocessing graph feeds two downstream graphs on one thread.
//!
//! A camera frame is routed into `preprocess` (`scale`), whose output is linked to two
//! downstream graphs, `mask_a` (`add`) and `mask_b` (`negate`). Each frame runs `scale` once,
//! and both downstream graphs read its output as a shared `Arc` payload, not a copy. Downstream
//! graphs can be added or removed between ticks without recompiling the others.

use daedalus::{
    NodeHandle,
    engine::{Engine, EngineConfig, ExecutionDomain, HostGraph, LinkMode, MetricsLevel},
    macros::{node, plugin},
    planner::Graph,
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
};

#[node(id = "scale", inputs("x"), outputs("y"))]
fn scale(x: i64) -> Result<i64, NodeError> {
    Ok(x * 2)
}

#[node(id = "add", inputs("x"), outputs("y"))]
fn add(x: i64) -> Result<i64, NodeError> {
    Ok(x + 1)
}

#[node(id = "negate", inputs("x"), outputs("y"))]
fn negate(x: i64) -> Result<i64, NodeError> {
    Ok(-x)
}

#[plugin(id = "example.domain", nodes(scale, add, negate))]
struct DomainPlugin;

/// `input -> stages... -> output`. A shared input is declared so by-value stages get a planned
/// copy while by-reference readers keep the `Arc`.
fn chain(
    registry: &PluginRegistry,
    stages: &[&str],
    input: &str,
    output: &str,
) -> Result<Graph, Box<dyn std::error::Error>> {
    let handles: Vec<NodeHandle> = stages
        .iter()
        .enumerate()
        .map(|(at, stage)| {
            NodeHandle::new(format!("example.domain:{stage}")).alias(format!("s{at}"))
        })
        .collect();
    let mut builder = registry
        .graph_builder()?
        .input_typed::<i64>(input)?
        .shared_input(input);
    for handle in &handles {
        builder = builder.try_node(handle)?;
    }
    builder = builder.try_connect(input, &handles[0].input("x"))?;
    for pair in handles.windows(2) {
        builder = builder.try_connect(&pair[0].output("y"), &pair[1].input("x"))?;
    }
    Ok(builder
        .try_connect(&handles[handles.len() - 1].output("y"), output)?
        .build())
}

fn compile(
    engine: &Engine,
    registry: &PluginRegistry,
    graph: Graph,
) -> Result<HostGraph<HandlerRegistry>, Box<dyn std::error::Error>> {
    Ok(engine.compile_registry(registry, graph)?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    registry.install(&DomainPlugin::new())?;
    let engine = Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Off))?;

    let mut domain = ExecutionDomain::new();
    domain.add_graph(
        "preprocess",
        compile(&engine, &registry, chain(&registry, &["scale"], "x", "y")?)?,
    )?;
    domain.route_input("camera", "preprocess", "x")?;
    for (name, stage) in [("mask_a", "add"), ("mask_b", "negate")] {
        domain.add_graph(
            name,
            compile(&engine, &registry, chain(&registry, &[stage], "y", "out")?)?,
        )?;
        domain.link("preprocess", "y", name, "y", LinkMode::Latest)?;
    }
    println!("tick order: {:?}", domain.tick_order().collect::<Vec<_>>());

    for frame in 1..=3_i64 {
        domain.push("camera", frame)?;
        let tick = domain.tick();
        let mask_a = domain.take_payload("mask_a", "out");
        let mask_b = domain.take_payload("mask_b", "out");
        println!(
            "frame {frame}: ran {} graphs, mask_a={:?} mask_b={:?}",
            tick.ran,
            mask_a.as_ref().and_then(|p| p.get_ref::<i64>()),
            mask_b.as_ref().and_then(|p| p.get_ref::<i64>()),
        );
    }

    // A downstream graph joins between ticks; the others keep running.
    domain.add_graph(
        "mask_c",
        compile(
            &engine,
            &registry,
            chain(&registry, &["add", "add"], "y", "out")?,
        )?,
    )?;
    domain.link("preprocess", "y", "mask_c", "y", LinkMode::Latest)?;
    domain.push("camera", 4_i64)?;
    domain.tick();
    println!(
        "frame 4 with mask_c: mask_c={:?}",
        domain
            .take_payload("mask_c", "out")
            .and_then(|p| p.get_ref::<i64>().copied())
    );

    let stats = domain.stats();
    let up = stats.graph("preprocess").expect("preprocess stats");
    println!(
        "preprocess ran {} times for {} consumers (avoided {} runs)",
        up.runs, up.consumers, up.avoided_runs
    );
    println!("{}", domain.explain());
    Ok(())
}
