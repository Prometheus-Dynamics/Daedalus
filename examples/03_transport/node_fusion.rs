//! Node fusion: a chain of CPU nodes whose edges have one consumer runs as one fused unit, and
//! the plan explanation names the units and the edges that break them.
//!
//! The graph is `in -> inc -> double -> negate -> out`, with `double`'s output also tapped to
//! `tap`. `inc -> double` fuses (one consumer, direct slot, CPU nodes), so `inc` and `double` form
//! one unit. `double -> negate` does not: `double` now feeds two consumers (`negate` and the
//! `tap` host output), so the explanation reports the fan-out as the block. Fusion never changes
//! what a node sees; run it with `EngineConfig::with_node_fusion(false)` to compare.

use daedalus::{
    engine::{Engine, EngineConfig, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "fusion.inc", inputs("value"), outputs("value"))]
fn inc(value: i64) -> Result<i64, NodeError> {
    Ok(value + 1)
}

#[node(id = "fusion.double", inputs("value"), outputs("value"))]
fn double(value: i64) -> Result<i64, NodeError> {
    Ok(value * 2)
}

#[node(id = "fusion.negate", inputs("value"), outputs("value"))]
fn negate(value: i64) -> Result<i64, NodeError> {
    Ok(-value)
}

#[plugin(id = "example.node_fusion", nodes(inc, double, negate))]
struct FusionPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = FusionPlugin::new();
    registry.install(&plugin)?;

    let inc = plugin.inc.alias("inc");
    let double = plugin.double.alias("double");
    let negate = plugin.negate.alias("negate");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
            g.output("tap");
        })
        .nodes(|g| {
            g.add_handle(&inc);
            g.add_handle(&double);
            g.add_handle(&negate);
        })
        .try_edges(|g| {
            let inc = g.node("inc");
            let double = g.node("double");
            let negate = g.node("negate");
            g.try_connect("in", &inc.input("value"))?;
            g.try_connect(&inc.output("value"), &double.input("value"))?;
            g.try_connect(&double.output("value"), &negate.input("value"))?;
            g.try_connect(&double.output("value"), "tap")?;
            g.try_connect(&negate.output("value"), "out")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Basic))?;
    let mut runtime = engine.compile_registry(&registry, graph)?;

    let explanation = runtime.explain_plan();
    println!("fused units:");
    for unit in &explanation.fused_units {
        println!("  {}", unit.node_ids.join(" -> "));
    }
    println!("edges:");
    for edge in &explanation.edges {
        let verdict = match (&edge.fused, &edge.fusion_block) {
            (true, _) => "fused".to_string(),
            (false, Some(block)) => format!("not fused: {block}"),
            (false, None) => "not fused".to_string(),
        };
        println!(
            "  {}.{} -> {}.{}: {verdict}",
            edge.from_node_id, edge.from_port, edge.to_node_id, edge.to_port
        );
    }

    runtime.push("in", 4_i64);
    runtime.tick_until_idle()?;
    println!(
        "out={:?} tap={:?}",
        runtime.take::<i64>("out"),
        runtime.take::<i64>("tap")
    );
    Ok(())
}
