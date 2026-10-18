//! Hot reload: swap a running graph for an edited one between ticks.
//!
//! The graph runs `in -> accumulate -> <stage> -> out`. Mid-run it is reloaded with `scale`
//! swapped for `offset`. `accumulate` keeps its typed state (same node instance and type), the
//! host ports `in` and `out` keep running, and `scale` is dropped. A document that fails to
//! compile leaves the running graph untouched.

use daedalus::{
    engine::{Engine, EngineConfig, HostGraph, HostGraphReload, MetricsLevel},
    macros::{node, plugin},
    planner::Graph,
    prelude::{GraphDocument, PluginRequirement},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
};

/// Running total of every step the node has seen.
#[derive(Default)]
struct Accumulator {
    total: i64,
}

#[node(
    id = "reload.accumulate",
    inputs("step"),
    outputs("value"),
    state(Accumulator)
)]
fn accumulate(step: i64, state: &mut Accumulator) -> Result<i64, NodeError> {
    state.total += step;
    Ok(state.total)
}

#[node(id = "reload.scale", inputs("value"), outputs("value"))]
fn scale(value: i64) -> Result<i64, NodeError> {
    Ok(value * 3)
}

#[node(id = "reload.offset", inputs("value"), outputs("value"))]
fn offset(value: i64) -> Result<i64, NodeError> {
    Ok(value + 100)
}

#[plugin(id = "example.hot_reload", nodes(accumulate, scale, offset))]
struct ReloadPlugin;

/// `in -> accumulate -> scale -> out`.
fn scaled(registry: &PluginRegistry) -> Result<Graph, Box<dyn std::error::Error>> {
    let plugin = ReloadPlugin::new();
    let accumulate = plugin.accumulate.alias("accumulate");
    let stage = plugin.scale.alias("stage");
    Ok(registry
        .graph_builder()?
        .input_typed::<i64>("in")?
        .try_node(&accumulate)?
        .try_node(&stage)?
        .try_connect("in", &accumulate.inputs.step)?
        .try_connect(&accumulate.outputs.value, &stage.inputs.value)?
        .try_connect(&stage.outputs.value, "out")?
        .build())
}

/// `in -> accumulate -> offset -> out`.
fn offset_graph(registry: &PluginRegistry) -> Result<Graph, Box<dyn std::error::Error>> {
    let plugin = ReloadPlugin::new();
    let accumulate = plugin.accumulate.alias("accumulate");
    let stage = plugin.offset.alias("stage");
    Ok(registry
        .graph_builder()?
        .input_typed::<i64>("in")?
        .try_node(&accumulate)?
        .try_node(&stage)?
        .try_connect("in", &accumulate.inputs.step)?
        .try_connect(&accumulate.outputs.value, &stage.inputs.value)?
        .try_connect(&stage.outputs.value, "out")?
        .build())
}

/// Runs `steps` through the graph and prints what came out.
fn run(
    graph: &mut HostGraph<HandlerRegistry>,
    steps: &[i64],
) -> Result<(), Box<dyn std::error::Error>> {
    for step in steps {
        graph.push("in", *step);
        graph.tick_until_idle()?;
        println!("  step {step}: out={:?}", graph.take::<i64>("out"));
    }
    Ok(())
}

fn print_reload(reload: &HostGraphReload) {
    println!(
        "  kept={:?} added={:?} removed={:?}",
        reload.nodes_kept, reload.nodes_added, reload.nodes_removed
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    registry.install(&ReloadPlugin::new())?;
    let engine = Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Off))?;

    // Version 1: the running graph. The accumulator totals 1 then 3, and scale triples each.
    let mut graph = engine.compile_registry(&registry, scaled(&registry)?)?;
    println!("version 1 (scale):");
    run(&mut graph, &[1, 2])?;

    // Version 2: reloaded between ticks. The accumulator keeps its total of 3, so 3 + 3 = 6 then +100.
    let document = GraphDocument::new(offset_graph(&registry)?)
        .require(PluginRequirement::new("example.hot_reload"));
    let reload = graph.reload_document(&engine, &registry, document, registry.handlers())?;
    println!("reloaded to version 2 (offset):");
    print_reload(&reload);
    run(&mut graph, &[3])?;

    // A broken version is refused; version 2 keeps running untouched.
    let broken = GraphDocument::new(Graph::default())
        .require(PluginRequirement::new("example.not_installed"));
    match graph.reload_document(&engine, &registry, broken, registry.handlers()) {
        Ok(_) => println!("unexpected: a broken document was loaded"),
        Err(err) => println!("refused a broken document: {err}"),
    }
    println!("version 2 still running:");
    run(&mut graph, &[4])?;
    Ok(())
}
