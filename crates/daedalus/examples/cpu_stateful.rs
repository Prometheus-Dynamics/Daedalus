//! Demonstrates a stateful node using the ExecutionContext state store.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example cpu_stateful
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

/// Simple source that emits three integers.
#[node(id = "stateful_source", outputs("n"))]
fn source() -> Result<Vec<i32>, NodeError> {
    Ok(vec![1, 2, 3])
}

/// Stateful accumulator: keeps a running sum across invocations for this node id.
/// State is stored in the ExecutionContext `state` map keyed by node id.
#[derive(Default)]
struct AccumState {
    running_sum: i32,
}

#[node(
    id = "stateful_accumulate",
    inputs("n"),
    outputs("sum"),
    state(AccumState)
)]
fn accumulate(values: Vec<i32>, state: &mut AccumState) -> Result<i32, NodeError> {
    state.running_sum += values.iter().sum::<i32>();
    Ok(state.running_sum)
}

#[node(id = "stateful_sink", inputs("sum"))]
fn sink(sum: i32) -> Result<(), NodeError> {
    println!("running sum: {sum}");
    Ok(())
}

declare_plugin!(
    StatefulPlugin,
    "example.stateful",
    [source, accumulate, sink]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = StatefulPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let src = plugin.source.alias("source");
    let acc = plugin.accumulate.alias("accum");
    let sk = plugin.sink.alias("sink");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&acc)
        .node(&sk)
        .connect(&src.outputs.n, &acc.inputs.n)
        .connect(&acc.outputs.sum, &sk.inputs.sum)
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.gpu = GpuBackend::Cpu;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&reg.registry, graph, handlers)?;
    println!("telemetry: {:?}", result.telemetry);
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
fn main() {
    eprintln!("enable `engine` and `plugins` features to run this example");
}
