//! Demonstrates nesting a graph so it can be reused as a single stage in another graph.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example nested_graph

#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::{GraphBuilder, NestedGraph},
    host_bridge::host_port,
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

#[node(id = "numbers", outputs("lhs", "rhs"))]
fn numbers() -> Result<(i32, i32), NodeError> {
    Ok((3, 4))
}

#[node(id = "add_i32", inputs("lhs", "rhs"), outputs("sum"))]
fn add_i32(lhs: i32, rhs: i32) -> Result<i32, NodeError> {
    Ok(lhs + rhs)
}

#[node(id = "negate", inputs("value"), outputs("out"))]
fn negate(value: i32) -> Result<i32, NodeError> {
    Ok(-value)
}

#[node(id = "double", inputs("value"), outputs("out"))]
fn double(value: i32) -> Result<i32, NodeError> {
    Ok(value * 2)
}

#[node(id = "sink_sum", inputs("value"))]
fn sink_sum(value: i32) -> Result<(), NodeError> {
    println!("sum doubled: {value}");
    Ok(())
}

#[node(id = "sink_negated", inputs("value"))]
fn sink_negated(value: i32) -> Result<(), NodeError> {
    println!("negated sum: {value}");
    Ok(())
}

declare_plugin!(
    NestedGraphsPlugin,
    "example.nested_graphs",
    [numbers, add_i32, negate, double, sink_sum, sink_negated]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = NestedGraphsPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let add = plugin.add_i32.alias("inner_add");
    let neg = plugin.negate.alias("inner_negate");

    // Build the inner graph. Host bridge ports define the nested interface.
    let inner_graph = GraphBuilder::new(&reg.registry)
        .host_bridge("inner")
        .node(&add)
        .node(&neg)
        .connect(&host_port("inner", "lhs"), &add.inputs.lhs)
        .connect(&host_port("inner", "rhs"), &add.inputs.rhs)
        .connect(&add.outputs.sum, &neg.inputs.value)
        .connect(&add.outputs.sum, &host_port("inner", "sum"))
        .connect(&neg.outputs.out, &host_port("inner", "negated"))
        .build();
    let nested = NestedGraph::new(inner_graph, "inner").expect("inner host bridge missing");

    let src = plugin.numbers.alias("numbers");
    let dbl = plugin.double.alias("double");
    let sink_sum_handle = plugin.sink_sum.alias("sum_sink");
    let sink_neg = plugin.sink_negated.alias("neg_sink");

    // Inline the inner graph so it behaves like a single stage inside the outer graph.
    let (builder, nested_handle) = GraphBuilder::new(&reg.registry)
        .node(&src)
        .nest(&nested, "grouped");

    let graph = builder
        .node(&dbl)
        .node(&sink_sum_handle)
        .node(&sink_neg)
        .connect(&src.outputs.lhs, &nested_handle.input("lhs"))
        .connect(&src.outputs.rhs, &nested_handle.input("rhs"))
        .connect(&nested_handle.output("sum"), &dbl.inputs.value)
        .connect(&nested_handle.output("negated"), &sink_neg.inputs.value)
        .connect(&dbl.outputs.out, &sink_sum_handle.inputs.value)
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.pool_size = None;
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
