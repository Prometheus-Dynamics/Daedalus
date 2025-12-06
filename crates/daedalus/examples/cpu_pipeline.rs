//! Minimal CPU-only pipeline using the facade + engine feature with node macros.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example cpu_pipeline
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

#[node(id = "pipeline_source", outputs("out"))]
fn source() -> Result<i32, NodeError> {
    Ok(1)
}

#[node(id = "pipeline_borrow", inputs("inp"), outputs("out"))]
fn borrow(inp: &i32) -> Result<i32, NodeError> {
    Ok(*inp)
}

#[node(id = "pipeline_mutate", inputs("inp"), outputs("out"))]
fn mutate(mut inp: i32) -> Result<i32, NodeError> {
    inp += 1;
    Ok(inp)
}

#[node(id = "pipeline_sink", inputs("inp"))]
fn sink(val: i32) -> Result<(), NodeError> {
    println!("pipeline result: {}", val);
    Ok(())
}

declare_plugin!(
    PipelinePlugin,
    "example.pipeline",
    [source, borrow, mutate, sink]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = PipelinePlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let src = plugin.source.alias("src");
    let brw = plugin.borrow.alias("brw");
    let mutn = plugin.mutate.alias("mut");
    let sk = plugin.sink.alias("sink");

    // Optional graph-level I/O markers (purely descriptive today).
    let graph_inputs = [src.outputs.out.clone()];
    let graph_outputs = [sk.inputs.inp.clone()];

    let graph = GraphBuilder::new(&reg.registry)
        .inputs(&graph_inputs)
        .outputs(&graph_outputs)
        .node(&src)
        .node(&brw)
        .node(&mutn)
        .node(&sk)
        .connect(&src.outputs.out, &brw.inputs.inp)
        .connect(&brw.outputs.out, &mutn.inputs.inp)
        .connect(&mutn.outputs.out, &sk.inputs.inp)
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
compile_error!("Enable the `engine` and `plugins` features to build this example");
