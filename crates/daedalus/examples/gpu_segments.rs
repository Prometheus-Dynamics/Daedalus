//! Illustrates how compute affinity defaults surface in a graph and how to spot GPU-to-GPU
//! chains that can share device buffers. No pooling yet—this just identifies the spots.
//! Run with: `cargo run -p daedalus-rs --features "engine,plugins" --example gpu_segments`
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    declare_plugin,
    engine::{Engine, EngineConfig},
    graph_builder::GraphBuilder,
    macros::node,
    planner::Graph,
    runtime::{
        handles::NodeHandle,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

// GPU chain starter.
#[node(
    id = "gpu_a",
    outputs("buf"),
    compute(::daedalus::ComputeAffinity::GpuPreferred)
)]
fn gpu_a() -> Result<String, daedalus_runtime::NodeError> {
    Ok("gpu_a_output".into())
}

// GPU mid node.
#[node(
    id = "gpu_b",
    inputs("buf"),
    outputs("buf2"),
    compute(::daedalus::ComputeAffinity::GpuPreferred)
)]
fn gpu_b(buf: String) -> Result<String, daedalus_runtime::NodeError> {
    Ok(format!("{buf}_b"))
}

// GPU node declared on a CPU-style handler (no shader) to push the system.
#[node(
    id = "gpu_c",
    inputs("buf"),
    outputs("buf3"),
    compute(::daedalus::ComputeAffinity::GpuPreferred)
)]
fn gpu_c(buf: String) -> Result<String, daedalus_runtime::NodeError> {
    Ok(format!("{buf}_c"))
}

// CPU sink.
#[node(id = "cpu_sink", inputs("buf"))]
fn cpu_sink(buf: String) -> Result<(), daedalus_runtime::NodeError> {
    println!("CPU sink saw: {buf}");
    Ok(())
}

declare_plugin!(
    GpuSegPlugin,
    "example.gpu.seg",
    [gpu_a, gpu_b, gpu_c, cpu_sink]
);
// Extra GPU chains to demonstrate multiple buffers.
#[node(
    id = "gpu_d",
    outputs("buf"),
    compute(::daedalus::ComputeAffinity::GpuPreferred)
)]
fn gpu_d() -> Result<String, daedalus_runtime::NodeError> {
    Ok("gpu_d_output".into())
}

#[node(
    id = "gpu_e",
    inputs("buf"),
    outputs("buf2"),
    compute(::daedalus::ComputeAffinity::GpuPreferred)
)]
fn gpu_e(buf: String) -> Result<String, daedalus_runtime::NodeError> {
    Ok(format!("{buf}_e"))
}

#[node(
    id = "gpu_g",
    outputs("buf"),
    compute(::daedalus::ComputeAffinity::GpuPreferred)
)]
fn gpu_g() -> Result<String, daedalus_runtime::NodeError> {
    Ok("gpu_g_output".into())
}

declare_plugin!(
    ExtraGpuPlugin,
    "example.gpu.extra",
    [gpu_d, gpu_e, gpu_g, cpu_sink]
);

fn graph_from_registry(reg: &PluginRegistry) -> Graph {
    let a = NodeHandle::new("example.gpu.seg:gpu_a").alias("a");
    let b = NodeHandle::new("example.gpu.seg:gpu_b").alias("b");
    let c = NodeHandle::new("example.gpu.seg:gpu_c").alias("c");
    let d = NodeHandle::new("example.gpu.extra:gpu_d").alias("d");
    let e = NodeHandle::new("example.gpu.extra:gpu_e").alias("e");
    let g = NodeHandle::new("example.gpu.extra:gpu_g").alias("g");
    let cpu1 = NodeHandle::new("example.gpu.seg:cpu_sink").alias("cpu1");
    let cpu2 = NodeHandle::new("example.gpu.seg:cpu_sink").alias("cpu2");
    let cpu3 = NodeHandle::new("example.gpu.seg:cpu_sink").alias("cpu3");
    let cpu4 = NodeHandle::new("example.gpu.seg:cpu_sink").alias("cpu4");

    GraphBuilder::new(&reg.registry)
        .node(&a)
        .node(&b)
        .node(&c)
        .node(&d)
        .node(&e)
        .node(&g)
        .node(&cpu1)
        .node(&cpu2)
        .node(&cpu3)
        .node(&cpu4)
        // GPU chain: a -> b -> c (three buffers in flight)
        .connect("a:buf", "b:buf")
        .connect("b:buf2", "c:buf")
        // Split: c feeds cpu sink, and a also feeds a cpu sink directly.
        .connect("c:buf3", "cpu1:buf")
        .connect("a:buf", "cpu2:buf")
        // Second GPU chain: d -> e -> cpu sink
        .connect("d:buf", "e:buf")
        .connect("e:buf2", "cpu3:buf")
        // Third GPU node standalone -> cpu sink
        .connect("g:buf", "cpu4:buf")
        .build()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    reg.install_plugin(&GpuSegPlugin::new())?;
    reg.install_plugin(&ExtraGpuPlugin::new())?;

    let graph = graph_from_registry(&reg);

    println!("Nodes and their compute affinity:");
    for (idx, n) in graph.nodes.iter().enumerate() {
        println!(
            "  {} ({}) => {:?}",
            n.label.clone().unwrap_or_else(|| n.id.0.clone()),
            idx,
            n.compute
        );
    }

    let (segments, edge_info) = graph.gpu_buffers();

    println!("\nGPU buffer segments (shared across contiguous GPU nodes):");
    for seg in &segments {
        let names: Vec<String> = seg
            .nodes
            .iter()
            .map(|nr| {
                graph.nodes[nr.0]
                    .label
                    .clone()
                    .unwrap_or_else(|| graph.nodes[nr.0].id.0.clone())
            })
            .collect();
        println!("  buffer{}: {:?}", seg.buffer_id, names);
    }

    println!("\nEdges (gpu_gpu marks where a buffer is reused):");
    for info in &edge_info {
        let e = &graph.edges[info.edge_index];
        let from = &graph.nodes[e.from.node.0];
        let to = &graph.nodes[e.to.node.0];
        let buf = info
            .buffer_id
            .map(|b| format!("buffer{b}"))
            .unwrap_or_else(|| "cpu".into());
        println!(
            "  {}:{} -> {}:{}  gpu_gpu={} via {}",
            from.label.clone().unwrap_or_else(|| from.id.0.clone()),
            e.from.port,
            to.label.clone().unwrap_or_else(|| to.id.0.clone()),
            e.to.port,
            info.gpu_fast_path,
            buf
        );
    }

    // Run the graph to validate outputs when GPU chains hit CPU sinks.
    let handlers = reg.take_handlers();
    let engine = Engine::new(EngineConfig::default())?;
    let run = engine.run(&reg.registry, graph, handlers)?;
    println!(
        "\nExecution telemetry: nodes_executed={}, gpu_segments={}, gpu_fallbacks={}, cpu_segments={}",
        run.telemetry.nodes_executed,
        run.telemetry.gpu_segments,
        run.telemetry.gpu_fallbacks,
        run.telemetry.cpu_segments
    );

    Ok(())
}
