//! Demonstrates sync groups using typed node signatures (no direct NodeIo/ctx):
//! - AllReady waits for correlated inputs (with an intentional delay)
//! - Latest emits immediately with newest per port
//! - Drop-oldest shows bounded behavior on bursty sources
//! - ZipByTag (now correlation-based) matches paired ints
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::engine::{Engine, EngineConfig};
use daedalus::runtime::{EdgePolicyKind, NodeError, io::NodeIo, plugins::RegistryPluginExt};
use daedalus::{
    BackpressureStrategy, PluginRegistry, SyncGroup, SyncPolicy, declare_plugin,
    graph_builder::GraphBuilder, macros::node,
};
use std::thread;
use std::time::Duration;

// Sources -------------------------------------------------------------------

#[node(
    id = "src_pair_delay",
    outputs(
        port(name = "a", ty = TypeExpr::Scalar(ValueType::Int)),
        port(name = "b", ty = TypeExpr::Scalar(ValueType::Int))
    )
)]
fn src_pair_delay(io: &mut NodeIo) -> Result<(), NodeError> {
    println!("src_pair_delay: producing a=1 then sleeping before b...");
    io.push_any(Some("a"), 1i32);
    thread::sleep(Duration::from_millis(800));
    io.push_any(Some("b"), 10i32);
    Ok(())
}

#[node(
    id = "src_burst",
    outputs(
        port(name = "a", ty = TypeExpr::Scalar(ValueType::Int)),
        port(name = "b", ty = TypeExpr::Scalar(ValueType::Int))
    )
)]
fn src_burst(io: &mut NodeIo) -> Result<(), NodeError> {
    // Emit several pairs quickly; bounded queues will drop oldest.
    for (a, b) in [(0, 0), (1, 10), (2, 20)] {
        io.push_any(Some("a"), a);
        io.push_any(Some("b"), b);
    }
    Ok(())
}

#[node(id = "src_simple_a", outputs("out"))]
fn src_simple_a() -> Result<i32, NodeError> {
    Ok(1)
}

#[node(id = "src_simple_b", outputs("out"))]
fn src_simple_b() -> Result<i32, NodeError> {
    Ok(100)
}

// Operators -----------------------------------------------------------------

#[node(
    id = "allready",
    inputs("a", "b"),
    outputs("out"),
    sync_groups(vec![SyncGroup {
        name: "ab".into(),
        policy: SyncPolicy::AllReady,
        backpressure: None,
        capacity: Some(1),
        ports: vec!["a".into(), "b".into()],
    }])
)]
fn all_ready(a: i32, b: i32) -> Result<String, NodeError> {
    Ok(format!("AllReady fired a={a} b={b}"))
}

#[node(
    id = "latest",
    inputs("a", "b"),
    outputs("out"),
    sync_groups(vec![SyncGroup {
        name: "ab".into(),
        policy: SyncPolicy::Latest,
        backpressure: None,
        capacity: None,
        ports: vec!["a".into(), "b".into()],
    }])
)]
fn latest(a: i32, b: i32) -> Result<String, NodeError> {
    Ok(format!("Latest fired a={a} b={b}"))
}

#[node(
    id = "zip",
    inputs("a", "b"),
    outputs("out"),
    sync_groups(vec![SyncGroup {
        name: "ab".into(),
        policy: SyncPolicy::ZipByTag,
        backpressure: None,
        capacity: None,
        ports: vec!["a".into(), "b".into()],
    }])
)]
fn zip(a: i32, b: i32) -> Result<String, NodeError> {
    Ok(format!("ZipByTag (correlation) fired a={a} b={b}"))
}

#[node(id = "sink", inputs("msg"))]
fn sink(msg: String) -> Result<(), NodeError> {
    println!("{msg}");
    Ok(())
}

// Plugin --------------------------------------------------------------------

declare_plugin!(
    SyncDemoPlugin,
    "example.sync",
    [
        src_pair_delay,
        src_burst,
        src_simple_a,
        src_simple_b,
        all_ready,
        latest,
        zip,
        sink
    ]
);

// Main ----------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = SyncDemoPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let mut cfg = EngineConfig::default();
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::BoundedQueues;
    let engine = Engine::new(cfg)?;

    // Graph 1: AllReady waits for both ports (b is delayed).
    let src = plugin.src_pair_delay.clone().alias("src");
    let all = plugin.all_ready.clone().alias("all");
    let sink_all = plugin.sink.clone().alias("sink_all");
    let g_allready = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&all)
        .connect(&src.outputs.a, &all.inputs.a)
        .connect(&src.outputs.b, &all.inputs.b)
        .node(&sink_all)
        .connect(&all.outputs.out, &sink_all.inputs.msg)
        .build();

    // Graph 2: Latest emits immediately with newest per port.
    let src = plugin.src_pair_delay.clone().alias("src");
    let latest_h = plugin.latest.clone().alias("latest");
    let sink_latest = plugin.sink.clone().alias("sink_latest");
    let g_latest = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&latest_h)
        .connect(&src.outputs.a, &latest_h.inputs.a)
        .connect(&src.outputs.b, &latest_h.inputs.b)
        .node(&sink_latest)
        .connect(&latest_h.outputs.out, &sink_latest.inputs.msg)
        .build();

    // Graph 3: Drop-oldest demo with bounded capacity (shows only newest pair survives).
    let src = plugin.src_burst.clone().alias("src");
    let all_drop = plugin.all_ready.clone().alias("all_drop");
    let sink_drop = plugin.sink.clone().alias("sink_drop");
    let g_drop = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&all_drop)
        .connect(&src.outputs.a, &all_drop.inputs.a)
        .connect(&src.outputs.b, &all_drop.inputs.b)
        .node(&sink_drop)
        .connect(&all_drop.outputs.out, &sink_drop.inputs.msg)
        .build();

    // Graph 4: ZipByTag (correlation-based) requires matching correlation ids, so use a single
    // multi-output source.
    let src = plugin.src_pair_delay.clone().alias("src");
    let zip_h = plugin.zip.clone().alias("zip");
    let sink_zip = plugin.sink.clone().alias("sink_zip");
    let g_zip = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&zip_h)
        .connect(&src.outputs.a, &zip_h.inputs.a)
        .connect(&src.outputs.b, &zip_h.inputs.b)
        .node(&sink_zip)
        .connect(&zip_h.outputs.out, &sink_zip.inputs.msg)
        .build();

    println!("-- AllReady --");
    engine.run(&reg.registry, g_allready, handlers.clone_arc())?;
    println!("-- Latest --");
    engine.run(&reg.registry, g_latest, handlers.clone_arc())?;
    println!("-- DropOldest --");
    engine.run(&reg.registry, g_drop, handlers.clone_arc())?;
    println!("-- ZipByTag --");
    engine.run(&reg.registry, g_zip, handlers)?;
    Ok(())
}
