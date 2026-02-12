//! Demonstrates graph diffs (patches) applied to a running executor.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example graph_patch
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    data::model::Value,
    declare_plugin,
    graph_builder::GraphBuilder,
    macros::node,
    planner::{
        Graph, GraphMetadataSelector, GraphNodeSelector, GraphPatch, GraphPatchOp, PlannerConfig,
        PlannerInput, build_plan,
    },
    runtime::{
        NodeError, SchedulerConfig, build_runtime,
        executor::OwnedExecutor,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};
use std::{error::Error, sync::Arc};

#[node(id = "patch.source", outputs("out"))]
fn patch_source() -> Result<i32, NodeError> {
    Ok(10)
}

#[node(id = "patch.scale", inputs("value", "factor"), outputs("out"))]
fn patch_scale(value: i32, factor: i32) -> Result<i32, NodeError> {
    Ok(value * factor)
}

#[node(id = "patch.sink", inputs("value"))]
fn patch_sink(value: i32) -> Result<(), NodeError> {
    println!("patch_sink output: {value}");
    Ok(())
}

declare_plugin!(
    GraphPatchPlugin,
    "example.graph_patch",
    [patch_source, patch_scale, patch_sink]
);

fn const_inputs_for(graph: &Graph, marker: &Value) -> Vec<(String, Value)> {
    for node in &graph.nodes {
        if node
            .metadata
            .get("helios.ui.node_id")
            .is_some_and(|value| value == marker)
        {
            return node.const_inputs.clone();
        }
    }
    Vec::new()
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = GraphPatchPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let src = plugin.patch_source.alias("src");
    let scale = plugin.patch_scale.alias("scale");
    let sink = plugin.patch_sink.alias("sink");
    let node_marker = Value::String("scale-node".into());

    let graph = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&scale)
        .node(&sink)
        .connect(&src.outputs.out, &scale.inputs.value)
        .connect(&scale.outputs.out, &sink.inputs.value)
        .const_input(&scale.inputs.factor, Some(Value::Int(2)))
        .node_metadata_by_id("scale", "helios.ui.node_id", node_marker.clone())
        .build();

    println!(
        "const_inputs before patch: {:?}",
        const_inputs_for(&graph, &node_marker)
    );

    let patch = GraphPatch {
        version: 1,
        ops: vec![GraphPatchOp::SetNodeConst {
            node: GraphNodeSelector {
                metadata: Some(GraphMetadataSelector {
                    key: "helios.ui.node_id".to_string(),
                    value: node_marker.clone(),
                }),
                ..Default::default()
            },
            port: "factor".to_string(),
            value: Some(Value::Int(5)),
        }],
    };
    println!(
        "graph_patch diff:\n{}",
        serde_json::to_string_pretty(&patch)?
    );

    let mut patched_graph = graph.clone();
    let report = patch.apply_to_graph(&mut patched_graph);
    println!("patch report (graph): {:?}", report);
    println!(
        "const_inputs after patch: {:?}",
        const_inputs_for(&patched_graph, &node_marker)
    );

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &reg.registry,
        },
        PlannerConfig::default(),
    )
    .plan;
    let runtime_plan = build_runtime(&plan, &SchedulerConfig::default());
    let mut exec = OwnedExecutor::new(Arc::new(runtime_plan), handlers);

    println!("run 1 (factor=2):");
    exec.run_in_place()?;

    let runtime_report = exec.apply_patch(&patch);
    println!("patch report (runtime): {:?}", runtime_report);

    println!("run 2 (factor=5):");
    exec.run_in_place()?;
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
fn main() {
    eprintln!("enable `engine` and `plugins` features to run this example");
}
