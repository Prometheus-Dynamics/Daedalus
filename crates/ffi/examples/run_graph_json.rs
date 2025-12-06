//! Execute a graph loaded from JSON with a single host-bridge image input.

use daedalus::{
    ErasedPayload, PluginLibrary,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    host_bridge::install_host_bridge,
    runtime::{executor::EdgePayload, host_bridge::HostBridgeManager},
    runtime::plugins::PluginRegistry,
};
use daedalus_runtime::executor::Executor;
use daedalus_data::model::Value as DaedalusValue;
use daedalus_planner::Graph;
use image::{DynamicImage, ImageBuffer, Rgb};
use std::env;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_path = env::var("DAEDALUS_PLUGIN_PATH")
        .map_err(|_| "DAEDALUS_PLUGIN_PATH is required")?;
    let graph_path = env::var("DAEDALUS_GRAPH_PATH")
        .map_err(|_| "DAEDALUS_GRAPH_PATH is required")?;

    let graph_text = fs::read_to_string(&graph_path)?;
    let graph: Graph = serde_json::from_str(&graph_text)?;
    let graph = prune_host_output(graph);

    let host_alias = find_host_alias(&graph, "frame")
        .unwrap_or_else(|| "host".to_string());

    let mut plugins = PluginRegistry::new();
    let host_mgr = HostBridgeManager::new();
    install_host_bridge(&mut plugins, host_mgr.clone())?;

    let lib = unsafe { PluginLibrary::load(std::path::Path::new(&plugin_path))? };
    lib.install_into(&mut plugins)?;

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Serial;
    cfg.gpu = GpuBackend::Cpu;
    cfg.planner.enable_gpu = false;
    let engine = Engine::new(cfg)?;

    let plan_out = engine.plan(&plugins.registry, graph)?;
    let runtime_plan = engine.build_runtime_plan(&plan_out.plan)?;
    let mgr = HostBridgeManager::from_plan(&runtime_plan);
    let handle = mgr
        .handle(&host_alias)
        .ok_or("host bridge handle missing")?;

    let width = env::var("DAEDALUS_IMG_W")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1280);
    let height = env::var("DAEDALUS_IMG_H")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(800);

    let img = DynamicImage::ImageRgb8(ImageBuffer::from_pixel(
        width,
        height,
        Rgb([7, 8, 9]),
    ));
    let ep = ErasedPayload::from_cpu::<DynamicImage>(img);
    handle.push("frame", EdgePayload::Payload(ep), None);

    let handlers = plugins.take_handlers();
    let exec = Executor::new(&runtime_plan, handlers)
        .with_host_bridges(mgr)
        .with_const_coercers(plugins.const_coercers.clone());
    let telemetry = exec.run()?;
    eprintln!("ok: executed graph, telemetry={:?}", telemetry);

    Ok(())
}

fn find_host_alias(graph: &Graph, port: &str) -> Option<String> {
    let target = port.to_ascii_lowercase();
    graph.nodes.iter().find_map(|node| {
        let is_host = matches!(node.metadata.get("host_bridge"), Some(DaedalusValue::Bool(true)));
        if !is_host {
            return None;
        }
        if !node.outputs.iter().any(|p| p.eq_ignore_ascii_case(&target)) {
            return None;
        }
        let alias = node
            .label
            .clone()
            .unwrap_or_else(|| node.id.0.to_string());
        Some(alias)
    })
}

fn prune_host_output(mut graph: Graph) -> Graph {
    let keep: Vec<bool> = graph
        .nodes
        .iter()
        .map(|n| n.id.0 != "io.host_output")
        .collect();
    if keep.iter().all(|v| *v) {
        return graph;
    }

    let mut map: Vec<Option<usize>> = vec![None; graph.nodes.len()];
    let mut new_nodes = Vec::with_capacity(graph.nodes.len());
    for (idx, node) in graph.nodes.into_iter().enumerate() {
        if keep[idx] {
            map[idx] = Some(new_nodes.len());
            new_nodes.push(node);
        }
    }

    let mut new_edges = Vec::new();
    for edge in graph.edges.into_iter() {
        let from = match map.get(edge.from.node.0).and_then(|v| *v) {
            Some(idx) => idx,
            None => continue,
        };
        let to = match map.get(edge.to.node.0).and_then(|v| *v) {
            Some(idx) => idx,
            None => continue,
        };
        new_edges.push(daedalus_planner::Edge {
            from: daedalus_planner::PortRef {
                node: daedalus_planner::NodeRef(from),
                port: edge.from.port,
            },
            to: daedalus_planner::PortRef {
                node: daedalus_planner::NodeRef(to),
                port: edge.to.port,
            },
            metadata: edge.metadata,
        });
    }

    graph.nodes = new_nodes;
    graph.edges = new_edges;
    graph
}
