//! Execute the Helios Aruco mask tuning graph locally with the CV plugin.

use daedalus::{
    ErasedPayload, PluginLibrary,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    host_bridge::install_host_bridge,
    runtime::{executor::EdgePayload, host_bridge::HostBridgeManager},
    runtime::plugins::PluginRegistry,
};
use daedalus_runtime::host_bridge::HOST_BRIDGE_META_KEY;
use daedalus_data::model::Value as DaedalusValue;
use daedalus_planner::{Edge, Graph, NodeRef, PortRef};
use daedalus_registry::store::NodeDescriptorBuilder;
use image::GrayImage;
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
    let graph = rewrite_graph_for_local(graph);

    let host_alias = find_host_alias(&graph, "frame")
        .unwrap_or_else(|| "host".to_string());

    let mut plugins = PluginRegistry::new();
    let host_mgr = HostBridgeManager::new();
    install_host_bridge(&mut plugins, host_mgr.clone())?;
    install_host_output(&mut plugins, host_mgr.clone())?;

    let lib = unsafe { PluginLibrary::load(std::path::Path::new(&plugin_path))? };
    lib.install_into(&mut plugins)?;

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Serial;
    cfg.gpu = GpuBackend::Cpu;
    cfg.planner.enable_gpu = false;
    let engine = Engine::new(cfg)?;

    let plan_out = engine.plan(&plugins.registry, graph.clone())?;
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
    let exec = daedalus_runtime::executor::Executor::new(&runtime_plan, handlers)
        .with_host_bridges(mgr.clone())
        .with_const_coercers(plugins.const_coercers.clone());
    let telemetry = exec.run()?;
    eprintln!("ok: executed graph, telemetry={:?}", telemetry);

    for alias in find_output_aliases(&graph) {
        let Some(out) = mgr.handle(&alias) else { continue };
        for port in out.incoming_ports() {
            let Some(payload) = out.try_pop(port.name()) else {
                eprintln!("output {}:{} -> empty", alias, port.name());
                continue;
            };
            match payload.inner {
                EdgePayload::Any(any) => {
                    let ty = std::any::type_name_of_val(any.as_ref());
                    let is_dyn = any.is::<DynamicImage>();
                    let is_gray = any.is::<GrayImage>();
                    let is_arc_any = any.is::<std::sync::Arc<dyn std::any::Any + Send + Sync>>();
                    let is_arc_dyn = any.is::<std::sync::Arc<DynamicImage>>();
                    let is_arc_gray = any.is::<std::sync::Arc<GrayImage>>();
                    eprintln!(
                        "output {}:{} -> Any type={} dyn={} gray={} arc_any={} arc_dyn={} arc_gray={}",
                        alias,
                        port.name(),
                        ty,
                        is_dyn,
                        is_gray,
                        is_arc_any,
                        is_arc_dyn,
                        is_arc_gray
                    );
                }
                other => {
                    eprintln!(
                        "output {}:{} -> payload={:?}",
                        alias,
                        port.name(),
                        other
                    );
                }
            }
        }
    }

    Ok(())
}

fn rewrite_graph_for_local(mut graph: Graph) -> Graph {
    let grayscale_id = "cv:color:grayscale_arc";
    let replacement_id = "cv:image:to_gray";

    for node in &mut graph.nodes {
        if node.id.0 == grayscale_id {
            node.id.0 = replacement_id.to_string();
            node.inputs.retain(|p| p != "mode");
            node.const_inputs.retain(|(name, _)| name != "mode");
        }
        if node.id.0 == "io.host_bridge" {
            node.outputs.retain(|p| p != "mode");
        }
    }

    graph.edges.retain(|edge| edge.from.port != "mode" && edge.to.port != "mode");

    graph
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

fn find_output_aliases(graph: &Graph) -> Vec<String> {
    let mut out = Vec::new();
    for node in &graph.nodes {
        let is_host = matches!(node.metadata.get("host_bridge"), Some(DaedalusValue::Bool(true)));
        if !is_host {
            continue;
        }
        if node.id.0 != "io.host_output" {
            continue;
        }
        let alias = node
            .label
            .clone()
            .unwrap_or_else(|| node.id.0.to_string());
        out.push(alias);
    }
    out
}

fn install_host_output(
    registry: &mut PluginRegistry,
    manager: HostBridgeManager,
) -> Result<(), &'static str> {
    let qualified_id = if let Some(prefix) = registry.current_prefix.clone() {
        format!("{prefix}:io.host_output")
    } else {
        "io.host_output".to_string()
    };

    let desc = NodeDescriptorBuilder::new(&qualified_id)
        .metadata(HOST_BRIDGE_META_KEY, DaedalusValue::Bool(true))
        .metadata("dynamic_inputs", DaedalusValue::String("generic".into()))
        .build()
        .map_err(|_| "host output descriptor build failed")?;
    registry
        .registry
        .register_node(desc)
        .map_err(|_| "host output descriptor register failed")?;

    let mut handler = daedalus::runtime::host_bridge::bridge_handler(manager);
    registry
        .handlers
        .on_stateful(&qualified_id, move |node, ctx, io| handler(node, ctx, io));
    Ok(())
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

    let mut new_edges: Vec<Edge> = Vec::new();
    for edge in graph.edges.into_iter() {
        let from = match map.get(edge.from.node.0).and_then(|v| *v) {
            Some(idx) => idx,
            None => continue,
        };
        let to = match map.get(edge.to.node.0).and_then(|v| *v) {
            Some(idx) => idx,
            None => continue,
        };
        new_edges.push(Edge {
            from: PortRef {
                node: NodeRef(from),
                port: edge.from.port,
            },
            to: PortRef {
                node: NodeRef(to),
                port: edge.to.port,
            },
            metadata: edge.metadata,
        });
    }

    graph.nodes = new_nodes;
    graph.edges = new_edges;
    graph
}
