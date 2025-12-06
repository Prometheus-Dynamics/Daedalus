//! Benchmark a graph loaded from JSON with a synthetic host-bridge image input.

use daedalus::{
    ErasedPayload, PluginLibrary,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    host_bridge::install_host_bridge,
    runtime::{executor::EdgePayload, host_bridge::HostBridgeManager},
    runtime::plugins::PluginRegistry,
};
use daedalus_data::model::Value as DaedalusValue;
use daedalus_planner::Graph;
use daedalus_runtime::executor::ExecutionTelemetry;
use image::{DynamicImage, ImageBuffer, Rgb};
use serde_json::json;
use std::env;
use std::fs;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_path = env::var("DAEDALUS_PLUGIN_PATH")
        .map_err(|_| "DAEDALUS_PLUGIN_PATH is required")?;
    let graph_path = env::var("DAEDALUS_GRAPH_PATH")
        .map_err(|_| "DAEDALUS_GRAPH_PATH is required")?;
    let runs: usize = env::var("DAEDALUS_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    let warmup: usize = env::var("DAEDALUS_WARMUP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let runtime_mode = env::var("DAEDALUS_RUNTIME_MODE").unwrap_or_else(|_| "serial".into());

    let graph_text = fs::read_to_string(&graph_path)?;
    let graph: Graph = serde_json::from_str(&graph_text)?;
    let graph = prune_host_output(graph);

    let input_port = env::var("DAEDALUS_INPUT_PORT").unwrap_or_else(|_| "frame".into());
    let mode_port = env::var("DAEDALUS_MODE_PORT")
        .ok()
        .filter(|v| !v.trim().is_empty());

    let host_alias = find_host_alias(&graph, &input_port)
        .unwrap_or_else(|| "host".to_string());

    let mut plugins = PluginRegistry::new();
    let host_mgr = HostBridgeManager::new();
    install_host_bridge(&mut plugins, host_mgr.clone())?;

    let lib = unsafe { PluginLibrary::load(std::path::Path::new(&plugin_path))? };
    lib.install_into(&mut plugins)?;

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = match runtime_mode.to_ascii_lowercase().as_str() {
        "parallel" => RuntimeMode::Parallel,
        _ => RuntimeMode::Serial,
    };
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

    let handlers = plugins.take_handlers();
    let mut exec = daedalus_runtime::executor::Executor::new(&runtime_plan, handlers)
        .with_host_bridges(mgr)
        .with_const_coercers(plugins.const_coercers.clone());

    for _ in 0..warmup {
        let mode_id = mode_port.as_deref().map(|port| {
            handle.push(
                port,
                EdgePayload::Value(daedalus_data::model::Value::String("auto".into())),
                None,
            )
        });
        let payload = ErasedPayload::from_cpu::<DynamicImage>(img.clone());
        handle.push(
            &input_port,
            EdgePayload::Payload(payload),
            mode_id,
        );
        let _ = exec.run_in_place()?;
    }

    let mut merged = ExecutionTelemetry::default();
    let mut graph_total = Duration::from_secs(0);
    let mut graph_samples = 0usize;
    let cpu_start = read_cpu_ticks();
    let rss_start = read_rss_bytes();

    for _ in 0..runs {
        let mode_id = mode_port.as_deref().map(|port| {
            handle.push(
                port,
                EdgePayload::Value(daedalus_data::model::Value::String("auto".into())),
                None,
            )
        });
        let payload = ErasedPayload::from_cpu::<DynamicImage>(img.clone());
        handle.push(
            &input_port,
            EdgePayload::Payload(payload),
            mode_id,
        );
        let telem = exec.run_in_place()?;
        graph_total += telem.graph_duration;
        graph_samples += 1;
        merged.merge(telem);
    }

    let cpu_end = read_cpu_ticks();
    let rss_end = read_rss_bytes();

    let mut node_rows = Vec::new();
    for (idx, node) in runtime_plan.nodes.iter().enumerate() {
        let metrics = merged.node_metrics.get(&idx);
        let (avg_ms, calls) = if let Some(metrics) = metrics {
            let calls = metrics.calls.max(1) as f64;
            (metrics.total_duration.as_secs_f64() * 1000.0 / calls, metrics.calls)
        } else {
            (0.0, 0)
        };
        node_rows.push(json!({
            "index": idx,
            "id": node.id.to_string(),
            "label": node.label.clone(),
            "avg_ms": avg_ms,
            "calls": calls,
        }));
    }

    let mut edge_rows = Vec::new();
    for (idx, (from, from_port, to, to_port, _)) in runtime_plan.edges.iter().enumerate() {
        let metrics = merged.edge_metrics.get(&idx);
        let (avg_ms, samples) = if let Some(metrics) = metrics {
            let samples = metrics.samples.max(1) as f64;
            (metrics.total_wait.as_secs_f64() * 1000.0 / samples, metrics.samples)
        } else {
            (0.0, 0)
        };
        edge_rows.push(json!({
            "index": idx,
            "from": format!("{}:{}", runtime_plan.nodes[from.0].id.to_string(), from_port),
            "to": format!("{}:{}", runtime_plan.nodes[to.0].id.to_string(), to_port),
            "avg_wait_ms": avg_ms,
            "samples": samples,
        }));
    }

    let graph_avg_ms = if graph_samples > 0 {
        graph_total.as_secs_f64() * 1000.0 / graph_samples as f64
    } else {
        0.0
    };

    let output = json!({
        "runs": runs,
        "warmup": warmup,
        "graph_avg_ms": graph_avg_ms,
        "nodes": node_rows,
        "edges": edge_rows,
        "rss_bytes_start": rss_start,
        "rss_bytes_end": rss_end,
        "cpu_ticks_start": cpu_start,
        "cpu_ticks_end": cpu_end,
        "cpu_ticks_delta": cpu_end.and_then(|end| cpu_start.map(|start| end.saturating_sub(start))),
    });

    println!("{}", serde_json::to_string_pretty(&output)?);
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

#[cfg(target_os = "linux")]
fn read_rss_bytes() -> Option<u64> {
    let data = fs::read_to_string("/proc/self/statm").ok()?;
    let mut parts = data.split_whitespace();
    let _total = parts.next()?;
    let rss_pages: u64 = parts.next()?.parse().ok()?;
    Some(rss_pages.saturating_mul(4096))
}

#[cfg(not(target_os = "linux"))]
fn read_rss_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn read_cpu_ticks() -> Option<u64> {
    let data = fs::read_to_string("/proc/self/stat").ok()?;
    let end = data.rfind(')')?;
    let after = data.get(end + 2..)?;
    let mut parts = after.split_whitespace();
    let _state = parts.next()?;
    for _ in 0..10 {
        parts.next()?;
    }
    let utime: u64 = parts.next()?.parse().ok()?;
    let stime: u64 = parts.next()?.parse().ok()?;
    Some(utime.saturating_add(stime))
}

#[cfg(not(target_os = "linux"))]
fn read_cpu_ticks() -> Option<u64> {
    None
}
