use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::sync::OnceLock;

use crate::executor::{EdgeStorage, ExecuteError, Executor, edge_maps};
use crate::executor::crash_diag;
use crate::executor::NodeError;
#[cfg(feature = "gpu")]
use crate::executor::EdgePayload;
use crate::io::NodeIo;
use crate::state::ExecutionContext;
use crate::{HOST_BRIDGE_META_KEY, bridge_handler};
use daedalus_data::model::Value;
use daedalus_planner::ComputeAffinity;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HostBridgePhase {
    Pre,
    Post,
}

static HOST_BRIDGE_DIAG_COUNT: AtomicUsize = AtomicUsize::new(0);
static NODE_TRACE_DIAG_COUNT: AtomicUsize = AtomicUsize::new(0);
static NODE_TRACE_ENABLED: OnceLock<bool> = OnceLock::new();

fn node_trace_enabled() -> bool {
    *NODE_TRACE_ENABLED.get_or_init(|| {
        std::env::var("DAEDALUS_TRACE_NODES")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

fn preflight_inputs(_ctx: &ExecutionContext, _io: &NodeIo) -> Result<(), NodeError> {
    // If the node is running without a GPU context but is receiving GPU-resident payloads,
    // fail fast with an actionable error instead of letting downstream unsafe code crash.
    #[cfg(feature = "gpu")]
    {
        let ctx = _ctx;
        let io = _io;
        if ctx.gpu.is_none() {
            for (port, payload) in io.inputs() {
                match &payload.inner {
                    EdgePayload::GpuImage(_) => {
                        return Err(NodeError::InvalidInput(format!(
                            "port '{port}' contains a GPU image handle but no GPU context is available; configure a GPU backend or insert a CPU download/convert step"
                        )));
                    }
                    EdgePayload::Payload(ep) if ep.is_gpu() => {
                        return Err(NodeError::InvalidInput(format!(
                            "port '{port}' contains a GPU-resident payload but no GPU context is available; configure a GPU backend or insert a CPU download/convert step"
                        )));
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

pub fn run<H: crate::executor::NodeHandler>(
    mut exec: Executor<'_, H>,
) -> Result<crate::executor::ExecutionTelemetry, ExecuteError> {
    crash_diag::install_if_enabled(&exec.nodes);
    let (incoming, outgoing) = edge_maps(exec.edges);
    let queues = exec.queues.clone();
    let warnings_seen = exec.warnings_seen.clone();
    let host_nodes: Vec<daedalus_planner::NodeRef> = exec
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(idx, n)| {
            n.metadata
                .get(HOST_BRIDGE_META_KEY)
                .map(|v| matches!(v, Value::Bool(true)))
                .unwrap_or(false)
                .then_some(daedalus_planner::NodeRef(idx))
        })
        .collect();
    let graph_start = Instant::now();

    // Pre-pass: let host bridges inject inputs before other nodes run.
    run_host_bridges(
        &mut exec,
        &host_nodes,
        &incoming,
        &outgoing,
        &queues,
        &warnings_seen,
        HostBridgePhase::Pre,
    )?;

    for (seg_idx, segment) in exec.segments.iter().enumerate() {
        match segment.compute {
            ComputeAffinity::CpuOnly => {
                exec.telemetry.cpu_segments += 1;
                run_segment(
                    &mut exec,
                    seg_idx,
                    segment,
                    &host_nodes,
                    &incoming,
                    &outgoing,
                    &queues,
                    &warnings_seen,
                )?;
            }
            ComputeAffinity::GpuPreferred => {
                if exec.gpu_available {
                    exec.telemetry.gpu_segments += 1;
                    run_segment(
                        &mut exec,
                        seg_idx,
                        segment,
                        &host_nodes,
                        &incoming,
                        &outgoing,
                        &queues,
                        &warnings_seen,
                    )?;
                } else {
                    exec.telemetry.gpu_fallbacks += 1;
                    record_warning(
                        &format!("gpu_preferred_fallback_cpu_seg_{seg_idx}"),
                        &warnings_seen,
                        &mut exec.telemetry,
                    );
                    run_segment(
                        &mut exec,
                        seg_idx,
                        segment,
                        &host_nodes,
                        &incoming,
                        &outgoing,
                        &queues,
                        &warnings_seen,
                    )?;
                }
            }
            ComputeAffinity::GpuRequired => {
                if !exec.gpu_available {
                    return Err(ExecuteError::GpuUnavailable {
                        segment: segment.nodes.clone(),
                    });
                }
                exec.telemetry.gpu_segments += 1;
                run_segment(
                    &mut exec,
                    seg_idx,
                    segment,
                    &host_nodes,
                    &incoming,
                    &outgoing,
                    &queues,
                    &warnings_seen,
                )?;
            }
        }
    }

    // Post-pass: capture outputs destined for host bridges.
    run_host_bridges(
        &mut exec,
        &host_nodes,
        &incoming,
        &outgoing,
        &queues,
        &warnings_seen,
        HostBridgePhase::Post,
    )?;
    exec.telemetry.graph_duration = graph_start.elapsed();
    exec.telemetry.aggregate_groups(&exec.nodes);
    Ok(exec.telemetry)
}

#[allow(clippy::too_many_arguments)]
fn run_segment<H: crate::executor::NodeHandler>(
    exec: &mut Executor<'_, H>,
    seg_idx: usize,
    segment: &crate::plan::RuntimeSegment,
    host_nodes: &[daedalus_planner::NodeRef],
    incoming: &[Vec<usize>],
    outgoing: &[Vec<usize>],
    queues: &Arc<Vec<EdgeStorage>>,
    warnings_seen: &Arc<Mutex<HashSet<String>>>,
) -> Result<(), ExecuteError> {
    for node_ref in &segment.nodes {
        if host_nodes.contains(node_ref) {
            continue;
        }
        let Some(node) = exec.nodes.get(node_ref.0) else {
            continue;
        };

        let mut metadata: BTreeMap<String, Value> = node.metadata.clone();
        if let Some(label) = &node.label {
            metadata
                .entry("label".to_string())
                .or_insert_with(|| Value::String(label.clone().into()));
        }
        if let Some(bundle) = &node.bundle {
            metadata
                .entry("bundle".to_string())
                .or_insert_with(|| Value::String(bundle.clone().into()));
        }

        #[allow(unused_mut)]
        let mut ctx = ExecutionContext {
            state: exec.state.clone(),
            metadata,
            graph_metadata: exec.graph_metadata.clone(),
            #[cfg(feature = "gpu")]
            gpu: exec.gpu.clone(),
        };
        #[cfg(feature = "gpu")]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            exec.edges,
            node.sync_groups.clone(),
            &exec.gpu_entry_set,
            &exec.gpu_exit_set,
            seg_idx,
            node.id.clone(),
            &mut exec.telemetry,
            exec.backpressure.clone(),
            &exec.const_inputs[node_ref.0],
            exec.const_coercers.clone(),
            exec.output_packers.clone(),
            ctx.gpu.clone(),
            node.compute,
        );
        #[cfg(not(feature = "gpu"))]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            exec.edges,
            node.sync_groups.clone(),
            seg_idx,
            node.id.clone(),
            &mut exec.telemetry,
            exec.backpressure.clone(),
            &exec.const_inputs[node_ref.0],
            exec.const_coercers.clone(),
            exec.output_packers.clone(),
        );

        if log::log_enabled!(log::Level::Debug) && node.id == "cv:image:to_gray" {
            let has_image = io.get_any::<image::DynamicImage>("frame").is_some();
            let inputs: Vec<String> = io
                .inputs()
                .iter()
                .map(|(port, payload)| match &payload.inner {
                    crate::executor::EdgePayload::Any(any) => format!(
                        "{port}:Any({}) is_dynamic_image={}",
                        std::any::type_name_of_val(any.as_ref()),
                        any.is::<image::DynamicImage>()
                    ),
                    #[cfg(feature = "gpu")]
                    crate::executor::EdgePayload::Payload(ep) => format!("{port}:Payload({ep:?})"),
                    #[cfg(feature = "gpu")]
                    crate::executor::EdgePayload::GpuImage(_) => format!("{port}:GpuImage"),
                    crate::executor::EdgePayload::Bytes(bytes) => format!("{port}:Bytes({}b)", bytes.len()),
                    crate::executor::EdgePayload::Value(value) => format!("{port}:Value({value:?})"),
                    crate::executor::EdgePayload::Unit => format!("{port}:Unit"),
                })
                .collect();
            log::debug!("to_gray inputs = {:?} get_any={}", inputs, has_image);
        }

        if !io.sync_groups().is_empty() && io.inputs().is_empty() {
            continue;
        }

        let node_start = Instant::now();
        crash_diag::set_current_node(node_ref.0);
        preflight_inputs(&ctx, &io).map_err(|error| ExecuteError::HandlerFailed {
            node: node.id.clone(),
            error,
        })?;
        if node_trace_enabled() {
            let count = NODE_TRACE_DIAG_COUNT.fetch_add(1, Ordering::Relaxed);
            if count < 50 {
                log::debug!(
                    "daedalus-runtime: exec node seg={} idx={} id={} label={:?} inputs={:?}",
                    seg_idx,
                    node_ref.0,
                    node.id,
                    node.label,
                    io.inputs().iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>(),
                );
            }
        }
        let run_result = match catch_unwind(AssertUnwindSafe(|| exec.handler.run(node, &ctx, &mut io)))
        {
            Ok(r) => r,
            Err(p) => {
                let msg = if let Some(s) = p.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = p.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "non-string panic payload".to_string()
                };
                return Err(ExecuteError::HandlerPanicked {
                    node: node.id.clone(),
                    message: msg,
                });
            }
        };
        let elapsed = node_start.elapsed();
        match run_result {
            Ok(_) => {
                io.flush()?;
                exec.telemetry.record_node_duration(node_ref.0, elapsed);
            }
            Err(e) => {
                exec.telemetry.record_node_duration(node_ref.0, elapsed);
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error: e,
                });
            }
        }
        exec.telemetry.nodes_executed += 1;
    }
    Ok(())
}

fn run_host_bridges<H: crate::executor::NodeHandler>(
    exec: &mut Executor<'_, H>,
    host_nodes: &[daedalus_planner::NodeRef],
    incoming: &[Vec<usize>],
    outgoing: &[Vec<usize>],
    queues: &Arc<Vec<EdgeStorage>>,
    warnings_seen: &Arc<Mutex<HashSet<String>>>,
    phase: HostBridgePhase,
) -> Result<(), ExecuteError> {
    let Some(manager) = exec.host_bridges.clone() else {
        return Ok(());
    };
    let mut bridge = bridge_handler(manager);
    for node_ref in host_nodes {
        let has_incoming = incoming
            .get(node_ref.0)
            .is_some_and(|edges| !edges.is_empty());
        let has_outgoing = outgoing
            .get(node_ref.0)
            .is_some_and(|edges| !edges.is_empty());
        if log::log_enabled!(log::Level::Debug) {
            let outgoing_desc: Vec<String> = outgoing
                .get(node_ref.0)
                .into_iter()
                .flat_map(|edges| edges.iter())
                .filter_map(|edge_idx| exec.edges.get(*edge_idx))
                .filter_map(|(_, from_port, to, to_port, _)| {
                    let to_node = exec.nodes.get(to.0)?;
                    let to_label = to_node.label.as_deref().unwrap_or(&to_node.id);
                    Some(format!("{from_port} -> {to_label}:{to_port}"))
                })
                .collect();
            let incoming_desc: Vec<String> = incoming
                .get(node_ref.0)
                .into_iter()
                .flat_map(|edges| edges.iter())
                .filter_map(|edge_idx| exec.edges.get(*edge_idx))
                .filter_map(|(from, from_port, _, to_port, _)| {
                    let from_node = exec.nodes.get(from.0)?;
                    let from_label = from_node.label.as_deref().unwrap_or(&from_node.id);
                    Some(format!("{from_label}:{from_port} -> {to_port}"))
                })
                .collect();
            log::debug!(
                "host bridge edges node_id={} outgoing={:?} incoming={:?}",
                node_ref.0,
                outgoing_desc,
                incoming_desc
            );
        }
        match phase {
            HostBridgePhase::Pre if !has_outgoing => continue,
            HostBridgePhase::Post if !has_incoming => continue,
            _ => {}
        }
        let Some(node) = exec.nodes.get(node_ref.0) else {
            continue;
        };

        let mut metadata: BTreeMap<String, Value> = node.metadata.clone();
        if let Some(label) = &node.label {
            metadata
                .entry("label".to_string())
                .or_insert_with(|| Value::String(label.clone().into()));
        }
        if let Some(bundle) = &node.bundle {
            metadata
                .entry("bundle".to_string())
                .or_insert_with(|| Value::String(bundle.clone().into()));
        }

        #[allow(unused_mut)]
        let mut ctx = ExecutionContext {
            state: exec.state.clone(),
            metadata,
            graph_metadata: exec.graph_metadata.clone(),
            #[cfg(feature = "gpu")]
            gpu: exec.gpu.clone(),
        };
        #[cfg(feature = "gpu")]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            exec.edges,
            node.sync_groups.clone(),
            &exec.gpu_entry_set,
            &exec.gpu_exit_set,
            0,
            node.id.clone(),
            &mut exec.telemetry,
            exec.backpressure.clone(),
            &exec.const_inputs[node_ref.0],
            exec.const_coercers.clone(),
            exec.output_packers.clone(),
            ctx.gpu.clone(),
            node.compute,
        );
        #[cfg(not(feature = "gpu"))]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            exec.edges,
            node.sync_groups.clone(),
            0,
            node.id.clone(),
            &mut exec.telemetry,
            exec.backpressure.clone(),
            &exec.const_inputs[node_ref.0],
            exec.const_coercers.clone(),
            exec.output_packers.clone(),
        );

        if matches!(phase, HostBridgePhase::Post) && !io.sync_groups().is_empty() && io.inputs().is_empty() {
            continue;
        }
        if matches!(phase, HostBridgePhase::Post) && has_incoming {
            let count = HOST_BRIDGE_DIAG_COUNT.fetch_add(1, Ordering::Relaxed);
            if count < 5 {
                let ports: Vec<_> = io.inputs().iter().map(|(p, _)| p.as_str()).collect();
                log::debug!(
                    "host bridge post-pass inputs alias={} node_id={} ports={:?}",
                    node.label.as_deref().unwrap_or(&node.id),
                    node.id,
                    ports
                );
            }
        }

        let node_start = Instant::now();
        let run_result = bridge(node, &ctx, &mut io);
        let elapsed = node_start.elapsed();
        match run_result {
            Ok(_) => {
                io.flush()?;
                exec.telemetry.record_node_duration(node_ref.0, elapsed);
            }
            Err(e) => {
                exec.telemetry.record_node_duration(node_ref.0, elapsed);
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error: e,
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn record_warning(
    label: &str,
    seen: &Arc<Mutex<HashSet<String>>>,
    telem: &mut crate::executor::ExecutionTelemetry,
) {
    if let Ok(mut s) = seen.lock()
        && s.insert(label.to_string())
    {
        telem.warnings.push(label.to_string());
    }
}
