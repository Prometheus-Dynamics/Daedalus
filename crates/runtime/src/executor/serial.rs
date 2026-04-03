use std::collections::HashSet;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::executor::NodeError;
#[cfg(feature = "gpu")]
use crate::executor::RuntimeValue;
use crate::executor::crash_diag;
use crate::executor::{EdgeStorage, ExecuteError, Executor, edge_maps};
use crate::io::NodeIo;
use crate::perf;
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

fn node_exec_trace_filter() -> Option<String> {
    std::env::var("DAEDALUS_TRACE_NODE_EXEC_FILTER")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn node_exec_trace_enabled_for(node_id: &str) -> bool {
    if std::env::var_os("DAEDALUS_TRACE_NODE_EXEC_STDERR").is_none() {
        return false;
    }
    match node_exec_trace_filter() {
        None => true,
        Some(filter) => node_id.contains(&filter),
    }
}

fn host_bridge_trace_stderr_enabled() -> bool {
    std::env::var("DAEDALUS_TRACE_HOST_BRIDGE_STDERR")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
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
                    RuntimeValue::Any(any) if any.is::<daedalus_gpu::GpuImageHandle>() => {
                        return Err(NodeError::InvalidInput(format!(
                            "port '{port}' contains a GPU image handle but no GPU context is available; configure a GPU backend or insert a CPU download/convert step"
                        )));
                    }
                    RuntimeValue::Data(ep) if ep.is_gpu() => {
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
    let any_conversion_cache = crate::io::new_any_conversion_cache();
    #[cfg(feature = "gpu")]
    let materialization_cache = crate::io::new_materialization_cache();
    let active_nodes = exec.active_nodes.clone();
    let node_is_active = |idx: usize| {
        active_nodes
            .as_deref()
            .and_then(|v| v.get(idx).copied())
            .unwrap_or(true)
    };

    let host_nodes: Vec<daedalus_planner::NodeRef> = exec
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(idx, n)| {
            if !node_is_active(idx) {
                return None;
            }
            n.metadata
                .get(HOST_BRIDGE_META_KEY)
                .map(|v| matches!(v, Value::Bool(true)))
                .unwrap_or(false)
                .then_some(daedalus_planner::NodeRef(idx))
        })
        .collect();
    let graph_start = Instant::now();
    let mut failed_nodes = vec![false; exec.nodes.len()];

    // Pre-pass: let host bridges inject inputs before other nodes run.
    run_host_bridges(
        &mut exec,
        &host_nodes,
        &mut failed_nodes,
        &incoming,
        &outgoing,
        &queues,
        &warnings_seen,
        &any_conversion_cache,
        #[cfg(feature = "gpu")]
        &materialization_cache,
        &graph_start,
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
                    &active_nodes,
                    &mut failed_nodes,
                    &incoming,
                    &outgoing,
                    &queues,
                    &warnings_seen,
                    &any_conversion_cache,
                    #[cfg(feature = "gpu")]
                    &materialization_cache,
                    &graph_start,
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
                        &active_nodes,
                        &mut failed_nodes,
                        &incoming,
                        &outgoing,
                        &queues,
                        &warnings_seen,
                        &any_conversion_cache,
                        #[cfg(feature = "gpu")]
                        &materialization_cache,
                        &graph_start,
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
                        &active_nodes,
                        &mut failed_nodes,
                        &incoming,
                        &outgoing,
                        &queues,
                        &warnings_seen,
                        &any_conversion_cache,
                        #[cfg(feature = "gpu")]
                        &materialization_cache,
                        &graph_start,
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
                    &active_nodes,
                    &mut failed_nodes,
                    &incoming,
                    &outgoing,
                    &queues,
                    &warnings_seen,
                    &any_conversion_cache,
                    #[cfg(feature = "gpu")]
                    &materialization_cache,
                    &graph_start,
                )?;
            }
        }
    }

    // Post-pass: always capture outputs destined for host bridges.
    //
    // Even when `host_outputs_in_graph` is enabled, schedule ordering can execute
    // `io.host_output` before its producers in demand-driven graphs. Running a final
    // post-pass guarantees late-produced payloads are drained to host outputs.
    run_host_bridges(
        &mut exec,
        &host_nodes,
        &mut failed_nodes,
        &incoming,
        &outgoing,
        &queues,
        &warnings_seen,
        &any_conversion_cache,
        #[cfg(feature = "gpu")]
        &materialization_cache,
        &graph_start,
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
    active_nodes: &Option<Arc<Vec<bool>>>,
    failed_nodes: &mut [bool],
    incoming: &[Vec<usize>],
    outgoing: &[Vec<usize>],
    queues: &Arc<Vec<EdgeStorage>>,
    warnings_seen: &Arc<Mutex<HashSet<String>>>,
    any_conversion_cache: &crate::io::AnyConversionCacheHandle,
    #[cfg(feature = "gpu")] materialization_cache: &crate::io::MaterializationCacheHandle,
    graph_start: &Instant,
) -> Result<(), ExecuteError> {
    if std::env::var_os("DAEDALUS_TRACE_SCHEDULE").is_some() {
        let order: Vec<String> = segment
            .nodes
            .iter()
            .filter_map(|node_ref| {
                exec.nodes
                    .get(node_ref.0)
                    .map(|node| format!("{}:{}", node_ref.0, node.id))
            })
            .collect();
        log::warn!("segment order seg={} nodes={:?}", seg_idx, order);
    }
    for node_ref in &segment.nodes {
        if let Some(active) = active_nodes.as_deref()
            && !active.get(node_ref.0).copied().unwrap_or(false)
        {
            continue;
        }
        let Some(node) = exec.nodes.get(node_ref.0) else {
            continue;
        };
        // Error isolation: skip nodes that depend on a failed upstream node unless boundary.
        let is_error_boundary = node
            .metadata
            .get("daedalus.error_boundary")
            .map(|v| matches!(v, Value::Bool(true)))
            .unwrap_or(false);
        if !is_error_boundary {
            let has_failed_dep = incoming
                .get(node_ref.0)
                .into_iter()
                .flat_map(|v| v.iter())
                .any(|edge_idx| {
                    exec.edges
                        .get(*edge_idx)
                        .map(|(from, _, _, _, _)| {
                            failed_nodes.get(from.0).copied().unwrap_or(false)
                        })
                        .unwrap_or(false)
                });
            if has_failed_dep {
                failed_nodes[node_ref.0] = true;
                continue;
            }
        }
        if host_nodes.contains(node_ref) {
            // Optional responsiveness mode: execute `io.host_output` nodes in-graph (so fast outputs
            // can be published without waiting for unrelated slow branches).
            if exec.host_outputs_in_graph && node.id.ends_with("io.host_output") {
                run_host_output_in_graph(
                    exec,
                    *node_ref,
                    seg_idx,
                    incoming,
                    outgoing,
                    queues,
                    warnings_seen,
                    any_conversion_cache,
                    #[cfg(feature = "gpu")]
                    materialization_cache,
                    graph_start,
                )?;
            }
            continue;
        }
        #[allow(unused_mut)]
        let mut ctx = ExecutionContext {
            state: exec.state.clone(),
            node_id: node.id.clone().into(),
            metadata: exec.node_metadata[node_ref.0].clone(),
            graph_metadata: exec.graph_metadata.clone(),
            #[cfg(feature = "gpu")]
            gpu: exec.gpu.clone(),
        };
        let resources = ctx.resources();
        let _ = resources.before_frame();
        let metrics_level = exec.telemetry.metrics_level;
        let const_inputs_guard = exec
            .const_inputs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let const_inputs = const_inputs_guard
            .get(node_ref.0)
            .map(|inputs| inputs.as_slice())
            .unwrap_or(&[]);

        let incoming_edge_indices = incoming.get(node_ref.0).cloned().unwrap_or_default();
        if node_exec_trace_enabled_for(&node.id) {
            let mut sizes: Vec<String> = Vec::new();
            for edge_idx in &incoming_edge_indices {
                let len = match queues.get(*edge_idx) {
                    Some(EdgeStorage::Locked { queue, .. }) => {
                        queue.lock().ok().map(|q| q.len()).unwrap_or(0)
                    }
                    #[cfg(feature = "lockfree-queues")]
                    Some(EdgeStorage::BoundedLf { queue, .. }) => queue.len(),
                    None => 0,
                };
                sizes.push(format!("#{edge_idx}={len}"));
            }
            eprintln!(
                "daedalus-runtime: exec incoming queue sizes idx={} id={} sizes={:?}",
                node_ref.0, node.id, sizes
            );
        }

        exec.telemetry.start_node_call(node_ref.0);
        #[cfg(feature = "gpu")]
        let mut io = NodeIo::new(
            incoming_edge_indices,
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            exec.edges,
            node.sync_groups.clone(),
            &exec.gpu_entry_set,
            &exec.gpu_exit_set,
            &exec.data_edges,
            seg_idx,
            node_ref.0,
            node.id.clone(),
            active_nodes.as_deref().map(|v| &**v),
            &mut exec.telemetry,
            exec.backpressure.clone(),
            const_inputs,
            exec.const_coercers.clone(),
            exec.output_movers.clone(),
            any_conversion_cache.clone(),
            Some(materialization_cache.clone()),
            ctx.gpu.clone(),
            node.compute,
        );
        #[cfg(not(feature = "gpu"))]
        let mut io = NodeIo::new(
            incoming_edge_indices,
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            exec.edges,
            node.sync_groups.clone(),
            seg_idx,
            node_ref.0,
            node.id.clone(),
            active_nodes.as_deref().map(|v| &**v),
            &mut exec.telemetry,
            exec.backpressure.clone(),
            const_inputs,
            exec.const_coercers.clone(),
            exec.output_movers.clone(),
            any_conversion_cache.clone(),
        );

        if node_exec_trace_enabled_for(&node.id) {
            let in_edges = incoming.get(node_ref.0).map(|v| v.len()).unwrap_or(0);
            let out_edges = outgoing.get(node_ref.0).map(|v| v.len()).unwrap_or(0);
            let ports: Vec<&str> = io.inputs().iter().map(|(p, _)| p.as_str()).collect();
            let incoming_desc: Vec<String> = incoming
                .get(node_ref.0)
                .into_iter()
                .flat_map(|edges| {
                    edges.iter().filter_map(|edge_idx| {
                        exec.edges.get(*edge_idx).map(|edge| (*edge_idx, edge))
                    })
                })
                .filter_map(|(edge_idx, (from, from_port, _, to_port, _))| {
                    let from_node = exec.nodes.get(from.0)?;
                    Some(format!(
                        "#{edge_idx} {}:{} -> {to_port}",
                        from_node.id, from_port
                    ))
                })
                .collect();
            eprintln!(
                "daedalus-runtime: exec candidate seg={} idx={} id={} in_edges={} out_edges={} drained_inputs={:?} incoming={:?}",
                seg_idx, node_ref.0, node.id, in_edges, out_edges, ports, incoming_desc
            );
        }

        if log::log_enabled!(log::Level::Debug) && node.id == "cv:image:to_gray" {
            let has_image = io.get_any::<image::DynamicImage>("frame").is_some();
            let inputs: Vec<String> = io
                .inputs()
                .iter()
                .map(|(port, payload)| match &payload.inner {
                    #[cfg(feature = "gpu")]
                    crate::executor::RuntimeValue::Any(any)
                        if any.is::<daedalus_gpu::GpuImageHandle>() =>
                    {
                        format!("{port}:Any(GpuImageHandle)")
                    }
                    crate::executor::RuntimeValue::Any(any) => format!(
                        "{port}:Any({}) is_dynamic_image={}",
                        std::any::type_name_of_val(any.as_ref()),
                        any.is::<image::DynamicImage>()
                    ),
                    #[cfg(feature = "gpu")]
                    crate::executor::RuntimeValue::Data(ep) => format!("{port}:Data({ep:?})"),
                    crate::executor::RuntimeValue::Bytes(bytes) => {
                        format!("{port}:Bytes({}b)", bytes.len())
                    }
                    crate::executor::RuntimeValue::Value(value) => {
                        format!("{port}:Value({value:?})")
                    }
                    crate::executor::RuntimeValue::Unit => format!("{port}:Unit"),
                })
                .collect();
            let outgoing_desc: Vec<String> = outgoing
                .get(node_ref.0)
                .into_iter()
                .flat_map(|edges| {
                    edges.iter().filter_map(|edge_idx| {
                        exec.edges.get(*edge_idx).map(|edge| (*edge_idx, edge))
                    })
                })
                .filter_map(|(edge_idx, (_, from_port, to, to_port, _))| {
                    let to_node = exec.nodes.get(to.0)?;
                    let to_label = to_node.label.as_deref().unwrap_or(&to_node.id);
                    Some(format!("#{edge_idx} {from_port} -> {to_label}:{to_port}"))
                })
                .collect();
            let incoming_desc: Vec<String> = incoming
                .get(node_ref.0)
                .into_iter()
                .flat_map(|edges| {
                    edges.iter().filter_map(|edge_idx| {
                        exec.edges.get(*edge_idx).map(|edge| (*edge_idx, edge))
                    })
                })
                .filter_map(|(edge_idx, (from, from_port, _, to_port, _))| {
                    let from_node = exec.nodes.get(from.0)?;
                    let from_label = from_node.label.as_deref().unwrap_or(&from_node.id);
                    Some(format!("#{edge_idx} {from_label}:{from_port} -> {to_port}"))
                })
                .collect();
            log::debug!(
                "to_gray inputs={:?} get_any={} incoming={:?} outgoing={:?}",
                inputs,
                has_image,
                incoming_desc,
                outgoing_desc
            );
        }
        if log::log_enabled!(log::Level::Debug) && node.id == "cv:aruco:mask_downscale_gray" {
            let inputs: Vec<String> = io
                .inputs()
                .iter()
                .map(|(port, payload)| match &payload.inner {
                    #[cfg(feature = "gpu")]
                    crate::executor::RuntimeValue::Any(any)
                        if any.is::<daedalus_gpu::GpuImageHandle>() =>
                    {
                        format!("{port}:Any(GpuImageHandle)")
                    }
                    crate::executor::RuntimeValue::Any(any) => {
                        format!("{port}:Any({})", std::any::type_name_of_val(any.as_ref()))
                    }
                    #[cfg(feature = "gpu")]
                    crate::executor::RuntimeValue::Data(ep) => format!("{port}:Data({ep:?})"),
                    crate::executor::RuntimeValue::Bytes(bytes) => {
                        format!("{port}:Bytes({}b)", bytes.len())
                    }
                    crate::executor::RuntimeValue::Value(value) => {
                        format!("{port}:Value({value:?})")
                    }
                    crate::executor::RuntimeValue::Unit => format!("{port}:Unit"),
                })
                .collect();
            let outgoing_desc: Vec<String> = outgoing
                .get(node_ref.0)
                .into_iter()
                .flat_map(|edges| {
                    edges.iter().filter_map(|edge_idx| {
                        exec.edges.get(*edge_idx).map(|edge| (*edge_idx, edge))
                    })
                })
                .filter_map(|(edge_idx, (_, from_port, to, to_port, _))| {
                    let to_node = exec.nodes.get(to.0)?;
                    let to_label = to_node.label.as_deref().unwrap_or(&to_node.id);
                    Some(format!("#{edge_idx} {from_port} -> {to_label}:{to_port}"))
                })
                .collect();
            let incoming_desc: Vec<String> = incoming
                .get(node_ref.0)
                .into_iter()
                .flat_map(|edges| {
                    edges.iter().filter_map(|edge_idx| {
                        exec.edges.get(*edge_idx).map(|edge| (*edge_idx, edge))
                    })
                })
                .filter_map(|(edge_idx, (from, from_port, _, to_port, _))| {
                    let from_node = exec.nodes.get(from.0)?;
                    let from_label = from_node.label.as_deref().unwrap_or(&from_node.id);
                    Some(format!("#{edge_idx} {from_label}:{from_port} -> {to_port}"))
                })
                .collect();
            log::debug!(
                "mask_downscale inputs={:?} incoming={:?} outgoing={:?}",
                inputs,
                incoming_desc,
                outgoing_desc
            );
        }

        if !io.sync_groups().is_empty() && io.inputs().is_empty() {
            if node_exec_trace_enabled_for(&node.id) {
                eprintln!(
                    "daedalus-runtime: exec skip (sync_groups + no inputs) seg={} idx={} id={}",
                    seg_idx, node_ref.0, node.id
                );
            }
            continue;
        }

        let cpu_start = if metrics_level.is_detailed() {
            crate::executor::thread_cpu_time()
        } else {
            None
        };
        let perf_guard = if perf::node_perf_enabled() {
            match perf::PerfCounterGuard::start() {
                Ok(guard) => Some(guard),
                Err(err) => {
                    if perf::disable_node_perf() {
                        log::warn!("node perf counters disabled: {err}");
                    }
                    None
                }
            }
        } else {
            None
        };
        // Error isolation: skip nodes with a failed upstream dependency unless explicitly marked.
        let is_error_boundary = node
            .metadata
            .get("daedalus.error_boundary")
            .map(|v| matches!(v, Value::Bool(true)))
            .unwrap_or(false);
        if !is_error_boundary {
            let has_failed_dep = incoming
                .get(node_ref.0)
                .into_iter()
                .flat_map(|v| v.iter())
                .any(|edge_idx| {
                    exec.edges
                        .get(*edge_idx)
                        .map(|(from, _, _, _, _)| {
                            failed_nodes.get(from.0).copied().unwrap_or(false)
                        })
                        .unwrap_or(false)
                });
            if has_failed_dep {
                continue;
            }
        }

        let node_start = Instant::now();
        crash_diag::set_current_node(node_ref.0);
        if let Err(error) = preflight_inputs(&ctx, &io) {
            failed_nodes[node_ref.0] = true;
            exec.telemetry.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: error.code().to_string(),
                message: error.to_string(),
            });
            if exec.fail_fast {
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error,
                });
            }
            continue;
        }
        if node_trace_enabled() {
            let count = NODE_TRACE_DIAG_COUNT.fetch_add(1, Ordering::Relaxed);
            if count < 50 {
                log::debug!(
                    "daedalus-runtime: exec node seg={} idx={} id={} label={:?} inputs={:?}",
                    seg_idx,
                    node_ref.0,
                    node.id,
                    node.label,
                    io.inputs()
                        .iter()
                        .map(|(p, _)| p.as_str())
                        .collect::<Vec<_>>(),
                );
            }
        }
        let run_result =
            match catch_unwind(AssertUnwindSafe(|| exec.handler.run(node, &ctx, &mut io))) {
                Ok(r) => r,
                Err(p) => {
                    let msg = if let Some(s) = p.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else if let Some(s) = p.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "non-string panic payload".to_string()
                    };
                    failed_nodes[node_ref.0] = true;
                    exec.telemetry.errors.push(crate::executor::NodeFailure {
                        node_idx: node_ref.0,
                        node_id: node.id.clone(),
                        code: "handler_panicked".to_string(),
                        message: msg.clone(),
                    });
                    if exec.fail_fast {
                        return Err(ExecuteError::HandlerPanicked {
                            node: node.id.clone(),
                            message: msg,
                        });
                    }
                    // Skip downstream work for this node on this tick.
                    continue;
                }
            };
        if node_exec_trace_enabled_for(&node.id) {
            eprintln!(
                "daedalus-runtime: exec returned seg={} idx={} id={} ok={}",
                seg_idx,
                node_ref.0,
                node.id,
                run_result.is_ok()
            );
        }
        let elapsed = node_start.elapsed();
        let perf_sample = perf_guard.and_then(|guard| guard.finish().ok());
        let flush_error = if run_result.is_ok() {
            io.flush().err()
        } else {
            None
        };
        drop(io);
        let _ = resources.after_frame();
        if metrics_level.is_detailed()
            && let Ok(snapshot) = resources.snapshot()
        {
            exec.telemetry
                .record_node_resource_snapshot(node_ref.0, snapshot);
        }
        if let Some(sample) = perf_sample {
            exec.telemetry.record_node_perf(node_ref.0, sample);
        }
        if let Some(cpu_start) = cpu_start
            && let Some(cpu_end) = crate::executor::thread_cpu_time()
        {
            exec.telemetry
                .record_node_cpu_duration(node_ref.0, cpu_end.saturating_sub(cpu_start));
        }
        exec.telemetry.record_node_duration(node_ref.0, elapsed);
        exec.telemetry.record_trace_event(
            node_ref.0,
            node_start.saturating_duration_since(*graph_start),
            elapsed,
        );
        if let Err(e) = run_result {
            failed_nodes[node_ref.0] = true;
            exec.telemetry.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: e.code().to_string(),
                message: e.to_string(),
            });
            if exec.fail_fast {
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error: e,
                });
            }
        }
        if let Some(e) = flush_error {
            failed_nodes[node_ref.0] = true;
            exec.telemetry.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: e.code().to_string(),
                message: e.to_string(),
            });
            if exec.fail_fast {
                return Err(e);
            }
        }
        if node_exec_trace_enabled_for(&node.id) {
            eprintln!(
                "daedalus-runtime: exec done seg={} idx={} id={}",
                seg_idx, node_ref.0, node.id
            );
        }
        exec.telemetry.nodes_executed += 1;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_host_output_in_graph<H: crate::executor::NodeHandler>(
    exec: &mut Executor<'_, H>,
    node_ref: daedalus_planner::NodeRef,
    seg_idx: usize,
    incoming: &[Vec<usize>],
    outgoing: &[Vec<usize>],
    queues: &Arc<Vec<EdgeStorage>>,
    warnings_seen: &Arc<Mutex<HashSet<String>>>,
    any_conversion_cache: &crate::io::AnyConversionCacheHandle,
    #[cfg(feature = "gpu")] materialization_cache: &crate::io::MaterializationCacheHandle,
    graph_start: &Instant,
) -> Result<(), ExecuteError> {
    let Some(manager) = exec.host_bridges.clone() else {
        return Ok(());
    };
    let Some(node) = exec.nodes.get(node_ref.0) else {
        return Ok(());
    };
    if host_bridge_trace_stderr_enabled() {
        eprintln!(
            "daedalus-runtime: host_output_in_graph start seg={} idx={} id={}",
            seg_idx, node_ref.0, node.id
        );
    }
    let has_incoming = incoming
        .get(node_ref.0)
        .is_some_and(|edges| !edges.is_empty());
    if !has_incoming {
        return Ok(());
    }

    let mut bridge = bridge_handler(manager);

    #[allow(unused_mut)]
    let mut ctx = ExecutionContext {
        state: exec.state.clone(),
        node_id: node.id.clone().into(),
        metadata: exec.node_metadata[node_ref.0].clone(),
        graph_metadata: exec.graph_metadata.clone(),
        #[cfg(feature = "gpu")]
        gpu: exec.gpu.clone(),
    };
    let resources = ctx.resources();
    let _ = resources.before_frame();
    let metrics_level = exec.telemetry.metrics_level;
    let const_inputs_guard = exec
        .const_inputs
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let const_inputs = const_inputs_guard
        .get(node_ref.0)
        .map(|inputs| inputs.as_slice())
        .unwrap_or(&[]);
    exec.telemetry.start_node_call(node_ref.0);
    #[cfg(feature = "gpu")]
    let mut io = NodeIo::new(
        incoming.get(node_ref.0).cloned().unwrap_or_default(),
        outgoing.get(node_ref.0).cloned().unwrap_or_default(),
        queues,
        warnings_seen,
        exec.edges,
        // Host output nodes are "best effort" sinks: drain/forward whatever is available,
        // without sync-group alignment across ports. Sync-groups here can deadlock outputs
        // when branches produce at different rates.
        Vec::new(),
        &exec.gpu_entry_set,
        &exec.gpu_exit_set,
        &exec.data_edges,
        seg_idx,
        node_ref.0,
        node.id.clone(),
        exec.active_nodes.as_deref().map(|v| &**v),
        &mut exec.telemetry,
        exec.backpressure.clone(),
        const_inputs,
        exec.const_coercers.clone(),
        exec.output_movers.clone(),
        any_conversion_cache.clone(),
        Some(materialization_cache.clone()),
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
        // Host output nodes are "best effort" sinks: drain/forward whatever is available,
        // without sync-group alignment across ports. Sync-groups here can deadlock outputs
        // when branches produce at different rates.
        Vec::new(),
        seg_idx,
        node_ref.0,
        node.id.clone(),
        exec.active_nodes.as_deref().map(|v| &**v),
        &mut exec.telemetry,
        exec.backpressure.clone(),
        const_inputs,
        exec.const_coercers.clone(),
        exec.output_movers.clone(),
        any_conversion_cache.clone(),
    );

    let cpu_start = if metrics_level.is_detailed() {
        crate::executor::thread_cpu_time()
    } else {
        None
    };
    let perf_guard = if perf::node_perf_enabled() {
        match perf::PerfCounterGuard::start() {
            Ok(guard) => Some(guard),
            Err(err) => {
                if perf::disable_node_perf() {
                    log::warn!("node perf counters disabled: {err}");
                }
                None
            }
        }
    } else {
        None
    };
    let node_start = Instant::now();
    let run_result = bridge(node, &ctx, &mut io);
    if host_bridge_trace_stderr_enabled() {
        eprintln!(
            "daedalus-runtime: host_output_in_graph bridge returned seg={} idx={} id={} ok={}",
            seg_idx,
            node_ref.0,
            node.id,
            run_result.is_ok()
        );
    }
    let elapsed = node_start.elapsed();
    let perf_sample = perf_guard.and_then(|guard| guard.finish().ok());
    let flush_error = if run_result.is_ok() {
        io.flush().err()
    } else {
        None
    };
    drop(io);
    let _ = resources.after_frame();
    if metrics_level.is_detailed()
        && let Ok(snapshot) = resources.snapshot()
    {
        exec.telemetry
            .record_node_resource_snapshot(node_ref.0, snapshot);
    }
    if let Some(sample) = perf_sample {
        exec.telemetry.record_node_perf(node_ref.0, sample);
    }
    if let Some(cpu_start) = cpu_start
        && let Some(cpu_end) = crate::executor::thread_cpu_time()
    {
        exec.telemetry
            .record_node_cpu_duration(node_ref.0, cpu_end.saturating_sub(cpu_start));
    }
    exec.telemetry.record_node_duration(node_ref.0, elapsed);
    exec.telemetry.record_trace_event(
        node_ref.0,
        node_start.saturating_duration_since(*graph_start),
        elapsed,
    );
    if let Err(e) = run_result {
        exec.telemetry.errors.push(crate::executor::NodeFailure {
            node_idx: node_ref.0,
            node_id: node.id.clone(),
            code: e.code().to_string(),
            message: e.to_string(),
        });
        if exec.fail_fast {
            return Err(ExecuteError::HandlerFailed {
                node: node.id.clone(),
                error: e,
            });
        }
    }
    if let Some(e) = flush_error {
        exec.telemetry.errors.push(crate::executor::NodeFailure {
            node_idx: node_ref.0,
            node_id: node.id.clone(),
            code: e.code().to_string(),
            message: e.to_string(),
        });
        if exec.fail_fast {
            return Err(e);
        }
    }
    if host_bridge_trace_stderr_enabled() {
        eprintln!(
            "daedalus-runtime: host_output_in_graph done seg={} idx={} id={}",
            seg_idx, node_ref.0, node.id
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_host_bridges<H: crate::executor::NodeHandler>(
    exec: &mut Executor<'_, H>,
    host_nodes: &[daedalus_planner::NodeRef],
    failed_nodes: &mut [bool],
    incoming: &[Vec<usize>],
    outgoing: &[Vec<usize>],
    queues: &Arc<Vec<EdgeStorage>>,
    warnings_seen: &Arc<Mutex<HashSet<String>>>,
    any_conversion_cache: &crate::io::AnyConversionCacheHandle,
    #[cfg(feature = "gpu")] materialization_cache: &crate::io::MaterializationCacheHandle,
    graph_start: &Instant,
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
        if host_bridge_trace_stderr_enabled() {
            eprintln!(
                "daedalus-runtime: host_bridge phase={:?} start idx={} id={} has_incoming={} has_outgoing={}",
                phase, node_ref.0, node.id, has_incoming, has_outgoing
            );
        }

        // Error isolation: skip host nodes that depend on a failed upstream node unless boundary.
        let is_error_boundary = node
            .metadata
            .get("daedalus.error_boundary")
            .map(|v| matches!(v, Value::Bool(true)))
            .unwrap_or(false);
        if !is_error_boundary {
            let has_failed_dep = incoming
                .get(node_ref.0)
                .into_iter()
                .flat_map(|v| v.iter())
                .any(|edge_idx| {
                    exec.edges
                        .get(*edge_idx)
                        .map(|(from, _, _, _, _)| {
                            failed_nodes.get(from.0).copied().unwrap_or(false)
                        })
                        .unwrap_or(false)
                });
            if has_failed_dep {
                continue;
            }
        }

        #[allow(unused_mut)]
        let mut ctx = ExecutionContext {
            state: exec.state.clone(),
            node_id: node.id.clone().into(),
            metadata: exec.node_metadata[node_ref.0].clone(),
            graph_metadata: exec.graph_metadata.clone(),
            #[cfg(feature = "gpu")]
            gpu: exec.gpu.clone(),
        };
        let resources = ctx.resources();
        let _ = resources.before_frame();
        let metrics_level = exec.telemetry.metrics_level;
        let const_inputs_guard = exec
            .const_inputs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let const_inputs = const_inputs_guard
            .get(node_ref.0)
            .map(|inputs| inputs.as_slice())
            .unwrap_or(&[]);
        let use_best_effort_host_output_sync =
            matches!(phase, HostBridgePhase::Post) && node.id.ends_with("io.host_output");
        let host_sync_groups = if use_best_effort_host_output_sync {
            Vec::new()
        } else {
            node.sync_groups.clone()
        };
        exec.telemetry.start_node_call(node_ref.0);
        #[cfg(feature = "gpu")]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            exec.edges,
            host_sync_groups.clone(),
            &exec.gpu_entry_set,
            &exec.gpu_exit_set,
            &exec.data_edges,
            0,
            node_ref.0,
            node.id.clone(),
            exec.active_nodes.as_deref().map(|v| &**v),
            &mut exec.telemetry,
            exec.backpressure.clone(),
            const_inputs,
            exec.const_coercers.clone(),
            exec.output_movers.clone(),
            any_conversion_cache.clone(),
            Some(materialization_cache.clone()),
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
            host_sync_groups,
            0,
            node_ref.0,
            node.id.clone(),
            exec.active_nodes.as_deref().map(|v| &**v),
            &mut exec.telemetry,
            exec.backpressure.clone(),
            const_inputs,
            exec.const_coercers.clone(),
            exec.output_movers.clone(),
            any_conversion_cache.clone(),
        );

        if matches!(phase, HostBridgePhase::Post)
            && !io.sync_groups().is_empty()
            && io.inputs().is_empty()
        {
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

        let cpu_start = if metrics_level.is_detailed() {
            crate::executor::thread_cpu_time()
        } else {
            None
        };
        let perf_guard = if perf::node_perf_enabled() {
            match perf::PerfCounterGuard::start() {
                Ok(guard) => Some(guard),
                Err(err) => {
                    if perf::disable_node_perf() {
                        log::warn!("node perf counters disabled: {err}");
                    }
                    None
                }
            }
        } else {
            None
        };
        let node_start = Instant::now();
        let run_result = bridge(node, &ctx, &mut io);
        if host_bridge_trace_stderr_enabled() {
            eprintln!(
                "daedalus-runtime: host_bridge phase={:?} bridge returned idx={} id={} ok={}",
                phase,
                node_ref.0,
                node.id,
                run_result.is_ok()
            );
        }
        let elapsed = node_start.elapsed();
        let perf_sample = perf_guard.and_then(|guard| guard.finish().ok());
        let flush_error = if run_result.is_ok() {
            io.flush().err()
        } else {
            None
        };
        drop(io);
        let _ = resources.after_frame();
        if metrics_level.is_detailed()
            && let Ok(snapshot) = resources.snapshot()
        {
            exec.telemetry
                .record_node_resource_snapshot(node_ref.0, snapshot);
        }
        if let Some(sample) = perf_sample {
            exec.telemetry.record_node_perf(node_ref.0, sample);
        }
        if let Some(cpu_start) = cpu_start
            && let Some(cpu_end) = crate::executor::thread_cpu_time()
        {
            exec.telemetry
                .record_node_cpu_duration(node_ref.0, cpu_end.saturating_sub(cpu_start));
        }
        exec.telemetry.record_node_duration(node_ref.0, elapsed);
        exec.telemetry.record_trace_event(
            node_ref.0,
            node_start.saturating_duration_since(*graph_start),
            elapsed,
        );
        if let Err(e) = run_result {
            failed_nodes[node_ref.0] = true;
            exec.telemetry.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: e.code().to_string(),
                message: e.to_string(),
            });
            if exec.fail_fast {
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error: e,
                });
            }
        }
        if let Some(e) = flush_error {
            failed_nodes[node_ref.0] = true;
            exec.telemetry.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: e.code().to_string(),
                message: e.to_string(),
            });
            if exec.fail_fast {
                return Err(e);
            }
        }
        if host_bridge_trace_stderr_enabled() {
            eprintln!(
                "daedalus-runtime: host_bridge phase={:?} done idx={} id={}",
                phase, node_ref.0, node.id
            );
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
