use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use daedalus_data::model::Value;
use daedalus_planner::{ComputeAffinity, NodeRef};

use crate::HOST_BRIDGE_META_KEY;
use crate::NodeError;
#[cfg(feature = "gpu")]
use crate::executor::RuntimeValue;
use crate::executor::crash_diag;
use crate::io::NodeIo;
use crate::perf;
use crate::state::ExecutionContext;
use std::panic::{AssertUnwindSafe, catch_unwind};

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

use super::errors::ExecuteError;
use super::{EdgePolicyKind, EdgeStorage, ExecutionTelemetry, MaybeGpu, RuntimeSegment};

#[cfg(not(feature = "executor-pool"))]
use super::{Executor, edge_maps};
use crate::bridge_handler;
#[cfg(not(feature = "executor-pool"))]
use std::collections::VecDeque;
#[cfg(not(feature = "executor-pool"))]
use std::thread;

#[allow(clippy::type_complexity)]
type PolicyList = Vec<(NodeRef, String, NodeRef, String, EdgePolicyKind)>;

fn is_host_bridge(node: &crate::plan::RuntimeNode) -> bool {
    matches!(
        node.metadata.get(HOST_BRIDGE_META_KEY),
        Some(daedalus_data::model::Value::Bool(true))
    )
}

#[cfg(not(feature = "executor-pool"))]
pub fn run<H: crate::executor::NodeHandler + Send + Sync + 'static>(
    exec: Executor<'_, H>,
) -> Result<ExecutionTelemetry, ExecuteError> {
    #[allow(unused_variables)]
    #[cfg(feature = "gpu")]
    let Executor {
        nodes,
        edges,
        _gpu_entries: _,
        _gpu_exits: _,
        gpu_entry_set,
        gpu_exit_set,
        data_edges,
        segments,
        schedule_order,
        const_inputs,
        backpressure,
        handler,
        state,
        gpu_available,
        gpu,
        queues,
        warnings_seen,
        mut telemetry,
        pool_size,
        host_bridges,
        const_coercers,
        output_movers,
        graph_metadata,
        active_nodes,
        host_outputs_in_graph,
        fail_fast,
        ..
    } = exec;

    #[allow(unused_variables)]
    #[cfg(not(feature = "gpu"))]
    let Executor {
        nodes,
        edges,
        segments,
        schedule_order,
        const_inputs,
        backpressure,
        handler,
        state,
        gpu_available,
        gpu,
        queues,
        warnings_seen,
        mut telemetry,
        pool_size,
        host_bridges,
        const_coercers,
        output_movers,
        graph_metadata,
        active_nodes,
        host_outputs_in_graph,
        fail_fast,
        ..
    } = exec;

    let graph_start = Instant::now();
    let metrics_level = telemetry.metrics_level;
    crash_diag::install_if_enabled(&nodes);

    let failed_nodes: Arc<Vec<AtomicBool>> = Arc::new(
        (0..nodes.len())
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>(),
    );

    let node_is_active = |idx: usize| {
        active_nodes
            .as_deref()
            .and_then(|v| v.get(idx).copied())
            .unwrap_or(true)
    };
    let node_exec_active = |idx: usize| {
        if !node_is_active(idx) {
            return false;
        }
        let Some(node) = nodes.get(idx) else {
            return false;
        };
        if !is_host_bridge(node) {
            return true;
        }
        host_outputs_in_graph && node.id.ends_with("io.host_output")
    };

    // Map node -> segment
    let mut segment_of = vec![0usize; nodes.len()];
    for (sid, seg) in segments.iter().enumerate() {
        for node in &seg.nodes {
            segment_of[node.0] = sid;
        }
    }

    // Precompute segment rank based on schedule_order for determinism.
    let mut segment_rank: Vec<usize> = vec![usize::MAX; segments.len()];
    for (rank, node_ref) in schedule_order.iter().enumerate() {
        if !node_exec_active(node_ref.0) {
            continue;
        }
        let s = segment_of[node_ref.0];
        if rank < segment_rank[s] {
            segment_rank[s] = rank;
        }
    }

    // Build segment dependency graph.
    let mut segment_active = vec![false; segments.len()];
    for (sid, seg) in segments.iter().enumerate() {
        segment_active[sid] = seg.nodes.iter().any(|n| node_exec_active(n.0));
    }
    let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); segments.len()];
    let mut indegree = vec![0usize; segments.len()];
    for (from, _, to, _, _) in edges.iter() {
        if !node_exec_active(from.0) || !node_exec_active(to.0) {
            continue;
        }
        if is_host_bridge(&nodes[from.0]) || is_host_bridge(&nodes[to.0]) {
            continue;
        }
        let a = segment_of[from.0];
        let b = segment_of[to.0];
        if !segment_active[a] || !segment_active[b] {
            continue;
        }
        if a != b && adj[a].insert(b) {
            indegree[b] += 1;
        }
    }

    let handler = handler.clone();
    let state = state.clone();
    let queues = queues.clone();
    let warnings = warnings_seen.clone();
    let any_conversion_cache = crate::io::new_any_conversion_cache();
    #[cfg(feature = "gpu")]
    let materialization_cache = crate::io::new_materialization_cache();
    let (incoming, outgoing) = edge_maps(edges);
    let policies: Arc<PolicyList> = Arc::new(edges.to_vec());
    let nodes = nodes.clone();
    let mut first_err: Option<ExecuteError> = None;
    let backpressure = backpressure.clone();
    let max_workers = pool_size
        .or_else(|| thread::available_parallelism().map(|n| n.get()).ok())
        .unwrap_or(4)
        .max(1)
        .min(segments.len().max(1));
    let host_nodes: Vec<NodeRef> = nodes
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
                .then_some(NodeRef(idx))
        })
        .collect();

    // Pre-pass: run host bridges to inject inbound payloads.
    #[cfg(feature = "gpu")]
    let gpu_clone = gpu.clone();
    #[cfg(not(feature = "gpu"))]
    let gpu_clone = gpu;
    run_host_bridges(
        &nodes,
        &exec.node_metadata,
        &host_nodes,
        &segment_of,
        &incoming,
        &outgoing,
        &queues,
        &warnings,
        &policies,
        &const_inputs,
        const_coercers.clone(),
        output_movers.clone(),
        any_conversion_cache.clone(),
        active_nodes.clone(),
        #[cfg(feature = "gpu")]
        materialization_cache.clone(),
        &mut telemetry,
        backpressure.clone(),
        &graph_start,
        #[cfg(feature = "gpu")]
        &gpu_entry_set,
        #[cfg(feature = "gpu")]
        &gpu_exit_set,
        #[cfg(feature = "gpu")]
        &data_edges,
        gpu_clone,
        host_bridges.clone(),
        state.clone(),
        graph_metadata.clone(),
        &failed_nodes,
        fail_fast,
        HostBridgePhase::Pre,
    )?;

    // Clone these once so inner `move` closures don't capture and move the originals.
    let active_nodes_mask = active_nodes.clone();
    let host_bridges_for_workers = host_bridges.clone();
    let host_outputs_in_graph_flag = host_outputs_in_graph;

    thread::scope(|scope| {
        let (tx, rx) =
            std::sync::mpsc::channel::<(usize, Result<ExecutionTelemetry, ExecuteError>)>();
        let mut ready: Vec<usize> = indegree
            .iter()
            .enumerate()
            .filter_map(|(i, deg)| {
                if *deg == 0 && segment_active[i] {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        ready.sort_by_key(|sid| segment_rank.get(*sid).copied().unwrap_or(usize::MAX));
        let mut ready: VecDeque<usize> = ready.into();
        let mut running = 0usize;

        #[allow(unused_variables)]
        let spawn =
            |seg_id: usize,
             tx: std::sync::mpsc::Sender<(usize, Result<ExecutionTelemetry, ExecuteError>)>,
             handler: Arc<H>,
             state: crate::state::StateStore,
             gpu: MaybeGpu,
             queues: Arc<Vec<EdgeStorage>>,
             incoming: Vec<Vec<usize>>,
             outgoing: Vec<Vec<usize>>,
             warnings: Arc<Mutex<HashSet<String>>>,
             policies: Arc<PolicyList>,
             backpressure: crate::plan::BackpressureStrategy|
             -> Result<(), ExecuteError> {
                let segment = segments
                    .get(seg_id)
                    .ok_or_else(|| ExecuteError::HandlerFailed {
                        node: format!("segment_{seg_id}"),
                        error: NodeError::Handler("segment missing".into()),
                    })?
                    .clone();
                let nodes = nodes.clone();
                let queues = queues.clone();
                let warnings = warnings.clone();
                let incoming = incoming.clone();
                let outgoing = outgoing.clone();
                let policies = policies.clone();
                let const_inputs = const_inputs.clone();
                let const_coercers = const_coercers.clone();
                let output_movers = output_movers.clone();
                let any_conversion_cache = any_conversion_cache.clone();
                #[cfg(feature = "gpu")]
                let materialization_cache = materialization_cache.clone();
                let graph_metadata = graph_metadata.clone();
                let node_metadata = exec.node_metadata.clone();
                #[cfg(feature = "gpu")]
                let gpu_entry_set = gpu_entry_set.clone();
                #[cfg(feature = "gpu")]
                let gpu_exit_set = gpu_exit_set.clone();
                #[cfg(feature = "gpu")]
                let data_edges = data_edges.clone();
                let txc = tx.clone();
                let active_nodes = active_nodes_mask.clone();
                let host_outputs_in_graph = host_outputs_in_graph_flag;
                let host_bridges = host_bridges_for_workers.clone();
                let failed_nodes = failed_nodes.clone();
                scope.spawn(move || {
                    let res = run_segment_external(
                        &nodes,
                        node_metadata,
                        segment,
                        handler,
                        state,
                        graph_metadata,
                        gpu,
                        queues,
                        &incoming,
                        &outgoing,
                        &warnings,
                        &policies,
                        seg_id,
                        backpressure.clone(),
                        gpu_available,
                        &const_inputs,
                        const_coercers,
                        output_movers,
                        any_conversion_cache,
                        active_nodes,
                        host_outputs_in_graph,
                        host_bridges,
                        failed_nodes,
                        fail_fast,
                        #[cfg(feature = "gpu")]
                        materialization_cache,
                        graph_start,
                        metrics_level,
                        #[cfg(feature = "gpu")]
                        &gpu_entry_set,
                        #[cfg(feature = "gpu")]
                        &gpu_exit_set,
                        #[cfg(feature = "gpu")]
                        &data_edges,
                    );
                    let _ = txc.send((seg_id, res));
                });
                Ok(())
            };

        while running < max_workers {
            if let Some(seg_id) = ready.pop_front() {
                #[cfg(feature = "gpu")]
                let gpu_arg = gpu.clone();
                #[cfg(not(feature = "gpu"))]
                let gpu_arg = gpu;
                spawn(
                    seg_id,
                    tx.clone(),
                    handler.clone(),
                    state.clone(),
                    gpu_arg,
                    queues.clone(),
                    incoming.clone(),
                    outgoing.clone(),
                    warnings.clone(),
                    policies.clone(),
                    backpressure.clone(),
                )?;
                running += 1;
            } else {
                break;
            }
        }

        while running > 0 {
            if let Ok((seg_done, res)) = rx.recv() {
                running -= 1;
                match res {
                    Ok(partial) => telemetry.merge(partial),
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }

                for &next in &adj[seg_done] {
                    indegree[next] -= 1;
                    if indegree[next] == 0 {
                        ready.push_back(next);
                    }
                }
                let mut ready_vec: Vec<_> = ready.iter().copied().collect();
                ready_vec.sort_by_key(|sid| segment_rank.get(*sid).copied().unwrap_or(usize::MAX));
                ready = ready_vec.into();

                while running < max_workers {
                    if let Some(seg_id) = ready.pop_front() {
                        #[cfg(feature = "gpu")]
                        let gpu_arg = gpu.clone();
                        #[cfg(not(feature = "gpu"))]
                        let gpu_arg = gpu;
                        spawn(
                            seg_id,
                            tx.clone(),
                            handler.clone(),
                            state.clone(),
                            gpu_arg,
                            queues.clone(),
                            incoming.clone(),
                            outgoing.clone(),
                            warnings.clone(),
                            policies.clone(),
                            backpressure.clone(),
                        )?;
                        running += 1;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok::<(), ExecuteError>(())
    })?;

    // Post-pass: always capture outputs for host bridges.
    //
    // Even when `host_outputs_in_graph` is enabled, schedule ordering can execute
    // `io.host_output` before its producers in demand-driven graphs. Running a final
    // post-pass guarantees late-produced payloads are drained to host outputs.
    run_host_bridges(
        &nodes,
        &exec.node_metadata,
        &host_nodes,
        &segment_of,
        &incoming,
        &outgoing,
        &queues,
        &warnings,
        &policies,
        &const_inputs,
        const_coercers,
        output_movers,
        any_conversion_cache.clone(),
        active_nodes.clone(),
        #[cfg(feature = "gpu")]
        materialization_cache.clone(),
        &mut telemetry,
        backpressure,
        &graph_start,
        #[cfg(feature = "gpu")]
        &gpu_entry_set,
        #[cfg(feature = "gpu")]
        &gpu_exit_set,
        #[cfg(feature = "gpu")]
        &data_edges,
        gpu,
        host_bridges,
        state,
        graph_metadata,
        &failed_nodes,
        fail_fast,
        HostBridgePhase::Post,
    )?;

    telemetry.graph_duration = graph_start.elapsed();
    telemetry.aggregate_groups(&nodes);

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(telemetry)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(not(feature = "executor-pool"))]
enum HostBridgePhase {
    Pre,
    Post,
}

#[cfg(not(feature = "executor-pool"))]
static HOST_BRIDGE_DIAG_COUNT: AtomicUsize = AtomicUsize::new(0);

#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(feature = "gpu"), allow(unused_variables))]
#[cfg(not(feature = "executor-pool"))]
fn run_host_bridges(
    nodes: &[crate::plan::RuntimeNode],
    node_metadata: &Arc<Vec<Arc<BTreeMap<String, Value>>>>,
    host_nodes: &[NodeRef],
    segment_of: &[usize],
    incoming: &[Vec<usize>],
    outgoing: &[Vec<usize>],
    queues: &Arc<Vec<EdgeStorage>>,
    warnings_seen: &Arc<Mutex<HashSet<String>>>,
    policies: &Arc<PolicyList>,
    const_inputs: &super::ConstInputStore,
    const_coercers: Option<crate::io::ConstCoercerMap>,
    output_movers: Option<crate::io::OutputMoverMap>,
    any_conversion_cache: crate::io::AnyConversionCacheHandle,
    active_nodes: Option<Arc<Vec<bool>>>,
    #[cfg(feature = "gpu")] materialization_cache: crate::io::MaterializationCacheHandle,
    telemetry: &mut ExecutionTelemetry,
    backpressure: crate::plan::BackpressureStrategy,
    graph_start: &Instant,
    #[cfg(feature = "gpu")] gpu_entry_set: &Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")] gpu_exit_set: &Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")] data_edges: &Arc<HashSet<usize>>,
    gpu: MaybeGpu,
    host_mgr: Option<crate::HostBridgeManager>,
    state: crate::state::StateStore,
    graph_metadata: Arc<BTreeMap<String, Value>>,
    failed_nodes: &Arc<Vec<AtomicBool>>,
    fail_fast: bool,
    phase: HostBridgePhase,
) -> Result<(), ExecuteError> {
    let Some(manager) = host_mgr else {
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
        if tracing::enabled!(tracing::Level::DEBUG) {
            let outgoing_desc: Vec<String> = outgoing
                .get(node_ref.0)
                .into_iter()
                .flat_map(|edges| edges.iter())
                .filter_map(|edge_idx| policies.get(*edge_idx))
                .filter_map(|(_, from_port, to, to_port, _)| {
                    let to_node = nodes.get(to.0)?;
                    let to_label = to_node.label.as_deref().unwrap_or(&to_node.id);
                    Some(format!("{from_port} -> {to_label}:{to_port}"))
                })
                .collect();
            let incoming_desc: Vec<String> = incoming
                .get(node_ref.0)
                .into_iter()
                .flat_map(|edges| edges.iter())
                .filter_map(|edge_idx| policies.get(*edge_idx))
                .filter_map(|(from, from_port, _, to_port, _)| {
                    let from_node = nodes.get(from.0)?;
                    let from_label = from_node.label.as_deref().unwrap_or(&from_node.id);
                    Some(format!("{from_label}:{from_port} -> {to_port}"))
                })
                .collect();
            tracing::debug!(
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
        let Some(node) = nodes.get(node_ref.0) else {
            continue;
        };
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
                    policies
                        .get(*edge_idx)
                        .map(|(from, _, _, _, _)| {
                            failed_nodes
                                .get(from.0)
                                .map(|f| f.load(Ordering::Relaxed))
                                .unwrap_or(false)
                        })
                        .unwrap_or(false)
                });
            if has_failed_dep {
                continue;
            }
        }
        #[allow(unused_mut)]
        let mut ctx = ExecutionContext {
            state: state.clone(),
            node_id: node.id.clone().into(),
            metadata: node_metadata[node_ref.0].clone(),
            graph_metadata: graph_metadata.clone(),
            #[cfg(feature = "gpu")]
            gpu: gpu.clone(),
        };
        let resources = ctx.resources();
        let _ = resources.before_frame();
        let metrics_level = telemetry.metrics_level;
        let const_inputs_guard = const_inputs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let const_inputs_slice = const_inputs_guard
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
        telemetry.start_node_call(node_ref.0);
        #[cfg(feature = "gpu")]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            policies,
            host_sync_groups.clone(),
            gpu_entry_set,
            gpu_exit_set,
            data_edges,
            segment_of.get(node_ref.0).copied().unwrap_or(0),
            node_ref.0,
            node.id.clone(),
            active_nodes.as_deref().map(|v| &**v),
            telemetry,
            backpressure.clone(),
            const_inputs_slice,
            const_coercers.clone(),
            output_movers.clone(),
            any_conversion_cache.clone(),
            Some(materialization_cache.clone()),
            gpu.clone(),
            node.compute,
        );
        #[cfg(not(feature = "gpu"))]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            policies,
            host_sync_groups,
            segment_of.get(node_ref.0).copied().unwrap_or(0),
            node_ref.0,
            node.id.clone(),
            active_nodes.as_deref().map(|v| &**v),
            telemetry,
            backpressure.clone(),
            const_inputs_slice,
            const_coercers.clone(),
            output_movers.clone(),
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
                tracing::debug!(
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
                        tracing::warn!("node perf counters disabled: {err}");
                    }
                    None
                }
            }
        } else {
            None
        };
        let node_start = Instant::now();
        let run_result = bridge(node, &ctx, &mut io);
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
            telemetry.record_node_resource_snapshot(node_ref.0, snapshot);
        }
        if let Some(sample) = perf_sample {
            telemetry.record_node_perf(node_ref.0, sample);
        }
        if let Some(cpu_start) = cpu_start
            && let Some(cpu_end) = crate::executor::thread_cpu_time()
        {
            telemetry.record_node_cpu_duration(node_ref.0, cpu_end.saturating_sub(cpu_start));
        }
        telemetry.record_node_duration(node_ref.0, elapsed);
        telemetry.record_trace_event(
            node_ref.0,
            node_start.saturating_duration_since(*graph_start),
            elapsed,
        );
        if let Err(e) = run_result {
            if let Some(f) = failed_nodes.get(node_ref.0) {
                f.store(true, Ordering::Relaxed);
            }
            telemetry.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: e.code().to_string(),
                message: e.to_string(),
            });
            if fail_fast {
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error: e,
                });
            }
        }
        if let Some(e) = flush_error {
            if let Some(f) = failed_nodes.get(node_ref.0) {
                f.store(true, Ordering::Relaxed);
            }
            telemetry.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: e.code().to_string(),
                message: e.to_string(),
            });
            if fail_fast {
                return Err(e);
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_segment_external<H: crate::executor::NodeHandler>(
    nodes: &[crate::plan::RuntimeNode],
    node_metadata: Arc<Vec<Arc<BTreeMap<String, Value>>>>,
    segment: RuntimeSegment,
    handler: Arc<H>,
    state: crate::state::StateStore,
    graph_metadata: Arc<BTreeMap<String, Value>>,
    gpu: MaybeGpu,
    queues: Arc<Vec<EdgeStorage>>,
    incoming: &[Vec<usize>],
    outgoing: &[Vec<usize>],
    warnings_seen: &Arc<Mutex<HashSet<String>>>,
    policies: &Arc<PolicyList>,
    seg_idx: usize,
    backpressure: crate::plan::BackpressureStrategy,
    gpu_available: bool,
    const_inputs: &super::ConstInputStore,
    const_coercers: Option<crate::io::ConstCoercerMap>,
    output_movers: Option<crate::io::OutputMoverMap>,
    any_conversion_cache: crate::io::AnyConversionCacheHandle,
    active_nodes: Option<Arc<Vec<bool>>>,
    host_outputs_in_graph: bool,
    host_bridges: Option<crate::HostBridgeManager>,
    failed_nodes: Arc<Vec<AtomicBool>>,
    fail_fast: bool,
    #[cfg(feature = "gpu")] materialization_cache: crate::io::MaterializationCacheHandle,
    graph_start: Instant,
    metrics_level: crate::executor::MetricsLevel,
    #[cfg(feature = "gpu")] gpu_entry_set: &Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")] gpu_exit_set: &Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")] data_edges: &Arc<HashSet<usize>>,
) -> Result<ExecutionTelemetry, ExecuteError> {
    #[cfg(not(feature = "gpu"))]
    let _ = &gpu;
    let mut telem = ExecutionTelemetry::with_level(metrics_level);

    match segment.compute {
        ComputeAffinity::CpuOnly => telem.cpu_segments += 1,
        ComputeAffinity::GpuPreferred => {
            if gpu_available {
                telem.gpu_segments += 1;
            } else {
                telem.gpu_fallbacks += 1;
                super::serial::record_warning(
                    &format!("gpu_preferred_fallback_cpu_seg_{seg_idx}"),
                    warnings_seen,
                    &mut telem,
                );
            }
        }
        ComputeAffinity::GpuRequired => {
            if !gpu_available {
                return Err(ExecuteError::GpuUnavailable {
                    segment: segment.nodes.clone(),
                });
            }
            telem.gpu_segments += 1;
        }
    }

    for node_ref in &segment.nodes {
        if let Some(active) = active_nodes.as_deref()
            && !active.get(node_ref.0).copied().unwrap_or(false)
        {
            continue;
        }
        let Some(node) = nodes.get(node_ref.0) else {
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
                    policies
                        .get(*edge_idx)
                        .map(|(from, _, _, _, _)| {
                            failed_nodes
                                .get(from.0)
                                .map(|f| f.load(Ordering::Relaxed))
                                .unwrap_or(false)
                        })
                        .unwrap_or(false)
                });
            if has_failed_dep {
                continue;
            }
        }

        if is_host_bridge(node) {
            if host_outputs_in_graph
                && node.id.ends_with("io.host_output")
                && let Some(manager) = host_bridges.clone()
            {
                let mut bridge = bridge_handler(manager);
                #[allow(unused_mut)]
                let mut ctx = ExecutionContext {
                    state: state.clone(),
                    node_id: node.id.clone().into(),
                    metadata: node_metadata[node_ref.0].clone(),
                    graph_metadata: graph_metadata.clone(),
                    #[cfg(feature = "gpu")]
                    gpu: gpu.clone(),
                };
                let resources = ctx.resources();
                let _ = resources.before_frame();

                let const_inputs_guard = const_inputs
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let const_inputs_slice = const_inputs_guard
                    .get(node_ref.0)
                    .map(|inputs| inputs.as_slice())
                    .unwrap_or(&[]);
                telem.start_node_call(node_ref.0);
                #[cfg(feature = "gpu")]
                let mut io = NodeIo::new(
                    incoming.get(node_ref.0).cloned().unwrap_or_default(),
                    outgoing.get(node_ref.0).cloned().unwrap_or_default(),
                    &queues,
                    warnings_seen,
                    policies,
                    // Host output nodes are "best effort" sinks: drain/forward whatever is
                    // available, without sync-group alignment across ports.
                    Vec::new(),
                    gpu_entry_set,
                    gpu_exit_set,
                    data_edges,
                    seg_idx,
                    node_ref.0,
                    node.id.clone(),
                    active_nodes.as_deref().map(|v| &**v),
                    &mut telem,
                    backpressure.clone(),
                    const_inputs_slice,
                    const_coercers.clone(),
                    output_movers.clone(),
                    any_conversion_cache.clone(),
                    Some(materialization_cache.clone()),
                    ctx.gpu.clone(),
                    node.compute,
                );
                #[cfg(not(feature = "gpu"))]
                let mut io = NodeIo::new(
                    incoming.get(node_ref.0).cloned().unwrap_or_default(),
                    outgoing.get(node_ref.0).cloned().unwrap_or_default(),
                    &queues,
                    warnings_seen,
                    policies,
                    // Host output nodes are "best effort" sinks: drain/forward whatever is
                    // available, without sync-group alignment across ports.
                    Vec::new(),
                    seg_idx,
                    node_ref.0,
                    node.id.clone(),
                    active_nodes.as_deref().map(|v| &**v),
                    &mut telem,
                    backpressure.clone(),
                    const_inputs_slice,
                    const_coercers.clone(),
                    output_movers.clone(),
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
                                tracing::warn!("node perf counters disabled: {err}");
                            }
                            None
                        }
                    }
                } else {
                    None
                };
                let node_start = Instant::now();
                let run_result = bridge(node, &ctx, &mut io);
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
                    telem.record_node_resource_snapshot(node_ref.0, snapshot);
                }

                if let Some(sample) = perf_sample {
                    telem.record_node_perf(node_ref.0, sample);
                }
                if let Some(cpu_start) = cpu_start
                    && let Some(cpu_end) = crate::executor::thread_cpu_time()
                {
                    telem.record_node_cpu_duration(node_ref.0, cpu_end.saturating_sub(cpu_start));
                }
                telem.record_node_duration(node_ref.0, elapsed);
                telem.record_trace_event(
                    node_ref.0,
                    node_start.saturating_duration_since(graph_start),
                    elapsed,
                );
                if let Err(e) = run_result {
                    if let Some(f) = failed_nodes.get(node_ref.0) {
                        f.store(true, Ordering::Relaxed);
                    }
                    telem.errors.push(crate::executor::NodeFailure {
                        node_idx: node_ref.0,
                        node_id: node.id.clone(),
                        code: e.code().to_string(),
                        message: e.to_string(),
                    });
                    if fail_fast {
                        return Err(ExecuteError::HandlerFailed {
                            node: node.id.clone(),
                            error: e,
                        });
                    }
                }
                if let Some(e) = flush_error {
                    if let Some(f) = failed_nodes.get(node_ref.0) {
                        f.store(true, Ordering::Relaxed);
                    }
                    telem.errors.push(crate::executor::NodeFailure {
                        node_idx: node_ref.0,
                        node_id: node.id.clone(),
                        code: e.code().to_string(),
                        message: e.to_string(),
                    });
                    if fail_fast {
                        return Err(e);
                    }
                }
                continue;
            } else {
                continue;
            }
        }

        #[allow(unused_mut)]
        let mut ctx = ExecutionContext {
            state: state.clone(),
            node_id: node.id.clone().into(),
            metadata: node_metadata[node_ref.0].clone(),
            graph_metadata: graph_metadata.clone(),
            #[cfg(feature = "gpu")]
            gpu: gpu.clone(),
        };
        let resources = ctx.resources();
        let _ = resources.before_frame();
        let detailed = metrics_level.is_detailed();
        let const_inputs_guard = const_inputs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let const_inputs_slice = const_inputs_guard
            .get(node_ref.0)
            .map(|inputs| inputs.as_slice())
            .unwrap_or(&[]);
        telem.start_node_call(node_ref.0);
        #[cfg(feature = "gpu")]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            &queues,
            warnings_seen,
            policies,
            node.sync_groups.clone(),
            gpu_entry_set,
            gpu_exit_set,
            data_edges,
            seg_idx,
            node_ref.0,
            node.id.clone(),
            active_nodes.as_deref().map(|v| &**v),
            &mut telem,
            backpressure.clone(),
            const_inputs_slice,
            const_coercers.clone(),
            output_movers.clone(),
            any_conversion_cache.clone(),
            Some(materialization_cache.clone()),
            ctx.gpu.clone(),
            node.compute,
        );
        #[cfg(not(feature = "gpu"))]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            &queues,
            warnings_seen,
            policies,
            node.sync_groups.clone(),
            seg_idx,
            node_ref.0,
            node.id.clone(),
            active_nodes.as_deref().map(|v| &**v),
            &mut telem,
            backpressure.clone(),
            const_inputs_slice,
            const_coercers.clone(),
            output_movers.clone(),
            any_conversion_cache.clone(),
        );

        if !io.sync_groups().is_empty() && io.inputs().is_empty() {
            continue;
        }

        let cpu_start = if detailed {
            crate::executor::thread_cpu_time()
        } else {
            None
        };
        let perf_guard = if perf::node_perf_enabled() {
            match perf::PerfCounterGuard::start() {
                Ok(guard) => Some(guard),
                Err(err) => {
                    if perf::disable_node_perf() {
                        tracing::warn!("node perf counters disabled: {err}");
                    }
                    None
                }
            }
        } else {
            None
        };
        let node_start = Instant::now();
        if node_trace_enabled() {
            let count = NODE_TRACE_DIAG_COUNT.fetch_add(1, Ordering::Relaxed);
            if count < 50 {
                tracing::debug!(
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
        crash_diag::set_current_node(node_ref.0);
        if let Err(error) = preflight_inputs(&ctx, &io) {
            if let Some(f) = failed_nodes.get(node_ref.0) {
                f.store(true, Ordering::Relaxed);
            }
            telem.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: error.code().to_string(),
                message: error.to_string(),
            });
            if fail_fast {
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error,
                });
            }
            continue;
        }
        let run_result = match catch_unwind(AssertUnwindSafe(|| handler.run(node, &ctx, &mut io))) {
            Ok(r) => r,
            Err(p) => {
                let msg = if let Some(s) = p.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = p.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "non-string panic payload".to_string()
                };
                if let Some(f) = failed_nodes.get(node_ref.0) {
                    f.store(true, Ordering::Relaxed);
                }
                telem.errors.push(crate::executor::NodeFailure {
                    node_idx: node_ref.0,
                    node_id: node.id.clone(),
                    code: "handler_panicked".to_string(),
                    message: msg.clone(),
                });
                if fail_fast {
                    return Err(ExecuteError::HandlerPanicked {
                        node: node.id.clone(),
                        message: msg,
                    });
                }
                continue;
            }
        };
        let elapsed = node_start.elapsed();
        let perf_sample = perf_guard.and_then(|guard| guard.finish().ok());
        let flush_error = if run_result.is_ok() {
            io.flush().err()
        } else {
            None
        };
        drop(io);
        let _ = resources.after_frame();
        if detailed && let Ok(snapshot) = resources.snapshot() {
            telem.record_node_resource_snapshot(node_ref.0, snapshot);
        }
        if let Some(sample) = perf_sample {
            telem.record_node_perf(node_ref.0, sample);
        }
        if let Some(cpu_start) = cpu_start
            && let Some(cpu_end) = crate::executor::thread_cpu_time()
        {
            telem.record_node_cpu_duration(node_ref.0, cpu_end.saturating_sub(cpu_start));
        }
        telem.record_node_duration(node_ref.0, elapsed);
        telem.record_trace_event(
            node_ref.0,
            node_start.saturating_duration_since(graph_start),
            elapsed,
        );
        if let Err(e) = run_result {
            if let Some(f) = failed_nodes.get(node_ref.0) {
                f.store(true, Ordering::Relaxed);
            }
            telem.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: e.code().to_string(),
                message: e.to_string(),
            });
            if fail_fast {
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error: e,
                });
            }
        }
        if let Some(e) = flush_error {
            if let Some(f) = failed_nodes.get(node_ref.0) {
                f.store(true, Ordering::Relaxed);
            }
            telem.errors.push(crate::executor::NodeFailure {
                node_idx: node_ref.0,
                node_id: node.id.clone(),
                code: e.code().to_string(),
                message: e.to_string(),
            });
            if fail_fast {
                return Err(e);
            }
        }
        telem.nodes_executed += 1;
    }

    Ok(telem)
}
