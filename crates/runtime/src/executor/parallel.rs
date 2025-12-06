use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use daedalus_data::model::Value;
use daedalus_planner::{ComputeAffinity, NodeRef};

use crate::HOST_BRIDGE_META_KEY;
use crate::executor::crash_diag;
#[cfg(feature = "gpu")]
use crate::executor::EdgePayload;
use crate::io::NodeIo;
use crate::NodeError;
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

use super::errors::ExecuteError;
use super::{EdgePolicyKind, EdgeStorage, ExecutionTelemetry, MaybeGpu, RuntimeSegment};

#[cfg(not(feature = "executor-pool"))]
#[cfg(not(feature = "executor-pool"))]
use super::{Executor, edge_maps};
#[cfg(not(feature = "executor-pool"))]
use crate::{HostBridgeManager, bridge_handler};
#[cfg(not(feature = "executor-pool"))]
use std::collections::VecDeque;
#[cfg(not(feature = "executor-pool"))]
use std::thread;

#[allow(clippy::type_complexity)]
type PolicyList = Vec<(NodeRef, String, NodeRef, String, EdgePolicyKind)>;

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
        output_packers,
        graph_metadata,
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
        output_packers,
        graph_metadata,
        ..
    } = exec;

    let graph_start = Instant::now();
    crash_diag::install_if_enabled(&nodes);

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
        let s = segment_of[node_ref.0];
        if rank < segment_rank[s] {
            segment_rank[s] = rank;
        }
    }

    // Build segment dependency graph.
    let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); segments.len()];
    let mut indegree = vec![0usize; segments.len()];
    for (from, _, to, _, _) in edges.iter() {
        if is_host_bridge(&nodes[from.0]) || is_host_bridge(&nodes[to.0]) {
            continue;
        }
        let a = segment_of[from.0];
        let b = segment_of[to.0];
        if a != b && adj[a].insert(b) {
            indegree[b] += 1;
        }
    }

    let handler = handler.clone();
    let state = state.clone();
    let queues = queues.clone();
    let warnings = warnings_seen.clone();
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
        &host_nodes,
        &segment_of,
        &incoming,
        &outgoing,
        &queues,
        &warnings,
        &policies,
        &const_inputs,
        const_coercers.clone(),
        output_packers.clone(),
        &mut telemetry,
        backpressure.clone(),
        #[cfg(feature = "gpu")]
        &gpu_entry_set,
        #[cfg(feature = "gpu")]
        &gpu_exit_set,
        gpu_clone,
        host_bridges.clone(),
        state.clone(),
        graph_metadata.clone(),
        HostBridgePhase::Pre,
    )?;

    thread::scope(|scope| {
        let (tx, rx) =
            std::sync::mpsc::channel::<(usize, Result<ExecutionTelemetry, ExecuteError>)>();
        let mut ready: Vec<usize> = indegree
            .iter()
            .enumerate()
            .filter_map(|(i, deg)| if *deg == 0 { Some(i) } else { None })
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
                let output_packers = output_packers.clone();
                let graph_metadata = graph_metadata.clone();
                #[cfg(feature = "gpu")]
                let gpu_entry_set = gpu_entry_set.clone();
                #[cfg(feature = "gpu")]
                let gpu_exit_set = gpu_exit_set.clone();
                let txc = tx.clone();
                scope.spawn(move || {
                    let res = run_segment_external(
                        &nodes,
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
                        output_packers,
                        #[cfg(feature = "gpu")]
                        &gpu_entry_set,
                        #[cfg(feature = "gpu")]
                        &gpu_exit_set,
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

    // Post-pass: capture outputs for host bridges.
    run_host_bridges(
        &nodes,
        &host_nodes,
        &segment_of,
        &incoming,
        &outgoing,
        &queues,
        &warnings,
        &policies,
        &const_inputs,
        const_coercers,
        output_packers,
        &mut telemetry,
        backpressure,
        #[cfg(feature = "gpu")]
        &gpu_entry_set,
        #[cfg(feature = "gpu")]
        &gpu_exit_set,
        gpu,
        host_bridges,
        state,
        graph_metadata,
        HostBridgePhase::Post,
    )?;

    telemetry.graph_duration = graph_start.elapsed();
    telemetry.aggregate_groups(&nodes);

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(telemetry)
}

#[cfg(not(feature = "executor-pool"))]
fn is_host_bridge(node: &crate::plan::RuntimeNode) -> bool {
    matches!(
        node.metadata.get(HOST_BRIDGE_META_KEY),
        Some(daedalus_data::model::Value::Bool(true))
    )
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
    host_nodes: &[NodeRef],
    segment_of: &[usize],
    incoming: &[Vec<usize>],
    outgoing: &[Vec<usize>],
    queues: &Arc<Vec<EdgeStorage>>,
    warnings_seen: &Arc<Mutex<HashSet<String>>>,
    policies: &Arc<PolicyList>,
    const_inputs: &Arc<Vec<Vec<(String, daedalus_data::model::Value)>>>,
    const_coercers: Option<crate::io::ConstCoercerMap>,
    output_packers: Option<crate::io::OutputPackerMap>,
    telemetry: &mut ExecutionTelemetry,
    backpressure: crate::plan::BackpressureStrategy,
    #[cfg(feature = "gpu")] gpu_entry_set: &Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")] gpu_exit_set: &Arc<HashSet<usize>>,
    gpu: MaybeGpu,
    host_mgr: Option<HostBridgeManager>,
    state: crate::state::StateStore,
    graph_metadata: Arc<BTreeMap<String, Value>>,
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
        if log::log_enabled!(log::Level::Debug) {
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
        let Some(node) = nodes.get(node_ref.0) else {
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
            state: state.clone(),
            metadata,
            graph_metadata: graph_metadata.clone(),
            #[cfg(feature = "gpu")]
            gpu: gpu.clone(),
        };
        #[cfg(feature = "gpu")]
        let mut io = NodeIo::new(
            incoming.get(node_ref.0).cloned().unwrap_or_default(),
            outgoing.get(node_ref.0).cloned().unwrap_or_default(),
            queues,
            warnings_seen,
            policies,
            node.sync_groups.clone(),
            gpu_entry_set,
            gpu_exit_set,
            segment_of.get(node_ref.0).copied().unwrap_or(0),
            node.id.clone(),
            telemetry,
            backpressure.clone(),
            &const_inputs[node_ref.0],
            const_coercers.clone(),
            output_packers.clone(),
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
            node.sync_groups.clone(),
            segment_of.get(node_ref.0).copied().unwrap_or(0),
            node.id.clone(),
            telemetry,
            backpressure.clone(),
            &const_inputs[node_ref.0],
            const_coercers.clone(),
            output_packers.clone(),
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
                telemetry.record_node_duration(node_ref.0, elapsed);
            }
            Err(e) => {
                telemetry.record_node_duration(node_ref.0, elapsed);
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error: e,
                });
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_segment_external<H: crate::executor::NodeHandler>(
    nodes: &[crate::plan::RuntimeNode],
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
    const_inputs: &Arc<Vec<Vec<(String, daedalus_data::model::Value)>>>,
    const_coercers: Option<crate::io::ConstCoercerMap>,
    output_packers: Option<crate::io::OutputPackerMap>,
    #[cfg(feature = "gpu")] gpu_entry_set: &Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")] gpu_exit_set: &Arc<HashSet<usize>>,
) -> Result<ExecutionTelemetry, ExecuteError> {
    #[cfg(not(feature = "gpu"))]
    let _ = &gpu;
    let mut telem = ExecutionTelemetry::default();

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
        let Some(node) = nodes.get(node_ref.0) else {
            continue;
        };
        if matches!(
            node.metadata.get(HOST_BRIDGE_META_KEY),
            Some(daedalus_data::model::Value::Bool(true))
        ) {
            continue;
        }

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
            state: state.clone(),
            metadata,
            graph_metadata: graph_metadata.clone(),
            #[cfg(feature = "gpu")]
            gpu: gpu.clone(),
        };
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
            seg_idx,
            node.id.clone(),
            &mut telem,
            backpressure.clone(),
            &const_inputs[node_ref.0],
            const_coercers.clone(),
            output_packers.clone(),
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
            node.id.clone(),
            &mut telem,
            backpressure.clone(),
            &const_inputs[node_ref.0],
            const_coercers.clone(),
            output_packers.clone(),
        );

        if !io.sync_groups().is_empty() && io.inputs().is_empty() {
            continue;
        }

        let node_start = Instant::now();
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
        crash_diag::set_current_node(node_ref.0);
        preflight_inputs(&ctx, &io).map_err(|error| ExecuteError::HandlerFailed {
            node: node.id.clone(),
            error,
        })?;
        let run_result = match catch_unwind(AssertUnwindSafe(|| handler.run(node, &ctx, &mut io)))
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
                telem.record_node_duration(node_ref.0, elapsed);
            }
            Err(e) => {
                telem.record_node_duration(node_ref.0, elapsed);
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error: e,
                });
            }
        }
        telem.nodes_executed += 1;
    }

    Ok(telem)
}
