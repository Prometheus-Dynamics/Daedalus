use std::collections::{HashSet, VecDeque};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use daedalus_planner::NodeRef;
use rayon::ThreadPoolBuilder;

use super::errors::{ExecuteError, NodeError};
use super::parallel::run_segment_external;
use super::{EdgePolicyKind, EdgeStorage, ExecutionTelemetry, Executor, MaybeGpu, edge_maps};

type PolicyList = Vec<(NodeRef, String, NodeRef, String, EdgePolicyKind)>;

pub fn run<H>(exec: Executor<'_, H>) -> Result<ExecutionTelemetry, ExecuteError>
where
    H: crate::executor::NodeHandler + Send + Sync + 'static,
{
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
        #[cfg(feature = "gpu")]
        gpu_entry_set,
        #[cfg(feature = "gpu")]
        gpu_exit_set,
        #[cfg(feature = "gpu")]
        data_edges,
        const_coercers,
        output_movers,
        graph_metadata,
        active_nodes,
        host_outputs_in_graph,
        host_bridges,
        fail_fast,
        ..
    } = exec;

    let graph_start = Instant::now();
    let metrics_level = telemetry.metrics_level;
    let any_conversion_cache = crate::io::new_any_conversion_cache();
    let failed_nodes: Arc<Vec<AtomicBool>> = Arc::new(
        (0..nodes.len())
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>(),
    );
    #[cfg(feature = "gpu")]
    let materialization_cache = crate::io::new_materialization_cache();

    // Map node -> segment
    let mut segment_of = vec![0usize; nodes.len()];
    for (sid, seg) in segments.iter().enumerate() {
        for node in &seg.nodes {
            segment_of[node.0] = sid;
        }
    }

    let mut segment_rank: Vec<usize> = vec![usize::MAX; segments.len()];
    for (rank, node_ref) in schedule_order.iter().enumerate() {
        let s = segment_of[node_ref.0];
        if rank < segment_rank[s] {
            segment_rank[s] = rank;
        }
    }

    let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); segments.len()];
    let mut indegree = vec![0usize; segments.len()];
    for (from, _, to, _, _) in edges.iter() {
        let a = segment_of[from.0];
        let b = segment_of[to.0];
        if a != b && adj[a].insert(b) {
            indegree[b] += 1;
        }
    }

    let handler = handler.clone();
    let state = state.clone();
    let gpu = gpu.clone();
    let queues = queues.clone();
    let warnings = warnings_seen.clone();
    let (incoming, outgoing) = edge_maps(edges);
    let policies = Arc::new(edges.to_vec());
    let nodes = nodes.clone();
    let const_inputs = const_inputs.clone();
    let output_movers = output_movers.clone();
    let graph_metadata = graph_metadata.clone();
    let any_conversion_cache = any_conversion_cache.clone();
    let active_nodes = active_nodes.clone();
    let host_bridges = host_bridges.clone();
    let failed_nodes = failed_nodes.clone();
    #[cfg(feature = "gpu")]
    let gpu_entry_set = gpu_entry_set.clone();
    #[cfg(feature = "gpu")]
    let gpu_exit_set = gpu_exit_set.clone();
    #[cfg(feature = "gpu")]
    let data_edges = data_edges.clone();
    #[cfg(feature = "gpu")]
    let materialization_cache = materialization_cache.clone();
    let mut first_err: Option<ExecuteError> = None;
    let backpressure = backpressure.clone();

    let max_workers = pool_size
        .or_else(pool_size_override)
        .or_else(|| std::thread::available_parallelism().map(|n| n.get()).ok())
        .unwrap_or(4);
    let max_workers = max_workers.max(1).min(segments.len().max(1));

    // Build a pool per executor so concurrent engines can choose different sizes.
    let pool = ThreadPoolBuilder::new()
        .num_threads(max_workers)
        .build()
        .map_err(|e| ExecuteError::HandlerFailed {
            node: "pool_init".into(),
            error: NodeError::Handler(e.to_string()),
        })?;

    let (tx, rx) = std::sync::mpsc::channel::<(usize, Result<ExecutionTelemetry, ExecuteError>)>();
    let mut ready: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(i, deg)| if *deg == 0 { Some(i) } else { None })
        .collect();
    ready.sort_by_key(|sid| segment_rank.get(*sid).copied().unwrap_or(usize::MAX));
    let mut ready: VecDeque<usize> = ready.into();
    let mut running = 0usize;

    let spawn = |seg_id: usize,
                 tx: std::sync::mpsc::Sender<(usize, Result<ExecutionTelemetry, ExecuteError>)>,
                 handler: Arc<H>,
                 state: crate::state::StateStore,
                 gpu: MaybeGpu,
                 queues: Arc<Vec<EdgeStorage>>,
                 incoming: Vec<Vec<usize>>,
                 outgoing: Vec<Vec<usize>>,
                 warnings: Arc<Mutex<HashSet<String>>>,
                 policies: Arc<PolicyList>,
                 backpressure: crate::plan::BackpressureStrategy,
                 pool: &rayon::ThreadPool|
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
        let backpressure = backpressure.clone();
        let const_inputs = const_inputs.clone();
        let const_coercers = const_coercers.clone();
        let graph_metadata = graph_metadata.clone();
        let output_movers = output_movers.clone();
        let any_conversion_cache = any_conversion_cache.clone();
        let active_nodes = active_nodes.clone();
        let host_bridges = host_bridges.clone();
        let failed_nodes = failed_nodes.clone();
        #[cfg(feature = "gpu")]
        let gpu_entry_set = gpu_entry_set.clone();
        #[cfg(feature = "gpu")]
        let gpu_exit_set = gpu_exit_set.clone();
        #[cfg(feature = "gpu")]
        let data_edges = data_edges.clone();
        #[cfg(feature = "gpu")]
        let materialization_cache = materialization_cache.clone();
        let txc = tx.clone();
        pool.spawn(move || {
            let res = run_segment_external(
                &nodes,
                segment,
                handler,
                state,
                graph_metadata,
                gpu.clone(),
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
            spawn(
                seg_id,
                tx.clone(),
                handler.clone(),
                state.clone(),
                gpu.clone(),
                queues.clone(),
                incoming.clone(),
                outgoing.clone(),
                warnings.clone(),
                policies.clone(),
                backpressure.clone(),
                &pool,
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
                    spawn(
                        seg_id,
                        tx.clone(),
                        handler.clone(),
                        state.clone(),
                        gpu.clone(),
                        queues.clone(),
                        incoming.clone(),
                        outgoing.clone(),
                        warnings.clone(),
                        policies.clone(),
                        backpressure.clone(),
                        &pool,
                    )?;
                    running += 1;
                } else {
                    break;
                }
            }
        }
    }

    telemetry.graph_duration = graph_start.elapsed();
    telemetry.aggregate_groups(&nodes);

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(telemetry)
}

fn pool_size_override() -> Option<usize> {
    std::env::var("DAEDALUS_RUNTIME_POOL_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
}
