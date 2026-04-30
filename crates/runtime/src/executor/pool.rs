use std::panic::{self, AssertUnwindSafe};
use std::sync::Mutex;
use std::sync::mpsc;

use super::{
    DirectSlotAccess, ExecuteError, ExecutionTelemetry, Executor, NodeHandler,
    compiled_worker_pool, panic_message, schedule::ParallelDagScheduler, segment_failure, serial,
};

pub fn run<H>(mut exec: Executor<'_, H>) -> Result<ExecutionTelemetry, ExecuteError>
where
    H: NodeHandler + Send + Sync + 'static,
{
    serial::inject_host_inputs(&mut exec)?;
    if exec.core.pool_workers <= 1 {
        let order = exec.schedule_order.to_vec();
        return serial::run_order(exec, &order);
    }

    let graph = &exec.schedule.host_deferred_graph;
    if graph.ready_segments.is_empty() {
        exec.core
            .telemetry
            .recompute_unattributed_runtime_duration();
        return Ok(std::mem::take(&mut exec.core.telemetry));
    }

    let pool = compiled_worker_pool(&exec.core.worker_pool, exec.core.pool_workers)?;
    let max_workers = exec.core.pool_workers.max(1);
    let (tx, rx) = mpsc::channel::<(usize, Result<ExecutionTelemetry, ExecuteError>)>();
    let rx = Mutex::new(rx);
    let mut segment_template = exec.snapshot();
    segment_template.direct_slot_access = DirectSlotAccess::Shared;
    let mut scheduler = ParallelDagScheduler::new("worker_pool", graph);

    pool.scope(|scope| {
        let spawn_segment = |segment_idx: usize, tx: mpsc::Sender<_>| {
            let (segment_exec, order) = segment_template.segment_snapshot(segment_idx);

            scope.spawn(move |_| {
                let segment_span = tracing::debug_span!(
                    target: "daedalus_runtime::executor",
                    "runtime_segment_run",
                    executor = "worker_pool",
                    segment = segment_idx,
                    nodes = order.len(),
                );
                let _segment_span = segment_span.enter();
                tracing::trace!(
                    target: "daedalus_runtime::executor",
                    executor = "worker_pool",
                    segment = segment_idx,
                    nodes = order.len(),
                    "parallel segment started"
                );
                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                    serial::run_order(segment_exec, &order)
                }))
                .unwrap_or_else(|panic| {
                    Err(ExecuteError::HandlerPanicked {
                        node: format!("segment_{segment_idx}"),
                        message: panic_message(panic),
                    })
                });
                match &result {
                    Ok(_) => tracing::trace!(
                        target: "daedalus_runtime::executor",
                        executor = "worker_pool",
                        segment = segment_idx,
                        "parallel segment finished"
                    ),
                    Err(error) => tracing::trace!(
                        target: "daedalus_runtime::executor",
                        executor = "worker_pool",
                        segment = segment_idx,
                        error = %error,
                        "parallel segment failed"
                    ),
                }
                let _ = tx.send((segment_idx, result));
            });
        };

        scheduler.spawn_ready(max_workers, |segment_idx| {
            spawn_segment(segment_idx, tx.clone());
        });

        while scheduler.has_running() {
            let received = {
                let Ok(receiver) = rx.lock() else {
                    return Err(ExecuteError::HandlerPanicked {
                        node: "executor_pool".into(),
                        message: "pool result receiver lock poisoned".into(),
                    });
                };
                receiver.recv()
            };
            let Ok((segment_idx, result)) = received else {
                break;
            };
            scheduler.complete_segment(segment_idx);
            match result {
                Ok(partial) => exec.core.telemetry.merge(partial),
                Err(error) if exec.core.run_config.fail_fast => return Err(error),
                Err(error) => {
                    exec.core
                        .telemetry
                        .errors
                        .push(segment_failure(segment_idx, &error));
                }
            }

            scheduler.spawn_ready(max_workers, |segment_idx| {
                spawn_segment(segment_idx, tx.clone());
            });

            if scheduler.is_drained() {
                break;
            }
        }

        scheduler.log_incomplete("daedalus-runtime: pool executor");
        Ok::<(), ExecuteError>(())
    })?;

    exec.core
        .telemetry
        .recompute_unattributed_runtime_duration();
    let nodes = exec.nodes.clone();
    exec.core.telemetry.aggregate_groups(&nodes);
    Ok(std::mem::take(&mut exec.core.telemetry))
}
