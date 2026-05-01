use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc;

use super::{
    DirectSlotAccess, ExecuteError, ExecutionTelemetry, Executor, NodeHandler, panic_message,
    schedule::ParallelDagScheduler, segment_failure, serial,
};

pub fn run<H>(exec: Executor<'_, H>) -> Result<ExecutionTelemetry, ExecuteError>
where
    H: NodeHandler + Send + Sync + 'static,
{
    run_scoped(exec)
}

fn run_scoped<H>(mut exec: Executor<'_, H>) -> Result<ExecutionTelemetry, ExecuteError>
where
    H: NodeHandler + Send + Sync + 'static,
{
    serial::inject_host_inputs(&mut exec)?;
    let graph = &exec.schedule.host_deferred_graph;
    if graph.ready_segments.is_empty() {
        exec.core
            .telemetry
            .recompute_unattributed_runtime_duration();
        return Ok(std::mem::take(&mut exec.core.telemetry));
    }

    let max_workers = exec
        .core
        .run_config
        .pool_size
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
        .max(1);
    let (tx, rx) = mpsc::channel::<(usize, Result<ExecutionTelemetry, ExecuteError>)>();
    let mut segment_template = exec.snapshot();
    segment_template.direct_slot_access = DirectSlotAccess::Shared;
    let mut scheduler = ParallelDagScheduler::new("thread_scoped", graph);

    std::thread::scope(|scope| {
        let spawn_segment = |segment_idx: usize, tx: mpsc::Sender<_>| {
            let (segment_exec, order) = segment_template.segment_snapshot(segment_idx);

            scope.spawn(move || {
                let segment_span = tracing::debug_span!(
                    target: "daedalus_runtime::executor",
                    "runtime_segment_run",
                    executor = "thread_scoped",
                    segment = segment_idx,
                    nodes = order.len(),
                );
                let _segment_span = segment_span.enter();
                tracing::trace!(
                    target: "daedalus_runtime::executor",
                    executor = "thread_scoped",
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
                        executor = "thread_scoped",
                        segment = segment_idx,
                        "parallel segment finished"
                    ),
                    Err(error) => tracing::trace!(
                        target: "daedalus_runtime::executor",
                        executor = "thread_scoped",
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
            let Ok((segment_idx, result)) = rx.recv() else {
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

        scheduler.log_incomplete("daedalus-runtime: parallel executor");
        Ok::<(), ExecuteError>(())
    })?;

    exec.core
        .telemetry
        .recompute_unattributed_runtime_duration();
    let nodes = exec.nodes.clone();
    exec.core.telemetry.aggregate_groups(&nodes);
    Ok(std::mem::take(&mut exec.core.telemetry))
}
