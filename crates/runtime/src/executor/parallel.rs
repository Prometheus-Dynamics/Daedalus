//! Parallel runs: the segment DAG drains through one shared ready queue that every worker pulls
//! from, so a frame costs one fan-out to the worker pool plus a lock per segment, with no
//! per-segment task, channel or executor snapshot.

use daedalus_core::platform::Clock;
use std::panic::{self, AssertUnwindSafe};

use crate::sync::{Condvar, Mutex, MutexGuard};
use smallvec::SmallVec;

use super::{
    CompiledSegmentGraph, DirectSlotAccess, ExecuteError, ExecutionTelemetry, Executor,
    NodeHandler, WorkerPool, panic_message, segment_failure, serial,
};

/// Per-run segment bookkeeping; inline for graphs up to 32 segments, so a run allocates none.
type SegmentList = SmallVec<[usize; 32]>;

/// Run `exec`'s segments on the worker pool, independent ones concurrently. With `costs`, each
/// executed segment's wall time in nanoseconds is written at its index.
///
/// With fail-fast, the first segment error stops further scheduling; segments already running
/// finish before the error is returned.
pub(crate) fn run<H>(
    exec: &mut Executor<'_, H>,
    costs: Option<&mut [u64]>,
) -> Result<ExecutionTelemetry, ExecuteError>
where
    H: NodeHandler + Send + Sync + 'static,
{
    serial::inject_host_inputs(exec)?;
    let schedule = exec.schedule.clone();
    let graph = &schedule.host_deferred_graph;
    let workers = exec.core.parallel_workers.min(graph.width);
    if graph.ready_segments.is_empty() || workers <= 1 {
        let order = exec.schedule_order;
        return serial::run_order(exec, order);
    }

    let pool = WorkerPool::get_or_init(&exec.core.worker_pool, workers)?;
    let clock = costs.is_some().then(|| exec.core.clock.clone());
    let queue = SegmentQueue::new(graph, exec.core.run_config.fail_fast, clock, costs);
    {
        let shared: &Executor<'_, H> = exec;
        pool.fan_out(workers, &|| {
            let mut worker = shared.snapshot_with_direct_slot_access(DirectSlotAccess::Shared);
            queue.drain(|segment| {
                let order = shared
                    .segments
                    .get(segment)
                    .map_or(&[][..], |segment| segment.nodes.as_slice());
                serial::run_order(&mut worker, order)
            });
        });
    }
    let (telemetry, error) = queue.finish();
    if let Some(error) = error {
        return Err(error);
    }
    exec.core.telemetry.merge(telemetry);
    exec.core
        .telemetry
        .recompute_unattributed_runtime_duration();
    let nodes = exec.nodes.clone();
    exec.core.telemetry.aggregate_groups(&nodes);
    Ok(core::mem::take(&mut exec.core.telemetry))
}

struct SegmentQueue<'g, 'c> {
    graph: &'g CompiledSegmentGraph,
    fail_fast: bool,
    /// Times each segment when costs are collected.
    clock: Option<Clock>,
    state: Mutex<QueueState<'c>>,
    wake: Condvar,
}

struct QueueState<'c> {
    indegree: SegmentList,
    /// FIFO of ready segments: `ready[next_ready..]` are still queued.
    ready: SegmentList,
    next_ready: usize,
    running: usize,
    completed: usize,
    /// Workers waiting on `wake`.
    idle: usize,
    /// First error under fail-fast; stops scheduling.
    error: Option<ExecuteError>,
    telemetry: ExecutionTelemetry,
    costs: Option<&'c mut [u64]>,
}

impl<'g, 'c> SegmentQueue<'g, 'c> {
    fn new(
        graph: &'g CompiledSegmentGraph,
        fail_fast: bool,
        clock: Option<Clock>,
        costs: Option<&'c mut [u64]>,
    ) -> Self {
        Self {
            graph,
            fail_fast,
            clock,
            state: Mutex::new(QueueState {
                indegree: graph.indegree.iter().copied().collect(),
                ready: graph.ready_segments.iter().copied().collect(),
                next_ready: 0,
                running: 0,
                completed: 0,
                idle: 0,
                error: None,
                telemetry: ExecutionTelemetry::default(),
                costs,
            }),
            wake: Condvar::new(),
        }
    }

    /// Pull and run ready segments until the run drains or fails fast.
    fn drain(&self, mut run: impl FnMut(usize) -> Result<ExecutionTelemetry, ExecuteError>) {
        let mut state = self.state.lock();
        loop {
            let Some(segment) = state.pop() else {
                if state.running == 0 || state.error.is_some() {
                    return;
                }
                state.idle += 1;
                self.wake.wait(&mut state);
                state.idle -= 1;
                continue;
            };
            state.running += 1;
            let (result, nanos) = MutexGuard::unlocked(&mut state, || {
                let start = self.clock.as_ref().map(|clock| (clock, clock.now()));
                let result = run_segment(segment, &mut run);
                let nanos = start.map_or(0, |(clock, start)| clock.elapsed(start).as_nanos());
                (result, nanos as u64)
            });
            state.running -= 1;
            state.completed += 1;
            if let Some(cost) = state
                .costs
                .as_deref_mut()
                .and_then(|costs| costs.get_mut(segment))
            {
                *cost = nanos;
            }
            match result {
                Ok(partial) => state.telemetry.merge(partial),
                Err(error) if self.fail_fast => {
                    state.error.get_or_insert(error);
                    self.wake.notify_all();
                    return;
                }
                Err(error) => state
                    .telemetry
                    .errors
                    .push(segment_failure(segment, &error)),
            }
            state.unblock(self.graph, segment);
            let queued = state.ready.len() - state.next_ready;
            if queued == 0 && state.running == 0 {
                self.wake.notify_all();
            } else {
                // This worker takes one queued segment itself.
                for _ in 0..queued.saturating_sub(1).min(state.idle) {
                    self.wake.notify_one();
                }
            }
        }
    }

    fn finish(self) -> (ExecutionTelemetry, Option<ExecuteError>) {
        let state = self.state.into_inner();
        if state.error.is_none() && state.completed < self.graph.total_segments {
            crate::trace::debug!(
                target: "daedalus_runtime::executor",
                completed = state.completed,
                total_segments = self.graph.total_segments,
                "daedalus-runtime: parallel executor: incomplete schedule"
            );
        }
        (state.telemetry, state.error)
    }
}

impl QueueState<'_> {
    fn pop(&mut self) -> Option<usize> {
        if self.error.is_some() {
            return None;
        }
        let segment = *self.ready.get(self.next_ready)?;
        self.next_ready += 1;
        Some(segment)
    }

    fn unblock(&mut self, graph: &CompiledSegmentGraph, segment: usize) {
        for &next in graph.adjacency.get(segment).into_iter().flatten() {
            if let Some(slot) = self.indegree.get_mut(next) {
                *slot = slot.saturating_sub(1);
                if *slot == 0 {
                    self.ready.push(next);
                }
            }
        }
    }
}

fn run_segment(
    segment: usize,
    run: &mut impl FnMut(usize) -> Result<ExecutionTelemetry, ExecuteError>,
) -> Result<ExecutionTelemetry, ExecuteError> {
    let _span = crate::trace::debug_span!(
        target: "daedalus_runtime::executor",
        "runtime_segment_run",
        segment,
    )
    .entered();
    panic::catch_unwind(AssertUnwindSafe(|| run(segment))).unwrap_or_else(|panic| {
        Err(ExecuteError::HandlerPanicked {
            node: format!("segment_{segment}"),
            message: panic_message(&*panic),
        })
    })
}
