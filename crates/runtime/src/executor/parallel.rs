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
/// Node priorities and the segments they rank, for ordering the ready queue.
#[derive(Clone, Copy)]
struct SegmentPriorities<'g> {
    nodes: &'g [i32],
    segments: &'g [crate::plan::RuntimeSegment],
}

impl SegmentPriorities<'_> {
    fn of(&self, segment: usize) -> i32 {
        super::node_policy::segment_priority(self.nodes, self.segments, segment)
    }
}

/// Run `exec`'s segments on the worker pool, independent ones concurrently. With `costs`, each
/// executed segment's wall time in nanoseconds is written at its index.
///
/// Also returns the segment time of the busiest thread (the one that ran the most segment work
/// this frame, the calling thread included), which bounds the frame's wall time from below.
///
/// With fail-fast, the first segment error stops further scheduling; segments already running
/// finish before the error is returned.
pub(crate) fn run<H>(
    exec: &mut Executor<'_, H>,
    costs: Option<&mut [u64]>,
) -> Result<(ExecutionTelemetry, u64), ExecuteError>
where
    H: NodeHandler + Send + Sync + 'static,
{
    serial::inject_host_inputs(exec)?;
    let schedule = exec.schedule.clone();
    let graph = &schedule.host_deferred_graph;
    let workers = exec.core.parallel_workers.min(graph.width);
    if graph.ready_segments.is_empty() || workers <= 1 {
        let order = exec.schedule_order;
        return serial::run_order(exec, order).map(|telemetry| (telemetry, 0));
    }

    let pool = WorkerPool::get_or_init(&exec.core.worker_pool, workers)?;
    let clock = costs.is_some().then(|| exec.core.clock.clone());
    let priorities = exec
        .core
        .run_config
        .policies
        .priorities()
        .map(|nodes| SegmentPriorities {
            nodes,
            segments: &schedule.segments,
        });
    let queue = SegmentQueue::new(
        graph,
        exec.core.run_config.fail_fast,
        clock,
        costs,
        priorities,
    );
    {
        let shared: &Executor<'_, H> = exec;
        pool.fan_out(workers, &|| {
            let mut worker = shared.snapshot_with_direct_slot_access(DirectSlotAccess::Shared);
            queue.drain(|segment| {
                let order = schedule
                    .segments
                    .get(segment)
                    .map_or(&[][..], |segment| segment.nodes.as_slice());
                serial::run_order(&mut worker, order)
            });
        });
    }
    let (telemetry, error, busiest) = queue.finish();
    if let Some(error) = error {
        return Err(error);
    }
    exec.core.telemetry.merge(telemetry);
    exec.core
        .telemetry
        .recompute_unattributed_runtime_duration();
    let groups = exec.core.node_groups.clone();
    exec.core.telemetry.aggregate_group_ids(&groups);
    Ok((core::mem::take(&mut exec.core.telemetry), busiest))
}

struct SegmentQueue<'g, 'c> {
    graph: &'g CompiledSegmentGraph,
    fail_fast: bool,
    /// With node priorities, the highest queued segment is taken first (FIFO among equals);
    /// `None` keeps plain FIFO.
    priorities: Option<SegmentPriorities<'g>>,
    /// Times each segment when costs are collected.
    clock: Option<Clock>,
    state: Mutex<QueueState<'c>>,
    wake: Condvar,
}

struct QueueState<'c> {
    indegree: SegmentList,
    /// Ready segments: `ready[next_ready..]` are still queued, in FIFO order.
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
    /// Largest segment time (ns) any one thread's [`SegmentQueue::drain`] ran.
    busiest_ns: u64,
}

impl<'g, 'c> SegmentQueue<'g, 'c> {
    fn new(
        graph: &'g CompiledSegmentGraph,
        fail_fast: bool,
        clock: Option<Clock>,
        costs: Option<&'c mut [u64]>,
        priorities: Option<SegmentPriorities<'g>>,
    ) -> Self {
        Self {
            graph,
            fail_fast,
            priorities,
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
                busiest_ns: 0,
            }),
            wake: Condvar::new(),
        }
    }

    /// Pull and run ready segments until the run drains or fails fast. The segment time this
    /// thread runs is folded into `busiest_ns` on the way out.
    fn drain(&self, mut run: impl FnMut(usize) -> Result<ExecutionTelemetry, ExecuteError>) {
        let mut state = self.state.lock();
        let mut busy = 0u64;
        loop {
            let Some(segment) = state.pop(self.priorities) else {
                if state.running == 0 || state.error.is_some() {
                    break;
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
            busy += nanos;
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
                    break;
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
        state.busiest_ns = state.busiest_ns.max(busy);
    }

    fn finish(self) -> (ExecutionTelemetry, Option<ExecuteError>, u64) {
        let state = self.state.into_inner();
        if state.error.is_none() && state.completed < self.graph.total_segments {
            crate::trace::debug!(
                target: "daedalus_runtime::executor",
                completed = state.completed,
                total_segments = self.graph.total_segments,
                "daedalus-runtime: parallel executor: incomplete schedule"
            );
        }
        (state.telemetry, state.error, state.busiest_ns)
    }
}

impl QueueState<'_> {
    fn pop(&mut self, priorities: Option<SegmentPriorities<'_>>) -> Option<usize> {
        if self.error.is_some() {
            return None;
        }
        if let Some(priorities) = priorities {
            self.prioritize(priorities);
        }
        let segment = *self.ready.get(self.next_ready)?;
        self.next_ready += 1;
        Some(segment)
    }

    /// Move the first queued segment of the highest priority to the front of the queue,
    /// keeping the rest in order.
    #[cold]
    fn prioritize(&mut self, priorities: SegmentPriorities<'_>) {
        let Some(queued) = self.ready.get(self.next_ready..) else {
            return;
        };
        let mut best = None::<(usize, i32)>;
        for (idx, &segment) in queued.iter().enumerate() {
            let priority = priorities.of(segment);
            if best.is_none_or(|(_, top)| priority > top) {
                best = Some((idx, priority));
            }
        }
        if let Some((best, _)) = best {
            self.ready[self.next_ready..=self.next_ready + best].rotate_right(1);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::RuntimeSegment;
    use daedalus_planner::{ComputeAffinity, NodeRef};

    fn state(ready: &[usize]) -> QueueState<'static> {
        QueueState {
            indegree: SegmentList::new(),
            ready: ready.iter().copied().collect(),
            next_ready: 0,
            running: 0,
            completed: 0,
            idle: 0,
            error: None,
            telemetry: ExecutionTelemetry::default(),
            costs: None,
            busiest_ns: 0,
        }
    }

    fn drain(mut state: QueueState<'_>, priorities: Option<SegmentPriorities<'_>>) -> Vec<usize> {
        core::iter::from_fn(|| state.pop(priorities)).collect()
    }

    #[test]
    fn ready_queue_is_fifo_without_priorities() {
        assert_eq!(drain(state(&[3, 1, 2]), None), vec![3, 1, 2]);
    }

    #[test]
    fn ready_queue_takes_higher_priority_first_and_fifo_among_equals() {
        // One node per segment; segments 0..4 have priorities 0, 5, 0, 5.
        let segments: Vec<RuntimeSegment> = (0..4)
            .map(|node| RuntimeSegment {
                nodes: vec![NodeRef(node)],
                compute: ComputeAffinity::CpuOnly,
            })
            .collect();
        let priorities = SegmentPriorities {
            nodes: &[0, 5, 0, 5],
            segments: &segments,
        };
        assert_eq!(
            drain(state(&[0, 1, 2, 3]), Some(priorities)),
            vec![1, 3, 0, 2]
        );
    }
}
