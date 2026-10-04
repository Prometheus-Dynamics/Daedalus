use smallvec::SmallVec;

use super::CompiledSegmentGraph;

/// Per-run segment bookkeeping; inline for graphs up to 32 segments, so a tick allocates none.
type SegmentList = SmallVec<[usize; 32]>;

pub(crate) struct ParallelDagScheduler<'a> {
    executor_name: &'static str,
    graph: &'a CompiledSegmentGraph,
    indegree: SegmentList,
    /// FIFO of ready segments: `ready[next_ready..]` are still queued.
    ready: SegmentList,
    next_ready: usize,
    running: usize,
    completed: usize,
}

impl<'a> ParallelDagScheduler<'a> {
    pub(crate) fn new(executor_name: &'static str, graph: &'a CompiledSegmentGraph) -> Self {
        let ready: SegmentList = graph.ready_segments.iter().copied().collect();
        for &segment_idx in &ready {
            tracing::trace!(
                target: "daedalus_runtime::executor",
                executor = executor_name,
                segment = segment_idx,
                "parallel segment queued"
            );
        }
        Self {
            executor_name,
            graph,
            indegree: graph.indegree.iter().copied().collect(),
            ready,
            next_ready: 0,
            running: 0,
            completed: 0,
        }
    }

    pub(crate) fn spawn_ready<F>(&mut self, max_workers: usize, mut spawn: F)
    where
        F: FnMut(usize),
    {
        while self.running < max_workers {
            let Some(&segment_idx) = self.ready.get(self.next_ready) else {
                break;
            };
            self.next_ready += 1;
            spawn(segment_idx);
            self.running += 1;
        }
    }

    pub(crate) fn has_running(&self) -> bool {
        self.running > 0
    }

    pub(crate) fn complete_segment(&mut self, segment_idx: usize) {
        self.running = self.running.saturating_sub(1);
        self.completed += 1;
        for next in self
            .graph
            .adjacency
            .get(segment_idx)
            .into_iter()
            .flatten()
            .copied()
        {
            if let Some(slot) = self.indegree.get_mut(next) {
                *slot = slot.saturating_sub(1);
                if *slot == 0 {
                    tracing::trace!(
                        target: "daedalus_runtime::executor",
                        executor = self.executor_name,
                        segment = next,
                        upstream = segment_idx,
                        "parallel downstream segment unblocked"
                    );
                    self.ready.push(next);
                }
            }
        }
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.running == 0 && self.next_ready == self.ready.len()
    }

    pub(crate) fn log_incomplete(&self, message: &'static str) {
        if self.completed < self.graph.total_segments {
            let completed = self.completed;
            let total_segments = self.graph.total_segments;
            tracing::debug!(
                target: "daedalus_runtime::executor",
                completed,
                total_segments,
                "{message}: incomplete schedule"
            );
        }
    }
}
