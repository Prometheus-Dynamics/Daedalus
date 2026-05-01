use std::collections::{BTreeSet, VecDeque};

use super::CompiledSegmentGraph;

pub(crate) struct ParallelDagScheduler<'a> {
    executor_name: &'static str,
    graph: &'a CompiledSegmentGraph,
    indegree: Vec<usize>,
    ready: VecDeque<usize>,
    running: usize,
    completed: usize,
    total_segments: usize,
}

impl<'a> ParallelDagScheduler<'a> {
    pub(crate) fn new(executor_name: &'static str, graph: &'a CompiledSegmentGraph) -> Self {
        let ready = graph
            .ready_segments
            .iter()
            .copied()
            .collect::<VecDeque<_>>();
        for &segment_idx in &ready {
            tracing::trace!(
                target: "daedalus_runtime::executor",
                executor = executor_name,
                segment = segment_idx,
                "parallel segment queued"
            );
        }
        let total_segments = graph
            .ready_segments
            .iter()
            .copied()
            .chain(
                graph
                    .adjacency
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, next)| (!next.is_empty()).then_some(idx)),
            )
            .collect::<BTreeSet<_>>()
            .len()
            .max(graph.ready_segments.len());
        Self {
            executor_name,
            graph,
            indegree: (*graph.indegree).clone(),
            ready,
            running: 0,
            completed: 0,
            total_segments,
        }
    }

    pub(crate) fn spawn_ready<F>(&mut self, max_workers: usize, mut spawn: F)
    where
        F: FnMut(usize),
    {
        while self.running < max_workers {
            let Some(segment_idx) = self.ready.pop_front() else {
                break;
            };
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
                    self.ready.push_back(next);
                }
            }
        }
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.running == 0 && self.ready.is_empty()
    }

    pub(crate) fn log_incomplete(&self, message: &'static str) {
        if self.completed < self.total_segments {
            let completed = self.completed;
            let total_segments = self.total_segments;
            tracing::debug!(
                target: "daedalus_runtime::executor",
                completed,
                total_segments,
                "{message}: incomplete schedule"
            );
        }
    }
}
