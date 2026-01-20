use crate::plan::{BackpressureStrategy, EdgePolicyKind, RuntimePlan};
use daedalus_planner::ExecutionPlan;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Scheduler configuration for edge policies and backpressure.
#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    /// Default policy applied to all edges unless overridden.
    pub default_policy: EdgePolicyKind,
    /// Backpressure strategy for edge queues.
    pub backpressure: BackpressureStrategy,
    /// Prefer lock-free bounded edge queues when available.
    pub lockfree_queues: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            default_policy: EdgePolicyKind::Fifo,
            backpressure: BackpressureStrategy::None,
            lockfree_queues: false,
        }
    }
}

/// Build a runtime plan from an execution plan; later will wire policies and orchestrator.
pub fn build_runtime(plan: &ExecutionPlan, config: &SchedulerConfig) -> RuntimePlan {
    let mut runtime = RuntimePlan::from_execution(plan);
    runtime.default_policy = config.default_policy.clone();
    runtime.backpressure = config.backpressure.clone();
    runtime.lockfree_queues = config.lockfree_queues;

    // Assign configured default policy to all edges for now.
    runtime
        .edges
        .iter_mut()
        .for_each(|edge| edge.4 = config.default_policy.clone());

    if let Some(order) = plan.graph.metadata.get("schedule_order") {
        let mut id_to_ref = std::collections::HashMap::new();
        for (idx, node) in runtime.nodes.iter().enumerate() {
            id_to_ref.insert(node.id.as_str(), daedalus_planner::NodeRef(idx));
        }
        let schedule: Vec<daedalus_planner::NodeRef> = order
            .split(',')
            .filter_map(|id| id_to_ref.get(id.trim()).copied())
            .collect();
        if !schedule.is_empty() {
            runtime.schedule_order = schedule;
            return runtime;
        }
    }

    if let Some(order) = topo_order(&runtime) {
        runtime.schedule_order = order;
        return runtime;
    }

    // Fallback scheduler: order nodes by compute priority, then original index.
    let mut idxs: Vec<(usize, u8)> = runtime
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let p = match n.compute {
                daedalus_planner::ComputeAffinity::GpuRequired => 0,
                daedalus_planner::ComputeAffinity::GpuPreferred => 1,
                daedalus_planner::ComputeAffinity::CpuOnly => 2,
            };
            (i, p)
        })
        .collect();
    idxs.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    runtime.schedule_order = idxs
        .into_iter()
        .map(|(i, _)| daedalus_planner::NodeRef(i))
        .collect();

    runtime
}

fn topo_order(runtime: &RuntimePlan) -> Option<Vec<daedalus_planner::NodeRef>> {
    let node_count = runtime.nodes.len();
    if node_count == 0 {
        return Some(Vec::new());
    }
    let mut indegree = vec![0usize; node_count];
    let mut adj = vec![Vec::new(); node_count];
    for (from, _, to, _, _) in &runtime.edges {
        let from_idx = from.0;
        let to_idx = to.0;
        adj[from_idx].push(to_idx);
        indegree[to_idx] += 1;
    }

    let mut heap: BinaryHeap<Reverse<(u8, usize)>> = BinaryHeap::new();
    for (idx, &count) in indegree.iter().enumerate() {
        if count == 0 {
            heap.push(Reverse((node_priority(runtime, idx), idx)));
        }
    }

    let mut order = Vec::with_capacity(node_count);
    while let Some(Reverse((_prio, idx))) = heap.pop() {
        order.push(daedalus_planner::NodeRef(idx));
        for &next in &adj[idx] {
            indegree[next] = indegree[next].saturating_sub(1);
            if indegree[next] == 0 {
                heap.push(Reverse((node_priority(runtime, next), next)));
            }
        }
    }

    if order.len() == node_count {
        Some(order)
    } else {
        None
    }
}

fn node_priority(runtime: &RuntimePlan, idx: usize) -> u8 {
    match runtime.nodes[idx].compute {
        daedalus_planner::ComputeAffinity::GpuRequired => 0,
        daedalus_planner::ComputeAffinity::GpuPreferred => 1,
        daedalus_planner::ComputeAffinity::CpuOnly => 2,
    }
}
