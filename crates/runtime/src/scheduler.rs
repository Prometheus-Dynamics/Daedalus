use crate::plan::{
    BackpressureStrategy, EDGE_FRESHNESS_POLICY_KEY, EDGE_PRESSURE_POLICY_KEY, RuntimeEdgePolicy,
    RuntimePlan,
};
use crate::prelude::*;
use alloc::collections::BinaryHeap;
use core::cmp::Reverse;
use daedalus_core::metadata::PLAN_SCHEDULE_ORDER_KEY;
use daedalus_data::model::Value;
use daedalus_planner::{ExecutionPlan, StableHash};

/// Scheduler configuration for edge policies and backpressure.
#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    /// Default policy applied to all edges unless overridden.
    pub default_policy: RuntimeEdgePolicy,
    /// Backpressure strategy for edge queues.
    pub backpressure: BackpressureStrategy,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        }
    }
}

impl SchedulerConfig {
    pub fn stable_hash(&self) -> StableHash {
        #[derive(serde::Serialize)]
        struct SchedulerConfigFingerprint<'a> {
            default_policy: &'a RuntimeEdgePolicy,
            backpressure: &'a BackpressureStrategy,
        }

        let fingerprint = SchedulerConfigFingerprint {
            default_policy: &self.default_policy,
            backpressure: &self.backpressure,
        };
        let mut bytes = b"daedalus_runtime::SchedulerConfig\0".to_vec();
        match serde_json::to_vec(&fingerprint) {
            Ok(serialized) => bytes.extend_from_slice(&serialized),
            Err(error) => {
                bytes.extend_from_slice(b"serde_error");
                bytes.extend_from_slice(error.to_string().as_bytes());
            }
        }
        StableHash::from_bytes(&bytes)
    }
}

/// Build a runtime plan from an execution plan; later will wire policies and orchestrator.
pub fn build_runtime(plan: &ExecutionPlan, config: &SchedulerConfig) -> RuntimePlan {
    let mut runtime = RuntimePlan::from_execution(plan);
    runtime.default_policy = config.default_policy.clone();
    runtime.backpressure = config.backpressure.clone();

    // The configured default applies to every edge whose metadata does not set its own pressure
    // or freshness policy (`edge_latest_only`, `edge_bounded`, graph documents, ...).
    for (edge, planned) in runtime.edges.iter_mut().zip(&plan.graph.edges) {
        let policy = edge.policy_mut();
        if !planned.metadata.contains_key(EDGE_PRESSURE_POLICY_KEY) {
            policy.pressure = config.default_policy.pressure.clone();
        }
        if !planned.metadata.contains_key(EDGE_FRESHNESS_POLICY_KEY) {
            policy.freshness = config.default_policy.freshness.clone();
        }
    }

    if let Some(order) = plan
        .graph
        .metadata
        .get(PLAN_SCHEDULE_ORDER_KEY)
        .and_then(Value::as_string_list)
    {
        let mut used = vec![false; runtime.nodes.len()];
        let schedule: Vec<daedalus_planner::NodeRef> = order
            .iter()
            .filter_map(|id| {
                let idx = runtime.nodes.iter().enumerate().position(|(idx, node)| {
                    !used[idx]
                        && (node.label.as_deref() == Some(id.as_str()) || node.id.as_str() == id)
                })?;
                used[idx] = true;
                Some(daedalus_planner::NodeRef(idx))
            })
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
    for edge in &runtime.edges {
        let from_idx = edge.from().0;
        let to_idx = edge.to().0;
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
