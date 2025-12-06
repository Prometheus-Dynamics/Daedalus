use crate::plan::{BackpressureStrategy, EdgePolicyKind, RuntimePlan};
use daedalus_planner::ExecutionPlan;

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

    // Simple scheduler: order nodes by compute priority, then original index.
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
