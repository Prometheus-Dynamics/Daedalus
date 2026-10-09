use crate::plan::{
    BackpressureStrategy, EDGE_FRESHNESS_POLICY_KEY, EDGE_PRESSURE_POLICY_KEY, RuntimeEdgePolicy,
    RuntimePlan,
};
use crate::prelude::*;
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

    // `schedule_order` is already final: `RuntimePlan::from_execution` ranks the nodes by the
    // planner's schedule order (node indices) and resolves it into a dependency order.
    runtime
}
