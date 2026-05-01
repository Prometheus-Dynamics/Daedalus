use std::sync::Arc;

use daedalus_runtime::{RuntimePlan, SchedulerConfig, build_runtime};

use crate::cache::{CacheStatus, EngineCaches, runtime_plan_cache_key};
use crate::error::EngineError;

pub struct PreparedPlan {
    pub(crate) output: daedalus_planner::PlannerOutput,
    pub(crate) cache_status: CacheStatus,
    pub(crate) scheduler: SchedulerConfig,
    pub(crate) caches: Arc<EngineCaches>,
}

impl PreparedPlan {
    pub fn cache_status(&self) -> CacheStatus {
        self.cache_status
    }

    pub fn is_cached(&self) -> bool {
        self.cache_status.is_cached()
    }

    pub fn plan(&self) -> &daedalus_planner::ExecutionPlan {
        &self.output.plan
    }

    pub fn planner_output(&self) -> &daedalus_planner::PlannerOutput {
        &self.output
    }

    pub fn build(&self) -> Result<PreparedRuntimePlan, EngineError> {
        let cache_key = runtime_plan_cache_key(&self.output.plan, &self.scheduler);
        if let Some(runtime_plan) = self.caches.runtime_get(&cache_key) {
            return Ok(PreparedRuntimePlan {
                runtime_plan,
                cache_status: CacheStatus::Hit,
            });
        }

        let runtime_plan = build_runtime(&self.output.plan, &self.scheduler);
        self.caches.runtime_insert(cache_key, runtime_plan.clone());
        Ok(PreparedRuntimePlan {
            runtime_plan,
            cache_status: CacheStatus::Miss,
        })
    }
}

pub struct PreparedRuntimePlan {
    runtime_plan: RuntimePlan,
    cache_status: CacheStatus,
}

impl PreparedRuntimePlan {
    pub fn cache_status(&self) -> CacheStatus {
        self.cache_status
    }

    pub fn is_cached(&self) -> bool {
        self.cache_status.is_cached()
    }

    pub fn runtime_plan(&self) -> &RuntimePlan {
        &self.runtime_plan
    }

    pub fn into_runtime_plan(self) -> RuntimePlan {
        self.runtime_plan
    }
}
