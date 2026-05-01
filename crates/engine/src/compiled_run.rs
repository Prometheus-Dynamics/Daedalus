use std::sync::Arc;

use daedalus_runtime::executor::{NodeHandler, OwnedExecutor};
use daedalus_runtime::{ExecutionTelemetry, RuntimePlan};

use crate::cache::{CacheStatus, EngineCacheMetrics, EngineCaches};
use crate::config::RuntimeMode;
use crate::error::EngineError;

/// Result of a full engine run.
///
pub struct RunResult {
    pub runtime_plan: RuntimePlan,
    pub telemetry: ExecutionTelemetry,
    pub planner_cache: CacheStatus,
    pub runtime_plan_cache: CacheStatus,
    pub cache_metrics: EngineCacheMetrics,
}

pub struct CompiledRun<H: NodeHandler> {
    pub(crate) runtime_plan: Arc<RuntimePlan>,
    pub(crate) executor: OwnedExecutor<H>,
    pub(crate) runtime_mode: RuntimeMode,
    pub(crate) planner_cache: CacheStatus,
    pub(crate) runtime_plan_cache: CacheStatus,
    pub(crate) caches: Arc<EngineCaches>,
}

impl<H: NodeHandler + Send + Sync + 'static> CompiledRun<H> {
    pub fn runtime_plan(&self) -> &RuntimePlan {
        self.runtime_plan.as_ref()
    }

    pub fn run(&mut self) -> Result<RunResult, EngineError> {
        let telemetry = match self.runtime_mode {
            RuntimeMode::Serial => self.executor.run_in_place(),
            RuntimeMode::Parallel => self.executor.run_parallel_in_place(),
            RuntimeMode::Adaptive => self.executor.run_adaptive_in_place(),
        }?;
        Ok(RunResult {
            runtime_plan: self.runtime_plan.as_ref().clone(),
            telemetry,
            planner_cache: self.planner_cache,
            runtime_plan_cache: self.runtime_plan_cache,
            cache_metrics: self.caches.metrics(),
        })
    }

    pub fn run_telemetry(&mut self) -> Result<ExecutionTelemetry, EngineError> {
        Ok(match self.runtime_mode {
            RuntimeMode::Serial => self.executor.run_in_place(),
            RuntimeMode::Parallel => self.executor.run_parallel_in_place(),
            RuntimeMode::Adaptive => self.executor.run_adaptive_in_place(),
        }?)
    }
}
