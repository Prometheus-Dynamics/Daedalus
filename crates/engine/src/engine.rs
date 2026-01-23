#[cfg_attr(not(feature = "gpu"), allow(unused_imports))]
use std::sync::Arc;

use daedalus_planner::{Graph, PlannerConfig, PlannerInput, build_plan};
use daedalus_registry::store::Registry;
use daedalus_runtime::ExecutionTelemetry;
use daedalus_runtime::executor::{Executor, NodeHandler};
use daedalus_runtime::{HostBridgeManager, RuntimePlan, SchedulerConfig, build_runtime};

use crate::config::{EngineConfig, GpuBackend, RuntimeMode};
use crate::error::EngineError;

/// Result of a full engine run.
///
/// ```
/// use daedalus_engine::RunResult;
/// use daedalus_runtime::ExecutionTelemetry;
/// use daedalus_runtime::RuntimePlan;
///
/// let result = RunResult {
///     runtime_plan: RuntimePlan::from_execution(&daedalus_planner::ExecutionPlan::new(
///         daedalus_planner::Graph::default(),
///         vec![],
///     )),
///     telemetry: ExecutionTelemetry::default(),
/// };
/// assert!(result.runtime_plan.nodes.is_empty());
/// ```
pub struct RunResult {
    pub runtime_plan: RuntimePlan,
    pub telemetry: ExecutionTelemetry,
}

/// High-level engine facade for planning and execution.
///
/// ```no_run
/// use daedalus_engine::{Engine, EngineConfig};
/// let engine = Engine::new(EngineConfig::default()).unwrap();
/// let _ = engine.config();
/// ```
pub struct Engine {
    config: EngineConfig,
    #[cfg(feature = "gpu")]
    gpu_handle: std::sync::Mutex<Option<Arc<daedalus_gpu::GpuContextHandle>>>,
}

impl Engine {
    /// Create a new engine from configuration.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        config.validate().map_err(EngineError::Config)?;
        if matches!(config.gpu, GpuBackend::Device | GpuBackend::Mock) && !cfg!(feature = "gpu") {
            return Err(EngineError::FeatureDisabled("gpu"));
        }
        if config.runtime.lockfree_queues && !cfg!(feature = "lockfree-queues") {
            return Err(EngineError::FeatureDisabled("lockfree-queues"));
        }
        Ok(Self {
            config,
            #[cfg(feature = "gpu")]
            gpu_handle: std::sync::Mutex::new(None),
        })
    }

    /// Return a reference to the engine config.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Run planner on the provided graph + registry.
    ///
    /// ```no_run
    /// use daedalus_engine::{Engine, EngineConfig};
    /// use daedalus_registry::store::Registry;
    /// use daedalus_planner::Graph;
    /// let engine = Engine::new(EngineConfig::default()).unwrap();
    /// let registry = Registry::new();
    /// let _ = engine.plan(&registry, Graph::default());
    /// ```
    pub fn plan(
        &self,
        registry: &Registry,
        graph: Graph,
    ) -> Result<daedalus_planner::PlannerOutput, EngineError> {
        let planner_cfg = self.planner_config()?;
        let output = build_plan(PlannerInput { graph, registry }, planner_cfg);
        let has_errors = output
            .diagnostics
            .iter()
            .any(|d| !matches!(d.code, daedalus_planner::DiagnosticCode::LintWarning));
        if has_errors {
            return Err(EngineError::Planner(output.diagnostics));
        }
        Ok(output)
    }

    /// Construct a runtime plan from a planner plan using configured policies.
    ///
    /// ```no_run
    /// use daedalus_engine::{Engine, EngineConfig};
    /// use daedalus_planner::{ExecutionPlan, Graph};
    /// let engine = Engine::new(EngineConfig::default()).unwrap();
    /// let plan = ExecutionPlan::new(Graph::default(), vec![]);
    /// let _ = engine.build_runtime_plan(&plan);
    /// ```
    pub fn build_runtime_plan(
        &self,
        plan: &daedalus_planner::ExecutionPlan,
    ) -> Result<RuntimePlan, EngineError> {
        let sched = SchedulerConfig {
            default_policy: self.config.runtime.default_policy.clone(),
            backpressure: self.config.runtime.backpressure.clone(),
            lockfree_queues: self.config.runtime.lockfree_queues,
        };
        Ok(build_runtime(plan, &sched))
    }

    /// Execute a runtime plan using the provided handler.
    ///
    /// ```no_run
    /// use daedalus_engine::{Engine, EngineConfig};
    /// use daedalus_runtime::{RuntimePlan, RuntimeNode};
    /// use daedalus_runtime::executor::NodeError;
    /// use daedalus_planner::{ExecutionPlan, Graph};
    ///
    /// let engine = Engine::new(EngineConfig::default()).unwrap();
    /// let plan = RuntimePlan::from_execution(&ExecutionPlan::new(Graph::default(), vec![]));
    /// let handler = |_node: &RuntimeNode,
    ///               _ctx: &daedalus_runtime::state::ExecutionContext,
    ///               _io: &mut daedalus_runtime::io::NodeIo|
    ///        -> Result<(), NodeError> { Ok(()) };
    /// let _ = engine.execute(plan, handler);
    /// ```
    pub fn execute<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        handler: H,
    ) -> Result<ExecutionTelemetry, EngineError> {
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_metrics_level(self.config.runtime.metrics_level);
        #[cfg(feature = "gpu")]
        {
            if let Some(gpu) = self.get_gpu_handle()? {
                exec = exec.with_gpu((*gpu).clone());
            }
        }
        #[cfg(not(feature = "gpu"))]
        {
            if !matches!(self.config.gpu, GpuBackend::Cpu) {
                return Err(EngineError::FeatureDisabled("gpu"));
            }
            let _ = exec;
        }

        if matches!(self.config.runtime.mode, RuntimeMode::Parallel) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }

        let telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel => exec.run_parallel(),
        }?;
        Ok(telemetry)
    }

    /// Execute a runtime plan with host bridge support.
    ///
    /// ```no_run
    /// use daedalus_engine::{Engine, EngineConfig};
    /// use daedalus_runtime::{HostBridgeManager, RuntimePlan, RuntimeNode};
    /// use daedalus_runtime::executor::NodeError;
    /// use daedalus_planner::{ExecutionPlan, Graph};
    ///
    /// let engine = Engine::new(EngineConfig::default()).unwrap();
    /// let plan = RuntimePlan::from_execution(&ExecutionPlan::new(Graph::default(), vec![]));
    /// let host = HostBridgeManager::new();
    /// let handler = |_node: &RuntimeNode,
    ///               _ctx: &daedalus_runtime::state::ExecutionContext,
    ///               _io: &mut daedalus_runtime::io::NodeIo|
    ///        -> Result<(), NodeError> { Ok(()) };
    /// let _ = engine.execute_with_host(plan, host, handler);
    /// ```
    pub fn execute_with_host<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        host: HostBridgeManager,
        handler: H,
    ) -> Result<ExecutionTelemetry, EngineError> {
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_host_bridges(host)
            .with_metrics_level(self.config.runtime.metrics_level);
        #[cfg(feature = "gpu")]
        {
            if let Some(gpu) = self.get_gpu_handle()? {
                exec = exec.with_gpu((*gpu).clone());
            }
        }
        #[cfg(not(feature = "gpu"))]
        {
            if !matches!(self.config.gpu, GpuBackend::Cpu) {
                return Err(EngineError::FeatureDisabled("gpu"));
            }
            let _ = exec;
        }

        if matches!(self.config.runtime.mode, RuntimeMode::Parallel) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }

        let telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel => exec.run_parallel(),
        }?;
        Ok(telemetry)
    }

    /// Full run: load registry (if not provided), plan, and execute.
    ///
    /// ```no_run
    /// use daedalus_engine::{Engine, EngineConfig};
    /// use daedalus_registry::store::Registry;
    /// use daedalus_planner::Graph;
    /// use daedalus_runtime::executor::NodeError;
    ///
    /// let engine = Engine::new(EngineConfig::default()).unwrap();
    /// let registry = Registry::new();
    /// let handler = |_node: &daedalus_runtime::RuntimeNode,
    ///               _ctx: &daedalus_runtime::state::ExecutionContext,
    ///               _io: &mut daedalus_runtime::io::NodeIo|
    ///        -> Result<(), NodeError> { Ok(()) };
    /// let _ = engine.run(&registry, Graph::default(), handler);
    /// ```
    pub fn run<H: NodeHandler + Send + Sync + 'static>(
        &self,
        registry: &Registry,
        graph: Graph,
        handler: H,
    ) -> Result<RunResult, EngineError> {
        let planner_output = self.plan(registry, graph)?;
        let runtime_plan = self.build_runtime_plan(&planner_output.plan)?;
        let telemetry = self.execute(runtime_plan.clone(), handler)?;
        Ok(RunResult {
            runtime_plan,
            telemetry,
        })
    }

    fn planner_config(&self) -> Result<PlannerConfig, EngineError> {
        if self.config.planner.enable_gpu
            && !cfg!(feature = "gpu")
            && matches!(self.config.gpu, GpuBackend::Device | GpuBackend::Mock)
        {
            return Err(EngineError::FeatureDisabled("gpu"));
        }
        #[allow(unused_mut)]
        let mut cfg = PlannerConfig {
            enable_gpu: self.config.planner.enable_gpu,
            enable_lints: self.config.planner.enable_lints,
            active_features: self.config.planner.active_features.clone(),
            #[cfg(feature = "gpu")]
            gpu_caps: None,
        };
        #[cfg(feature = "gpu")]
        {
            if self.config.planner.enable_gpu {
                let ctx = self.get_gpu_handle()?;
                cfg.gpu_caps = ctx.as_ref().map(|c| c.capabilities());
            }
        }
        #[cfg(not(feature = "gpu"))]
        {
            let _ = cfg.enable_gpu;
        }
        Ok(cfg)
    }

    #[cfg(feature = "gpu")]
    fn get_gpu_handle(&self) -> Result<Option<Arc<daedalus_gpu::GpuContextHandle>>, EngineError> {
        use daedalus_gpu::{GpuBackendKind, GpuOptions, select_backend};
        if matches!(self.config.gpu, GpuBackend::Cpu) {
            return Ok(None);
        }
        let mut guard = self.gpu_handle.lock().unwrap();
        if let Some(handle) = guard.as_ref() {
            return Ok(Some(handle.clone()));
        }
        let opts = match self.config.gpu {
            GpuBackend::Cpu => return Ok(None),
            GpuBackend::Mock => GpuOptions {
                preferred_backend: Some(GpuBackendKind::Mock),
                adapter_label: None,
                allow_software: true,
            },
            GpuBackend::Device => GpuOptions {
                preferred_backend: Some(GpuBackendKind::Wgpu),
                adapter_label: None,
                allow_software: false,
            },
        };
        let handle = Arc::new(select_backend(&opts)?);
        *guard = Some(handle.clone());
        Ok(Some(handle))
    }
}
