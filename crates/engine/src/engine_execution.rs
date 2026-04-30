use std::collections::HashSet;
use std::sync::Arc;

use daedalus_planner::{Graph, GraphPatch};
use daedalus_runtime::executor::{Executor, NodeHandler};
#[cfg(feature = "plugins")]
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_runtime::{
    ExecutionTelemetry, HostBridgeManager, RuntimePlan, RuntimeSink, RuntimeTransport,
    build_runtime,
};

use crate::compiled_run::RunResult;
#[cfg(not(feature = "gpu"))]
use crate::config::GpuBackend;
use crate::config::RuntimeMode;
use crate::engine::Engine;
use crate::error::EngineError;

impl Engine {
    pub fn build_runtime_plan(
        &self,
        plan: &daedalus_planner::ExecutionPlan,
    ) -> Result<RuntimePlan, EngineError> {
        let sched = self.scheduler_config();
        Ok(build_runtime(plan, &sched))
    }

    pub fn execute<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        handler: H,
    ) -> Result<ExecutionTelemetry, EngineError> {
        if self.config.runtime.demand_driven && !self.config.runtime.demand_sinks.is_empty() {
            return self.execute_scoped(runtime_plan, &self.config.runtime.demand_sinks, handler);
        }
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_runtime_debug_config(self.config.runtime.debug_config);
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

        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }

        let telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel | RuntimeMode::Adaptive => exec.run_parallel(),
        }?;
        Ok(telemetry)
    }

    pub fn execute_with_transport<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        handler: H,
        transport: RuntimeTransport,
    ) -> Result<ExecutionTelemetry, EngineError> {
        if self.config.runtime.demand_driven && !self.config.runtime.demand_sinks.is_empty() {
            return self.execute_scoped_with_transport(
                runtime_plan,
                &self.config.runtime.demand_sinks,
                handler,
                transport,
            );
        }
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_runtime_transport(transport)
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_runtime_debug_config(self.config.runtime.debug_config);
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

        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }

        let telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel | RuntimeMode::Adaptive => exec.run_parallel(),
        }?;
        Ok(telemetry)
    }

    #[cfg(feature = "plugins")]
    pub fn execute_with_plugin_registry<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        handler: H,
        plugins: &PluginRegistry,
    ) -> Result<ExecutionTelemetry, EngineError> {
        if self.config.runtime.demand_driven && !self.config.runtime.demand_sinks.is_empty() {
            return self.execute_scoped_with_plugin_registry(
                runtime_plan,
                &self.config.runtime.demand_sinks,
                handler,
                plugins,
            );
        }
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_runtime_transport(plugins.runtime_transport.clone())
            .with_capabilities(plugins.capabilities.clone())
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_runtime_debug_config(self.config.runtime.debug_config);
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

        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }

        let telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel | RuntimeMode::Adaptive => exec.run_parallel(),
        }?;
        Ok(telemetry)
    }

    pub fn execute_scoped<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        sinks: &[RuntimeSink],
        handler: H,
    ) -> Result<ExecutionTelemetry, EngineError> {
        let slice = runtime_plan
            .demand_slice_for_sinks(sinks)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let demand = runtime_plan.demand_summary_for_slice(sinks, &slice);
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_active_nodes(slice.active_nodes.clone())
            .with_active_edges_mask(Some(Arc::new(slice.active_edges.clone())))
            .with_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())))
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_runtime_debug_config(self.config.runtime.debug_config);
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
        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }
        let mut telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel | RuntimeMode::Adaptive => exec.run_parallel(),
        }?;
        telemetry.demand = demand;
        Ok(telemetry)
    }

    pub fn execute_scoped_with_transport<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        sinks: &[RuntimeSink],
        handler: H,
        transport: RuntimeTransport,
    ) -> Result<ExecutionTelemetry, EngineError> {
        let slice = runtime_plan
            .demand_slice_for_sinks(sinks)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let demand = runtime_plan.demand_summary_for_slice(sinks, &slice);
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_runtime_transport(transport)
            .with_active_nodes(slice.active_nodes.clone())
            .with_active_edges_mask(Some(Arc::new(slice.active_edges.clone())))
            .with_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())))
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_runtime_debug_config(self.config.runtime.debug_config);
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
        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }
        let mut telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel | RuntimeMode::Adaptive => exec.run_parallel(),
        }?;
        telemetry.demand = demand;
        Ok(telemetry)
    }

    #[cfg(feature = "plugins")]
    pub fn execute_scoped_with_plugin_registry<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        sinks: &[RuntimeSink],
        handler: H,
        plugins: &PluginRegistry,
    ) -> Result<ExecutionTelemetry, EngineError> {
        let slice = runtime_plan
            .demand_slice_for_sinks(sinks)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let demand = runtime_plan.demand_summary_for_slice(sinks, &slice);
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_runtime_transport(plugins.runtime_transport.clone())
            .with_capabilities(plugins.capabilities.clone())
            .with_active_nodes(slice.active_nodes.clone())
            .with_active_edges_mask(Some(Arc::new(slice.active_edges.clone())))
            .with_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())))
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_runtime_debug_config(self.config.runtime.debug_config);
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
        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }
        let mut telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel | RuntimeMode::Adaptive => exec.run_parallel(),
        }?;
        telemetry.demand = demand;
        Ok(telemetry)
    }

    pub fn execute_with_host<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        host: HostBridgeManager,
        handler: H,
    ) -> Result<ExecutionTelemetry, EngineError> {
        if self.config.runtime.demand_driven && !self.config.runtime.demand_sinks.is_empty() {
            return self.execute_with_host_scoped(
                runtime_plan,
                host,
                &self.config.runtime.demand_sinks,
                handler,
            );
        }
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_host_bridges(host)
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_runtime_debug_config(self.config.runtime.debug_config);
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

        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }

        let telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel | RuntimeMode::Adaptive => exec.run_parallel(),
        }?;
        Ok(telemetry)
    }

    pub fn execute_with_host_scoped<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        host: HostBridgeManager,
        sinks: &[RuntimeSink],
        handler: H,
    ) -> Result<ExecutionTelemetry, EngineError> {
        let slice = runtime_plan
            .demand_slice_for_sinks(sinks)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let demand = runtime_plan.demand_summary_for_slice(sinks, &slice);
        let mut exec = Executor::new(&runtime_plan, handler)
            .with_active_nodes(slice.active_nodes.clone())
            .with_active_edges_mask(Some(Arc::new(slice.active_edges.clone())))
            .with_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())))
            .with_selected_host_output_ports(Some(Arc::new(
                slice
                    .host_output_ports
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>(),
            )))
            .with_host_bridges(host)
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_runtime_debug_config(self.config.runtime.debug_config);
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
        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
        }
        let mut telemetry = match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel | RuntimeMode::Adaptive => exec.run_parallel(),
        }?;
        telemetry.demand = demand;
        Ok(telemetry)
    }

    pub fn run<H: NodeHandler + Send + Sync + 'static>(
        &self,
        graph: Graph,
        handler: H,
    ) -> Result<RunResult, EngineError> {
        let prepared = self.prepare_plan(graph)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = prepared_runtime.into_runtime_plan();
        let telemetry = self.execute(runtime_plan.clone(), handler)?;
        Ok(RunResult {
            runtime_plan,
            telemetry,
            planner_cache,
            runtime_plan_cache,
            cache_metrics: self.cache_metrics(),
        })
    }

    pub fn run_with_transport<H: NodeHandler + Send + Sync + 'static>(
        &self,
        graph: Graph,
        handler: H,
        transport: RuntimeTransport,
    ) -> Result<RunResult, EngineError> {
        let prepared = self.prepare_plan(graph)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = prepared_runtime.into_runtime_plan();
        let telemetry = self.execute_with_transport(runtime_plan.clone(), handler, transport)?;
        Ok(RunResult {
            runtime_plan,
            telemetry,
            planner_cache,
            runtime_plan_cache,
            cache_metrics: self.cache_metrics(),
        })
    }

    #[cfg(feature = "plugins")]
    pub fn run_plugin_registry<H: NodeHandler + Send + Sync + 'static>(
        &self,
        plugins: &PluginRegistry,
        graph: Graph,
        handler: H,
    ) -> Result<RunResult, EngineError> {
        super::engine::validate_boundary_contracts(plugins, &graph)?;
        let planner_cfg = plugins
            .planner_config_with_transport(self.planner_config()?)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let prepared = self.prepare_plan_with_config(graph, planner_cfg)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = prepared_runtime.into_runtime_plan();
        let telemetry =
            self.execute_with_plugin_registry(runtime_plan.clone(), handler, plugins)?;
        Ok(RunResult {
            runtime_plan,
            telemetry,
            planner_cache,
            runtime_plan_cache,
            cache_metrics: self.cache_metrics(),
        })
    }

    pub fn run_with_patch<H: NodeHandler + Send + Sync + 'static>(
        &self,
        base_graph: &Graph,
        patch: &GraphPatch,
        handler: H,
    ) -> Result<RunResult, EngineError> {
        let impact = patch.analyze(base_graph);
        let mut graph = base_graph.clone();
        patch.apply_to_graph(&mut graph);

        let prepared = self.prepare_plan(graph)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = prepared_runtime.into_runtime_plan();

        let telemetry = if !impact.requires_full_rerun && !impact.sink_nodes.is_empty() {
            let sinks: Vec<RuntimeSink> = impact
                .sink_nodes
                .iter()
                .copied()
                .map(|idx| RuntimeSink {
                    node: daedalus_planner::GraphNodeSelector {
                        index: Some(idx),
                        ..Default::default()
                    },
                    port: None,
                })
                .collect();
            self.execute_scoped(runtime_plan.clone(), &sinks, handler)?
        } else {
            self.execute(runtime_plan.clone(), handler)?
        };

        Ok(RunResult {
            runtime_plan,
            telemetry,
            planner_cache,
            runtime_plan_cache,
            cache_metrics: self.cache_metrics(),
        })
    }
}
