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
        let exec = self.configure_executor(Executor::try_new(&runtime_plan, handler)?)?;
        self.run_configured_executor(exec)
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
        let exec = self.configure_executor(
            Executor::try_new(&runtime_plan, handler)?.with_runtime_transport(transport),
        )?;
        self.run_configured_executor(exec)
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
        let exec = Executor::try_new(&runtime_plan, handler)?
            .with_runtime_transport(plugins.runtime_transport.clone())
            .with_capabilities(plugins.capabilities.clone());
        let exec = self.configure_executor(exec)?;
        self.run_configured_executor(exec)
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
        let exec = Executor::try_new(&runtime_plan, handler)?
            .try_with_active_nodes(slice.active_nodes.clone())?
            .try_with_active_edges_mask(Some(Arc::new(slice.active_edges.clone())))?
            .try_with_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())))?;
        let exec = self.configure_executor(exec)?;
        let mut telemetry = self.run_configured_executor(exec)?;
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
        let exec = Executor::try_new(&runtime_plan, handler)?
            .with_runtime_transport(transport)
            .try_with_active_nodes(slice.active_nodes.clone())?
            .try_with_active_edges_mask(Some(Arc::new(slice.active_edges.clone())))?
            .try_with_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())))?;
        let exec = self.configure_executor(exec)?;
        let mut telemetry = self.run_configured_executor(exec)?;
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
        let exec = Executor::try_new(&runtime_plan, handler)?
            .with_runtime_transport(plugins.runtime_transport.clone())
            .with_capabilities(plugins.capabilities.clone())
            .try_with_active_nodes(slice.active_nodes.clone())?
            .try_with_active_edges_mask(Some(Arc::new(slice.active_edges.clone())))?
            .try_with_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())))?;
        let exec = self.configure_executor(exec)?;
        let mut telemetry = self.run_configured_executor(exec)?;
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
        let exec = self.configure_executor(
            Executor::try_new(&runtime_plan, handler)?.with_host_bridges(host),
        )?;
        self.run_configured_executor(exec)
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
        let exec = Executor::try_new(&runtime_plan, handler)?
            .try_with_active_nodes(slice.active_nodes.clone())?
            .try_with_active_edges_mask(Some(Arc::new(slice.active_edges.clone())))?
            .try_with_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())))?
            .with_selected_host_output_ports(Some(Arc::new(
                slice
                    .host_output_ports
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>(),
            )))
            .with_host_bridges(host);
        let exec = self.configure_executor(exec)?;
        let mut telemetry = self.run_configured_executor(exec)?;
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
