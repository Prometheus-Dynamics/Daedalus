use crate::portable::Arc;
use crate::prelude::*;

use daedalus_planner::{GpuPlanCaps, Graph, PlannerConfig, PlannerInput, build_plan};
use daedalus_runtime::executor::{ExecutionTelemetry, Executor, NodeHandler, OwnedExecutor};
#[cfg(feature = "plugins")]
use daedalus_runtime::handler_registry::HandlerRegistry;
#[cfg(feature = "plugins")]
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_runtime::{HostBridgeManager, RuntimePlan, RuntimeTransport, SchedulerConfig};
#[cfg(feature = "plugins")]
use daedalus_transport::{AccessMode, BoundaryCapabilities};

use crate::cache::{CacheStatus, EngineCacheMetrics, EngineCaches, planner_cache_key};
use crate::compiled_run::CompiledRun;
use crate::config::{EngineConfig, GpuBackend, RuntimeMode};
use crate::error::EngineError;
use crate::host_graph::HostGraph;
use crate::prepared_plan::PreparedPlan;

#[cfg(feature = "plugins")]
pub(crate) fn validate_boundary_contracts(
    plugins: &PluginRegistry,
    graph: &Graph,
) -> Result<(), EngineError> {
    fn required_capabilities(access: AccessMode, is_output: bool) -> BoundaryCapabilities {
        let mut required = BoundaryCapabilities::default();
        if is_output {
            required.owned_move = true;
            return required;
        }
        match access {
            AccessMode::Read | AccessMode::View => required.borrow_ref = true,
            AccessMode::Move => required.owned_move = true,
            AccessMode::Modify => required.borrow_mut = true,
        }
        required
    }

    for instance in &graph.nodes {
        let Some(decl) = plugins.transport_capabilities.node_decl(&instance.id) else {
            continue;
        };
        for port in &decl.inputs {
            let Some(contract) = plugins.boundary_contract(&port.type_key) else {
                continue;
            };
            let required = daedalus_transport::BoundaryTypeContract {
                type_key: port.type_key.clone(),
                rust_type_name: contract.rust_type_name.clone(),
                abi_version: daedalus_transport::BoundaryTypeContract::ABI_VERSION,
                layout_hash: contract.layout_hash.clone(),
                capabilities: required_capabilities(port.access, false),
            };
            contract.compatible_with(&required).map_err(|err| {
                EngineError::Config(format!(
                    "boundary contract for node `{}` input `{}` is incompatible: {err}",
                    instance.id, port.name
                ))
            })?;
        }
        for port in &decl.outputs {
            let Some(contract) = plugins.boundary_contract(&port.type_key) else {
                continue;
            };
            let required = daedalus_transport::BoundaryTypeContract {
                type_key: port.type_key.clone(),
                rust_type_name: contract.rust_type_name.clone(),
                abi_version: daedalus_transport::BoundaryTypeContract::ABI_VERSION,
                layout_hash: contract.layout_hash.clone(),
                capabilities: required_capabilities(port.access, true),
            };
            contract.compatible_with(&required).map_err(|err| {
                EngineError::Config(format!(
                    "boundary contract for node `{}` output `{}` is incompatible: {err}",
                    instance.id, port.name
                ))
            })?;
        }
    }
    Ok(())
}

mod gpu_caps;
#[cfg(feature = "gpu")]
mod gpu_context;

/// High-level engine facade for planning and execution.
///
pub struct Engine {
    pub(crate) config: EngineConfig,
    caches: Arc<EngineCaches>,
    /// The GPU context, opened on first use (see `gpu_context`).
    #[cfg(feature = "gpu")]
    pub(crate) lazy_gpu: Arc<gpu_context::LazyGpu>,
}

impl Engine {
    /// Create a new engine from configuration.
    pub fn new(config: impl Into<EngineConfig>) -> Result<Self, EngineError> {
        let config = config.into();
        config.validate()?;
        if matches!(config.gpu, GpuBackend::Device | GpuBackend::Mock) && !cfg!(feature = "gpu") {
            return Err(EngineError::FeatureDisabled("gpu"));
        }
        #[cfg(feature = "gpu")]
        let lazy_gpu = Arc::new(gpu_context::LazyGpu::new(&config));
        Ok(Self {
            caches: crate::cache::new_caches(
                config.cache.planner_max_entries,
                config.cache.runtime_plan_max_entries,
            ),
            config,
            #[cfg(feature = "gpu")]
            lazy_gpu,
        })
    }

    /// Return a reference to the engine config.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    fn configure_owned_executor<H: NodeHandler + Send + Sync + 'static>(
        &self,
        mut exec: OwnedExecutor<H>,
    ) -> Result<OwnedExecutor<H>, EngineError> {
        let rt = &self.config.runtime;
        exec = exec
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_recovery(self.config.runtime.recovery)
            .with_backoff(rt.backoff_base, rt.backoff_cap)
            .with_backoff_after(rt.backoff_after)
            .with_auto_off(rt.auto_off_after)
            .with_blocking_backpressure(self.config.runtime.blocking_backpressure)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_clock(self.config.runtime.clock.clone())
            .with_runtime_debug_config(self.config.runtime.debug_config);
        if !self.config.runtime.node_fusion {
            exec = exec.with_node_fusion(false);
        }
        if self.config.runtime.demand_driven && !self.config.runtime.demand_sinks.is_empty() {
            exec = exec.with_demand_sinks(self.config.runtime.demand_sinks.clone());
        }
        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
            if let Some(overhead) = self.config.runtime.adaptive_dispatch_overhead {
                exec = exec.with_adaptive_dispatch_overhead(overhead);
            }
            #[cfg(feature = "threads")]
            exec.prewarm_worker_pool()?;
        }
        #[cfg(feature = "gpu")]
        {
            if !matches!(self.config.gpu, GpuBackend::Cpu) {
                exec = exec.with_gpu_source(self.lazy_gpu.source.clone());
            }
        }
        #[cfg(not(feature = "gpu"))]
        {
            if !matches!(self.config.gpu, GpuBackend::Cpu) {
                return Err(EngineError::FeatureDisabled("gpu"));
            }
        }
        Ok(exec)
    }

    pub(crate) fn configure_executor<'a, H: NodeHandler + Send + Sync + 'static>(
        &self,
        mut exec: Executor<'a, H>,
    ) -> Result<Executor<'a, H>, EngineError> {
        let rt = &self.config.runtime;
        exec = exec
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_recovery(self.config.runtime.recovery)
            .with_backoff(rt.backoff_base, rt.backoff_cap)
            .with_backoff_after(rt.backoff_after)
            .with_auto_off(rt.auto_off_after)
            .with_blocking_backpressure(self.config.runtime.blocking_backpressure)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_clock(self.config.runtime.clock.clone())
            .with_runtime_debug_config(self.config.runtime.debug_config);
        if !self.config.runtime.node_fusion {
            exec = exec.with_node_fusion(false);
        }
        if self.config.runtime.demand_driven && !self.config.runtime.demand_sinks.is_empty() {
            exec = exec.with_demand_sinks(self.config.runtime.demand_sinks.clone());
        }
        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
            if let Some(overhead) = self.config.runtime.adaptive_dispatch_overhead {
                exec = exec.with_adaptive_dispatch_overhead(overhead);
            }
            #[cfg(feature = "threads")]
            exec.prewarm_worker_pool()?;
        }
        #[cfg(feature = "gpu")]
        {
            if !matches!(self.config.gpu, GpuBackend::Cpu) {
                exec = exec.with_gpu_source(self.lazy_gpu.source.clone());
            }
        }
        #[cfg(not(feature = "gpu"))]
        {
            if !matches!(self.config.gpu, GpuBackend::Cpu) {
                return Err(EngineError::FeatureDisabled("gpu"));
            }
        }
        Ok(exec)
    }

    pub(crate) fn run_configured_executor<H: NodeHandler + Send + Sync + 'static>(
        &self,
        exec: Executor<'_, H>,
    ) -> Result<ExecutionTelemetry, EngineError> {
        Ok(match self.config.runtime.mode {
            RuntimeMode::Serial => exec.run(),
            RuntimeMode::Parallel => exec.run_parallel(),
            RuntimeMode::Adaptive => exec.run_adaptive(),
        }?)
    }

    fn configure_host_bridges(&self, bridges: &HostBridgeManager) -> Result<(), EngineError> {
        bridges
            .apply_config(&self.config.runtime.host_bridge_config())
            .map_err(|err| EngineError::Config(err.to_string()))?;
        bridges.set_clock(self.config.runtime.clock.clone());
        Ok(())
    }

    pub fn compile_runtime_plan<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        handler: H,
    ) -> Result<CompiledRun<H>, EngineError> {
        let runtime_plan = Arc::new(runtime_plan);
        let executor =
            self.configure_owned_executor(OwnedExecutor::try_new(runtime_plan.clone(), handler)?)?;
        Ok(CompiledRun {
            runtime_plan,
            executor,
            runtime_mode: self.config.runtime.mode.clone(),
            planner_cache: CacheStatus::Miss,
            runtime_plan_cache: CacheStatus::Miss,
            caches: self.caches.clone(),
        })
    }

    /// Compiles `runtime_plan` for the node handler `H` with an explicit transport.
    pub fn compile_runtime_plan_with_transport<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        handler: H,
        transport: RuntimeTransport,
    ) -> Result<CompiledRun<H>, EngineError> {
        let runtime_plan = Arc::new(runtime_plan);
        let executor = OwnedExecutor::try_new(runtime_plan.clone(), handler)?
            .with_runtime_transport(transport);
        let executor = self.configure_owned_executor(executor)?;
        Ok(CompiledRun {
            runtime_plan,
            executor,
            runtime_mode: self.config.runtime.mode.clone(),
            planner_cache: CacheStatus::Miss,
            runtime_plan_cache: CacheStatus::Miss,
            caches: self.caches.clone(),
        })
    }

    pub fn cache_metrics(&self) -> EngineCacheMetrics {
        self.caches.metrics()
    }

    pub fn clear_caches(&self) -> EngineCacheMetrics {
        self.caches.clear()
    }

    /// Run planner on the provided graph.
    ///
    pub fn plan(&self, graph: Graph) -> Result<daedalus_planner::PlannerOutput, EngineError> {
        let planner_cfg = self.planner_config_for(&graph)?;
        self.plan_with_config(graph, planner_cfg)
    }

    /// Plan using a plugin registry's native transport capabilities.
    #[cfg(feature = "plugins")]
    #[doc(hidden)]
    pub fn plan_plugin_registry(
        &self,
        plugins: &PluginRegistry,
        graph: Graph,
    ) -> Result<daedalus_planner::PlannerOutput, EngineError> {
        let planner_cfg = plugins
            .planner_config_with_transport(self.planner_config_for(&graph)?)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        self.plan_with_config(graph, planner_cfg)
    }

    /// Plans `graph` with the given configuration.
    pub fn plan_with_config(
        &self,
        graph: Graph,
        planner_cfg: PlannerConfig,
    ) -> Result<daedalus_planner::PlannerOutput, EngineError> {
        let output = build_plan(PlannerInput { graph }, planner_cfg);
        let has_errors = output
            .diagnostics
            .iter()
            .any(|d| !matches!(d.code, daedalus_planner::DiagnosticCode::LintWarning));
        if has_errors {
            return Err(EngineError::Planner(output.diagnostics));
        }
        Ok(output)
    }

    /// Run planner through the engine's cache layer and return a prepared plan.
    pub fn prepare_plan(&self, graph: Graph) -> Result<PreparedPlan, EngineError> {
        let planner_cfg = self.planner_config_for(&graph)?;
        self.prepare_plan_with_config(graph, planner_cfg)
    }

    /// Prepare a plan using a plugin registry's native transport capabilities.
    #[cfg(feature = "plugins")]
    pub fn prepare_plugin_registry(
        &self,
        plugins: &PluginRegistry,
        graph: Graph,
    ) -> Result<PreparedPlan, EngineError> {
        let planner_cfg = plugins
            .planner_config_with_transport(self.planner_config_for(&graph)?)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        self.prepare_plan_with_config(graph, planner_cfg)
    }

    pub fn prepare_plan_with_config(
        &self,
        mut graph: Graph,
        planner_cfg: PlannerConfig,
    ) -> Result<PreparedPlan, EngineError> {
        if matches!(self.config.gpu, GpuBackend::Cpu) {
            crate::devices::use_cpu_fallbacks(&mut graph);
        }
        crate::devices::record_devices(
            &mut graph,
            planner_cfg.enable_gpu,
            !matches!(self.config.gpu, GpuBackend::Cpu),
        );
        let cache_key = planner_cache_key(&graph, &planner_cfg);
        let output = if let Some(cached) = self.caches.planner_get(&cache_key) {
            (cached, CacheStatus::Hit)
        } else {
            let output = self.plan_with_config(graph, planner_cfg)?;
            self.caches.planner_insert(cache_key, output.clone());
            (output, CacheStatus::Miss)
        };
        Ok(PreparedPlan {
            output: output.0,
            cache_status: output.1,
            scheduler: self.scheduler_config(),
            caches: Arc::clone(&self.caches),
        })
    }

    /// Compile a graph into a stream-capable retained runtime graph.
    pub fn compile<H: NodeHandler + Send + Sync + 'static>(
        &self,
        graph: Graph,
        handler: H,
    ) -> Result<HostGraph<H>, EngineError> {
        self.compile_host_graph(graph, handler, "host")
    }

    /// Compile a graph into a stream-capable retained runtime graph with an explicit host alias.
    pub fn compile_host_graph<H: NodeHandler + Send + Sync + 'static>(
        &self,
        graph: Graph,
        handler: H,
        host_alias: impl Into<String>,
    ) -> Result<HostGraph<H>, EngineError> {
        self.compile_host_graph_with_bridges(graph, handler, HostBridgeManager::new(), host_alias)
    }

    /// [`Self::compile_host_graph`] onto `bridges`, which may already hold the host state of an
    /// earlier compile (see `HostGraph::reload`).
    pub(crate) fn compile_host_graph_with_bridges<H: NodeHandler + Send + Sync + 'static>(
        &self,
        graph: Graph,
        handler: H,
        bridges: HostBridgeManager,
        host_alias: impl Into<String>,
    ) -> Result<HostGraph<H>, EngineError> {
        let host_alias = host_alias.into();
        let prepared = self.prepare_plan(graph)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = Arc::new(prepared_runtime.into_runtime_plan());
        self.configure_host_bridges(&bridges)?;
        bridges.populate_from_plan(runtime_plan.as_ref());
        let host = bridges.ensure_handle(host_alias);
        let executor = OwnedExecutor::try_new(runtime_plan.clone(), handler)?
            .with_host_bridges(bridges.clone());
        let executor = self.configure_owned_executor(executor)?;
        let node_labels = Arc::from(
            runtime_plan
                .nodes
                .iter()
                .map(|node| node.label.clone().unwrap_or_else(|| node.id.clone()))
                .collect::<Vec<_>>(),
        );
        Ok(HostGraph {
            runner: CompiledRun {
                runtime_plan,
                executor,
                runtime_mode: self.config.runtime.mode.clone(),
                planner_cache,
                runtime_plan_cache,
                caches: self.caches.clone(),
            },
            bridges,
            host,
            node_labels,
            value_serializers: daedalus_runtime::host_bridge::primitive_value_serializer_map(),
            types: daedalus_runtime::TypeIndex::default(),
            frame_overhead: None,
            selected: None,
            #[cfg(feature = "gpu-wgpu")]
            pipeline_cache: self.pipeline_cache_saver(),
        }
        .with_configured_frame_overhead(self.config.runtime.frame_overhead_window))
    }

    /// Compile a graph into the lower-level executor runner.
    #[doc(hidden)]
    pub fn compile_runner<H: NodeHandler + Send + Sync + 'static>(
        &self,
        graph: Graph,
        handler: H,
    ) -> Result<CompiledRun<H>, EngineError> {
        let prepared = self.prepare_plan(graph)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = Arc::new(prepared_runtime.into_runtime_plan());
        let executor =
            self.configure_owned_executor(OwnedExecutor::try_new(runtime_plan.clone(), handler)?)?;
        Ok(CompiledRun {
            runtime_plan,
            executor,
            runtime_mode: self.config.runtime.mode.clone(),
            planner_cache,
            runtime_plan_cache,
            caches: self.caches.clone(),
        })
    }

    /// Compile a graph and transport table into a reusable hot runner.
    pub fn compile_with_transport<H: NodeHandler + Send + Sync + 'static>(
        &self,
        graph: Graph,
        handler: H,
        transport: RuntimeTransport,
    ) -> Result<CompiledRun<H>, EngineError> {
        let prepared = self.prepare_plan(graph)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = Arc::new(prepared_runtime.into_runtime_plan());
        let executor = OwnedExecutor::try_new(runtime_plan.clone(), handler)?
            .with_runtime_transport(transport);
        let executor = self.configure_owned_executor(executor)?;
        Ok(CompiledRun {
            runtime_plan,
            executor,
            runtime_mode: self.config.runtime.mode.clone(),
            planner_cache,
            runtime_plan_cache,
            caches: self.caches.clone(),
        })
    }

    /// Compile a plugin-registry graph into a reusable hot runner.
    #[cfg(feature = "plugins")]
    pub fn compile_plugin_registry<H: NodeHandler + Send + Sync + 'static>(
        &self,
        plugins: &PluginRegistry,
        graph: Graph,
        handler: H,
    ) -> Result<CompiledRun<H>, EngineError> {
        validate_boundary_contracts(plugins, &graph)?;
        let planner_cfg = plugins
            .planner_config_with_transport(self.planner_config_for(&graph)?)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let prepared = self.prepare_plan_with_config(graph, planner_cfg)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = Arc::new(prepared_runtime.into_runtime_plan());
        let executor = OwnedExecutor::try_new(runtime_plan.clone(), handler)?
            .with_runtime_transport(plugins.runtime_transport.clone())
            .with_capabilities(plugins.capabilities.clone())
            .with_const_coercers(plugins.const_coercers.clone())
            .with_type_index(plugins.type_index());
        let executor = self.configure_owned_executor(executor)?;
        Ok(CompiledRun {
            runtime_plan,
            executor,
            runtime_mode: self.config.runtime.mode.clone(),
            planner_cache,
            runtime_plan_cache,
            caches: self.caches.clone(),
        })
    }

    /// Compile a plugin-registry graph into a retained host-fed runner.
    ///
    /// The returned graph is meant for stream-style usage:
    /// push one or more payloads into host ports, call `tick`, then drain host outputs.
    #[cfg(feature = "plugins")]
    pub fn compile_host_graph_plugin_registry<H: NodeHandler + Send + Sync + 'static>(
        &self,
        plugins: &PluginRegistry,
        graph: Graph,
        handler: H,
        bridges: HostBridgeManager,
        host_alias: impl Into<String>,
    ) -> Result<HostGraph<H>, EngineError> {
        let host_alias = host_alias.into();
        validate_boundary_contracts(plugins, &graph)?;
        let planner_cfg = plugins
            .planner_config_with_transport(self.planner_config_for(&graph)?)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let prepared = self.prepare_plan_with_config(graph, planner_cfg)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = Arc::new(prepared_runtime.into_runtime_plan());
        let types = plugins.type_index();
        self.configure_host_bridges(&bridges)?;
        bridges.set_type_index(types.clone());
        bridges.populate_from_plan(runtime_plan.as_ref());
        let host = bridges.ensure_handle(host_alias);
        let executor = OwnedExecutor::try_new(runtime_plan.clone(), handler)?
            .with_runtime_transport(plugins.runtime_transport.clone())
            .with_capabilities(plugins.capabilities.clone())
            .with_const_coercers(plugins.const_coercers.clone())
            .with_type_index(types.clone())
            .with_host_bridges(bridges.clone());
        let executor = self.configure_owned_executor(executor)?;
        let node_labels = Arc::from(
            runtime_plan
                .nodes
                .iter()
                .map(|node| node.label.clone().unwrap_or_else(|| node.id.clone()))
                .collect::<Vec<_>>(),
        );
        Ok(HostGraph {
            runner: CompiledRun {
                runtime_plan,
                executor,
                runtime_mode: self.config.runtime.mode.clone(),
                planner_cache,
                runtime_plan_cache,
                caches: self.caches.clone(),
            },
            bridges,
            host,
            node_labels,
            value_serializers: plugins.value_serializers.clone(),
            types,
            frame_overhead: None,
            selected: None,
            #[cfg(feature = "gpu-wgpu")]
            pipeline_cache: self.pipeline_cache_saver(),
        }
        .with_configured_frame_overhead(self.config.runtime.frame_overhead_window))
    }

    /// Compile a plugin-registry graph into the default host-fed streaming runtime.
    #[cfg(feature = "plugins")]
    pub fn compile_registry(
        &self,
        plugins: &PluginRegistry,
        graph: Graph,
    ) -> Result<HostGraph<HandlerRegistry>, EngineError> {
        self.compile_host_graph_plugin_registry(
            plugins,
            graph,
            plugins.handlers(),
            HostBridgeManager::new(),
            "host",
        )
    }

    pub(crate) fn scheduler_config(&self) -> SchedulerConfig {
        SchedulerConfig {
            default_policy: self.config.runtime.default_policy.clone(),
            backpressure: self.config.runtime.backpressure.clone(),
        }
    }

    pub fn planner_config(&self) -> Result<PlannerConfig, EngineError> {
        self.planner_config_with_caps(true)
    }

    /// The planner configuration for `graph`. The GPU capabilities, which open the device, are
    /// read only when the graph has a node that prefers or requires a GPU: a CPU-only graph plans
    /// without creating a GPU context. (The capabilities only validate GPU-affinity nodes.)
    pub(crate) fn planner_config_for(&self, graph: &Graph) -> Result<PlannerConfig, EngineError> {
        let wants_gpu = graph.nodes.iter().any(|node| {
            !matches!(
                node.compute,
                daedalus_core::compute::ComputeAffinity::CpuOnly
            )
        });
        self.planner_config_with_caps(wants_gpu)
    }

    fn planner_config_with_caps(&self, want_caps: bool) -> Result<PlannerConfig, EngineError> {
        if self.config.planner.enable_gpu
            && !cfg!(feature = "gpu")
            && matches!(self.config.gpu, GpuBackend::Device | GpuBackend::Mock)
        {
            return Err(EngineError::FeatureDisabled("gpu"));
        }
        // The caps come from the GPU caps source, never from a device: planning opens nothing.
        let gpu_caps = if self.config.planner.enable_gpu && want_caps {
            gpu_caps::plan_gpu_caps(&self.config)
        } else {
            GpuPlanCaps::default()
        };
        Ok(PlannerConfig {
            enable_gpu: self.config.planner.enable_gpu,
            enable_lints: self.config.planner.enable_lints,
            active_features: self.config.planner.active_features.clone(),
            transport_capabilities: None,
            lowerings: Default::default(),
            strict_port_declarations: false,
            gpu_caps,
        })
    }
}

#[cfg(all(test, feature = "plugins"))]
mod boundary_tests {
    use super::*;
    use alloc::collections::BTreeMap;
    use daedalus_planner::NodeInstance;
    use daedalus_registry::capability::{NodeDecl, PortDecl};
    use daedalus_transport::{BoundaryTypeContract, LayoutHash, TypeKey};

    fn graph_with_node(id: &str) -> Graph {
        Graph {
            nodes: vec![NodeInstance::new(id)],
            edges: vec![],
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn boundary_compile_validation_accepts_compatible_contract() {
        let mut plugins = PluginRegistry::bare();
        let key = TypeKey::new("test:frame");
        plugins
            .register_boundary_contract(BoundaryTypeContract::new(
                key.clone(),
                LayoutHash::new("same-layout"),
                BoundaryCapabilities::rust_value(),
            ))
            .unwrap();
        plugins
            .register_node_decl(
                NodeDecl::new("test.node")
                    .input(PortDecl::new("frame", key.clone()).access(AccessMode::Read))
                    .output(PortDecl::new("out", key).access(AccessMode::Read)),
            )
            .unwrap();

        validate_boundary_contracts(&plugins, &graph_with_node("test.node")).unwrap();
    }

    #[test]
    fn boundary_compile_validation_rejects_missing_capability() {
        let mut plugins = PluginRegistry::bare();
        let key = TypeKey::new("test:owned-only");
        plugins
            .register_boundary_contract(BoundaryTypeContract::new(
                key.clone(),
                LayoutHash::new("same-layout"),
                BoundaryCapabilities::owned(),
            ))
            .unwrap();
        plugins
            .register_node_decl(
                NodeDecl::new("test.node")
                    .input(PortDecl::new("frame", key).access(AccessMode::Read)),
            )
            .unwrap();

        let err = validate_boundary_contracts(&plugins, &graph_with_node("test.node"))
            .expect_err("read input should require borrow_ref");
        assert!(err.to_string().contains("boundary capabilities mismatch"));
    }

    #[test]
    fn boundary_registry_rejects_incompatible_layout() {
        let mut plugins = PluginRegistry::bare();
        let key = TypeKey::new("test:layout");
        plugins
            .register_boundary_contract(BoundaryTypeContract::new(
                key.clone(),
                LayoutHash::new("layout-a"),
                BoundaryCapabilities::rust_value(),
            ))
            .unwrap();
        let err = plugins
            .register_boundary_contract(BoundaryTypeContract::new(
                key,
                LayoutHash::new("layout-b"),
                BoundaryCapabilities::rust_value(),
            ))
            .expect_err("same type key with different layout should fail");
        assert!(err.to_string().contains("boundary layout mismatch"));
    }

    #[test]
    fn boundary_registry_accepts_compatible_contracts_without_feature_identity() {
        let mut plugins = PluginRegistry::bare();
        let key = TypeKey::new("example:frame");
        let mut host_contract = BoundaryTypeContract::new(
            key.clone(),
            LayoutHash::new("framelease-layout-v1"),
            BoundaryCapabilities::frame_like(),
        );
        host_contract.rust_type_name = Some("host_build::Frame".to_string());
        let mut plugin_contract = BoundaryTypeContract::new(
            key,
            LayoutHash::new("framelease-layout-v1"),
            BoundaryCapabilities::frame_like(),
        );
        plugin_contract.rust_type_name = Some("plugin_build::Frame".to_string());

        plugins.register_boundary_contract(host_contract).unwrap();
        plugins
            .register_boundary_contract(plugin_contract)
            .expect("layout/capability-compatible contracts should not require identical cargo feature identity");
    }
}
