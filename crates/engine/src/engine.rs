use std::sync::Arc;

use daedalus_planner::{Graph, PlannerConfig, PlannerInput, build_plan};
use daedalus_runtime::executor::{NodeHandler, OwnedExecutor};
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

/// High-level engine facade for planning and execution.
///
/// ```no_run
/// use daedalus_engine::{Engine, EngineConfig};
/// let engine = Engine::new(EngineConfig::default()).unwrap();
/// let _ = engine.config();
/// ```
pub struct Engine {
    pub(crate) config: EngineConfig,
    caches: Arc<EngineCaches>,
    #[cfg(feature = "gpu")]
    gpu_handle: std::sync::Mutex<Option<Arc<daedalus_gpu::GpuContextHandle>>>,
}

impl Engine {
    /// Create a new engine from configuration.
    pub fn new(config: impl Into<EngineConfig>) -> Result<Self, EngineError> {
        let config = config.into();
        config.validate()?;
        if matches!(config.gpu, GpuBackend::Device | GpuBackend::Mock) && !cfg!(feature = "gpu") {
            return Err(EngineError::FeatureDisabled("gpu"));
        }
        Ok(Self {
            caches: crate::cache::new_caches(
                config.cache.planner_max_entries,
                config.cache.runtime_plan_max_entries,
            ),
            config,
            #[cfg(feature = "gpu")]
            gpu_handle: std::sync::Mutex::new(None),
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
        exec = exec
            .with_fail_fast(self.config.runtime.fail_fast)
            .with_metrics_level(self.config.runtime.metrics_level)
            .with_runtime_debug_config(self.config.runtime.debug_config);
        if self.config.runtime.demand_driven && !self.config.runtime.demand_sinks.is_empty() {
            exec = exec.with_demand_sinks(self.config.runtime.demand_sinks.clone());
        }
        if matches!(
            self.config.runtime.mode,
            RuntimeMode::Parallel | RuntimeMode::Adaptive
        ) {
            exec = exec.with_pool_size(self.config.runtime.pool_size);
            #[cfg(feature = "executor-pool")]
            exec.prewarm_worker_pool()?;
        }
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
        }
        Ok(exec)
    }

    fn configure_host_bridges(&self, bridges: &HostBridgeManager) -> Result<(), EngineError> {
        bridges
            .apply_config(&self.config.runtime.host_bridge_config())
            .map_err(|err| EngineError::Config(err.to_string()))?;
        Ok(())
    }

    pub fn compile_runtime_plan<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        handler: H,
    ) -> Result<CompiledRun<H>, EngineError> {
        let runtime_plan = Arc::new(runtime_plan);
        let executor =
            self.configure_owned_executor(OwnedExecutor::new(runtime_plan.clone(), handler))?;
        Ok(CompiledRun {
            runtime_plan,
            executor,
            runtime_mode: self.config.runtime.mode.clone(),
            planner_cache: CacheStatus::Miss,
            runtime_plan_cache: CacheStatus::Miss,
            caches: self.caches.clone(),
        })
    }

    pub fn compile_runtime_plan_with_transport<H: NodeHandler + Send + Sync + 'static>(
        &self,
        runtime_plan: RuntimePlan,
        handler: H,
        transport: RuntimeTransport,
    ) -> Result<CompiledRun<H>, EngineError> {
        let runtime_plan = Arc::new(runtime_plan);
        let executor =
            OwnedExecutor::new(runtime_plan.clone(), handler).with_runtime_transport(transport);
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
    /// ```no_run
    /// use daedalus_engine::{Engine, EngineConfig};
    /// use daedalus_planner::Graph;
    /// let engine = Engine::new(EngineConfig::default()).unwrap();
    /// let _ = engine.plan(Graph::default());
    /// ```
    pub fn plan(&self, graph: Graph) -> Result<daedalus_planner::PlannerOutput, EngineError> {
        let planner_cfg = self.planner_config()?;
        self.plan_with_config(graph, planner_cfg)
    }

    /// Plan using a plugin registry's native transport capabilities.
    #[cfg(feature = "plugins")]
    pub fn plan_plugin_registry(
        &self,
        plugins: &PluginRegistry,
        graph: Graph,
    ) -> Result<daedalus_planner::PlannerOutput, EngineError> {
        let planner_cfg = plugins
            .planner_config_with_transport(self.planner_config()?)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        self.plan_with_config(graph, planner_cfg)
    }

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
        let planner_cfg = self.planner_config()?;
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
            .planner_config_with_transport(self.planner_config()?)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        self.prepare_plan_with_config(graph, planner_cfg)
    }

    pub fn prepare_plan_with_config(
        &self,
        graph: Graph,
        planner_cfg: PlannerConfig,
    ) -> Result<PreparedPlan, EngineError> {
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
        let host_alias = host_alias.into();
        let prepared = self.prepare_plan(graph)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = Arc::new(prepared_runtime.into_runtime_plan());
        let bridges = HostBridgeManager::new();
        self.configure_host_bridges(&bridges)?;
        bridges.populate_from_plan(runtime_plan.as_ref());
        let host = bridges.ensure_handle(host_alias);
        let executor =
            OwnedExecutor::new(runtime_plan.clone(), handler).with_host_bridges(bridges.clone());
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
        })
    }

    /// Compile a graph into the lower-level executor runner.
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
            self.configure_owned_executor(OwnedExecutor::new(runtime_plan.clone(), handler))?;
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
        let executor =
            OwnedExecutor::new(runtime_plan.clone(), handler).with_runtime_transport(transport);
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
            .planner_config_with_transport(self.planner_config()?)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let prepared = self.prepare_plan_with_config(graph, planner_cfg)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = Arc::new(prepared_runtime.into_runtime_plan());
        let executor = OwnedExecutor::new(runtime_plan.clone(), handler)
            .with_runtime_transport(plugins.runtime_transport.clone())
            .with_capabilities(plugins.capabilities.clone());
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
            .planner_config_with_transport(self.planner_config()?)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let prepared = self.prepare_plan_with_config(graph, planner_cfg)?;
        let planner_cache = prepared.cache_status();
        let prepared_runtime = prepared.build()?;
        let runtime_plan_cache = prepared_runtime.cache_status();
        let runtime_plan = Arc::new(prepared_runtime.into_runtime_plan());
        self.configure_host_bridges(&bridges)?;
        bridges.populate_from_plan(runtime_plan.as_ref());
        let host = bridges.ensure_handle(host_alias);
        let executor = OwnedExecutor::new(runtime_plan.clone(), handler)
            .with_runtime_transport(plugins.runtime_transport.clone())
            .with_capabilities(plugins.capabilities.clone())
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
        })
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
        if self.config.planner.enable_gpu
            && !cfg!(feature = "gpu")
            && matches!(self.config.gpu, GpuBackend::Device | GpuBackend::Mock)
        {
            return Err(EngineError::FeatureDisabled("gpu"));
        }
        #[cfg(feature = "gpu")]
        let mut cfg = PlannerConfig {
            enable_gpu: self.config.planner.enable_gpu,
            enable_lints: self.config.planner.enable_lints,
            active_features: self.config.planner.active_features.clone(),
            transport_capabilities: None,
            lowerings: Default::default(),
            strict_port_declarations: false,
            gpu_caps: None,
        };
        #[cfg(not(feature = "gpu"))]
        let cfg = PlannerConfig {
            enable_gpu: self.config.planner.enable_gpu,
            enable_lints: self.config.planner.enable_lints,
            active_features: self.config.planner.active_features.clone(),
            transport_capabilities: None,
            lowerings: Default::default(),
            strict_port_declarations: false,
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
    pub(crate) fn get_gpu_handle(
        &self,
    ) -> Result<Option<Arc<daedalus_gpu::GpuContextHandle>>, EngineError> {
        use daedalus_gpu::{GpuBackendKind, GpuOptions, select_backend};
        if matches!(self.config.gpu, GpuBackend::Cpu) {
            return Ok(None);
        }
        let mut guard = self
            .gpu_handle
            .lock()
            .map_err(|_| EngineError::Config("gpu handle lock poisoned".into()))?;
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

#[cfg(all(test, feature = "plugins"))]
mod boundary_tests {
    use super::*;
    use daedalus_planner::{ComputeAffinity, NodeInstance};
    use daedalus_registry::capability::{NodeDecl, PortDecl};
    use daedalus_registry::ids::NodeId;
    use daedalus_transport::{BoundaryTypeContract, LayoutHash, TypeKey};
    use std::collections::BTreeMap;

    fn graph_with_node(id: &str) -> Graph {
        Graph {
            nodes: vec![NodeInstance {
                id: NodeId::new(id),
                bundle: None,
                label: None,
                inputs: vec![],
                outputs: vec![],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: BTreeMap::new(),
            }],
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
        let key = TypeKey::new("styx:framelease");
        let mut host_contract = BoundaryTypeContract::new(
            key.clone(),
            LayoutHash::new("framelease-layout-v1"),
            BoundaryCapabilities::frame_like(),
        );
        host_contract.rust_type_name = Some("host_build::FrameLease".to_string());
        let mut plugin_contract = BoundaryTypeContract::new(
            key,
            LayoutHash::new("framelease-layout-v1"),
            BoundaryCapabilities::frame_like(),
        );
        plugin_contract.rust_type_name = Some("plugin_build::FrameLease".to_string());

        plugins.register_boundary_contract(host_contract).unwrap();
        plugins
            .register_boundary_contract(plugin_contract)
            .expect("layout/capability-compatible contracts should not require identical cargo feature identity");
    }
}
