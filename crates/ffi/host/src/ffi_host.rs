//! One-call FFI host: registry install, runner startup, and invoke-by-node-id.
//!
//! [`FfiHost`] is a thin composition over [`HostInstallPlan`], [`RunnerPool`], and
//! [`FfiHostTelemetry`]. It does not add a second lifecycle system: packages are installed with
//! [`install_package_with_ffi_telemetry`], runners are started through the same path as
//! [`install_plan_runners`](crate::install_plan_runners), and invokes go through
//! [`RunnerPool::invoke`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use daedalus_ffi_core::{BackendConfig, InvokeRequest, InvokeResponse, PluginPackage};
use daedalus_registry::capability::CapabilityRegistry;
use thiserror::Error;

use crate::installer::start_plan_runners;
use crate::{
    BackendRunner, BackendRunnerFactory, FfiHostTelemetry, HostInstallError, HostInstallPlan,
    PersistentWorkerRunner, RunnerHealth, RunnerKey, RunnerLimits, RunnerPool, RunnerPoolError,
    RunnerPoolOptions, RunnerShutdownError, install_package_with_ffi_telemetry,
};

/// Errors returned by [`FfiHost`].
#[derive(Debug, Error)]
pub enum FfiHostError {
    /// Package validation, registry install, or runner startup failed.
    #[error(transparent)]
    Install(#[from] HostInstallError),
    /// A package declares a node id that another package in this host already installed.
    #[error("FFI node `{node_id}` is already installed in this host")]
    DuplicateNode { node_id: String },
    /// No installed package declares this node id.
    #[error("unknown FFI node `{node_id}`")]
    UnknownNode { node_id: String },
    /// The node runs in process and is not served by the runner pool.
    #[error(
        "FFI node `{node_id}` uses the in_process_abi runtime model; native Rust plugins are \
         loaded through the facade `dylib-plugins` feature, not the runner pool"
    )]
    InProcessNode { node_id: String },
    /// The node's package was installed with deferred runner startup.
    #[error("runner for FFI node `{node_id}` has not been started; call FfiHost::start_runners")]
    RunnerNotStarted { node_id: String },
    /// The request names a different node than the one it was sent to.
    #[error("invoke request targets node `{request_node_id}` but was sent to `{node_id}`")]
    RequestNodeMismatch {
        node_id: String,
        request_node_id: String,
    },
    /// The runner serving this node failed.
    #[error("runner for FFI node `{node_id}` failed: {source}")]
    Runner {
        node_id: String,
        #[source]
        source: RunnerPoolError,
    },
    /// One or more runners failed to shut down cleanly.
    #[error("failed to shut down FFI runners: {0}")]
    Shutdown(#[source] RunnerShutdownError),
}

/// Builder for [`FfiHost`] options: shared telemetry, pool options, and deferred startup.
#[derive(Clone, Debug, Default)]
pub struct FfiHostBuilder {
    telemetry: Option<FfiHostTelemetry>,
    pool_options: RunnerPoolOptions,
    defer_runner_startup: bool,
}

impl FfiHostBuilder {
    /// Create a builder with a fresh telemetry collector and default pool options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Share an existing telemetry collector with the host, its pool, and its runners.
    pub fn telemetry(mut self, telemetry: FfiHostTelemetry) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Set the runner pool options (idle timeout and runner limits).
    pub fn pool_options(mut self, options: RunnerPoolOptions) -> Self {
        self.pool_options = options;
        self
    }

    /// When `true`, packages are installed into the registry but runners are started only by
    /// [`FfiHost::start_runners`].
    pub fn defer_runner_startup(mut self, defer: bool) -> Self {
        self.defer_runner_startup = defer;
        self
    }

    /// Build an empty host.
    pub fn build(self) -> FfiHost {
        let telemetry = self.telemetry.unwrap_or_default();
        FfiHost {
            pool: RunnerPool::with_options(self.pool_options).with_ffi_telemetry(telemetry.clone()),
            telemetry,
            plans: Vec::new(),
            routes: BTreeMap::new(),
            started: BTreeSet::new(),
            pending: Vec::new(),
            defer_runner_startup: self.defer_runner_startup,
        }
    }

    /// Build a host and install `package` into it.
    pub fn install_package(
        self,
        registry: &mut CapabilityRegistry,
        package: &PluginPackage,
        factory: &impl BackendRunnerFactory,
    ) -> Result<FfiHost, FfiHostError> {
        let mut host = self.build();
        host.add_package(registry, package, factory)?;
        Ok(host)
    }
}

struct NodeRoute {
    plan: usize,
    backend: BackendConfig,
    key: Option<RunnerKey>,
}

/// Installed FFI packages plus the runner pool that serves their nodes.
///
/// Persistent-worker backends get one runner per distinct [`RunnerKey`], shared across nodes and
/// packages. `in_process_abi` backends are registered but never started here; native Rust `cdylib`
/// plugins are loaded through the facade `dylib-plugins` feature (see `docs/dynamic-plugins.md`).
///
/// Dropping the host drops its [`RunnerPool`], which shuts every runner down. Use
/// [`FfiHost::shutdown`] to observe shutdown errors.
pub struct FfiHost {
    pool: RunnerPool,
    telemetry: FfiHostTelemetry,
    plans: Vec<HostInstallPlan>,
    routes: BTreeMap<String, NodeRoute>,
    started: BTreeSet<RunnerKey>,
    pending: Vec<usize>,
    defer_runner_startup: bool,
}

impl Default for FfiHost {
    fn default() -> Self {
        FfiHostBuilder::new().build()
    }
}

impl FfiHost {
    /// Start configuring a host.
    pub fn builder() -> FfiHostBuilder {
        FfiHostBuilder::new()
    }

    /// Create an empty host with fresh telemetry and default pool options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Install `package` into `registry` and start its persistent-worker runners.
    pub fn install_package(
        registry: &mut CapabilityRegistry,
        package: &PluginPackage,
        factory: &impl BackendRunnerFactory,
    ) -> Result<Self, FfiHostError> {
        FfiHostBuilder::new().install_package(registry, package, factory)
    }

    /// Install another package into `registry` and this host, sharing the pool and telemetry.
    ///
    /// The install is atomic: if validation, registry install, or runner startup fails, the
    /// package's registry entries are removed and runners started for it are shut down. Runners
    /// whose backend config matches an already running runner are reused.
    pub fn add_package(
        &mut self,
        registry: &mut CapabilityRegistry,
        package: &PluginPackage,
        factory: &impl BackendRunnerFactory,
    ) -> Result<&HostInstallPlan, FfiHostError> {
        if let Some(schema) = &package.schema
            && let Some(node) = schema
                .nodes
                .iter()
                .find(|node| self.routes.contains_key(&node.id))
        {
            return Err(FfiHostError::DuplicateNode {
                node_id: node.id.clone(),
            });
        }
        let plan = install_package_with_ffi_telemetry(registry, package, &self.telemetry)?;
        let index = self.plans.len();
        self.plans.push(plan);
        let result = plan_routes(&self.plans[index], index).and_then(|routes| {
            self.routes.extend(routes);
            if self.defer_runner_startup {
                self.pending.push(index);
                Ok(())
            } else {
                self.start_plan(index, factory).map(drop)
            }
        });
        if let Err(err) = result {
            self.remove_package(registry, index);
            return Err(err.into());
        }
        Ok(&self.plans[index])
    }

    /// Start runners for packages installed with deferred startup. Returns new runner keys.
    ///
    /// `registry` must be the registry the pending packages were installed into. Packages start
    /// in install order. If one fails, it is rolled back like a failed [`FfiHost::add_package`]:
    /// its registry entries and host routes are removed and runners started for it are shut down.
    /// Packages started before it keep running; later packages stay pending.
    pub fn start_runners(
        &mut self,
        registry: &mut CapabilityRegistry,
        factory: &impl BackendRunnerFactory,
    ) -> Result<Vec<RunnerKey>, FfiHostError> {
        let mut started = Vec::new();
        while !self.pending.is_empty() {
            let index = self.pending.remove(0);
            match self.start_plan(index, factory) {
                Ok(keys) => started.extend(keys),
                Err(err) => {
                    self.remove_package(registry, index);
                    return Err(err.into());
                }
            }
        }
        Ok(started)
    }

    /// Start the runners of plan `index`; on failure the runners it started are already stopped.
    fn start_plan(
        &mut self,
        index: usize,
        factory: &impl BackendRunnerFactory,
    ) -> Result<Vec<RunnerKey>, HostInstallError> {
        let keys = start_plan_runners(&mut self.pool, &self.plans[index], factory, &self.started)?;
        self.started.extend(keys.iter().cloned());
        Ok(keys)
    }

    /// Roll back plan `index`: remove its registry entries, routes, and pending slot.
    fn remove_package(&mut self, registry: &mut CapabilityRegistry, index: usize) {
        let plan = self.plans.remove(index);
        registry.remove_plugin(&plan.plugin.id);
        self.routes.retain(|_, route| route.plan != index);
        self.pending.retain(|&pending| pending != index);
        let shift = |plan: &mut usize| *plan -= usize::from(*plan > index);
        self.routes
            .values_mut()
            .for_each(|route| shift(&mut route.plan));
        self.pending.iter_mut().for_each(shift);
    }

    /// Invoke `node_id` on its runner.
    ///
    /// An empty `request.node_id` is filled in; a different non-empty one is rejected.
    pub fn invoke(
        &self,
        node_id: &str,
        mut request: InvokeRequest,
    ) -> Result<InvokeResponse, FfiHostError> {
        let backend = self.runner_backend(node_id)?;
        bind_request(node_id, &mut request)?;
        self.pool
            .invoke(backend, request)
            .map_err(|source| runner_error(node_id, source))
    }

    /// Invoke `node_id` with a batch of requests on its runner.
    pub fn invoke_batch(
        &self,
        node_id: &str,
        mut requests: Vec<InvokeRequest>,
    ) -> Result<Vec<InvokeResponse>, FfiHostError> {
        let backend = self.runner_backend(node_id)?;
        for request in &mut requests {
            bind_request(node_id, request)?;
        }
        self.pool
            .invoke_batch(backend, requests)
            .map_err(|source| runner_error(node_id, source))
    }

    /// Health of the runner serving `node_id`.
    pub fn health(&self, node_id: &str) -> Result<RunnerHealth, FfiHostError> {
        let backend = self.runner_backend(node_id)?;
        self.pool
            .health(backend)
            .map_err(|source| runner_error(node_id, source))
    }

    /// Runner key serving `node_id`, or `None` for unknown and `in_process_abi` nodes.
    pub fn runner_key(&self, node_id: &str) -> Option<&RunnerKey> {
        self.routes.get(node_id)?.key.as_ref()
    }

    /// Backend config for `node_id`.
    pub fn backend(&self, node_id: &str) -> Option<&BackendConfig> {
        self.routes.get(node_id).map(|route| &route.backend)
    }

    /// Install plan of the package that declares `node_id`.
    pub fn plan_for_node(&self, node_id: &str) -> Option<&HostInstallPlan> {
        self.routes
            .get(node_id)
            .map(|route| &self.plans[route.plan])
    }

    /// Install plan of the package with plugin id `plugin_id`.
    pub fn plan(&self, plugin_id: &str) -> Option<&HostInstallPlan> {
        self.plans.iter().find(|plan| plan.plugin.id == plugin_id)
    }

    /// Install plans, in install order.
    pub fn plans(&self) -> &[HostInstallPlan] {
        &self.plans
    }

    /// Installed node ids, sorted.
    pub fn node_ids(&self) -> impl Iterator<Item = &str> {
        self.routes.keys().map(String::as_str)
    }

    /// Whether any installed package declares `node_id`.
    pub fn contains_node(&self, node_id: &str) -> bool {
        self.routes.contains_key(node_id)
    }

    /// Whether any package is waiting for [`FfiHost::start_runners`].
    pub fn has_pending_runners(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Shared telemetry collector used by install, pool, and runners.
    pub fn telemetry(&self) -> &FfiHostTelemetry {
        &self.telemetry
    }

    /// Underlying runner pool (payload leases, pool telemetry).
    pub fn pool(&self) -> &RunnerPool {
        &self.pool
    }

    /// Mutable runner pool, for advanced lifecycle control such as `prune_idle`.
    pub fn pool_mut(&mut self) -> &mut RunnerPool {
        &mut self.pool
    }

    /// Factory for the built-in persistent-worker runner, sharing this host's telemetry and pool
    /// runner limits.
    pub fn persistent_worker_factory(&self) -> PersistentWorkerRunnerFactory {
        PersistentWorkerRunnerFactory::new()
            .with_limits(self.pool.options().limits.clone())
            .with_ffi_telemetry(self.telemetry.clone())
    }

    /// Shut down every runner, reporting every failure after attempting all of them.
    pub fn shutdown(mut self) -> Result<(), FfiHostError> {
        self.pool.shutdown_all().map_err(FfiHostError::Shutdown)
    }

    fn runner_backend(&self, node_id: &str) -> Result<&BackendConfig, FfiHostError> {
        let route = self
            .routes
            .get(node_id)
            .ok_or_else(|| FfiHostError::UnknownNode {
                node_id: node_id.to_owned(),
            })?;
        let key = route
            .key
            .as_ref()
            .ok_or_else(|| FfiHostError::InProcessNode {
                node_id: node_id.to_owned(),
            })?;
        if !self.started.contains(key) {
            return Err(FfiHostError::RunnerNotStarted {
                node_id: node_id.to_owned(),
            });
        }
        Ok(&route.backend)
    }
}

fn plan_routes(
    plan: &HostInstallPlan,
    index: usize,
) -> Result<Vec<(String, NodeRoute)>, HostInstallError> {
    let mut keys = plan.runner_keys()?;
    Ok(plan
        .backends
        .iter()
        .map(|(node_id, backend)| {
            let route = NodeRoute {
                plan: index,
                backend: backend.clone(),
                key: keys.remove(node_id.as_str()),
            };
            (node_id.clone(), route)
        })
        .collect())
}

fn bind_request(node_id: &str, request: &mut InvokeRequest) -> Result<(), FfiHostError> {
    if request.node_id.is_empty() {
        request.node_id = node_id.to_owned();
    } else if request.node_id != node_id {
        return Err(FfiHostError::RequestNodeMismatch {
            node_id: node_id.to_owned(),
            request_node_id: request.node_id.clone(),
        });
    }
    Ok(())
}

fn runner_error(node_id: &str, source: RunnerPoolError) -> FfiHostError {
    FfiHostError::Runner {
        node_id: node_id.to_owned(),
        source,
    }
}

/// [`BackendRunnerFactory`] for the built-in [`PersistentWorkerRunner`].
///
/// Serves Python, Node, Java, and other `persistent_worker` backends. It rejects
/// `in_process_abi` configs, which [`FfiHost`] never asks it to build.
#[derive(Clone, Debug, Default)]
pub struct PersistentWorkerRunnerFactory {
    limits: RunnerLimits,
    telemetry: Option<FfiHostTelemetry>,
}

impl PersistentWorkerRunnerFactory {
    /// Factory with default runner limits and no telemetry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Runner limits applied to every worker this factory builds.
    pub fn with_limits(mut self, limits: RunnerLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Telemetry collector passed to every worker this factory builds.
    pub fn with_ffi_telemetry(mut self, telemetry: FfiHostTelemetry) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Runner limits applied to built workers.
    pub fn limits(&self) -> &RunnerLimits {
        &self.limits
    }
}

impl BackendRunnerFactory for PersistentWorkerRunnerFactory {
    fn build_runner(
        &self,
        _node_id: &str,
        backend: &BackendConfig,
    ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        let runner = match &self.telemetry {
            Some(telemetry) => PersistentWorkerRunner::from_backend_with_limits_and_telemetry(
                backend,
                &self.limits,
                telemetry.clone(),
            )?,
            None => PersistentWorkerRunner::from_backend_with_limits(backend, &self.limits)?,
        };
        Ok(Arc::new(runner))
    }
}

#[cfg(test)]
mod tests;
