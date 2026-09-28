use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_ffi_core::{
    BackendKind, NodeSchema, PluginSchema, PluginSchemaInfo, SCHEMA_VERSION, WirePort, WireValue,
};
use daedalus_registry::ids::NodeId;
use daedalus_transport::AccessMode;

use super::*;

#[derive(Default)]
struct Counters {
    builds: AtomicUsize,
    shutdowns: AtomicUsize,
}

struct EchoRunner {
    counters: Arc<Counters>,
    supported_nodes: Option<Vec<String>>,
}

impl BackendRunner for EchoRunner {
    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        Ok(InvokeResponse {
            protocol_version: request.protocol_version,
            correlation_id: request.correlation_id,
            outputs: BTreeMap::from([("out".into(), WireValue::String(request.node_id))]),
            state: None,
            events: Vec::new(),
        })
    }

    fn supported_nodes(&self) -> Option<Vec<String>> {
        self.supported_nodes.clone()
    }

    fn shutdown(&self) -> Result<(), RunnerPoolError> {
        self.counters.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct EchoFactory {
    counters: Arc<Counters>,
    supported_nodes: Option<Vec<String>>,
    fail_executable: Option<&'static str>,
}

impl BackendRunnerFactory for EchoFactory {
    fn build_runner(
        &self,
        _node_id: &str,
        backend: &BackendConfig,
    ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        if self.fail_executable.is_some() && backend.executable.as_deref() == self.fail_executable {
            return Err(RunnerPoolError::Runner("worker failed to start".into()));
        }
        self.counters.builds.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(EchoRunner {
            counters: self.counters.clone(),
            supported_nodes: self.supported_nodes.clone(),
        }))
    }
}

fn port(name: &str) -> WirePort {
    WirePort {
        name: name.into(),
        ty: TypeExpr::Scalar(ValueType::Int),
        type_key: None,
        optional: false,
        access: AccessMode::Read,
        residency: None,
        layout: None,
        source: None,
        const_value: None,
    }
}

fn node(id: &str, backend: BackendKind) -> NodeSchema {
    NodeSchema {
        id: id.into(),
        backend,
        entrypoint: id.into(),
        label: None,
        stateful: false,
        feature_flags: Vec::new(),
        inputs: vec![port("value")],
        outputs: vec![port("out")],
        metadata: BTreeMap::new(),
    }
}

fn worker_backend(executable: &str) -> BackendConfig {
    BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some("demo.py".into()),
        entry_class: None,
        entry_symbol: Some("run".into()),
        executable: Some(executable.into()),
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

fn in_process_backend() -> BackendConfig {
    BackendConfig {
        backend: BackendKind::Rust,
        runtime_model: BackendRuntimeModel::InProcessAbi,
        entry_module: Some("libdemo_plugin.so".into()),
        entry_symbol: Some("daedalus_plugin_register".into()),
        executable: None,
        ..worker_backend("unused")
    }
}

fn package(plugin: &str, nodes: Vec<(NodeSchema, BackendConfig)>) -> PluginPackage {
    let backends = nodes
        .iter()
        .map(|(node, backend)| (node.id.clone(), backend.clone()))
        .collect();
    PluginPackage {
        schema_version: SCHEMA_VERSION,
        schema: Some(PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: plugin.into(),
                version: Some("1.0.0".into()),
                description: None,
                metadata: BTreeMap::new(),
            },
            dependencies: Vec::new(),
            required_host_capabilities: Vec::new(),
            feature_flags: Vec::new(),
            boundary_contracts: Vec::new(),
            nodes: nodes.into_iter().map(|(node, _)| node).collect(),
        }),
        backends,
        artifacts: Vec::new(),
        lockfile: None,
        manifest_hash: None,
        signature: None,
        metadata: BTreeMap::new(),
    }
}

fn python_package(plugin: &str, node_id: &str, executable: &str) -> PluginPackage {
    package(
        plugin,
        vec![(
            node(node_id, BackendKind::Python),
            worker_backend(executable),
        )],
    )
}

fn request(node_id: &str) -> InvokeRequest {
    InvokeRequest {
        protocol_version: daedalus_ffi_core::WORKER_PROTOCOL_VERSION,
        node_id: node_id.into(),
        correlation_id: Some("c1".into()),
        args: BTreeMap::from([("value".into(), WireValue::Int(2))]),
        state: None,
        context: BTreeMap::new(),
    }
}

fn out(response: &InvokeResponse) -> &WireValue {
    response.outputs.get("out").expect("out output")
}

#[test]
fn installs_package_and_invokes_by_node_id() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();

    let host = FfiHost::install_package(
        &mut registry,
        &python_package("demo.plugin", "demo.echo", "python"),
        &factory,
    )
    .expect("host installs");

    assert!(registry.plugin_manifest("demo.plugin").is_some());
    assert!(registry.node_decl(&NodeId::new("demo.echo")).is_some());
    assert_eq!(host.node_ids().collect::<Vec<_>>(), vec!["demo.echo"]);
    assert_eq!(host.plans().len(), 1);
    assert!(host.plan("demo.plugin").is_some());
    assert_eq!(
        host.plan_for_node("demo.echo").unwrap().plugin.id,
        "demo.plugin"
    );
    assert!(host.runner_key("demo.echo").is_some());
    assert_eq!(host.health("demo.echo").unwrap(), RunnerHealth::Ready);

    let response = host.invoke("demo.echo", request("")).expect("invoke");
    assert_eq!(out(&response), &WireValue::String("demo.echo".into()));
    assert_eq!(response.correlation_id.as_deref(), Some("c1"));

    let batch = host
        .invoke_batch("demo.echo", vec![request("demo.echo"), request("")])
        .expect("batch");
    assert_eq!(batch.len(), 2);

    assert_eq!(factory.counters.builds.load(Ordering::SeqCst), 1);
    assert_eq!(host.pool().telemetry().invokes, 3);
    let report = host.telemetry().snapshot();
    assert!(report.packages.contains_key("demo.plugin"));
    let key = host.runner_key("demo.echo").unwrap().as_str();
    assert_eq!(report.backends[key].invokes, 3);
}

#[test]
fn rejects_unknown_node_and_mismatched_request() {
    let mut registry = CapabilityRegistry::default();
    let host = FfiHost::install_package(
        &mut registry,
        &python_package("demo.plugin", "demo.echo", "python"),
        &EchoFactory::default(),
    )
    .expect("host installs");

    assert!(matches!(
        host.invoke("demo.missing", request("")),
        Err(FfiHostError::UnknownNode { node_id }) if node_id == "demo.missing"
    ));
    assert!(matches!(
        host.invoke("demo.echo", request("demo.other")),
        Err(FfiHostError::RequestNodeMismatch { .. })
    ));
    assert!(host.backend("demo.missing").is_none());
}

#[test]
fn shares_pool_and_telemetry_across_packages() {
    let telemetry = FfiHostTelemetry::new();
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let mut host = FfiHost::builder()
        .telemetry(telemetry.clone())
        .install_package(
            &mut registry,
            &python_package("demo.a", "demo.a.echo", "python"),
            &factory,
        )
        .expect("first package");

    // Same backend config: the running worker is reused.
    host.add_package(
        &mut registry,
        &python_package("demo.b", "demo.b.echo", "python"),
        &factory,
    )
    .expect("second package");
    // Different backend config: a new worker is started.
    host.add_package(
        &mut registry,
        &python_package("demo.c", "demo.c.echo", "python3"),
        &factory,
    )
    .expect("third package");

    assert_eq!(factory.counters.builds.load(Ordering::SeqCst), 2);
    assert_eq!(host.pool().len(), 2);
    assert_eq!(host.plans().len(), 3);
    assert_eq!(
        host.runner_key("demo.a.echo"),
        host.runner_key("demo.b.echo")
    );
    for node_id in ["demo.a.echo", "demo.b.echo", "demo.c.echo"] {
        assert!(registry.node_decl(&NodeId::new(node_id)).is_some());
        let response = host.invoke(node_id, request("")).expect("invoke");
        assert_eq!(out(&response), &WireValue::String(node_id.into()));
    }
    let report = telemetry.snapshot();
    assert_eq!(report.packages.len(), 3);
}

#[test]
fn duplicate_node_is_rejected_without_touching_registry() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let mut host = FfiHost::install_package(
        &mut registry,
        &python_package("demo.a", "demo.echo", "python"),
        &factory,
    )
    .expect("first package");
    let before = registry.clone();

    let err = host
        .add_package(
            &mut registry,
            &python_package("demo.b", "demo.echo", "python3"),
            &factory,
        )
        .expect_err("duplicate node");

    assert!(matches!(err, FfiHostError::DuplicateNode { node_id } if node_id == "demo.echo"));
    assert_eq!(registry, before);
    assert_eq!(factory.counters.builds.load(Ordering::SeqCst), 1);
}

#[test]
fn runner_failure_restores_registry_and_stops_started_runners() {
    let mut registry = CapabilityRegistry::default();
    let failing = EchoFactory {
        fail_executable: Some("python"),
        ..Default::default()
    };
    let err = FfiHost::install_package(
        &mut registry,
        &python_package("demo.plugin", "demo.echo", "python"),
        &failing,
    )
    .err()
    .expect("runner startup fails");
    assert!(matches!(
        err,
        FfiHostError::Install(HostInstallError::Runner { .. })
    ));
    assert!(registry.plugin_manifest("demo.plugin").is_none());

    // The runner is built but does not advertise the node: it never joins the pool.
    let unsupported = EchoFactory {
        supported_nodes: Some(vec!["other".into()]),
        ..Default::default()
    };
    let mut host = FfiHost::new();
    let err = host
        .add_package(
            &mut registry,
            &python_package("demo.plugin", "demo.echo", "python"),
            &unsupported,
        )
        .expect_err("entrypoint rejected");
    assert!(matches!(
        err,
        FfiHostError::Install(HostInstallError::UnsupportedRunnerEntrypoint { .. })
    ));
    assert!(registry.plugin_manifest("demo.plugin").is_none());
    assert!(host.pool().is_empty());
    assert!(!host.contains_node("demo.echo"));
    assert_eq!(unsupported.counters.builds.load(Ordering::SeqCst), 1);

    // The first worker starts, the second fails: the first is shut down again.
    let partial = EchoFactory {
        fail_executable: Some("python3"),
        ..Default::default()
    };
    let err = host
        .add_package(
            &mut registry,
            &package(
                "demo.plugin",
                vec![
                    (
                        node("demo.a", BackendKind::Python),
                        worker_backend("python"),
                    ),
                    (
                        node("demo.b", BackendKind::Python),
                        worker_backend("python3"),
                    ),
                ],
            ),
            &partial,
        )
        .expect_err("second runner fails");
    assert!(matches!(
        err,
        FfiHostError::Install(HostInstallError::Runner { node_id, .. }) if node_id == "demo.b"
    ));
    assert!(registry.plugin_manifest("demo.plugin").is_none());
    assert!(host.pool().is_empty());
    assert_eq!(partial.counters.builds.load(Ordering::SeqCst), 1);
    assert_eq!(partial.counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn in_process_abi_nodes_are_registered_but_not_started() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let host = FfiHost::install_package(
        &mut registry,
        &package(
            "demo.mixed",
            vec![
                (
                    node("demo.worker", BackendKind::Python),
                    worker_backend("python"),
                ),
                (node("demo.native", BackendKind::Rust), in_process_backend()),
            ],
        ),
        &factory,
    )
    .expect("host installs");

    assert!(registry.node_decl(&NodeId::new("demo.native")).is_some());
    assert_eq!(factory.counters.builds.load(Ordering::SeqCst), 1);
    assert!(host.runner_key("demo.native").is_none());
    assert!(host.backend("demo.native").is_some());
    assert!(matches!(
        host.invoke("demo.native", request("")),
        Err(FfiHostError::InProcessNode { .. })
    ));
    host.invoke("demo.worker", request(""))
        .expect("worker invoke");
}

#[test]
fn deferred_startup_installs_registry_first() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let mut host = FfiHost::builder()
        .defer_runner_startup(true)
        .install_package(
            &mut registry,
            &python_package("demo.plugin", "demo.echo", "python"),
            &factory,
        )
        .expect("host installs");

    assert!(registry.plugin_manifest("demo.plugin").is_some());
    assert!(host.has_pending_runners());
    assert!(host.pool().is_empty());
    assert!(matches!(
        host.invoke("demo.echo", request("")),
        Err(FfiHostError::RunnerNotStarted { .. })
    ));

    let keys = host.start_runners(&factory).expect("runners start");
    assert_eq!(keys.len(), 1);
    assert!(!host.has_pending_runners());
    host.invoke("demo.echo", request("")).expect("invoke");
    assert!(host.start_runners(&factory).unwrap().is_empty());
}

#[test]
fn shutdown_and_drop_stop_workers() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let telemetry = FfiHostTelemetry::new();
    let mut host = FfiHost::builder()
        .telemetry(telemetry.clone())
        .install_package(
            &mut registry,
            &python_package("demo.a", "demo.a.echo", "python"),
            &factory,
        )
        .expect("host installs");
    host.add_package(
        &mut registry,
        &python_package("demo.b", "demo.b.echo", "python3"),
        &factory,
    )
    .expect("second package");
    let key = host.runner_key("demo.a.echo").unwrap().as_str().to_owned();

    host.shutdown().expect("clean shutdown");
    assert_eq!(factory.counters.shutdowns.load(Ordering::SeqCst), 2);
    assert_eq!(telemetry.snapshot().backends[&key].runner_shutdowns, 1);

    let dropped = EchoFactory::default();
    let host = FfiHost::install_package(
        &mut CapabilityRegistry::default(),
        &python_package("demo.c", "demo.c.echo", "python"),
        &dropped,
    )
    .expect("host installs");
    drop(host);
    assert_eq!(dropped.counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn persistent_worker_factory_builds_without_spawning() {
    let host = FfiHost::builder()
        .pool_options(RunnerPoolOptions {
            idle_timeout: None,
            limits: RunnerLimits {
                stderr_capture_bytes: 1024,
                ..Default::default()
            },
        })
        .build();
    let factory = host.persistent_worker_factory();
    assert_eq!(factory.limits().stderr_capture_bytes, 1024);

    let runner = factory
        .build_runner("demo.echo", &worker_backend("definitely-not-a-worker"))
        .expect("runner builds lazily");
    assert_eq!(runner.health(), RunnerHealth::Starting);
    assert!(
        factory
            .build_runner("demo.native", &in_process_backend())
            .is_err()
    );
}
