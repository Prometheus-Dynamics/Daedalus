use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_data::model::TypeExpr;
use daedalus_ffi_core::{
    BackendConfig, BackendKind, BackendRuntimeModel, InvokeRequest, InvokeResponse, NodeSchema,
    PluginPackage, PluginSchema, PluginSchemaInfo, SCHEMA_VERSION, WirePort, WireValue,
};
use daedalus_transport::{AccessMode, BoundaryTypeContract, Residency, TypeKey};

use super::*;

struct FakeRunner;

impl BackendRunner for FakeRunner {
    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        Ok(InvokeResponse {
            protocol_version: request.protocol_version,
            correlation_id: request.correlation_id,
            outputs: BTreeMap::from([("out".into(), WireValue::Int(1))]),
            state: None,
            events: Vec::new(),
        })
    }
}

struct SupportedRunner {
    supported_nodes: Vec<String>,
}

impl BackendRunner for SupportedRunner {
    fn supported_nodes(&self) -> Option<Vec<String>> {
        Some(self.supported_nodes.clone())
    }

    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        Ok(InvokeResponse {
            protocol_version: request.protocol_version,
            correlation_id: request.correlation_id,
            outputs: BTreeMap::from([("out".into(), WireValue::Int(1))]),
            state: None,
            events: Vec::new(),
        })
    }
}

struct FakeRunnerFactory {
    builds: Arc<AtomicUsize>,
}

impl BackendRunnerFactory for FakeRunnerFactory {
    fn build_runner(
        &self,
        _node_id: &str,
        _backend: &BackendConfig,
    ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        self.builds.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(FakeRunner))
    }
}

struct SupportedRunnerFactory {
    supported_nodes: Vec<String>,
    builds: Arc<AtomicUsize>,
}

impl BackendRunnerFactory for SupportedRunnerFactory {
    fn build_runner(
        &self,
        _node_id: &str,
        _backend: &BackendConfig,
    ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        self.builds.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(SupportedRunner {
            supported_nodes: self.supported_nodes.clone(),
        }))
    }
}

fn schema() -> PluginSchema {
    PluginSchema {
        schema_version: SCHEMA_VERSION,
        plugin: PluginSchemaInfo {
            name: "demo.plugin".into(),
            version: Some("1.2.3".into()),
            description: None,
            metadata: BTreeMap::new(),
        },
        dependencies: vec!["base.plugin".into()],
        required_host_capabilities: vec!["camera".into()],
        feature_flags: vec!["ffi".into()],
        boundary_contracts: vec![BoundaryTypeContract {
            type_key: TypeKey::new("demo:i32"),
            rust_type_name: None,
            abi_version: BoundaryTypeContract::ABI_VERSION,
            layout_hash: "demo:i32".into(),
            capabilities: Default::default(),
        }],
        nodes: vec![NodeSchema {
            id: "demo.add".into(),
            backend: BackendKind::Python,
            entrypoint: "add".into(),
            label: Some("Add".into()),
            stateful: true,
            feature_flags: vec!["math".into()],
            inputs: vec![WirePort {
                name: "lhs".into(),
                ty: TypeExpr::Opaque("demo:i32".into()),
                type_key: Some(TypeKey::new("demo:i32")),
                optional: false,
                access: AccessMode::Modify,
                residency: Some(Residency::Cpu),
                layout: None,
                source: Some("camera".into()),
                const_value: Some(serde_json::json!(1)),
            }],
            outputs: vec![WirePort {
                name: "sum".into(),
                ty: TypeExpr::Opaque("demo:i32".into()),
                type_key: None,
                optional: false,
                access: AccessMode::Read,
                residency: Some(Residency::Cpu),
                layout: None,
                source: None,
                const_value: None,
            }],
            metadata: BTreeMap::from([("owner".into(), serde_json::json!("ffi"))]),
        }],
    }
}

fn two_node_schema() -> PluginSchema {
    let mut schema = schema();
    let mut second = schema.nodes[0].clone();
    second.id = "demo.sub".into();
    schema.nodes.push(second);
    schema
}

fn backend() -> BackendConfig {
    BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some("demo.py".into()),
        entry_class: None,
        entry_symbol: Some("add".into()),
        executable: Some("python".into()),
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
        entry_class: None,
        entry_symbol: Some("daedalus_plugin_register".into()),
        executable: None,
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

#[test]
fn builds_plugin_manifest_from_schema() {
    let schema = schema();
    let plugin = plugin_manifest_from_schema(&schema);

    assert_eq!(plugin.id, "demo.plugin");
    assert_eq!(plugin.version.as_deref(), Some("1.2.3"));
    assert_eq!(plugin.dependencies, vec!["base.plugin"]);
    assert_eq!(plugin.provided_nodes[0].0, "demo.add");
    assert_eq!(plugin.required_host_capabilities, vec!["camera"]);
    assert_eq!(plugin.boundary_contracts, schema.boundary_contracts);
}

#[test]
fn builds_node_and_ports_from_schema() {
    let schema = schema();
    let node = node_decl_from_schema(&schema.nodes[0]).expect("node lowers");

    assert_eq!(node.id.0, "demo.add");
    assert_eq!(node.execution_kind, NodeExecutionKind::External);
    assert_eq!(node.label.as_deref(), Some("Add"));
    assert_eq!(node.feature_flags, vec!["math"]);
    assert_eq!(node.inputs[0].name, "lhs");
    assert_eq!(node.inputs[0].type_key.as_str(), "demo:i32");
    assert_eq!(node.inputs[0].access, AccessMode::Modify);
    assert_eq!(node.inputs[0].residency, Some(Residency::Cpu));
    assert_eq!(node.inputs[0].source.as_deref(), Some("camera"));
    assert!(node.inputs[0].const_value_json.is_some());
    assert_eq!(node.outputs[0].type_key.as_str(), "demo:i32");
    assert_eq!(
        node.metadata_json.get("owner").map(String::as_str),
        Some("{\"type\":\"String\",\"value\":\"ffi\"}")
    );
    assert_eq!(
        node.metadata_json
            .get("daedalus.ffi.stateful")
            .map(String::as_str),
        Some("{\"type\":\"Bool\",\"value\":true}")
    );
}

#[test]
fn installs_schema_into_registry() {
    let schema = schema();
    let mut registry = CapabilityRegistry::default();

    let plan = install_schema(&mut registry, &schema).expect("schema installs");

    assert_eq!(plan.nodes.len(), 1);
    assert!(plan.backends.is_empty());
    assert!(registry.plugin_manifest("demo.plugin").is_some());
    assert!(
        registry
            .node_decl(&daedalus_registry::ids::NodeId::new("demo.add"))
            .is_some()
    );
}

#[test]
fn installs_python_schema_through_language_installer() {
    let schema = schema();
    let backends = BTreeMap::from([("demo.add".into(), backend())]);
    let mut registry = CapabilityRegistry::default();

    let plan = install_language_schema(&mut registry, &schema, &backends, BackendKind::Python)
        .expect("python schema installs through shared installer");

    assert_eq!(plan.backends["demo.add"].backend, BackendKind::Python);
    assert!(registry.plugin_manifest("demo.plugin").is_some());
    assert!(
        registry
            .node_decl(&daedalus_registry::ids::NodeId::new("demo.add"))
            .is_some()
    );

    let mut bad_schema = schema;
    bad_schema.nodes[0].backend = BackendKind::Node;
    let err = install_language_schema(
        &mut CapabilityRegistry::default(),
        &bad_schema,
        &backends,
        BackendKind::Python,
    )
    .expect_err("wrong language rejected");
    assert!(matches!(
        err,
        HostInstallError::InvalidSchema(FfiContractError::UnexpectedBackendKind { .. })
    ));
}

#[test]
fn builds_backend_aware_install_plan() {
    let schema = schema();
    let backends = BTreeMap::from([("demo.add".into(), backend())]);

    let plan = HostInstallPlan::from_schema_and_backends(&schema, &backends).expect("plan builds");

    assert_eq!(plan.nodes.len(), 1);
    assert_eq!(plan.backends["demo.add"].backend, BackendKind::Python);
    assert_eq!(
        plan.backends["demo.add"].runtime_model,
        BackendRuntimeModel::PersistentWorker
    );
}

#[test]
fn rejects_backend_plans_missing_node_configs() {
    let schema = schema();
    let err = HostInstallPlan::from_schema_and_backends(&schema, &BTreeMap::new())
        .expect_err("missing backend is rejected");

    assert!(matches!(
        err,
        HostInstallError::MissingBackend { node_id } if node_id == "demo.add"
    ));
}

#[test]
fn rejects_backend_plans_with_unknown_node_configs() {
    let schema = schema();
    let backends = BTreeMap::from([
        ("demo.add".into(), backend()),
        ("demo.missing".into(), backend()),
    ]);
    let err = HostInstallPlan::from_schema_and_backends(&schema, &backends)
        .expect_err("unknown backend is rejected");

    assert!(matches!(
        err,
        HostInstallError::UnknownBackend { node_id } if node_id == "demo.missing"
    ));
}

#[test]
fn rejects_backend_plans_with_mismatched_backend_kind() {
    let schema = schema();
    let mut backend = backend();
    backend.backend = BackendKind::Node;
    let backends = BTreeMap::from([("demo.add".into(), backend)]);
    let err = HostInstallPlan::from_schema_and_backends(&schema, &backends)
        .expect_err("mismatched backend is rejected");

    assert!(matches!(
        err,
        HostInstallError::BackendMismatch { node_id, .. } if node_id == "demo.add"
    ));
}

#[test]
fn installs_package_schema_and_backends_into_registry() {
    let schema = schema();
    let package = PluginPackage {
        schema_version: SCHEMA_VERSION,
        schema: Some(schema),
        backends: BTreeMap::from([("demo.add".into(), backend())]),
        artifacts: Vec::new(),
        lockfile: None,
        manifest_hash: None,
        signature: None,
        metadata: BTreeMap::new(),
    };
    let mut registry = CapabilityRegistry::default();

    let plan = install_package(&mut registry, &package).expect("package installs");

    assert_eq!(plan.backends.len(), 1);
    assert!(registry.plugin_manifest("demo.plugin").is_some());
    assert!(
        registry
            .node_decl(&daedalus_registry::ids::NodeId::new("demo.add"))
            .is_some()
    );
}

#[test]
fn installs_package_records_shared_ffi_telemetry() {
    let schema = schema();
    let package = PluginPackage {
        schema_version: SCHEMA_VERSION,
        schema: Some(schema),
        backends: BTreeMap::from([("demo.add".into(), backend())]),
        artifacts: Vec::new(),
        lockfile: None,
        manifest_hash: None,
        signature: None,
        metadata: BTreeMap::new(),
    };
    let telemetry = FfiHostTelemetry::new();
    let mut registry = CapabilityRegistry::default();

    install_package_with_ffi_telemetry(&mut registry, &package, &telemetry)
        .expect("package installs");

    let report = telemetry.snapshot();
    let package = report
        .packages
        .get("demo.plugin")
        .expect("package telemetry");
    assert_eq!(package.package_id, "demo.plugin");
    assert_eq!(package.backend_resolutions, 1);
    assert_eq!(package.artifact_checks, 0);
    assert_eq!(package.install_failures, 0);
    assert!(package.load_duration >= package.validation_duration);
}

#[test]
fn installs_plan_runners_with_fake_backend_factory() {
    let schema = schema();
    let backends = BTreeMap::from([("demo.add".into(), backend())]);
    let plan = HostInstallPlan::from_schema_and_backends(&schema, &backends).expect("plan builds");
    let builds = Arc::new(AtomicUsize::new(0));
    let factory = FakeRunnerFactory {
        builds: builds.clone(),
    };
    let mut pool = RunnerPool::new();

    let keys = install_plan_runners(&mut pool, &plan, &factory).expect("runners install");

    assert_eq!(keys.len(), 1);
    assert_eq!(pool.len(), 1);
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(pool.telemetry().starts, 1);
}

#[test]
fn install_plan_runners_skips_in_process_abi_backends() {
    let mut schema = two_node_schema();
    schema.nodes[1].backend = BackendKind::Rust;
    let backends = BTreeMap::from([
        ("demo.add".into(), backend()),
        ("demo.sub".into(), in_process_backend()),
    ]);
    let plan = HostInstallPlan::from_schema_and_backends(&schema, &backends).expect("plan builds");
    let builds = Arc::new(AtomicUsize::new(0));
    let factory = FakeRunnerFactory {
        builds: builds.clone(),
    };
    let mut pool = RunnerPool::new();

    let keys = install_plan_runners(&mut pool, &plan, &factory).expect("runners install");

    assert_eq!(keys.len(), 1);
    assert_eq!(pool.len(), 1);
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "only worker/spawn backends should be inserted into RunnerPool"
    );
    assert!(matches!(
        pool.get(&backends["demo.sub"]),
        Err(RunnerPoolError::MissingRunner)
    ));
}

#[test]
fn install_plan_runners_validates_advertised_worker_entrypoints() {
    let schema = two_node_schema();
    let backend = backend();
    let backends = BTreeMap::from([
        ("demo.add".into(), backend.clone()),
        ("demo.sub".into(), backend),
    ]);
    let plan = HostInstallPlan::from_schema_and_backends(&schema, &backends).expect("plan builds");
    let builds = Arc::new(AtomicUsize::new(0));
    let factory = SupportedRunnerFactory {
        supported_nodes: vec!["demo.add".into(), "demo.sub".into()],
        builds: builds.clone(),
    };
    let mut pool = RunnerPool::new();

    let keys = install_plan_runners(&mut pool, &plan, &factory).expect("runners install");

    assert_eq!(keys.len(), 1);
    assert_eq!(pool.len(), 1);
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[test]
fn install_plan_runners_rejects_missing_worker_entrypoint_before_invoke() {
    let schema = two_node_schema();
    let backend = backend();
    let backends = BTreeMap::from([
        ("demo.add".into(), backend.clone()),
        ("demo.sub".into(), backend),
    ]);
    let plan = HostInstallPlan::from_schema_and_backends(&schema, &backends).expect("plan builds");
    let builds = Arc::new(AtomicUsize::new(0));
    let factory = SupportedRunnerFactory {
        supported_nodes: vec!["demo.add".into()],
        builds,
    };
    let mut pool = RunnerPool::new();

    assert!(matches!(
        install_plan_runners(&mut pool, &plan, &factory),
        Err(HostInstallError::UnsupportedRunnerEntrypoint {
            node_id,
            supported_nodes,
        }) if node_id == "demo.sub" && supported_nodes == vec!["demo.add".to_string()]
    ));
    assert_eq!(pool.len(), 0);
}
