use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use daedalus_data::model::Value;
use daedalus_ffi_core::{
    BackendConfig, BackendKind, BackendRuntimeModel, FfiContractError, NodeSchema, PluginPackage,
    PluginSchema, WirePort,
};
use daedalus_registry::capability::{
    CapabilityRegistry, NodeDecl, NodeExecutionKind, PluginManifest, PortDecl,
};
use daedalus_registry::diagnostics::RegistryError;
use daedalus_runtime::{FfiPackageTelemetry, FfiTelemetryReport};
use thiserror::Error;

use crate::{BackendRunner, FfiHostTelemetry, RunnerKey, RunnerPool, RunnerPoolError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostInstallPlan {
    pub plugin: PluginManifest,
    pub nodes: Vec<NodeDecl>,
    pub backends: BTreeMap<String, BackendConfig>,
}

#[derive(Debug, Error)]
pub enum HostInstallError {
    #[error("invalid plugin schema: {0}")]
    InvalidSchema(#[from] FfiContractError),
    #[error("failed to encode schema metadata field {field}: {source}")]
    Metadata {
        field: String,
        source: serde_json::Error,
    },
    #[error("failed to decode schema metadata field {field}: {message}")]
    MetadataValue { field: String, message: String },
    #[error("failed to decode const value for port {port}: {message}")]
    ConstValue { port: String, message: String },
    #[error("plugin package does not include a schema")]
    MissingPackageSchema,
    #[error("backend config missing for node {node_id}")]
    MissingBackend { node_id: String },
    #[error("backend config provided for unknown node {node_id}")]
    UnknownBackend { node_id: String },
    #[error(
        "backend config for node {node_id} uses {backend:?}, but schema declares {schema_backend:?}"
    )]
    BackendMismatch {
        node_id: String,
        schema_backend: daedalus_ffi_core::BackendKind,
        backend: daedalus_ffi_core::BackendKind,
    },
    #[error("registry install failed: {0}")]
    Registry(#[from] RegistryError),
    #[error("runner install failed for node {node_id}: {source}")]
    Runner {
        node_id: String,
        source: RunnerPoolError,
    },
    #[error(
        "runner does not advertise requested node {node_id}; supported nodes: {supported_nodes:?}"
    )]
    UnsupportedRunnerEntrypoint {
        node_id: String,
        supported_nodes: Vec<String>,
    },
}

pub trait BackendRunnerFactory {
    fn build_runner(
        &self,
        node_id: &str,
        backend: &BackendConfig,
    ) -> Result<std::sync::Arc<dyn BackendRunner>, RunnerPoolError>;
}

pub fn install_schema(
    registry: &mut CapabilityRegistry,
    schema: &PluginSchema,
) -> Result<HostInstallPlan, HostInstallError> {
    let plan = HostInstallPlan::from_schema(schema)?;
    install_plan(registry, &plan)?;
    Ok(plan)
}

pub fn install_schema_with_backends(
    registry: &mut CapabilityRegistry,
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<HostInstallPlan, HostInstallError> {
    let plan = HostInstallPlan::from_schema_and_backends(schema, backends)?;
    install_plan(registry, &plan)?;
    Ok(plan)
}

pub fn install_language_schema(
    registry: &mut CapabilityRegistry,
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
    expected_backend: BackendKind,
) -> Result<HostInstallPlan, HostInstallError> {
    schema.validate_backend_kind(expected_backend)?;
    install_schema_with_backends(registry, schema, backends)
}

pub fn install_package(
    registry: &mut CapabilityRegistry,
    package: &PluginPackage,
) -> Result<HostInstallPlan, HostInstallError> {
    package.validate()?;
    let schema = package
        .schema
        .as_ref()
        .ok_or(HostInstallError::MissingPackageSchema)?;
    let plan = HostInstallPlan::from_schema_and_backends(schema, &package.backends)?;
    install_plan(registry, &plan)?;
    Ok(plan)
}

pub fn install_package_with_ffi_telemetry(
    registry: &mut CapabilityRegistry,
    package: &PluginPackage,
    telemetry: &FfiHostTelemetry,
) -> Result<HostInstallPlan, HostInstallError> {
    let started = Instant::now();
    let validation_started = Instant::now();
    if let Err(source) = package.validate() {
        record_package_telemetry(
            telemetry,
            package_id(package),
            FfiPackageTelemetry {
                validation_duration: validation_started.elapsed(),
                load_duration: started.elapsed(),
                install_failures: 1,
                ..Default::default()
            },
        );
        return Err(source.into());
    }
    let validation_duration = validation_started.elapsed();
    let schema = match package.schema.as_ref() {
        Some(schema) => schema,
        None => {
            record_package_telemetry(
                telemetry,
                package_id(package),
                FfiPackageTelemetry {
                    validation_duration,
                    load_duration: started.elapsed(),
                    install_failures: 1,
                    ..Default::default()
                },
            );
            return Err(HostInstallError::MissingPackageSchema);
        }
    };
    let plan = match HostInstallPlan::from_schema_and_backends(schema, &package.backends) {
        Ok(plan) => plan,
        Err(err) => {
            record_package_telemetry(
                telemetry,
                package_id(package),
                FfiPackageTelemetry {
                    validation_duration,
                    load_duration: started.elapsed(),
                    backend_resolutions: package.backends.len() as u64,
                    artifact_checks: package.artifacts.len() as u64,
                    install_failures: 1,
                    ..Default::default()
                },
            );
            return Err(err);
        }
    };
    if let Err(err) = install_plan(registry, &plan) {
        record_package_telemetry(
            telemetry,
            package_id(package),
            FfiPackageTelemetry {
                validation_duration,
                load_duration: started.elapsed(),
                backend_resolutions: package.backends.len() as u64,
                artifact_checks: package.artifacts.len() as u64,
                install_failures: 1,
                ..Default::default()
            },
        );
        return Err(err);
    }
    record_package_telemetry(
        telemetry,
        package_id(package),
        FfiPackageTelemetry {
            validation_duration,
            load_duration: started.elapsed(),
            backend_resolutions: package.backends.len() as u64,
            artifact_checks: package.artifacts.len() as u64,
            ..Default::default()
        },
    );
    Ok(plan)
}

pub fn install_language_package(
    registry: &mut CapabilityRegistry,
    package: &PluginPackage,
    expected_backend: BackendKind,
) -> Result<HostInstallPlan, HostInstallError> {
    package.validate()?;
    let schema = package
        .schema
        .as_ref()
        .ok_or(HostInstallError::MissingPackageSchema)?;
    schema.validate_backend_kind(expected_backend)?;
    let plan = HostInstallPlan::from_schema_and_backends(schema, &package.backends)?;
    install_plan(registry, &plan)?;
    Ok(plan)
}

pub fn install_language_package_with_ffi_telemetry(
    registry: &mut CapabilityRegistry,
    package: &PluginPackage,
    expected_backend: BackendKind,
    telemetry: &FfiHostTelemetry,
) -> Result<HostInstallPlan, HostInstallError> {
    let started = Instant::now();
    let validation_started = Instant::now();
    if let Err(source) = package.validate() {
        record_package_telemetry(
            telemetry,
            package_id(package),
            FfiPackageTelemetry {
                validation_duration: validation_started.elapsed(),
                load_duration: started.elapsed(),
                install_failures: 1,
                ..Default::default()
            },
        );
        return Err(source.into());
    }
    let validation_duration = validation_started.elapsed();
    let schema = match package.schema.as_ref() {
        Some(schema) => schema,
        None => {
            record_package_telemetry(
                telemetry,
                package_id(package),
                FfiPackageTelemetry {
                    validation_duration,
                    load_duration: started.elapsed(),
                    install_failures: 1,
                    ..Default::default()
                },
            );
            return Err(HostInstallError::MissingPackageSchema);
        }
    };
    if let Err(source) = schema.validate_backend_kind(expected_backend) {
        record_package_telemetry(
            telemetry,
            package_id(package),
            FfiPackageTelemetry {
                validation_duration,
                load_duration: started.elapsed(),
                install_failures: 1,
                ..Default::default()
            },
        );
        return Err(source.into());
    }
    let plan = match HostInstallPlan::from_schema_and_backends(schema, &package.backends) {
        Ok(plan) => plan,
        Err(err) => {
            record_package_telemetry(
                telemetry,
                package_id(package),
                FfiPackageTelemetry {
                    validation_duration,
                    load_duration: started.elapsed(),
                    backend_resolutions: package.backends.len() as u64,
                    artifact_checks: package.artifacts.len() as u64,
                    install_failures: 1,
                    ..Default::default()
                },
            );
            return Err(err);
        }
    };
    if let Err(err) = install_plan(registry, &plan) {
        record_package_telemetry(
            telemetry,
            package_id(package),
            FfiPackageTelemetry {
                validation_duration,
                load_duration: started.elapsed(),
                backend_resolutions: package.backends.len() as u64,
                artifact_checks: package.artifacts.len() as u64,
                install_failures: 1,
                ..Default::default()
            },
        );
        return Err(err);
    }
    record_package_telemetry(
        telemetry,
        package_id(package),
        FfiPackageTelemetry {
            validation_duration,
            load_duration: started.elapsed(),
            backend_resolutions: package.backends.len() as u64,
            artifact_checks: package.artifacts.len() as u64,
            ..Default::default()
        },
    );
    Ok(plan)
}

pub fn install_plan_runners(
    pool: &mut RunnerPool,
    plan: &HostInstallPlan,
    factory: &impl BackendRunnerFactory,
) -> Result<Vec<RunnerKey>, HostInstallError> {
    let mut installed = Vec::new();
    let mut seen = BTreeSet::new();
    for (node_id, backend) in &plan.backends {
        if backend.runtime_model == BackendRuntimeModel::InProcessAbi {
            continue;
        }
        let key = RunnerKey::from_backend(backend).map_err(|source| HostInstallError::Runner {
            node_id: node_id.clone(),
            source,
        })?;
        if !seen.insert(key.clone()) {
            continue;
        }
        let runner =
            factory
                .build_runner(node_id, backend)
                .map_err(|source| HostInstallError::Runner {
                    node_id: node_id.clone(),
                    source,
                })?;
        let node_ids = nodes_for_runner_key(plan, &key)?;
        validate_runner_entrypoints(runner.as_ref(), &node_ids)?;
        installed.push(pool.insert_shared(backend, runner).map_err(|source| {
            HostInstallError::Runner {
                node_id: node_id.clone(),
                source,
            }
        })?);
    }
    Ok(installed)
}

fn nodes_for_runner_key(
    plan: &HostInstallPlan,
    key: &RunnerKey,
) -> Result<Vec<String>, HostInstallError> {
    let mut node_ids = Vec::new();
    for (node_id, backend) in &plan.backends {
        if backend.runtime_model == BackendRuntimeModel::InProcessAbi {
            continue;
        }
        let backend_key =
            RunnerKey::from_backend(backend).map_err(|source| HostInstallError::Runner {
                node_id: node_id.clone(),
                source,
            })?;
        if &backend_key == key {
            node_ids.push(node_id.clone());
        }
    }
    Ok(node_ids)
}

fn validate_runner_entrypoints(
    runner: &dyn BackendRunner,
    node_ids: &[String],
) -> Result<(), HostInstallError> {
    let Some(supported_nodes) = runner.supported_nodes() else {
        return Ok(());
    };
    let supported: BTreeSet<_> = supported_nodes.iter().map(String::as_str).collect();
    for node_id in node_ids {
        if !supported.contains(node_id.as_str()) {
            return Err(HostInstallError::UnsupportedRunnerEntrypoint {
                node_id: node_id.clone(),
                supported_nodes: supported_nodes.clone(),
            });
        }
    }
    Ok(())
}

fn install_plan(
    registry: &mut CapabilityRegistry,
    plan: &HostInstallPlan,
) -> Result<(), HostInstallError> {
    registry.register_plugin(plan.plugin.clone())?;
    for node in &plan.nodes {
        registry.register_node(node.clone())?;
    }
    Ok(())
}

impl HostInstallPlan {
    pub fn from_schema(schema: &PluginSchema) -> Result<Self, HostInstallError> {
        schema.validate()?;
        Ok(Self {
            plugin: plugin_manifest_from_schema(schema),
            nodes: node_decls_from_schema(schema)?,
            backends: BTreeMap::new(),
        })
    }

    pub fn from_schema_and_backends(
        schema: &PluginSchema,
        backends: &BTreeMap<String, BackendConfig>,
    ) -> Result<Self, HostInstallError> {
        schema.validate()?;
        validate_backends(schema, backends)?;
        Ok(Self {
            plugin: plugin_manifest_from_schema(schema),
            nodes: node_decls_from_schema(schema)?,
            backends: backends.clone(),
        })
    }
}

fn validate_backends(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<(), HostInstallError> {
    let nodes: BTreeMap<_, _> = schema
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();

    for node in &schema.nodes {
        let backend = backends
            .get(&node.id)
            .ok_or_else(|| HostInstallError::MissingBackend {
                node_id: node.id.clone(),
            })?;
        if backend.backend != node.backend {
            return Err(HostInstallError::BackendMismatch {
                node_id: node.id.clone(),
                schema_backend: node.backend.clone(),
                backend: backend.backend.clone(),
            });
        }
        backend.validate_for_node(&node.id)?;
    }

    for node_id in backends.keys() {
        if !nodes.contains_key(node_id.as_str()) {
            return Err(HostInstallError::UnknownBackend {
                node_id: node_id.clone(),
            });
        }
    }

    Ok(())
}

pub fn plugin_manifest_from_schema(schema: &PluginSchema) -> PluginManifest {
    let mut plugin = PluginManifest::new(schema.plugin.name.clone());
    if let Some(version) = &schema.plugin.version {
        plugin = plugin.version(version.clone());
    }
    for dependency in &schema.dependencies {
        plugin = plugin.dependency(dependency.clone());
    }
    for node in &schema.nodes {
        plugin = plugin.provided_node(node.id.clone());
    }
    for contract in &schema.boundary_contracts {
        plugin = plugin.boundary_contract(contract.clone());
    }
    for capability in &schema.required_host_capabilities {
        plugin = plugin.required_host_capability(capability.clone());
    }
    for flag in &schema.feature_flags {
        plugin = plugin.feature_flag(flag.clone());
    }
    plugin
}

pub fn node_decls_from_schema(schema: &PluginSchema) -> Result<Vec<NodeDecl>, HostInstallError> {
    schema.validate()?;
    schema.nodes.iter().map(node_decl_from_schema).collect()
}

pub fn node_decl_from_schema(node: &NodeSchema) -> Result<NodeDecl, HostInstallError> {
    let mut decl = NodeDecl::new(node.id.clone()).execution_kind(NodeExecutionKind::External);
    if let Some(label) = &node.label {
        decl = decl.label(label.clone());
    }
    for flag in &node.feature_flags {
        decl = decl.feature_flag(flag.clone());
    }
    for port in &node.inputs {
        decl = decl.input(port_decl_from_schema(port)?);
    }
    for port in &node.outputs {
        decl = decl.output(port_decl_from_schema(port)?);
    }

    let mut metadata_keys = BTreeSet::new();
    for (key, value) in &node.metadata {
        metadata_keys.insert(key.clone());
        let value =
            json_to_value(value.clone()).map_err(|message| HostInstallError::MetadataValue {
                field: key.clone(),
                message,
            })?;
        insert_metadata_value(&mut decl, key.clone(), value)?;
    }
    if node.stateful && !metadata_keys.contains("daedalus.ffi.stateful") {
        insert_metadata_value(&mut decl, "daedalus.ffi.stateful", Value::Bool(true))?;
    }
    insert_metadata_value(
        &mut decl,
        "daedalus.ffi.backend",
        Value::String(Cow::Owned(serde_json::to_string(&node.backend).map_err(
            |source| HostInstallError::Metadata {
                field: "daedalus.ffi.backend".into(),
                source,
            },
        )?)),
    )?;
    insert_metadata_value(
        &mut decl,
        "daedalus.ffi.entrypoint",
        Value::String(Cow::Owned(node.entrypoint.clone())),
    )?;

    Ok(decl)
}

fn insert_metadata_value(
    decl: &mut NodeDecl,
    key: impl Into<String>,
    value: Value,
) -> Result<(), HostInstallError> {
    let key = key.into();
    let json = serde_json::to_string(&value).map_err(|source| HostInstallError::Metadata {
        field: key.clone(),
        source,
    })?;
    decl.metadata_json.insert(key, json);
    Ok(())
}

pub fn port_decl_from_schema(port: &WirePort) -> Result<PortDecl, HostInstallError> {
    let type_key = port
        .type_key
        .clone()
        .unwrap_or_else(|| daedalus_registry::typeexpr_transport_key(&port.ty));
    let mut decl = PortDecl::new(port.name.clone(), type_key)
        .schema(port.ty.clone())
        .access(port.access);
    if let Some(residency) = port.residency {
        decl = decl.residency(residency);
    }
    if let Some(layout) = &port.layout {
        decl = decl.layout(layout.clone());
    }
    if let Some(source) = &port.source {
        decl = decl.source(source.clone());
    }
    if let Some(value) = &port.const_value {
        let value =
            json_to_value(value.clone()).map_err(|message| HostInstallError::ConstValue {
                port: port.name.clone(),
                message,
            })?;
        decl = decl.const_value(value);
    }
    Ok(decl)
}

fn json_to_value(value: serde_json::Value) -> Result<Value, String> {
    Ok(match value {
        serde_json::Value::Null => Value::Unit,
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Value::Int(value)
            } else if let Some(value) = value.as_f64() {
                Value::Float(value)
            } else {
                return Err(value.to_string());
            }
        }
        serde_json::Value::String(value) => Value::String(Cow::Owned(value)),
        serde_json::Value::Array(items) => Value::List(
            items
                .into_iter()
                .map(json_to_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        serde_json::Value::Object(map) => Value::Map(
            map.into_iter()
                .map(|(key, value)| Ok((Value::String(Cow::Owned(key)), json_to_value(value)?)))
                .collect::<Result<Vec<_>, String>>()?,
        ),
    })
}

fn package_id(package: &PluginPackage) -> String {
    package
        .schema
        .as_ref()
        .map(|schema| schema.plugin.name.clone())
        .or_else(|| {
            package
                .metadata
                .get("package_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

fn record_package_telemetry(
    telemetry: &FfiHostTelemetry,
    package_id: String,
    mut update: FfiPackageTelemetry,
) {
    if update.package_id.is_empty() {
        update.package_id = package_id.clone();
    }
    let mut report = FfiTelemetryReport::default();
    report.packages.insert(package_id, update);
    telemetry.merge(report);
}

#[cfg(test)]
mod tests;
