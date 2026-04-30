//! Python FFI worker and packaging integration.

use std::collections::BTreeMap;

use core::{
    BackendConfig, BackendKind, FfiContractError, PluginSchema, validate_language_backends,
};

pub use daedalus_ffi_core as core;

pub fn validate_python_schema(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<(), FfiContractError> {
    validate_language_backends(schema, backends, BackendKind::Python)
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, ValueType};
    use daedalus_ffi_core::{
        BackendRuntimeModel, NodeSchema, PluginSchemaInfo, SCHEMA_VERSION, WirePort,
    };

    fn schema_for_backend(backend: BackendKind) -> PluginSchema {
        PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: "demo.python".into(),
                version: None,
                description: None,
                metadata: Default::default(),
            },
            dependencies: Vec::new(),
            required_host_capabilities: Vec::new(),
            feature_flags: Vec::new(),
            boundary_contracts: Vec::new(),
            nodes: vec![NodeSchema {
                id: "demo:add".into(),
                backend,
                entrypoint: "add".into(),
                label: None,
                stateful: false,
                feature_flags: Vec::new(),
                inputs: vec![WirePort {
                    name: "a".into(),
                    ty: TypeExpr::scalar(ValueType::Int),
                    type_key: None,
                    optional: false,
                    access: Default::default(),
                    residency: None,
                    layout: None,
                    source: None,
                    const_value: None,
                }],
                outputs: Vec::new(),
                metadata: Default::default(),
            }],
        }
    }

    fn backend(backend: BackendKind) -> BackendConfig {
        BackendConfig {
            backend,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some("demo".into()),
            entry_class: None,
            entry_symbol: Some("add".into()),
            executable: Some("python".into()),
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: Default::default(),
            options: Default::default(),
        }
    }

    #[test]
    fn validates_python_schema_and_backends() {
        let schema = schema_for_backend(BackendKind::Python);
        let backends = BTreeMap::from([("demo:add".into(), backend(BackendKind::Python))]);
        validate_python_schema(&schema, &backends).expect("valid python schema");

        let bad = schema_for_backend(BackendKind::Node);
        assert!(matches!(
            validate_python_schema(&bad, &backends),
            Err(FfiContractError::UnexpectedBackendKind { .. })
        ));
    }
}
