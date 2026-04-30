//! Node.js FFI worker and packaging integration.

use std::collections::BTreeMap;

use core::{
    BackendConfig, BackendKind, FfiContractError, PluginSchema, validate_language_backends,
};

pub use daedalus_ffi_core as core;

pub fn validate_node_schema(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<(), FfiContractError> {
    validate_language_backends(schema, backends, BackendKind::Node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, ValueType};
    use daedalus_ffi_core::{
        BackendRuntimeModel, NodeSchema, PluginSchemaInfo, SCHEMA_VERSION, WirePort,
    };

    #[test]
    fn validates_node_schema_and_backends() {
        let schema = PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: "demo.node".into(),
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
                backend: BackendKind::Node,
                entrypoint: "add".into(),
                label: None,
                stateful: false,
                feature_flags: Vec::new(),
                inputs: Vec::new(),
                outputs: vec![WirePort {
                    name: "out".into(),
                    ty: TypeExpr::scalar(ValueType::Int),
                    type_key: None,
                    optional: false,
                    access: Default::default(),
                    residency: None,
                    layout: None,
                    source: None,
                    const_value: None,
                }],
                metadata: Default::default(),
            }],
        };
        let backends = BTreeMap::from([(
            "demo:add".into(),
            BackendConfig {
                backend: BackendKind::Node,
                runtime_model: BackendRuntimeModel::PersistentWorker,
                entry_module: Some("demo.mjs".into()),
                entry_class: None,
                entry_symbol: Some("add".into()),
                executable: Some("node".into()),
                args: Vec::new(),
                classpath: Vec::new(),
                native_library_paths: Vec::new(),
                working_dir: None,
                env: Default::default(),
                options: Default::default(),
            },
        )]);

        validate_node_schema(&schema, &backends).expect("valid node schema");
        assert!(matches!(
            validate_node_schema(&schema, &BTreeMap::new()),
            Err(FfiContractError::MissingBackendConfig { .. })
        ));
    }
}
