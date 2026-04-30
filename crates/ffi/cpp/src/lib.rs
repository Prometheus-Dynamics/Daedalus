//! C and C++ FFI ABI and packaging integration.

use std::collections::BTreeMap;

use core::{
    BackendConfig, BackendKind, BackendRuntimeModel, FfiContractError, NodeSchema, PluginSchema,
    PluginSchemaInfo, SCHEMA_VERSION, WirePort, validate_language_backends,
};

pub use daedalus_ffi_core as core;

pub fn cpp_in_process_backend_config(
    library_path: impl Into<String>,
    symbol: impl Into<String>,
) -> BackendConfig {
    BackendConfig {
        backend: BackendKind::CCpp,
        runtime_model: BackendRuntimeModel::InProcessAbi,
        entry_module: Some(library_path.into()),
        entry_class: None,
        entry_symbol: Some(symbol.into()),
        executable: None,
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

pub fn cpp_node_schema(
    node_id: impl Into<String>,
    symbol: impl Into<String>,
    inputs: Vec<WirePort>,
    outputs: Vec<WirePort>,
) -> NodeSchema {
    NodeSchema {
        id: node_id.into(),
        backend: BackendKind::CCpp,
        entrypoint: symbol.into(),
        label: None,
        stateful: false,
        feature_flags: Vec::new(),
        inputs,
        outputs,
        metadata: BTreeMap::new(),
    }
}

pub fn cpp_plugin_schema(
    plugin_name: impl Into<String>,
    version: Option<String>,
    nodes: Vec<NodeSchema>,
) -> Result<PluginSchema, FfiContractError> {
    let mut schema = PluginSchema {
        schema_version: SCHEMA_VERSION,
        plugin: PluginSchemaInfo {
            name: plugin_name.into(),
            version,
            description: None,
            metadata: BTreeMap::new(),
        },
        dependencies: Vec::new(),
        required_host_capabilities: Vec::new(),
        feature_flags: Vec::new(),
        boundary_contracts: Vec::new(),
        nodes,
    };
    schema.nodes.sort_by(|a, b| a.id.cmp(&b.id));
    schema.validate_backend_kind(BackendKind::CCpp)?;
    Ok(schema)
}

pub fn validate_cpp_schema(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<(), FfiContractError> {
    validate_language_backends(schema, backends, BackendKind::CCpp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, ValueType};

    fn port(name: &str) -> WirePort {
        WirePort {
            name: name.into(),
            ty: TypeExpr::scalar(ValueType::Int),
            type_key: None,
            optional: false,
            access: Default::default(),
            residency: None,
            layout: None,
            source: None,
            const_value: None,
        }
    }

    #[test]
    fn builds_and_validates_cpp_schema_helpers() {
        let node = cpp_node_schema("demo:add", "add_i32", vec![port("a")], vec![port("out")]);
        let schema =
            cpp_plugin_schema("demo.cpp", Some("1.0.0".into()), vec![node]).expect("schema");
        let backends = BTreeMap::from([(
            "demo:add".into(),
            cpp_in_process_backend_config("libdemo.so", "add_i32"),
        )]);

        validate_cpp_schema(&schema, &backends).expect("valid cpp schema");
        assert!(matches!(
            cpp_plugin_schema(
                "bad",
                None,
                vec![NodeSchema {
                    id: "bad:add".into(),
                    backend: BackendKind::Python,
                    entrypoint: "add".into(),
                    label: None,
                    stateful: false,
                    feature_flags: Vec::new(),
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                    metadata: BTreeMap::new(),
                }],
            ),
            Err(FfiContractError::UnexpectedBackendKind { .. })
        ));
    }
}
