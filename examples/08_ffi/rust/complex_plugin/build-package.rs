use std::collections::BTreeMap;

use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_ffi_core::{
    BackendConfig, BackendKind, BackendRuntimeModel, NodeSchema, PluginSchema, PluginSchemaInfo,
    SCHEMA_VERSION, WirePort, rust_complete_plugin_package,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = PluginSchema {
        schema_version: SCHEMA_VERSION,
        plugin: PluginSchemaInfo {
            name: "ffi_showcase".into(),
            version: Some("1.0.0".into()),
            description: Some("Rust FFI showcase package".into()),
            metadata: BTreeMap::new(),
        },
        dependencies: Vec::new(),
        required_host_capabilities: Vec::new(),
        feature_flags: Vec::new(),
        boundary_contracts: Vec::new(),
        nodes: vec![NodeSchema {
            id: "scalar_add".into(),
            backend: BackendKind::Rust,
            entrypoint: "scalar_add".into(),
            label: Some("Scalar Add".into()),
            stateful: false,
            feature_flags: Vec::new(),
            inputs: vec![
                WirePort {
                    name: "a".into(),
                    ty: TypeExpr::scalar(ValueType::Int),
                    type_key: None,
                    optional: false,
                    access: Default::default(),
                    residency: None,
                    layout: None,
                    source: None,
                    const_value: None,
                },
                WirePort {
                    name: "b".into(),
                    ty: TypeExpr::scalar(ValueType::Int),
                    type_key: None,
                    optional: false,
                    access: Default::default(),
                    residency: None,
                    layout: None,
                    source: None,
                    const_value: None,
                },
            ],
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
            metadata: BTreeMap::new(),
        }],
    };
    let backends = BTreeMap::from([(
        "scalar_add".into(),
        BackendConfig {
            backend: BackendKind::Rust,
            runtime_model: BackendRuntimeModel::InProcessAbi,
            entry_module: None,
            entry_class: None,
            entry_symbol: Some("scalar_add".into()),
            executable: None,
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        },
    )]);
    let package = rust_complete_plugin_package(
        schema,
        backends,
        vec!["target/release/libffi_showcase.so".into()],
        vec!["src/lib.rs".into(), "build-package.rs".into()],
    )?;
    package.write_descriptor("plugin.json")?;
    package.write_lockfile("plugin.lock.json")?;
    Ok(())
}
