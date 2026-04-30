use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path};

use daedalus_data::model::{
    EnumValue, EnumVariant, StructField, StructFieldValue, TypeExpr, Value, ValueType,
};
use daedalus_transport::{AccessMode, BoundaryTypeContract, Layout, Payload, Residency, TypeKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const SCHEMA_VERSION: u32 = 1;
pub const WORKER_PROTOCOL_VERSION: u32 = 1;
pub const DEFAULT_CORRELATION_ID: &str = "fixture-0";

/// Explicit layer map for the FFI rewrite.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FfiLayer {
    PackageDiscovery,
    Schema,
    HostCore,
    BackendRuntime,
    Transport,
}

/// Backend execution models supported as first-class paths.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendRuntimeModel {
    InProcessAbi,
    PersistentWorker,
}

/// Language/backend family for a node or plugin schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    Rust,
    Python,
    Node,
    Java,
    CCpp,
    Shader,
    Other(String),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureLanguage {
    Rust,
    Python,
    Node,
    Java,
    CCpp,
}

impl FixtureLanguage {
    pub fn backend(self) -> BackendKind {
        match self {
            Self::Rust => BackendKind::Rust,
            Self::Python => BackendKind::Python,
            Self::Node => BackendKind::Node,
            Self::Java => BackendKind::Java,
            Self::CCpp => BackendKind::CCpp,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Python => "python",
            Self::Node => "node",
            Self::Java => "java",
            Self::CCpp => "c_cpp",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalFixtureKind {
    Scalar,
    Bytes,
    Image,
    Struct,
    Enum,
    Optional,
    List,
    Tuple,
    Map,
    MultiOutput,
    RawIo,
    Stateful,
    Shader,
    CapabilityBacked,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CanonicalFixtureSpec {
    pub kind: CanonicalFixtureKind,
    pub name: String,
    pub node_id: String,
    pub inputs: Vec<WirePort>,
    pub outputs: Vec<WirePort>,
    pub request_inputs: BTreeMap<String, WireValue>,
    pub expected_outputs: BTreeMap<String, WireValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_state: Option<WireValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_state: Option<WireValue>,
    #[serde(default)]
    pub expected_events: Vec<InvokeEvent>,
    #[serde(default)]
    pub required_host_capabilities: Vec<String>,
    #[serde(default)]
    pub backend: Option<BackendKind>,
    #[serde(default)]
    pub stateful: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GeneratedFixtureFile {
    pub path: String,
    pub contents: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GeneratedLanguageFixture {
    pub language: FixtureLanguage,
    pub schema: PluginSchema,
    pub backends: BTreeMap<String, BackendConfig>,
    pub request: InvokeRequest,
    pub expected_response: InvokeResponse,
    pub files: Vec<GeneratedFixtureFile>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GeneratedPackageFixture {
    pub language: FixtureLanguage,
    pub package: PluginPackage,
    pub files: Vec<GeneratedFixtureFile>,
}

pub fn scalar_add_fixture_spec() -> CanonicalFixtureSpec {
    CanonicalFixtureSpec {
        kind: CanonicalFixtureKind::Scalar,
        name: "scalar_add".into(),
        node_id: "ffi.conformance.scalar_add:add".into(),
        inputs: vec![
            WirePort {
                name: "a".into(),
                ty: TypeExpr::scalar(daedalus_data::model::ValueType::Int),
                type_key: None,
                optional: false,
                access: AccessMode::Read,
                residency: None,
                layout: None,
                source: None,
                const_value: None,
            },
            WirePort {
                name: "b".into(),
                ty: TypeExpr::scalar(daedalus_data::model::ValueType::Int),
                type_key: None,
                optional: false,
                access: AccessMode::Read,
                residency: None,
                layout: None,
                source: None,
                const_value: None,
            },
        ],
        outputs: vec![WirePort {
            name: "out".into(),
            ty: TypeExpr::scalar(daedalus_data::model::ValueType::Int),
            type_key: None,
            optional: false,
            access: AccessMode::Read,
            residency: None,
            layout: None,
            source: None,
            const_value: None,
        }],
        request_inputs: BTreeMap::from([
            ("a".into(), WireValue::Int(2)),
            ("b".into(), WireValue::Int(40)),
        ]),
        expected_outputs: BTreeMap::from([("out".into(), WireValue::Int(42))]),
        request_state: None,
        expected_state: None,
        expected_events: Vec::new(),
        required_host_capabilities: Vec::new(),
        backend: None,
        stateful: false,
    }
}

pub fn canonical_fixture_specs() -> Vec<CanonicalFixtureSpec> {
    vec![
        scalar_add_fixture_spec(),
        bytes_fixture_spec(),
        image_fixture_spec(),
        struct_fixture_spec(),
        enum_fixture_spec(),
        optional_fixture_spec(),
        list_fixture_spec(),
        tuple_fixture_spec(),
        map_fixture_spec(),
        multi_output_fixture_spec(),
        raw_io_fixture_spec(),
        stateful_fixture_spec(),
        shader_fixture_spec(),
        capability_backed_fixture_spec(),
    ]
}

fn bytes_fixture_spec() -> CanonicalFixtureSpec {
    let ty = TypeExpr::scalar(ValueType::Bytes);
    unary_fixture_spec(
        CanonicalFixtureKind::Bytes,
        "bytes_echo",
        "payload",
        ty.clone(),
        WireValue::Bytes(BytePayload {
            data: vec![1, 2, 3, 4],
            encoding: ByteEncoding::Raw,
        }),
        "out",
        ty,
        WireValue::Bytes(BytePayload {
            data: vec![1, 2, 3, 4],
            encoding: ByteEncoding::Raw,
        }),
    )
}

fn image_fixture_spec() -> CanonicalFixtureSpec {
    let image = WireValue::Image(ImagePayload {
        data: vec![0, 64, 128, 255],
        width: 2,
        height: 2,
        channels: 1,
        dtype: ScalarDType::U8,
        layout: ImageLayout::Hwc,
    });
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::Image,
        "image_passthrough",
        "image",
        TypeExpr::opaque("ffi:image:u8"),
        image.clone(),
        "image",
        TypeExpr::opaque("ffi:image:u8"),
        image,
    );
    spec.backend = Some(BackendKind::Python);
    spec
}

fn struct_fixture_spec() -> CanonicalFixtureSpec {
    let ty = TypeExpr::r#struct(vec![
        StructField {
            name: "name".into(),
            ty: TypeExpr::scalar(ValueType::String),
        },
        StructField {
            name: "count".into(),
            ty: TypeExpr::scalar(ValueType::Int),
        },
    ]);
    let input = WireValue::Record(BTreeMap::from([
        ("name".into(), WireValue::String("demo".into())),
        ("count".into(), WireValue::Int(2)),
    ]));
    let output = WireValue::Record(BTreeMap::from([
        ("name".into(), WireValue::String("demo".into())),
        ("count".into(), WireValue::Int(3)),
    ]));
    unary_fixture_spec(
        CanonicalFixtureKind::Struct,
        "struct_update",
        "input",
        ty.clone(),
        input,
        "out",
        ty,
        output,
    )
}

fn enum_fixture_spec() -> CanonicalFixtureSpec {
    let ty = TypeExpr::r#enum(vec![
        EnumVariant {
            name: "Ready".into(),
            ty: None,
        },
        EnumVariant {
            name: "Value".into(),
            ty: Some(TypeExpr::scalar(ValueType::Int)),
        },
    ]);
    unary_fixture_spec(
        CanonicalFixtureKind::Enum,
        "enum_roundtrip",
        "input",
        ty.clone(),
        WireValue::Enum(WireEnumValue {
            name: "Value".into(),
            value: Some(Box::new(WireValue::Int(7))),
        }),
        "out",
        ty,
        WireValue::Enum(WireEnumValue {
            name: "Value".into(),
            value: Some(Box::new(WireValue::Int(7))),
        }),
    )
}

fn optional_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::Optional,
        "optional_default",
        "maybe",
        TypeExpr::optional(TypeExpr::scalar(ValueType::Int)),
        WireValue::Unit,
        "out",
        TypeExpr::scalar(ValueType::Int),
        WireValue::Int(5),
    );
    spec.inputs[0].optional = true;
    spec
}

fn list_fixture_spec() -> CanonicalFixtureSpec {
    let ty = TypeExpr::list(TypeExpr::scalar(ValueType::Int));
    unary_fixture_spec(
        CanonicalFixtureKind::List,
        "list_sum",
        "items",
        ty,
        WireValue::List(vec![
            WireValue::Int(1),
            WireValue::Int(2),
            WireValue::Int(3),
        ]),
        "out",
        TypeExpr::scalar(ValueType::Int),
        WireValue::Int(6),
    )
}

fn tuple_fixture_spec() -> CanonicalFixtureSpec {
    let ty = TypeExpr::Tuple(vec![
        TypeExpr::scalar(ValueType::String),
        TypeExpr::scalar(ValueType::Bool),
    ]);
    unary_fixture_spec(
        CanonicalFixtureKind::Tuple,
        "tuple_unpack",
        "pair",
        ty,
        WireValue::List(vec![
            WireValue::String("left".into()),
            WireValue::Bool(true),
        ]),
        "out",
        TypeExpr::scalar(ValueType::String),
        WireValue::String("left:true".into()),
    )
}

fn map_fixture_spec() -> CanonicalFixtureSpec {
    let ty = TypeExpr::map(
        TypeExpr::scalar(ValueType::String),
        TypeExpr::scalar(ValueType::Int),
    );
    unary_fixture_spec(
        CanonicalFixtureKind::Map,
        "map_lookup",
        "items",
        ty,
        WireValue::Record(BTreeMap::from([
            ("a".into(), WireValue::Int(1)),
            ("b".into(), WireValue::Int(2)),
        ])),
        "out",
        TypeExpr::scalar(ValueType::Int),
        WireValue::Int(2),
    )
}

fn multi_output_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = CanonicalFixtureSpec {
        kind: CanonicalFixtureKind::MultiOutput,
        name: "multi_output".into(),
        node_id: "ffi.conformance.multi_output:split".into(),
        inputs: vec![wire_port("value", TypeExpr::scalar(ValueType::Int))],
        outputs: vec![
            wire_port("double", TypeExpr::scalar(ValueType::Int)),
            wire_port("label", TypeExpr::scalar(ValueType::String)),
        ],
        request_inputs: BTreeMap::from([("value".into(), WireValue::Int(21))]),
        expected_outputs: BTreeMap::from([
            ("double".into(), WireValue::Int(42)),
            ("label".into(), WireValue::String("value:21".into())),
        ]),
        request_state: None,
        expected_state: None,
        expected_events: Vec::new(),
        required_host_capabilities: Vec::new(),
        backend: None,
        stateful: false,
    };
    spec.outputs[1].optional = false;
    spec
}

fn raw_io_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = bytes_fixture_spec();
    spec.kind = CanonicalFixtureKind::RawIo;
    spec.name = "raw_io_event".into();
    spec.node_id = "ffi.conformance.raw_io:event".into();
    spec.expected_events = vec![InvokeEvent {
        level: InvokeEventLevel::Info,
        message: "raw payload accepted".into(),
        metadata: BTreeMap::from([("bytes".into(), serde_json::json!(4))]),
    }];
    spec
}

fn stateful_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::Stateful,
        "stateful_counter",
        "delta",
        TypeExpr::scalar(ValueType::Int),
        WireValue::Int(3),
        "count",
        TypeExpr::scalar(ValueType::Int),
        WireValue::Int(10),
    );
    spec.stateful = true;
    spec.request_state = Some(WireValue::Record(BTreeMap::from([(
        "count".into(),
        WireValue::Int(7),
    )])));
    spec.expected_state = Some(WireValue::Record(BTreeMap::from([(
        "count".into(),
        WireValue::Int(10),
    )])));
    spec
}

fn shader_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::Shader,
        "shader_write_u32",
        "input",
        TypeExpr::scalar(ValueType::U32),
        WireValue::Int(41),
        "out",
        TypeExpr::scalar(ValueType::U32),
        WireValue::Int(42),
    );
    spec.backend = Some(BackendKind::Shader);
    spec
}

fn capability_backed_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::CapabilityBacked,
        "capability_camera_frame",
        "frame",
        TypeExpr::opaque("ffi:frame"),
        WireValue::Handle(WirePayloadHandle {
            id: "frame-0".into(),
            type_key: TypeKey::new("ffi:frame"),
            access: AccessMode::Read,
            residency: Some(Residency::Gpu),
            layout: None,
            capabilities: vec!["camera.read".into()],
            metadata: BTreeMap::new(),
        }),
        "ok",
        TypeExpr::scalar(ValueType::Bool),
        WireValue::Bool(true),
    );
    spec.required_host_capabilities = vec!["camera.read".into()];
    spec
}

fn unary_fixture_spec(
    kind: CanonicalFixtureKind,
    name: &str,
    input_name: &str,
    input_ty: TypeExpr,
    input: WireValue,
    output_name: &str,
    output_ty: TypeExpr,
    output: WireValue,
) -> CanonicalFixtureSpec {
    CanonicalFixtureSpec {
        kind,
        name: name.into(),
        node_id: format!("ffi.conformance.{name}:run"),
        inputs: vec![wire_port(input_name, input_ty)],
        outputs: vec![wire_port(output_name, output_ty)],
        request_inputs: BTreeMap::from([(input_name.into(), input)]),
        expected_outputs: BTreeMap::from([(output_name.into(), output)]),
        request_state: None,
        expected_state: None,
        expected_events: Vec::new(),
        required_host_capabilities: Vec::new(),
        backend: None,
        stateful: false,
    }
}

fn wire_port(name: &str, ty: TypeExpr) -> WirePort {
    WirePort {
        name: name.into(),
        ty,
        type_key: None,
        optional: false,
        access: AccessMode::Read,
        residency: None,
        layout: None,
        source: None,
        const_value: None,
    }
}

pub fn generate_scalar_add_fixtures() -> Result<Vec<GeneratedLanguageFixture>, FfiContractError> {
    let spec = scalar_add_fixture_spec();
    [
        FixtureLanguage::Rust,
        FixtureLanguage::Python,
        FixtureLanguage::Node,
        FixtureLanguage::Java,
        FixtureLanguage::CCpp,
    ]
    .into_iter()
    .map(|language| generate_language_fixture(&spec, language))
    .collect()
}

pub fn generate_scalar_add_package_fixtures()
-> Result<Vec<GeneratedPackageFixture>, FfiContractError> {
    generate_scalar_add_fixtures()?
        .into_iter()
        .map(package_fixture_from_language_fixture)
        .collect()
}

pub fn generate_language_fixture(
    spec: &CanonicalFixtureSpec,
    language: FixtureLanguage,
) -> Result<GeneratedLanguageFixture, FfiContractError> {
    let backend_kind = language.backend();
    let backend_kind = spec.backend.clone().unwrap_or(backend_kind);
    let schema = PluginSchema {
        schema_version: SCHEMA_VERSION,
        plugin: PluginSchemaInfo {
            name: format!("ffi.conformance.{}.{}", language.as_str(), spec.name),
            version: Some("1.0.0".into()),
            description: Some(format!("Generated {} conformance fixture", spec.name)),
            metadata: BTreeMap::from([("fixture_kind".into(), serde_json::json!(spec.kind))]),
        },
        dependencies: Vec::new(),
        required_host_capabilities: spec.required_host_capabilities.clone(),
        feature_flags: Vec::new(),
        boundary_contracts: Vec::new(),
        nodes: vec![NodeSchema {
            id: spec.node_id.clone(),
            backend: backend_kind.clone(),
            entrypoint: language_entrypoint(language).into(),
            label: Some("Scalar Add".into()),
            stateful: spec.stateful,
            feature_flags: Vec::new(),
            inputs: spec.inputs.clone(),
            outputs: spec.outputs.clone(),
            metadata: BTreeMap::from([("generated_from".into(), serde_json::json!(spec.name))]),
        }],
    };
    schema.validate()?;
    let backend = fixture_backend_config(language);
    backend.validate_for_node(&spec.node_id)?;
    let backends = BTreeMap::from([(spec.node_id.clone(), backend)]);
    let request = InvokeRequest {
        protocol_version: WORKER_PROTOCOL_VERSION,
        node_id: spec.node_id.clone(),
        correlation_id: Some(DEFAULT_CORRELATION_ID.into()),
        args: spec.request_inputs.clone(),
        state: spec.request_state.clone(),
        context: BTreeMap::new(),
    };
    let expected_response = InvokeResponse {
        protocol_version: WORKER_PROTOCOL_VERSION,
        correlation_id: Some(DEFAULT_CORRELATION_ID.into()),
        outputs: spec.expected_outputs.clone(),
        state: spec.expected_state.clone(),
        events: spec.expected_events.clone(),
    };
    Ok(GeneratedLanguageFixture {
        language,
        schema,
        backends,
        request,
        expected_response,
        files: fixture_files(language),
    })
}

fn fixture_backend_config(language: FixtureLanguage) -> BackendConfig {
    match language {
        FixtureLanguage::Rust => BackendConfig {
            backend: BackendKind::Rust,
            runtime_model: BackendRuntimeModel::InProcessAbi,
            entry_module: None,
            entry_class: None,
            entry_symbol: Some("add".into()),
            executable: None,
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        },
        FixtureLanguage::Python => worker_backend(
            BackendKind::Python,
            Some("scalar_add.py".into()),
            None,
            Some("add".into()),
            Some("python".into()),
        ),
        FixtureLanguage::Node => worker_backend(
            BackendKind::Node,
            Some("scalar_add.mjs".into()),
            None,
            Some("add".into()),
            Some("node".into()),
        ),
        FixtureLanguage::Java => {
            let mut backend = worker_backend(
                BackendKind::Java,
                None,
                Some("ffi.conformance.ScalarAdd".into()),
                Some("add".into()),
                Some("java".into()),
            );
            backend.classpath.push("classes".into());
            backend
        }
        FixtureLanguage::CCpp => BackendConfig {
            backend: BackendKind::CCpp,
            runtime_model: BackendRuntimeModel::InProcessAbi,
            entry_module: Some("libscalar_add.so".into()),
            entry_class: None,
            entry_symbol: Some("add_i64".into()),
            executable: None,
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        },
    }
}

fn worker_backend(
    backend: BackendKind,
    entry_module: Option<String>,
    entry_class: Option<String>,
    entry_symbol: Option<String>,
    executable: Option<String>,
) -> BackendConfig {
    BackendConfig {
        backend,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module,
        entry_class,
        entry_symbol,
        executable,
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

fn language_entrypoint(language: FixtureLanguage) -> &'static str {
    match language {
        FixtureLanguage::Rust => "add",
        FixtureLanguage::Python => "add",
        FixtureLanguage::Node => "add",
        FixtureLanguage::Java => "add",
        FixtureLanguage::CCpp => "add_i64",
    }
}

fn fixture_files(language: FixtureLanguage) -> Vec<GeneratedFixtureFile> {
    match language {
        FixtureLanguage::Rust => vec![GeneratedFixtureFile {
            path: "src/lib.rs".into(),
            contents: "#[daedalus::node(id = \"ffi.conformance.scalar_add:add\")]\npub fn add(a: i64, b: i64) -> i64 { a + b }\n".into(),
        }],
        FixtureLanguage::Python => vec![GeneratedFixtureFile {
            path: "scalar_add.py".into(),
            contents: "def add(a: int, b: int) -> int:\n    return a + b\n".into(),
        }],
        FixtureLanguage::Node => vec![GeneratedFixtureFile {
            path: "scalar_add.mjs".into(),
            contents: "export function add(a, b) {\n  return a + b;\n}\n".into(),
        }],
        FixtureLanguage::Java => vec![GeneratedFixtureFile {
            path: "ffi/conformance/ScalarAdd.java".into(),
            contents: "package ffi.conformance;\n\npublic final class ScalarAdd {\n  public static long add(long a, long b) { return a + b; }\n}\n".into(),
        }],
        FixtureLanguage::CCpp => vec![GeneratedFixtureFile {
            path: "scalar_add.cpp".into(),
            contents: "#include <cstdint>\nextern \"C\" int64_t add_i64(int64_t a, int64_t b) { return a + b; }\n".into(),
        }],
    }
}

fn package_fixture_from_language_fixture(
    fixture: GeneratedLanguageFixture,
) -> Result<GeneratedPackageFixture, FfiContractError> {
    let mut files = Vec::with_capacity(fixture.files.len());
    let mut artifacts = Vec::with_capacity(fixture.files.len());
    for file in fixture.files {
        let path = bundled_artifact_path(PackageArtifactKind::SourceFile, &file.path, None)?;
        files.push(GeneratedFixtureFile {
            path: path.clone(),
            contents: file.contents,
        });
        artifacts.push(PackageArtifact {
            path,
            kind: PackageArtifactKind::SourceFile,
            backend: Some(fixture.language.backend()),
            platform: None,
            sha256: None,
            metadata: BTreeMap::from([
                (
                    "language".into(),
                    serde_json::json!(fixture.language.as_str()),
                ),
                ("fixture".into(), serde_json::json!("scalar_add")),
            ]),
        });
    }
    let package = PluginPackage {
        schema_version: SCHEMA_VERSION,
        schema: Some(fixture.schema),
        backends: fixture.backends,
        artifacts,
        lockfile: None,
        manifest_hash: None,
        signature: None,
        metadata: BTreeMap::from([
            (
                "language".into(),
                serde_json::json!(fixture.language.as_str()),
            ),
            ("fixture".into(), serde_json::json!("scalar_add")),
        ]),
    };
    package.validate()?;
    Ok(GeneratedPackageFixture {
        language: fixture.language,
        package,
        files,
    })
}

/// Core schema boundary for plugin metadata and node shape.
///
/// Runtime process details and physical packaging are deliberately excluded from this type.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginSchema {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub plugin: PluginSchemaInfo,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub required_host_capabilities: Vec<String>,
    #[serde(default)]
    pub feature_flags: Vec<String>,
    #[serde(default)]
    pub boundary_contracts: Vec<BoundaryTypeContract>,
    #[serde(default)]
    pub nodes: Vec<NodeSchema>,
}

fn default_schema_version() -> u32 {
    SCHEMA_VERSION
}

impl PluginSchema {
    pub fn validate(&self) -> Result<(), FfiContractError> {
        validate_schema_version("plugin schema", self.schema_version)?;
        validate_non_empty("plugin.name", &self.plugin.name)?;

        let mut node_ids = std::collections::BTreeSet::new();
        for node in &self.nodes {
            validate_non_empty("node.id", &node.id)?;
            validate_non_empty("node.entrypoint", &node.entrypoint)?;
            if !node_ids.insert(node.id.as_str()) {
                return Err(FfiContractError::DuplicateNode {
                    node_id: node.id.clone(),
                });
            }
            validate_ports(&node.id, "input", &node.inputs)?;
            validate_ports(&node.id, "output", &node.outputs)?;
        }

        Ok(())
    }

    pub fn validate_backend_kind(&self, expected: BackendKind) -> Result<(), FfiContractError> {
        self.validate()?;
        for node in &self.nodes {
            if node.backend != expected {
                return Err(FfiContractError::UnexpectedBackendKind {
                    node_id: node.id.clone(),
                    expected: expected.clone(),
                    found: node.backend.clone(),
                });
            }
        }
        Ok(())
    }
}

pub fn validate_language_backends(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
    expected: BackendKind,
) -> Result<(), FfiContractError> {
    schema.validate_backend_kind(expected)?;
    let expected_nodes = schema
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    for node in &schema.nodes {
        let backend =
            backends
                .get(&node.id)
                .ok_or_else(|| FfiContractError::MissingBackendConfig {
                    node_id: node.id.clone(),
                })?;
        if backend.backend != node.backend {
            return Err(FfiContractError::BackendMismatch {
                node_id: node.id.clone(),
                schema_backend: node.backend.clone(),
                config_backend: backend.backend.clone(),
            });
        }
        backend.validate_for_node(&node.id)?;
    }

    for node_id in backends.keys() {
        if !expected_nodes.contains(node_id.as_str()) {
            return Err(FfiContractError::MissingBackendConfig {
                node_id: node_id.clone(),
            });
        }
    }

    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginSchemaInfo {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeSchema {
    pub id: String,
    pub backend: BackendKind,
    pub entrypoint: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub stateful: bool,
    #[serde(default)]
    pub feature_flags: Vec<String>,
    #[serde(default)]
    pub inputs: Vec<WirePort>,
    #[serde(default)]
    pub outputs: Vec<WirePort>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WirePort {
    pub name: String,
    pub ty: TypeExpr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_key: Option<TypeKey>,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub access: AccessMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<Residency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub const_value: Option<serde_json::Value>,
}

/// Backend runtime config is deliberately separate from the schema surface.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendConfig {
    pub backend: BackendKind,
    pub runtime_model: BackendRuntimeModel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_module: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_symbol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub classpath: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_library_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub options: BTreeMap<String, serde_json::Value>,
}

impl BackendConfig {
    pub fn validate_for_node(&self, node_id: &str) -> Result<(), FfiContractError> {
        match (&self.backend, self.runtime_model) {
            (BackendKind::Python, BackendRuntimeModel::PersistentWorker) => {
                require_some(node_id, "entry_module", &self.entry_module)?;
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
                require_some(node_id, "executable", &self.executable)?;
            }
            (BackendKind::Node, BackendRuntimeModel::PersistentWorker) => {
                require_some(node_id, "entry_module", &self.entry_module)?;
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
                require_some(node_id, "executable", &self.executable)?;
            }
            (BackendKind::Java, BackendRuntimeModel::PersistentWorker) => {
                if self.classpath.is_empty() {
                    return Err(FfiContractError::MissingBackendField {
                        node_id: node_id.to_string(),
                        field: "classpath",
                    });
                }
                require_some(node_id, "entry_class", &self.entry_class)?;
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
                require_some(node_id, "executable", &self.executable)?;
            }
            (BackendKind::CCpp, BackendRuntimeModel::InProcessAbi) => {
                require_some(node_id, "entry_module", &self.entry_module)?;
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
            }
            (BackendKind::Rust, BackendRuntimeModel::InProcessAbi)
            | (BackendKind::Shader, BackendRuntimeModel::InProcessAbi) => {
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
            }
            (backend, model) => {
                if matches!(model, BackendRuntimeModel::PersistentWorker) {
                    require_some(node_id, "executable", &self.executable)?;
                }
                if matches!(backend, BackendKind::Other(_)) && self.entry_symbol.is_none() {
                    return Err(FfiContractError::MissingBackendField {
                        node_id: node_id.to_string(),
                        field: "entry_symbol",
                    });
                }
            }
        }
        Ok(())
    }
}

/// Physical package descriptor for an FFI plugin.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginPackage {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub schema: Option<PluginSchema>,
    #[serde(default)]
    pub backends: BTreeMap<String, BackendConfig>,
    #[serde(default)]
    pub artifacts: Vec<PackageArtifact>,
    #[serde(default)]
    pub lockfile: Option<String>,
    #[serde(default)]
    pub manifest_hash: Option<String>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl PluginPackage {
    pub fn validate(&self) -> Result<(), FfiContractError> {
        validate_schema_version("plugin package", self.schema_version)?;

        if let Some(schema) = &self.schema {
            schema.validate()?;
            for node in &schema.nodes {
                let backend = self.backends.get(&node.id).ok_or_else(|| {
                    FfiContractError::MissingBackendConfig {
                        node_id: node.id.clone(),
                    }
                })?;
                if backend.backend != node.backend {
                    return Err(FfiContractError::BackendMismatch {
                        node_id: node.id.clone(),
                        schema_backend: node.backend.clone(),
                        config_backend: backend.backend.clone(),
                    });
                }
                backend.validate_for_node(&node.id)?;
            }
        }

        for (node_id, backend) in &self.backends {
            validate_non_empty("backend node id", node_id)?;
            backend.validate_for_node(node_id)?;
        }

        for artifact in &self.artifacts {
            validate_non_empty("artifact.path", &artifact.path)?;
        }

        if let Some(lockfile) = &self.lockfile {
            validate_non_empty("package.lockfile", lockfile)?;
        }
        if let Some(hash) = &self.manifest_hash {
            validate_non_empty("package.manifest_hash", hash)?;
        }

        Ok(())
    }

    pub fn validate_artifact_files(
        &self,
        base_dir: impl AsRef<Path>,
    ) -> Result<(), FfiContractError> {
        self.validate()?;
        let base_dir = base_dir.as_ref();
        for artifact in &self.artifacts {
            validate_package_relative_path(&artifact.path)?;
            if !base_dir.join(&artifact.path).exists() {
                return Err(FfiContractError::MissingPackageArtifact {
                    path: artifact.path.clone(),
                    base_dir: base_dir.display().to_string(),
                });
            }
        }
        Ok(())
    }

    pub fn rewrite_artifact_paths_for_bundle(&mut self) -> Result<(), FfiContractError> {
        for artifact in &mut self.artifacts {
            artifact.path =
                bundled_artifact_path(artifact.kind, &artifact.path, artifact.platform.as_ref())?;
        }
        Ok(())
    }

    pub fn stamp_integrity(&mut self, base_dir: impl AsRef<Path>) -> Result<(), FfiContractError> {
        self.validate_artifact_files(base_dir.as_ref())?;
        let base_dir = base_dir.as_ref();
        for artifact in &mut self.artifacts {
            artifact.sha256 = Some(sha256_file_hex(base_dir.join(&artifact.path))?);
        }
        self.manifest_hash = Some(self.compute_manifest_hash()?);
        Ok(())
    }

    pub fn verify_integrity(&self, base_dir: impl AsRef<Path>) -> Result<(), FfiContractError> {
        self.validate_artifact_files(base_dir.as_ref())?;
        let base_dir = base_dir.as_ref();
        for artifact in &self.artifacts {
            let Some(expected) = &artifact.sha256 else {
                continue;
            };
            let actual = sha256_file_hex(base_dir.join(&artifact.path))?;
            if &actual != expected {
                return Err(FfiContractError::PackageHashMismatch {
                    path: artifact.path.clone(),
                    expected: expected.clone(),
                    actual,
                });
            }
        }
        if let Some(expected) = &self.manifest_hash {
            let actual = self.compute_manifest_hash()?;
            if &actual != expected {
                return Err(FfiContractError::PackageHashMismatch {
                    path: "manifest".into(),
                    expected: expected.clone(),
                    actual,
                });
            }
        }
        Ok(())
    }

    pub fn compute_manifest_hash(&self) -> Result<String, FfiContractError> {
        let mut package = self.clone();
        package.manifest_hash = None;
        package.signature = None;
        let bytes =
            serde_json::to_vec(&package).map_err(|err| FfiContractError::PackageHashError {
                message: err.to_string(),
            })?;
        Ok(sha256_bytes_hex(&bytes))
    }

    pub fn read_descriptor(path: impl AsRef<Path>) -> Result<Self, FfiContractError> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(|err| FfiContractError::PackageIo {
            path: path.display().to_string(),
            message: err.to_string(),
        })?;
        serde_json::from_slice(&bytes).map_err(|err| FfiContractError::PackageJson {
            path: path.display().to_string(),
            message: err.to_string(),
        })
    }

    pub fn write_descriptor(&self, path: impl AsRef<Path>) -> Result<(), FfiContractError> {
        let path = path.as_ref();
        let bytes =
            serde_json::to_vec_pretty(self).map_err(|err| FfiContractError::PackageJson {
                path: path.display().to_string(),
                message: err.to_string(),
            })?;
        fs::write(path, bytes).map_err(|err| FfiContractError::PackageIo {
            path: path.display().to_string(),
            message: err.to_string(),
        })
    }

    pub fn read_descriptor_and_verify(
        path: impl AsRef<Path>,
        base_dir: impl AsRef<Path>,
    ) -> Result<Self, FfiContractError> {
        let package = Self::read_descriptor(path)?;
        package.verify_integrity(base_dir)?;
        Ok(package)
    }

    pub fn generate_lockfile(&self) -> PluginLockfile {
        PluginLockfile::from_package(self)
    }

    pub fn write_lockfile(&self, path: impl AsRef<Path>) -> Result<(), FfiContractError> {
        self.generate_lockfile().write(path)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginLockfile {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub plugin_name: Option<String>,
    #[serde(default)]
    pub plugin_version: Option<String>,
    #[serde(default)]
    pub manifest_hash: Option<String>,
    #[serde(default)]
    pub backends: BTreeMap<String, BackendLockEntry>,
    #[serde(default)]
    pub artifacts: Vec<PackageLockArtifact>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl PluginLockfile {
    pub fn from_package(package: &PluginPackage) -> Self {
        let mut artifacts: Vec<_> = package
            .artifacts
            .iter()
            .map(PackageLockArtifact::from_artifact)
            .collect();
        artifacts.sort_by(|a, b| {
            a.path
                .cmp(&b.path)
                .then_with(|| format!("{:?}", a.kind).cmp(&format!("{:?}", b.kind)))
        });

        Self {
            schema_version: package.schema_version,
            plugin_name: package
                .schema
                .as_ref()
                .map(|schema| schema.plugin.name.clone()),
            plugin_version: package
                .schema
                .as_ref()
                .and_then(|schema| schema.plugin.version.clone()),
            manifest_hash: package.manifest_hash.clone(),
            backends: package
                .backends
                .iter()
                .map(|(node_id, backend)| (node_id.clone(), BackendLockEntry::from(backend)))
                .collect(),
            artifacts,
            metadata: package.metadata.clone(),
        }
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Self, FfiContractError> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(|err| FfiContractError::PackageIo {
            path: path.display().to_string(),
            message: err.to_string(),
        })?;
        serde_json::from_slice(&bytes).map_err(|err| FfiContractError::PackageJson {
            path: path.display().to_string(),
            message: err.to_string(),
        })
    }

    pub fn write(&self, path: impl AsRef<Path>) -> Result<(), FfiContractError> {
        let path = path.as_ref();
        let bytes =
            serde_json::to_vec_pretty(self).map_err(|err| FfiContractError::PackageJson {
                path: path.display().to_string(),
                message: err.to_string(),
            })?;
        fs::write(path, bytes).map_err(|err| FfiContractError::PackageIo {
            path: path.display().to_string(),
            message: err.to_string(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendLockEntry {
    pub backend: BackendKind,
    pub runtime_model: BackendRuntimeModel,
    #[serde(default)]
    pub entry_module: Option<String>,
    #[serde(default)]
    pub entry_class: Option<String>,
    #[serde(default)]
    pub entry_symbol: Option<String>,
    #[serde(default)]
    pub executable: Option<String>,
    #[serde(default)]
    pub classpath: Vec<String>,
    #[serde(default)]
    pub native_library_paths: Vec<String>,
    #[serde(default)]
    pub options: BTreeMap<String, serde_json::Value>,
}

impl From<&BackendConfig> for BackendLockEntry {
    fn from(backend: &BackendConfig) -> Self {
        Self {
            backend: backend.backend.clone(),
            runtime_model: backend.runtime_model,
            entry_module: backend.entry_module.clone(),
            entry_class: backend.entry_class.clone(),
            entry_symbol: backend.entry_symbol.clone(),
            executable: backend.executable.clone(),
            classpath: backend.classpath.clone(),
            native_library_paths: backend.native_library_paths.clone(),
            options: backend.options.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageLockArtifact {
    pub path: String,
    pub kind: PackageArtifactKind,
    #[serde(default)]
    pub backend: Option<BackendKind>,
    #[serde(default)]
    pub platform: Option<PackagePlatform>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl PackageLockArtifact {
    fn from_artifact(artifact: &PackageArtifact) -> Self {
        Self {
            path: artifact.path.clone(),
            kind: artifact.kind,
            backend: artifact.backend.clone(),
            platform: artifact.platform.clone(),
            sha256: artifact.sha256.clone(),
            metadata: artifact.metadata.clone(),
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum FfiContractError {
    #[error("{surface} version mismatch: expected {expected}, found {found}")]
    VersionMismatch {
        surface: &'static str,
        expected: u32,
        found: u32,
    },
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("duplicate node id `{node_id}`")]
    DuplicateNode { node_id: String },
    #[error("duplicate {direction} port `{port}` on node `{node_id}`")]
    DuplicatePort {
        node_id: String,
        direction: &'static str,
        port: String,
    },
    #[error("node `{node_id}` backend config missing `{field}`")]
    MissingBackendField {
        node_id: String,
        field: &'static str,
    },
    #[error("missing backend config for node `{node_id}`")]
    MissingBackendConfig { node_id: String },
    #[error(
        "backend mismatch for node `{node_id}`: schema has {schema_backend:?}, config has {config_backend:?}"
    )]
    BackendMismatch {
        node_id: String,
        schema_backend: BackendKind,
        config_backend: BackendKind,
    },
    #[error("node `{node_id}` uses backend {found:?}, expected {expected:?}")]
    UnexpectedBackendKind {
        node_id: String,
        expected: BackendKind,
        found: BackendKind,
    },
    #[error("package artifact path must be relative and stay inside package: `{path}`")]
    UnsafePackagePath { path: String },
    #[error("package artifact path must have a file name: `{path}`")]
    MissingArtifactFileName { path: String },
    #[error("package artifact `{path}` is missing under `{base_dir}`")]
    MissingPackageArtifact { path: String, base_dir: String },
    #[error("package hash mismatch for `{path}`: expected {expected}, found {actual}")]
    PackageHashMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("package hash error: {message}")]
    PackageHashError { message: String },
    #[error("package I/O error at `{path}`: {message}")]
    PackageIo { path: String, message: String },
    #[error("package JSON error at `{path}`: {message}")]
    PackageJson { path: String, message: String },
}

fn validate_schema_version(surface: &'static str, found: u32) -> Result<(), FfiContractError> {
    if found != SCHEMA_VERSION {
        return Err(FfiContractError::VersionMismatch {
            surface,
            expected: SCHEMA_VERSION,
            found,
        });
    }
    Ok(())
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), FfiContractError> {
    if value.trim().is_empty() {
        return Err(FfiContractError::EmptyField { field });
    }
    Ok(())
}

fn validate_package_relative_path(path: &str) -> Result<(), FfiContractError> {
    let path_ref = Path::new(path);
    if path_ref.is_absolute()
        || path_ref.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(FfiContractError::UnsafePackagePath {
            path: path.to_string(),
        });
    }
    Ok(())
}

pub fn bundled_artifact_path(
    kind: PackageArtifactKind,
    original_path: &str,
    platform: Option<&PackagePlatform>,
) -> Result<String, FfiContractError> {
    let file_name = package_file_name(original_path)?;
    let dir = match kind {
        PackageArtifactKind::SourceFile => "_bundle/src".to_string(),
        PackageArtifactKind::CompiledModule => "_bundle/modules".to_string(),
        PackageArtifactKind::Jar | PackageArtifactKind::ClassesDir => "_bundle/java".to_string(),
        PackageArtifactKind::SharedLibrary | PackageArtifactKind::NativeLibrary => {
            format!("_bundle/native/{}", package_platform_dir(platform))
        }
        PackageArtifactKind::ShaderAsset => "_bundle/shaders".to_string(),
        PackageArtifactKind::Lockfile => "_bundle/locks".to_string(),
        PackageArtifactKind::Other => "_bundle/assets".to_string(),
    };
    Ok(format!("{dir}/{file_name}"))
}

pub fn package_platform_dir(platform: Option<&PackagePlatform>) -> String {
    let Some(platform) = platform else {
        return "any".into();
    };
    [
        platform.os.as_deref().unwrap_or("any"),
        platform.arch.as_deref().unwrap_or("any"),
        platform.abi.as_deref().unwrap_or("any"),
    ]
    .join("-")
}

fn package_file_name(path: &str) -> Result<String, FfiContractError> {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .ok_or_else(|| FfiContractError::MissingArtifactFileName { path: path.into() })
}

fn sha256_file_hex(path: impl AsRef<Path>) -> Result<String, FfiContractError> {
    let path = path.as_ref();
    let mut file = File::open(path).map_err(|err| FfiContractError::PackageHashError {
        message: format!("failed to open {}: {err}", path.display()),
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|err| FfiContractError::PackageHashError {
                message: format!("failed to read {}: {err}", path.display()),
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn sha256_bytes_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn validate_ports(
    node_id: &str,
    direction: &'static str,
    ports: &[WirePort],
) -> Result<(), FfiContractError> {
    let mut names = std::collections::BTreeSet::new();
    for port in ports {
        validate_non_empty("port.name", &port.name)?;
        if !names.insert(port.name.as_str()) {
            return Err(FfiContractError::DuplicatePort {
                node_id: node_id.to_string(),
                direction,
                port: port.name.clone(),
            });
        }
    }
    Ok(())
}

fn require_some(
    node_id: &str,
    field: &'static str,
    value: &Option<String>,
) -> Result<(), FfiContractError> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(()),
        _ => Err(FfiContractError::MissingBackendField {
            node_id: node_id.to_string(),
            field,
        }),
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageArtifact {
    pub path: String,
    pub kind: PackageArtifactKind,
    #[serde(default)]
    pub backend: Option<BackendKind>,
    #[serde(default)]
    pub platform: Option<PackagePlatform>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageArtifactKind {
    SourceFile,
    CompiledModule,
    Jar,
    ClassesDir,
    SharedLibrary,
    NativeLibrary,
    ShaderAsset,
    Lockfile,
    Other,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackagePlatform {
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub arch: Option<String>,
    #[serde(default)]
    pub abi: Option<String>,
}

/// Typed transport contract for host/backend invocation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum WireValue {
    Unit,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(BytePayload),
    Image(ImagePayload),
    Handle(WirePayloadHandle),
    List(Vec<WireValue>),
    Record(BTreeMap<String, WireValue>),
    Enum(WireEnumValue),
}

impl WireValue {
    pub fn validate_contract(&self) -> Result<(), InvokeContractError> {
        match self {
            WireValue::Unit
            | WireValue::Bool(_)
            | WireValue::Int(_)
            | WireValue::Float(_)
            | WireValue::String(_)
            | WireValue::Bytes(_) => Ok(()),
            WireValue::Image(image) => {
                if image.width == 0 {
                    return Err(InvokeContractError::InvalidImage { field: "width" });
                }
                if image.height == 0 {
                    return Err(InvokeContractError::InvalidImage { field: "height" });
                }
                if image.channels == 0 {
                    return Err(InvokeContractError::InvalidImage { field: "channels" });
                }
                Ok(())
            }
            WireValue::Handle(handle) => {
                if handle.id.trim().is_empty() {
                    return Err(InvokeContractError::EmptyField { field: "handle.id" });
                }
                Ok(())
            }
            WireValue::List(items) => {
                for item in items {
                    item.validate_contract()?;
                }
                Ok(())
            }
            WireValue::Record(fields) => {
                for (name, value) in fields {
                    if name.trim().is_empty() {
                        return Err(InvokeContractError::EmptyField {
                            field: "record.field",
                        });
                    }
                    value.validate_contract()?;
                }
                Ok(())
            }
            WireValue::Enum(value) => {
                if value.name.trim().is_empty() {
                    return Err(InvokeContractError::EmptyField { field: "enum.name" });
                }
                if let Some(value) = &value.value {
                    value.validate_contract()?;
                }
                Ok(())
            }
        }
    }

    pub fn into_value(self) -> Result<Value, WireValueConversionError> {
        self.try_into()
    }

    pub fn from_value(value: Value) -> Result<Self, WireValueConversionError> {
        value.try_into()
    }

    pub fn into_payload(
        self,
        type_key: impl Into<TypeKey>,
    ) -> Result<Payload, WireValueConversionError> {
        let type_key = type_key.into();
        match self {
            WireValue::Bytes(payload) => Ok(Payload::bytes_with_type_key(
                type_key,
                std::sync::Arc::from(payload.data),
            )),
            value => Ok(Payload::owned(type_key, value.into_value()?)),
        }
    }

    pub fn from_payload(payload: &Payload) -> Result<Self, WireValueConversionError> {
        if let Some(value) = payload.get_ref::<Value>() {
            return WireValue::from_value(value.clone());
        }
        if let Some(bytes) = payload
            .value_any()
            .and_then(|value| value.downcast_ref::<std::sync::Arc<[u8]>>())
        {
            return Ok(WireValue::Bytes(BytePayload {
                data: bytes.as_ref().to_vec(),
                encoding: ByteEncoding::Raw,
            }));
        }
        Err(WireValueConversionError::UnsupportedPayload {
            type_key: payload.type_key().to_string(),
            rust_type_name: payload.storage_rust_type_name(),
        })
    }
}

impl TryFrom<WireValue> for Value {
    type Error = WireValueConversionError;

    fn try_from(value: WireValue) -> Result<Self, Self::Error> {
        Ok(match value {
            WireValue::Unit => Value::Unit,
            WireValue::Bool(value) => Value::Bool(value),
            WireValue::Int(value) => Value::Int(value),
            WireValue::Float(value) => Value::Float(value),
            WireValue::String(value) => Value::String(Cow::Owned(value)),
            WireValue::Bytes(payload) => Value::Bytes(Cow::Owned(payload.data)),
            WireValue::Image(_) => {
                return Err(WireValueConversionError::UnsupportedWireValue {
                    kind: "image",
                    target: "daedalus_data::model::Value",
                });
            }
            WireValue::Handle(_) => {
                return Err(WireValueConversionError::UnsupportedWireValue {
                    kind: "handle",
                    target: "daedalus_data::model::Value",
                });
            }
            WireValue::List(items) => Value::List(
                items
                    .into_iter()
                    .map(Value::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            WireValue::Record(fields) => Value::Struct(
                fields
                    .into_iter()
                    .map(|(name, value)| {
                        Ok(StructFieldValue {
                            name,
                            value: value.try_into()?,
                        })
                    })
                    .collect::<Result<Vec<_>, WireValueConversionError>>()?,
            ),
            WireValue::Enum(value) => Value::Enum(EnumValue {
                name: value.name,
                value: value
                    .value
                    .map(|value| Value::try_from(*value).map(Box::new))
                    .transpose()?,
            }),
        })
    }
}

impl TryFrom<Value> for WireValue {
    type Error = WireValueConversionError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Ok(match value {
            Value::Unit => WireValue::Unit,
            Value::Bool(value) => WireValue::Bool(value),
            Value::Int(value) => WireValue::Int(value),
            Value::Float(value) => WireValue::Float(value),
            Value::String(value) => WireValue::String(value.into_owned()),
            Value::Bytes(value) => WireValue::Bytes(BytePayload {
                data: value.into_owned(),
                encoding: ByteEncoding::Raw,
            }),
            Value::List(items) | Value::Tuple(items) => WireValue::List(
                items
                    .into_iter()
                    .map(WireValue::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Value::Map(entries) => {
                let mut record = BTreeMap::new();
                for (key, value) in entries {
                    let Value::String(key) = key else {
                        return Err(WireValueConversionError::UnsupportedValue {
                            kind: "map with non-string key",
                            target: "WireValue::Record",
                        });
                    };
                    record.insert(key.into_owned(), value.try_into()?);
                }
                WireValue::Record(record)
            }
            Value::Struct(fields) => WireValue::Record(
                fields
                    .into_iter()
                    .map(|field| Ok((field.name, field.value.try_into()?)))
                    .collect::<Result<BTreeMap<_, _>, WireValueConversionError>>()?,
            ),
            Value::Enum(value) => WireValue::Enum(WireEnumValue {
                name: value.name,
                value: value
                    .value
                    .map(|value| WireValue::try_from(*value).map(Box::new))
                    .transpose()?,
            }),
        })
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WireValueConversionError {
    #[error("unsupported wire value kind `{kind}` for target `{target}`")]
    UnsupportedWireValue {
        kind: &'static str,
        target: &'static str,
    },
    #[error("unsupported runtime value kind `{kind}` for target `{target}`")]
    UnsupportedValue {
        kind: &'static str,
        target: &'static str,
    },
    #[error(
        "unsupported payload `{type_key}` with storage `{rust_type_name:?}` for target `WireValue`"
    )]
    UnsupportedPayload {
        type_key: String,
        rust_type_name: Option<&'static str>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BytePayload {
    pub data: Vec<u8>,
    #[serde(default)]
    pub encoding: ByteEncoding,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ByteEncoding {
    #[default]
    Raw,
    Base64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImagePayload {
    pub data: Vec<u8>,
    pub width: u32,
    pub height: u32,
    pub channels: u8,
    #[serde(default)]
    pub dtype: ScalarDType,
    #[serde(default)]
    pub layout: ImageLayout,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WirePayloadHandle {
    pub id: String,
    pub type_key: TypeKey,
    #[serde(default)]
    pub access: AccessMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<Residency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[cfg(feature = "image-payload")]
impl ImagePayload {
    pub fn validate(&self) -> Result<(), ImagePayloadValidationError> {
        if self.width == 0 {
            return Err(ImagePayloadValidationError::InvalidDimension {
                field: "width",
                value: self.width,
            });
        }
        if self.height == 0 {
            return Err(ImagePayloadValidationError::InvalidDimension {
                field: "height",
                value: self.height,
            });
        }
        if !(1..=4).contains(&self.channels) {
            return Err(ImagePayloadValidationError::InvalidChannels {
                channels: self.channels,
            });
        }
        let expected = self.expected_data_len()?;
        if self.data.len() != expected {
            return Err(ImagePayloadValidationError::InvalidDataLength {
                expected,
                actual: self.data.len(),
            });
        }
        Ok(())
    }

    pub fn expected_data_len(&self) -> Result<usize, ImagePayloadValidationError> {
        let width =
            usize::try_from(self.width).map_err(|_| ImagePayloadValidationError::Overflow)?;
        let height =
            usize::try_from(self.height).map_err(|_| ImagePayloadValidationError::Overflow)?;
        width
            .checked_mul(height)
            .and_then(|pixels| pixels.checked_mul(usize::from(self.channels)))
            .and_then(|samples| samples.checked_mul(self.dtype.bytes_per_sample()))
            .ok_or(ImagePayloadValidationError::Overflow)
    }
}

#[cfg(feature = "image-payload")]
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ImagePayloadValidationError {
    #[error("image {field} must be greater than zero, found {value}")]
    InvalidDimension { field: &'static str, value: u32 },
    #[error("image channels must be between 1 and 4, found {channels}")]
    InvalidChannels { channels: u8 },
    #[error("image data length mismatch: expected {expected} bytes, found {actual}")]
    InvalidDataLength { expected: usize, actual: usize },
    #[error("image byte length overflow")]
    Overflow,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarDType {
    #[default]
    U8,
    U16,
    F32,
}

#[cfg(feature = "image-payload")]
impl ScalarDType {
    pub fn bytes_per_sample(self) -> usize {
        match self {
            ScalarDType::U8 => 1,
            ScalarDType::U16 => 2,
            ScalarDType::F32 => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageLayout {
    #[default]
    Hwc,
    Chw,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WireEnumValue {
    pub name: String,
    #[serde(default)]
    pub value: Option<Box<WireValue>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InvokeRequest {
    #[serde(default = "default_worker_protocol_version")]
    pub protocol_version: u32,
    pub node_id: String,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub args: BTreeMap<String, WireValue>,
    #[serde(default)]
    pub state: Option<WireValue>,
    #[serde(default)]
    pub context: BTreeMap<String, serde_json::Value>,
}

impl InvokeRequest {
    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_version(self.protocol_version)
    }

    pub fn validate_contract(&self) -> Result<(), InvokeContractError> {
        self.validate_protocol()?;
        if self.node_id.trim().is_empty() {
            return Err(InvokeContractError::EmptyField { field: "node_id" });
        }
        if let Some(correlation_id) = &self.correlation_id
            && correlation_id.trim().is_empty()
        {
            return Err(InvokeContractError::EmptyField {
                field: "correlation_id",
            });
        }
        for (name, value) in &self.args {
            if name.trim().is_empty() {
                return Err(InvokeContractError::EmptyField {
                    field: "request.arg",
                });
            }
            value.validate_contract()?;
        }
        if let Some(state) = &self.state {
            state.validate_contract()?;
        }
        Ok(())
    }

    pub fn validate_against_node(&self, node: &NodeSchema) -> Result<(), InvokeContractError> {
        self.validate_contract()?;
        if self.node_id != node.id {
            return Err(InvokeContractError::NodeMismatch {
                expected: node.id.clone(),
                found: self.node_id.clone(),
            });
        }
        validate_named_wire_values(
            "argument",
            &node.inputs,
            &self.args,
            InvokeContractError::MissingArgument,
            InvokeContractError::UnexpectedArgument,
        )
    }
}

fn default_worker_protocol_version() -> u32 {
    WORKER_PROTOCOL_VERSION
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InvokeResponse {
    #[serde(default = "default_worker_protocol_version")]
    pub protocol_version: u32,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub outputs: BTreeMap<String, WireValue>,
    #[serde(default)]
    pub state: Option<WireValue>,
    #[serde(default)]
    pub events: Vec<InvokeEvent>,
}

impl InvokeResponse {
    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_version(self.protocol_version)
    }

    pub fn validate_contract(&self) -> Result<(), InvokeContractError> {
        self.validate_protocol()?;
        if let Some(correlation_id) = &self.correlation_id
            && correlation_id.trim().is_empty()
        {
            return Err(InvokeContractError::EmptyField {
                field: "correlation_id",
            });
        }
        for (name, value) in &self.outputs {
            if name.trim().is_empty() {
                return Err(InvokeContractError::EmptyField {
                    field: "response.output",
                });
            }
            value.validate_contract()?;
        }
        if let Some(state) = &self.state {
            state.validate_contract()?;
        }
        for event in &self.events {
            event.validate_contract()?;
        }
        Ok(())
    }

    pub fn validate_against_node(&self, node: &NodeSchema) -> Result<(), InvokeContractError> {
        self.validate_contract()?;
        validate_named_wire_values(
            "output",
            &node.outputs,
            &self.outputs,
            InvokeContractError::MissingOutput,
            InvokeContractError::UnexpectedOutput,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerHello {
    #[serde(default = "default_worker_protocol_version")]
    pub protocol_version: u32,
    #[serde(default)]
    pub min_protocol_version: u32,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub backend: Option<BackendKind>,
    #[serde(default)]
    pub supported_nodes: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl WorkerHello {
    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_range(self.min_protocol_version, self.protocol_version)
    }

    pub fn negotiated_protocol_version(&self) -> Result<u32, WorkerProtocolError> {
        self.validate_protocol()?;
        Ok(WORKER_PROTOCOL_VERSION.min(self.protocol_version))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerProtocolAck {
    pub protocol_version: u32,
    #[serde(default)]
    pub worker_id: Option<String>,
}

impl WorkerProtocolAck {
    pub fn from_hello(hello: &WorkerHello) -> Result<Self, WorkerProtocolError> {
        Ok(Self {
            protocol_version: hello.negotiated_protocol_version()?,
            worker_id: hello.worker_id.clone(),
        })
    }

    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_version(self.protocol_version)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkerMessage {
    #[serde(default = "default_worker_protocol_version")]
    pub protocol_version: u32,
    #[serde(default)]
    pub correlation_id: Option<String>,
    pub payload: WorkerMessagePayload,
}

impl WorkerMessage {
    pub fn new(payload: WorkerMessagePayload, correlation_id: Option<String>) -> Self {
        Self {
            protocol_version: WORKER_PROTOCOL_VERSION,
            correlation_id,
            payload,
        }
    }

    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_version(self.protocol_version)?;
        match &self.payload {
            WorkerMessagePayload::Hello(hello) => hello.validate_protocol(),
            WorkerMessagePayload::Ack(ack) => ack.validate_protocol(),
            WorkerMessagePayload::Invoke(request) => request.validate_protocol(),
            WorkerMessagePayload::Response(response) => response.validate_protocol(),
            WorkerMessagePayload::Event(_) | WorkerMessagePayload::Error(_) => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum WorkerMessagePayload {
    Hello(WorkerHello),
    Ack(WorkerProtocolAck),
    Invoke(InvokeRequest),
    Response(InvokeResponse),
    Event(InvokeEvent),
    Error(WorkerError),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerError {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WorkerProtocolError {
    #[error("worker protocol version {found} is unsupported; supported range is {min}..={max}")]
    UnsupportedVersion { found: u32, min: u32, max: u32 },
    #[error("worker protocol range is invalid: min {min} is greater than max {max}")]
    InvalidRange { min: u32, max: u32 },
}

fn validate_worker_protocol_version(version: u32) -> Result<(), WorkerProtocolError> {
    if version == WORKER_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(WorkerProtocolError::UnsupportedVersion {
            found: version,
            min: WORKER_PROTOCOL_VERSION,
            max: WORKER_PROTOCOL_VERSION,
        })
    }
}

fn validate_worker_protocol_range(min: u32, max: u32) -> Result<(), WorkerProtocolError> {
    if min > max {
        return Err(WorkerProtocolError::InvalidRange { min, max });
    }
    if min <= WORKER_PROTOCOL_VERSION && WORKER_PROTOCOL_VERSION <= max {
        Ok(())
    } else {
        Err(WorkerProtocolError::UnsupportedVersion {
            found: max,
            min: WORKER_PROTOCOL_VERSION,
            max: WORKER_PROTOCOL_VERSION,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvokeEvent {
    pub level: InvokeEventLevel,
    pub message: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl InvokeEvent {
    pub fn validate_contract(&self) -> Result<(), InvokeContractError> {
        if self.message.trim().is_empty() {
            return Err(InvokeContractError::EmptyField {
                field: "event.message",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum InvokeContractError {
    #[error("transport protocol invalid: {0}")]
    Protocol(#[from] WorkerProtocolError),
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("request node mismatch: expected {expected}, found {found}")]
    NodeMismatch { expected: String, found: String },
    #[error("missing argument `{0}`")]
    MissingArgument(String),
    #[error("unexpected argument `{0}`")]
    UnexpectedArgument(String),
    #[error("missing output `{0}`")]
    MissingOutput(String),
    #[error("unexpected output `{0}`")]
    UnexpectedOutput(String),
    #[error("invalid image payload field `{field}`")]
    InvalidImage { field: &'static str },
}

fn validate_named_wire_values(
    direction: &'static str,
    ports: &[WirePort],
    values: &BTreeMap<String, WireValue>,
    missing: fn(String) -> InvokeContractError,
    unexpected: fn(String) -> InvokeContractError,
) -> Result<(), InvokeContractError> {
    let expected = ports
        .iter()
        .map(|port| (port.name.as_str(), port.optional))
        .collect::<BTreeMap<_, _>>();
    for (name, optional) in &expected {
        if !optional && !values.contains_key(*name) {
            return Err(missing((*name).to_string()));
        }
    }
    for name in values.keys() {
        if !expected.contains_key(name.as_str()) {
            return Err(unexpected(name.clone()));
        }
        if name.trim().is_empty() {
            return Err(InvokeContractError::EmptyField { field: direction });
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InvokeEventLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{EnumValue, StructFieldValue, TypeExpr, Value};

    #[test]
    fn plugin_schema_stays_separate_from_backend_config() {
        let schema = PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: "demo".into(),
                version: Some("0.1.0".into()),
                description: Some("demo plugin".into()),
                metadata: BTreeMap::new(),
            },
            dependencies: Vec::new(),
            required_host_capabilities: Vec::new(),
            feature_flags: Vec::new(),
            boundary_contracts: Vec::new(),
            nodes: vec![NodeSchema {
                id: "demo.blur".into(),
                backend: BackendKind::Python,
                entrypoint: "blur".into(),
                label: Some("Blur".into()),
                stateful: true,
                feature_flags: Vec::new(),
                inputs: vec![WirePort {
                    name: "image".into(),
                    ty: TypeExpr::opaque("image"),
                    type_key: None,
                    optional: false,
                    access: AccessMode::Read,
                    residency: None,
                    layout: None,
                    source: None,
                    const_value: None,
                }],
                outputs: vec![WirePort {
                    name: "image".into(),
                    ty: TypeExpr::opaque("image"),
                    type_key: None,
                    optional: false,
                    access: AccessMode::Read,
                    residency: None,
                    layout: None,
                    source: None,
                    const_value: None,
                }],
                metadata: BTreeMap::new(),
            }],
        };

        let backend = BackendConfig {
            backend: BackendKind::Python,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some("plugin".into()),
            entry_class: None,
            entry_symbol: Some("blur".into()),
            executable: Some("python".into()),
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: Some("examples/plugins/demo".into()),
            env: BTreeMap::from([(String::from("PYTHONUNBUFFERED"), String::from("1"))]),
            options: BTreeMap::new(),
        };

        let schema_json = serde_json::to_value(&schema).expect("serialize schema");
        let backend_json = serde_json::to_value(&backend).expect("serialize backend config");

        assert!(schema_json.get("plugin").is_some());
        assert!(schema_json.get("nodes").is_some());
        assert!(schema_json.get("runtime_model").is_none());
        assert!(backend_json.get("runtime_model").is_some());
        assert!(backend_json.get("plugin").is_none());
    }

    #[test]
    fn plugin_schema_validation_rejects_duplicate_nodes_and_ports() {
        let duplicated_port = WirePort {
            name: "value".into(),
            ty: TypeExpr::Scalar(daedalus_data::model::ValueType::Int),
            type_key: None,
            optional: false,
            access: AccessMode::Read,
            residency: None,
            layout: None,
            source: None,
            const_value: None,
        };
        let node = NodeSchema {
            id: "demo.node".into(),
            backend: BackendKind::Python,
            entrypoint: "run".into(),
            label: None,
            stateful: false,
            feature_flags: Vec::new(),
            inputs: vec![duplicated_port.clone(), duplicated_port],
            outputs: Vec::new(),
            metadata: BTreeMap::new(),
        };
        let schema = PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: "demo".into(),
                version: None,
                description: None,
                metadata: BTreeMap::new(),
            },
            dependencies: Vec::new(),
            required_host_capabilities: Vec::new(),
            feature_flags: Vec::new(),
            boundary_contracts: Vec::new(),
            nodes: vec![node],
        };

        assert!(matches!(
            schema.validate(),
            Err(FfiContractError::DuplicatePort {
                node_id,
                direction: "input",
                port
            }) if node_id == "demo.node" && port == "value"
        ));

        let duplicate_nodes = PluginSchema {
            nodes: vec![
                NodeSchema {
                    id: "demo.node".into(),
                    backend: BackendKind::Python,
                    entrypoint: "run".into(),
                    label: None,
                    stateful: false,
                    feature_flags: Vec::new(),
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                    metadata: BTreeMap::new(),
                },
                NodeSchema {
                    id: "demo.node".into(),
                    backend: BackendKind::Node,
                    entrypoint: "run".into(),
                    label: None,
                    stateful: false,
                    feature_flags: Vec::new(),
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                    metadata: BTreeMap::new(),
                },
            ],
            ..schema
        };

        assert!(matches!(
            duplicate_nodes.validate(),
            Err(FfiContractError::DuplicateNode { node_id }) if node_id == "demo.node"
        ));
    }

    #[test]
    fn backend_validation_enforces_language_entrypoints() {
        let backend = BackendConfig {
            backend: BackendKind::Java,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: None,
            entry_class: Some("demo.Nodes".into()),
            entry_symbol: Some("add".into()),
            executable: Some("java".into()),
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        };

        assert!(matches!(
            backend.validate_for_node("demo:add"),
            Err(FfiContractError::MissingBackendField {
                node_id,
                field: "classpath"
            }) if node_id == "demo:add"
        ));
    }

    #[test]
    fn package_validation_matches_schema_nodes_to_backends() {
        let schema = PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: "demo".into(),
                version: None,
                description: None,
                metadata: BTreeMap::new(),
            },
            dependencies: Vec::new(),
            required_host_capabilities: Vec::new(),
            feature_flags: Vec::new(),
            boundary_contracts: Vec::new(),
            nodes: vec![NodeSchema {
                id: "demo.add".into(),
                backend: BackendKind::Python,
                entrypoint: "add".into(),
                label: None,
                stateful: false,
                feature_flags: Vec::new(),
                inputs: Vec::new(),
                outputs: Vec::new(),
                metadata: BTreeMap::new(),
            }],
        };
        let package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: Some(schema),
            backends: BTreeMap::new(),
            artifacts: Vec::new(),
            lockfile: None,
            manifest_hash: None,
            signature: None,
            metadata: BTreeMap::new(),
        };

        assert!(matches!(
            package.validate(),
            Err(FfiContractError::MissingBackendConfig { node_id }) if node_id == "demo.add"
        ));
    }

    #[test]
    fn plugin_package_records_physical_artifacts_separately() {
        let package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: None,
            backends: BTreeMap::new(),
            artifacts: vec![PackageArtifact {
                path: "_bundle/java/demo.jar".into(),
                kind: PackageArtifactKind::Jar,
                backend: Some(BackendKind::Java),
                platform: None,
                sha256: Some("abc123".into()),
                metadata: BTreeMap::new(),
            }],
            lockfile: Some("plugin.lock".into()),
            manifest_hash: Some("hash".into()),
            signature: None,
            metadata: BTreeMap::new(),
        };

        let json = serde_json::to_value(&package).expect("serialize package");
        assert!(json.get("artifacts").is_some());
        assert!(json.get("schema").is_some());
        assert!(json.get("runtime_model").is_none());
    }

    #[test]
    fn package_validation_accepts_existing_artifact_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let artifact_dir = dir.path().join("_bundle/java");
        std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");
        std::fs::write(artifact_dir.join("demo.jar"), b"jar").expect("write artifact");

        let package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: None,
            backends: BTreeMap::new(),
            artifacts: vec![PackageArtifact {
                path: "_bundle/java/demo.jar".into(),
                kind: PackageArtifactKind::Jar,
                backend: Some(BackendKind::Java),
                platform: None,
                sha256: Some("abc123".into()),
                metadata: BTreeMap::new(),
            }],
            lockfile: None,
            manifest_hash: Some("manifest-hash".into()),
            signature: None,
            metadata: BTreeMap::new(),
        };

        package
            .validate_artifact_files(dir.path())
            .expect("artifact exists");
    }

    #[test]
    fn package_validation_rejects_missing_artifact_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: None,
            backends: BTreeMap::new(),
            artifacts: vec![PackageArtifact {
                path: "_bundle/java/missing.jar".into(),
                kind: PackageArtifactKind::Jar,
                backend: Some(BackendKind::Java),
                platform: None,
                sha256: None,
                metadata: BTreeMap::new(),
            }],
            lockfile: None,
            manifest_hash: None,
            signature: None,
            metadata: BTreeMap::new(),
        };

        assert!(matches!(
            package.validate_artifact_files(dir.path()),
            Err(FfiContractError::MissingPackageArtifact { path, .. })
                if path == "_bundle/java/missing.jar"
        ));
    }

    #[test]
    fn package_validation_rejects_artifact_paths_outside_package() {
        let dir = tempfile::tempdir().expect("tempdir");
        let package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: None,
            backends: BTreeMap::new(),
            artifacts: vec![PackageArtifact {
                path: "../outside.jar".into(),
                kind: PackageArtifactKind::Jar,
                backend: Some(BackendKind::Java),
                platform: None,
                sha256: None,
                metadata: BTreeMap::new(),
            }],
            lockfile: None,
            manifest_hash: None,
            signature: None,
            metadata: BTreeMap::new(),
        };

        assert!(matches!(
            package.validate_artifact_files(dir.path()),
            Err(FfiContractError::UnsafePackagePath { path }) if path == "../outside.jar"
        ));
    }

    #[test]
    fn package_artifact_bundle_paths_are_deterministic_by_kind() {
        let platform = PackagePlatform {
            os: Some("linux".into()),
            arch: Some("x86_64".into()),
            abi: Some("gnu".into()),
        };

        assert_eq!(
            bundled_artifact_path(PackageArtifactKind::SourceFile, "src/main.py", None)
                .expect("source path"),
            "_bundle/src/main.py"
        );
        assert_eq!(
            bundled_artifact_path(PackageArtifactKind::Jar, "build/libs/demo.jar", None)
                .expect("jar path"),
            "_bundle/java/demo.jar"
        );
        assert_eq!(
            bundled_artifact_path(
                PackageArtifactKind::NativeLibrary,
                "target/libopencv_java.so",
                Some(&platform),
            )
            .expect("native path"),
            "_bundle/native/linux-x86_64-gnu/libopencv_java.so"
        );
        assert_eq!(
            bundled_artifact_path(
                PackageArtifactKind::ShaderAsset,
                "shaders/invert.wgsl",
                None
            )
            .expect("shader path"),
            "_bundle/shaders/invert.wgsl"
        );
    }

    #[test]
    fn package_rewrites_artifact_paths_for_bundle_layout() {
        let mut package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: None,
            backends: BTreeMap::new(),
            artifacts: vec![
                PackageArtifact {
                    path: "rt.py".into(),
                    kind: PackageArtifactKind::SourceFile,
                    backend: Some(BackendKind::Python),
                    platform: None,
                    sha256: None,
                    metadata: BTreeMap::new(),
                },
                PackageArtifact {
                    path: "build/classes/java/main".into(),
                    kind: PackageArtifactKind::ClassesDir,
                    backend: Some(BackendKind::Java),
                    platform: None,
                    sha256: None,
                    metadata: BTreeMap::new(),
                },
            ],
            lockfile: None,
            manifest_hash: None,
            signature: None,
            metadata: BTreeMap::new(),
        };

        package
            .rewrite_artifact_paths_for_bundle()
            .expect("rewrite paths");

        assert_eq!(package.artifacts[0].path, "_bundle/src/rt.py");
        assert_eq!(package.artifacts[1].path, "_bundle/java/main");
    }

    #[test]
    fn package_integrity_stamps_and_verifies_artifact_hashes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let artifact_dir = dir.path().join("_bundle/assets");
        std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");
        std::fs::write(artifact_dir.join("data.bin"), b"payload").expect("write artifact");

        let mut package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: None,
            backends: BTreeMap::new(),
            artifacts: vec![PackageArtifact {
                path: "_bundle/assets/data.bin".into(),
                kind: PackageArtifactKind::Other,
                backend: None,
                platform: None,
                sha256: None,
                metadata: BTreeMap::new(),
            }],
            lockfile: None,
            manifest_hash: None,
            signature: None,
            metadata: BTreeMap::new(),
        };

        package
            .stamp_integrity(dir.path())
            .expect("stamp integrity");

        assert_eq!(package.artifacts[0].sha256.as_ref().unwrap().len(), 64);
        assert_eq!(package.manifest_hash.as_ref().unwrap().len(), 64);
        package
            .verify_integrity(dir.path())
            .expect("integrity verifies");
    }

    #[test]
    fn package_integrity_rejects_tampered_artifacts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let artifact_dir = dir.path().join("_bundle/assets");
        std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");
        std::fs::write(artifact_dir.join("data.bin"), b"payload").expect("write artifact");

        let mut package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: None,
            backends: BTreeMap::new(),
            artifacts: vec![PackageArtifact {
                path: "_bundle/assets/data.bin".into(),
                kind: PackageArtifactKind::Other,
                backend: None,
                platform: None,
                sha256: None,
                metadata: BTreeMap::new(),
            }],
            lockfile: None,
            manifest_hash: None,
            signature: None,
            metadata: BTreeMap::new(),
        };
        package
            .stamp_integrity(dir.path())
            .expect("stamp integrity");

        std::fs::write(artifact_dir.join("data.bin"), b"tampered").expect("tamper artifact");

        assert!(matches!(
            package.verify_integrity(dir.path()),
            Err(FfiContractError::PackageHashMismatch { path, .. })
                if path == "_bundle/assets/data.bin"
        ));
    }

    #[test]
    fn package_descriptor_loads_from_unpacked_root_without_repo_paths() {
        let root = tempfile::tempdir().expect("tempdir");
        let artifact_dir = root.path().join("_bundle/java");
        std::fs::create_dir_all(&artifact_dir).expect("create bundle dir");
        std::fs::write(artifact_dir.join("demo.jar"), b"jar").expect("write jar");

        let schema = PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: "demo.java".into(),
                version: Some("0.1.0".into()),
                description: None,
                metadata: BTreeMap::new(),
            },
            dependencies: Vec::new(),
            required_host_capabilities: Vec::new(),
            feature_flags: Vec::new(),
            boundary_contracts: Vec::new(),
            nodes: vec![NodeSchema {
                id: "demo.add".into(),
                backend: BackendKind::Java,
                entrypoint: "add".into(),
                label: None,
                stateful: false,
                feature_flags: Vec::new(),
                inputs: Vec::new(),
                outputs: Vec::new(),
                metadata: BTreeMap::new(),
            }],
        };
        let mut package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: Some(schema),
            backends: BTreeMap::from([(
                "demo.add".into(),
                BackendConfig {
                    backend: BackendKind::Java,
                    runtime_model: BackendRuntimeModel::PersistentWorker,
                    entry_module: None,
                    entry_class: Some("demo.Nodes".into()),
                    entry_symbol: Some("add".into()),
                    executable: Some("java".into()),
                    args: Vec::new(),
                    classpath: vec!["_bundle/java/demo.jar".into()],
                    native_library_paths: Vec::new(),
                    working_dir: None,
                    env: BTreeMap::new(),
                    options: BTreeMap::new(),
                },
            )]),
            artifacts: vec![PackageArtifact {
                path: "_bundle/java/demo.jar".into(),
                kind: PackageArtifactKind::Jar,
                backend: Some(BackendKind::Java),
                platform: None,
                sha256: None,
                metadata: BTreeMap::new(),
            }],
            lockfile: None,
            manifest_hash: None,
            signature: None,
            metadata: BTreeMap::new(),
        };
        package.stamp_integrity(root.path()).expect("stamp package");
        let descriptor_path = root.path().join("plugin.json");
        package
            .write_descriptor(&descriptor_path)
            .expect("write descriptor");

        let loaded = PluginPackage::read_descriptor_and_verify(&descriptor_path, root.path())
            .expect("load package");

        assert_eq!(loaded.schema.as_ref().unwrap().plugin.name, "demo.java");
        assert_eq!(loaded.artifacts[0].path, "_bundle/java/demo.jar");
        assert_eq!(
            loaded.backends["demo.add"].classpath,
            vec![String::from("_bundle/java/demo.jar")]
        );
    }

    #[test]
    fn package_lockfile_generation_is_deterministic_and_language_aware() {
        let package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: Some(PluginSchema {
                schema_version: SCHEMA_VERSION,
                plugin: PluginSchemaInfo {
                    name: "demo.multi".into(),
                    version: Some("1.0.0".into()),
                    description: None,
                    metadata: BTreeMap::new(),
                },
                dependencies: Vec::new(),
                required_host_capabilities: Vec::new(),
                feature_flags: Vec::new(),
                boundary_contracts: Vec::new(),
                nodes: Vec::new(),
            }),
            backends: BTreeMap::from([
                (
                    "cpp.node".into(),
                    BackendConfig {
                        backend: BackendKind::CCpp,
                        runtime_model: BackendRuntimeModel::InProcessAbi,
                        entry_module: Some("_bundle/native/linux-x86_64-gnu/libdemo.so".into()),
                        entry_class: None,
                        entry_symbol: Some("run".into()),
                        executable: None,
                        args: Vec::new(),
                        classpath: Vec::new(),
                        native_library_paths: Vec::new(),
                        working_dir: None,
                        env: BTreeMap::new(),
                        options: BTreeMap::new(),
                    },
                ),
                (
                    "java.node".into(),
                    BackendConfig {
                        backend: BackendKind::Java,
                        runtime_model: BackendRuntimeModel::PersistentWorker,
                        entry_module: None,
                        entry_class: Some("demo.Nodes".into()),
                        entry_symbol: Some("add".into()),
                        executable: Some("java".into()),
                        args: Vec::new(),
                        classpath: vec!["_bundle/java/demo.jar".into()],
                        native_library_paths: vec![
                            "_bundle/native/linux-x86_64-gnu/libopencv.so".into(),
                        ],
                        working_dir: None,
                        env: BTreeMap::new(),
                        options: BTreeMap::from([(
                            "maven_coordinates".into(),
                            serde_json::json!(["org.demo:demo:1.0.0"]),
                        )]),
                    },
                ),
                (
                    "node.node".into(),
                    BackendConfig {
                        backend: BackendKind::Node,
                        runtime_model: BackendRuntimeModel::PersistentWorker,
                        entry_module: Some("_bundle/src/index.mjs".into()),
                        entry_class: None,
                        entry_symbol: Some("run".into()),
                        executable: Some("node".into()),
                        args: Vec::new(),
                        classpath: Vec::new(),
                        native_library_paths: Vec::new(),
                        working_dir: None,
                        env: BTreeMap::new(),
                        options: BTreeMap::from([(
                            "package".into(),
                            serde_json::json!({"name":"demo-node","version":"1.0.0"}),
                        )]),
                    },
                ),
                (
                    "python.node".into(),
                    BackendConfig {
                        backend: BackendKind::Python,
                        runtime_model: BackendRuntimeModel::PersistentWorker,
                        entry_module: Some("_bundle/src/rt.py".into()),
                        entry_class: None,
                        entry_symbol: Some("run".into()),
                        executable: Some("python".into()),
                        args: Vec::new(),
                        classpath: Vec::new(),
                        native_library_paths: Vec::new(),
                        working_dir: None,
                        env: BTreeMap::new(),
                        options: BTreeMap::from([(
                            "requirements".into(),
                            serde_json::json!(["numpy==2.0.0"]),
                        )]),
                    },
                ),
            ]),
            artifacts: vec![
                PackageArtifact {
                    path: "_bundle/src/rt.py".into(),
                    kind: PackageArtifactKind::SourceFile,
                    backend: Some(BackendKind::Python),
                    platform: None,
                    sha256: Some("b".repeat(64)),
                    metadata: BTreeMap::new(),
                },
                PackageArtifact {
                    path: "_bundle/java/demo.jar".into(),
                    kind: PackageArtifactKind::Jar,
                    backend: Some(BackendKind::Java),
                    platform: None,
                    sha256: Some("a".repeat(64)),
                    metadata: BTreeMap::new(),
                },
            ],
            lockfile: Some("plugin.lock.json".into()),
            manifest_hash: Some("c".repeat(64)),
            signature: None,
            metadata: BTreeMap::new(),
        };

        let lock = package.generate_lockfile();

        assert_eq!(lock.plugin_name.as_deref(), Some("demo.multi"));
        assert_eq!(lock.plugin_version.as_deref(), Some("1.0.0"));
        assert_eq!(lock.backends["python.node"].backend, BackendKind::Python);
        assert_eq!(lock.backends["node.node"].backend, BackendKind::Node);
        assert_eq!(
            lock.backends["java.node"].classpath,
            vec!["_bundle/java/demo.jar"]
        );
        assert_eq!(lock.backends["cpp.node"].backend, BackendKind::CCpp);
        assert_eq!(lock.artifacts[0].path, "_bundle/java/demo.jar");
        assert_eq!(lock.artifacts[1].path, "_bundle/src/rt.py");
    }

    #[test]
    fn package_lockfile_round_trips_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("plugin.lock.json");
        let lock = PluginLockfile {
            schema_version: SCHEMA_VERSION,
            plugin_name: Some("demo".into()),
            plugin_version: Some("0.1.0".into()),
            manifest_hash: Some("a".repeat(64)),
            backends: BTreeMap::new(),
            artifacts: vec![PackageLockArtifact {
                path: "_bundle/src/rt.py".into(),
                kind: PackageArtifactKind::SourceFile,
                backend: Some(BackendKind::Python),
                platform: None,
                sha256: Some("b".repeat(64)),
                metadata: BTreeMap::new(),
            }],
            metadata: BTreeMap::new(),
        };

        lock.write(&path).expect("write lockfile");
        let loaded = PluginLockfile::read(&path).expect("read lockfile");

        assert_eq!(loaded, lock);
    }

    #[test]
    fn wire_value_roundtrips_typed_payloads() {
        let value = WireValue::Record(BTreeMap::from([
            (
                String::from("bytes"),
                WireValue::Bytes(BytePayload {
                    data: vec![1, 2, 3],
                    encoding: ByteEncoding::Raw,
                }),
            ),
            (
                String::from("image"),
                WireValue::Image(ImagePayload {
                    data: vec![255; 16],
                    width: 2,
                    height: 2,
                    channels: 4,
                    dtype: ScalarDType::U8,
                    layout: ImageLayout::Hwc,
                }),
            ),
            (
                String::from("handle"),
                WireValue::Handle(WirePayloadHandle {
                    id: "payload-1".into(),
                    type_key: TypeKey::new("demo:frame"),
                    access: AccessMode::Read,
                    residency: Some(Residency::Cpu),
                    layout: Some(Layout::new("hwc")),
                    capabilities: vec!["borrow_ref".into()],
                    metadata: BTreeMap::from([("bytes".into(), serde_json::json!(1024))]),
                }),
            ),
            (
                String::from("mode"),
                WireValue::Enum(WireEnumValue {
                    name: String::from("fast"),
                    value: None,
                }),
            ),
        ]));

        let json = serde_json::to_string(&value).expect("serialize wire value");
        let decoded: WireValue = serde_json::from_str(&json).expect("deserialize wire value");
        assert_eq!(decoded, value);
    }

    #[cfg(feature = "image-payload")]
    #[test]
    fn image_payload_validation_checks_layout_dtype_and_length() {
        let payload = ImagePayload {
            data: vec![0; 2 * 3 * 4 * 2],
            width: 2,
            height: 3,
            channels: 4,
            dtype: ScalarDType::U16,
            layout: ImageLayout::Hwc,
        };

        assert_eq!(payload.expected_data_len().expect("expected len"), 48);
        payload.validate().expect("valid payload");
    }

    #[cfg(feature = "image-payload")]
    #[test]
    fn image_payload_validation_rejects_bad_shapes() {
        let zero_width = ImagePayload {
            data: Vec::new(),
            width: 0,
            height: 1,
            channels: 4,
            dtype: ScalarDType::U8,
            layout: ImageLayout::Hwc,
        };
        assert!(matches!(
            zero_width.validate(),
            Err(ImagePayloadValidationError::InvalidDimension {
                field: "width",
                value: 0
            })
        ));

        let bad_channels = ImagePayload {
            data: vec![0; 5],
            width: 1,
            height: 1,
            channels: 5,
            dtype: ScalarDType::U8,
            layout: ImageLayout::Chw,
        };
        assert!(matches!(
            bad_channels.validate(),
            Err(ImagePayloadValidationError::InvalidChannels { channels: 5 })
        ));

        let bad_len = ImagePayload {
            data: vec![0; 3],
            width: 1,
            height: 1,
            channels: 1,
            dtype: ScalarDType::F32,
            layout: ImageLayout::Chw,
        };
        assert!(matches!(
            bad_len.validate(),
            Err(ImagePayloadValidationError::InvalidDataLength {
                expected: 4,
                actual: 3
            })
        ));
    }

    #[test]
    fn wire_value_converts_to_runtime_value() {
        let wire = WireValue::Record(BTreeMap::from([
            (String::from("unit"), WireValue::Unit),
            (String::from("ok"), WireValue::Bool(true)),
            (
                String::from("bytes"),
                WireValue::Bytes(BytePayload {
                    data: vec![1, 2, 3],
                    encoding: ByteEncoding::Raw,
                }),
            ),
            (
                String::from("items"),
                WireValue::List(vec![WireValue::Int(1), WireValue::String("two".into())]),
            ),
            (
                String::from("variant"),
                WireValue::Enum(WireEnumValue {
                    name: "Some".into(),
                    value: Some(Box::new(WireValue::Float(3.5))),
                }),
            ),
        ]));

        let value = wire.into_value().expect("wire converts to value");

        assert_eq!(
            value,
            Value::Struct(vec![
                StructFieldValue {
                    name: "bytes".into(),
                    value: Value::Bytes(Cow::Owned(vec![1, 2, 3])),
                },
                StructFieldValue {
                    name: "items".into(),
                    value: Value::List(vec![
                        Value::Int(1),
                        Value::String(Cow::Owned("two".into()))
                    ]),
                },
                StructFieldValue {
                    name: "ok".into(),
                    value: Value::Bool(true),
                },
                StructFieldValue {
                    name: "unit".into(),
                    value: Value::Unit,
                },
                StructFieldValue {
                    name: "variant".into(),
                    value: Value::Enum(EnumValue {
                        name: "Some".into(),
                        value: Some(Box::new(Value::Float(3.5))),
                    }),
                },
            ])
        );
    }

    #[test]
    fn runtime_value_converts_to_wire_value() {
        let value = Value::Struct(vec![
            StructFieldValue {
                name: "name".into(),
                value: Value::String(Cow::Owned("demo".into())),
            },
            StructFieldValue {
                name: "tuple".into(),
                value: Value::Tuple(vec![Value::Int(1), Value::Bool(false)]),
            },
            StructFieldValue {
                name: "mode".into(),
                value: Value::Enum(EnumValue {
                    name: "Ready".into(),
                    value: None,
                }),
            },
        ]);

        let wire = WireValue::from_value(value).expect("value converts to wire");

        assert_eq!(
            wire,
            WireValue::Record(BTreeMap::from([
                (
                    "mode".into(),
                    WireValue::Enum(WireEnumValue {
                        name: "Ready".into(),
                        value: None
                    })
                ),
                ("name".into(), WireValue::String("demo".into())),
                (
                    "tuple".into(),
                    WireValue::List(vec![WireValue::Int(1), WireValue::Bool(false)])
                ),
            ]))
        );
    }

    #[test]
    fn wire_value_conformance_covers_structured_shapes_and_optional_absence() {
        let mut record = BTreeMap::new();
        record.insert("unit".into(), WireValue::Unit);
        record.insert("optional_absent".into(), WireValue::Unit);
        record.insert(
            "list".into(),
            WireValue::List(vec![WireValue::Int(1), WireValue::Int(2)]),
        );
        record.insert(
            "tuple".into(),
            WireValue::List(vec![
                WireValue::String("left".into()),
                WireValue::Bool(true),
            ]),
        );
        record.insert(
            "map".into(),
            WireValue::Record(BTreeMap::from([
                ("a".into(), WireValue::Float(1.0)),
                ("b".into(), WireValue::Float(2.0)),
            ])),
        );
        record.insert(
            "enum_none".into(),
            WireValue::Enum(WireEnumValue {
                name: "None".into(),
                value: None,
            }),
        );
        record.insert(
            "enum_some".into(),
            WireValue::Enum(WireEnumValue {
                name: "Some".into(),
                value: Some(Box::new(WireValue::String("payload".into()))),
            }),
        );

        let wire = WireValue::Record(record);
        let value = wire.clone().into_value().expect("wire to value");
        let decoded = WireValue::from_value(value).expect("value back to wire");

        assert_eq!(decoded, wire);
    }

    #[test]
    fn value_to_wire_conformance_covers_map_tuple_enum_and_unit() {
        let value = Value::Map(vec![
            (Value::String(Cow::Owned("unit".into())), Value::Unit),
            (
                Value::String(Cow::Owned("tuple".into())),
                Value::Tuple(vec![
                    Value::String(Cow::Owned("left".into())),
                    Value::Bool(false),
                ]),
            ),
            (
                Value::String(Cow::Owned("list".into())),
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            ),
            (
                Value::String(Cow::Owned("enum".into())),
                Value::Enum(EnumValue {
                    name: "Ready".into(),
                    value: Some(Box::new(Value::Unit)),
                }),
            ),
        ]);

        let wire = WireValue::from_value(value).expect("value to wire");

        assert_eq!(
            wire,
            WireValue::Record(BTreeMap::from([
                (
                    "enum".into(),
                    WireValue::Enum(WireEnumValue {
                        name: "Ready".into(),
                        value: Some(Box::new(WireValue::Unit))
                    })
                ),
                (
                    "list".into(),
                    WireValue::List(vec![WireValue::Int(1), WireValue::Int(2)])
                ),
                (
                    "tuple".into(),
                    WireValue::List(vec![
                        WireValue::String("left".into()),
                        WireValue::Bool(false)
                    ])
                ),
                ("unit".into(), WireValue::Unit),
            ]))
        );
    }

    #[test]
    fn wire_value_conversion_reports_unsupported_shapes() {
        let image = WireValue::Image(ImagePayload {
            data: vec![255; 4],
            width: 1,
            height: 1,
            channels: 4,
            dtype: ScalarDType::U8,
            layout: ImageLayout::Hwc,
        });
        assert!(matches!(
            image.into_value(),
            Err(WireValueConversionError::UnsupportedWireValue { kind: "image", .. })
        ));

        let handle = WireValue::Handle(WirePayloadHandle {
            id: "payload-1".into(),
            type_key: TypeKey::new("demo:frame"),
            access: AccessMode::Read,
            residency: None,
            layout: None,
            capabilities: Vec::new(),
            metadata: BTreeMap::new(),
        });
        assert!(matches!(
            handle.into_value(),
            Err(WireValueConversionError::UnsupportedWireValue { kind: "handle", .. })
        ));

        let map = Value::Map(vec![(
            Value::Int(1),
            Value::String(Cow::Owned("bad".into())),
        )]);
        assert!(matches!(
            WireValue::from_value(map),
            Err(WireValueConversionError::UnsupportedValue {
                kind: "map with non-string key",
                ..
            })
        ));
    }

    #[test]
    fn wire_bytes_convert_to_raw_transport_payload() {
        let payload = WireValue::Bytes(BytePayload {
            data: vec![1, 2, 3, 4],
            encoding: ByteEncoding::Raw,
        })
        .into_payload("demo:bytes")
        .expect("wire bytes convert to payload");

        assert_eq!(payload.type_key().as_str(), "demo:bytes");
        assert_eq!(payload.bytes_estimate(), Some(4));

        let wire = WireValue::from_payload(&payload).expect("payload converts to wire bytes");
        assert_eq!(
            wire,
            WireValue::Bytes(BytePayload {
                data: vec![1, 2, 3, 4],
                encoding: ByteEncoding::Raw,
            })
        );
    }

    #[test]
    fn structured_wire_values_convert_through_transport_payloads() {
        let wire = WireValue::Record(BTreeMap::from([
            ("count".into(), WireValue::Int(2)),
            (
                "tags".into(),
                WireValue::List(vec![
                    WireValue::String("a".into()),
                    WireValue::String("b".into()),
                ]),
            ),
        ]));

        let payload = wire
            .clone()
            .into_payload("demo:record")
            .expect("wire record converts to payload");
        assert_eq!(payload.type_key().as_str(), "demo:record");
        assert!(payload.get_ref::<Value>().is_some());

        let decoded = WireValue::from_payload(&payload).expect("payload converts back to wire");
        assert_eq!(decoded, wire);
    }

    #[test]
    fn payload_to_wire_reports_unsupported_payload_storage() {
        let payload = Payload::owned("demo:u32", 42_u32);

        assert!(matches!(
            WireValue::from_payload(&payload),
            Err(WireValueConversionError::UnsupportedPayload { type_key, .. })
                if type_key == "demo:u32"
        ));
    }

    #[test]
    fn invoke_contract_preserves_structured_state_and_outputs() {
        let request = InvokeRequest {
            protocol_version: WORKER_PROTOCOL_VERSION,
            node_id: String::from("demo.normalize"),
            correlation_id: Some(String::from("req-1")),
            args: BTreeMap::from([(
                String::from("config"),
                WireValue::Record(BTreeMap::from([(
                    String::from("size"),
                    WireValue::Record(BTreeMap::from([(
                        String::from("width"),
                        WireValue::Int(512),
                    )])),
                )])),
            )]),
            state: Some(WireValue::Record(BTreeMap::from([(
                String::from("previous"),
                WireValue::Float(0.5),
            )]))),
            context: BTreeMap::from([(
                String::from("trace_id"),
                serde_json::Value::String(String::from("abc123")),
            )]),
        };

        let response = InvokeResponse {
            protocol_version: WORKER_PROTOCOL_VERSION,
            correlation_id: Some(String::from("req-1")),
            outputs: BTreeMap::from([(
                String::from("result"),
                WireValue::Record(BTreeMap::from([(
                    String::from("shape"),
                    WireValue::Record(BTreeMap::from([
                        (String::from("width"), WireValue::Int(512)),
                        (String::from("height"), WireValue::Int(512)),
                    ])),
                )])),
            )]),
            state: Some(WireValue::Record(BTreeMap::from([(
                String::from("previous"),
                WireValue::Float(0.75),
            )]))),
            events: vec![InvokeEvent {
                level: InvokeEventLevel::Info,
                message: String::from("normalized"),
                metadata: BTreeMap::new(),
            }],
        };

        let request_json = serde_json::to_string(&request).expect("serialize request");
        let response_json = serde_json::to_string(&response).expect("serialize response");

        let decoded_request: InvokeRequest =
            serde_json::from_str(&request_json).expect("deserialize request");
        let decoded_response: InvokeResponse =
            serde_json::from_str(&response_json).expect("deserialize response");

        assert_eq!(decoded_request, request);
        assert_eq!(decoded_response, response);
        decoded_request
            .validate_protocol()
            .expect("request protocol is supported");
        decoded_response
            .validate_protocol()
            .expect("response protocol is supported");
    }

    #[test]
    fn invoke_contract_validates_requests_responses_events_and_wire_values() {
        let int_port = |name: &str, optional: bool| WirePort {
            name: name.into(),
            ty: TypeExpr::scalar(daedalus_data::model::ValueType::Int),
            type_key: None,
            optional,
            access: AccessMode::Read,
            residency: None,
            layout: None,
            source: None,
            const_value: None,
        };
        let node = NodeSchema {
            id: "demo.contract".into(),
            backend: BackendKind::Python,
            entrypoint: "run".into(),
            label: None,
            stateful: false,
            feature_flags: Vec::new(),
            inputs: vec![int_port("a", false), int_port("maybe", true)],
            outputs: vec![int_port("out", false)],
            metadata: BTreeMap::new(),
        };
        let request = InvokeRequest {
            protocol_version: WORKER_PROTOCOL_VERSION,
            node_id: "demo.contract".into(),
            correlation_id: Some("req-1".into()),
            args: BTreeMap::from([("a".into(), WireValue::Int(1))]),
            state: Some(WireValue::Record(BTreeMap::from([(
                "state".into(),
                WireValue::Enum(WireEnumValue {
                    name: "Ready".into(),
                    value: None,
                }),
            )]))),
            context: BTreeMap::new(),
        };
        request
            .validate_against_node(&node)
            .expect("request contract");

        let response = InvokeResponse {
            protocol_version: WORKER_PROTOCOL_VERSION,
            correlation_id: Some("req-1".into()),
            outputs: BTreeMap::from([("out".into(), WireValue::Int(2))]),
            state: None,
            events: vec![InvokeEvent {
                level: InvokeEventLevel::Info,
                message: "ok".into(),
                metadata: BTreeMap::new(),
            }],
        };
        response
            .validate_against_node(&node)
            .expect("response contract");

        let mut missing = request.clone();
        missing.args.clear();
        assert!(matches!(
            missing.validate_against_node(&node),
            Err(InvokeContractError::MissingArgument(name)) if name == "a"
        ));

        let mut unexpected = response.clone();
        unexpected.outputs.insert("extra".into(), WireValue::Unit);
        assert!(matches!(
            unexpected.validate_against_node(&node),
            Err(InvokeContractError::UnexpectedOutput(name)) if name == "extra"
        ));

        let empty_event = InvokeEvent {
            level: InvokeEventLevel::Warn,
            message: " ".into(),
            metadata: BTreeMap::new(),
        };
        assert!(matches!(
            empty_event.validate_contract(),
            Err(InvokeContractError::EmptyField {
                field: "event.message"
            })
        ));

        assert!(matches!(
            WireValue::Enum(WireEnumValue {
                name: String::new(),
                value: None,
            })
            .validate_contract(),
            Err(InvokeContractError::EmptyField { field: "enum.name" })
        ));
    }

    #[test]
    fn worker_protocol_negotiates_supported_version_and_capability_summary() {
        let hello = WorkerHello {
            protocol_version: WORKER_PROTOCOL_VERSION,
            min_protocol_version: WORKER_PROTOCOL_VERSION,
            worker_id: Some("python-worker-1".into()),
            backend: Some(BackendKind::Python),
            supported_nodes: vec!["demo.add".into()],
            capabilities: vec!["stateful".into(), "raw_io".into()],
            metadata: BTreeMap::from([("pid".into(), serde_json::json!(1234))]),
        };

        assert_eq!(
            hello.negotiated_protocol_version().expect("negotiate"),
            WORKER_PROTOCOL_VERSION
        );

        let ack = WorkerProtocolAck::from_hello(&hello).expect("ack");
        assert_eq!(ack.protocol_version, WORKER_PROTOCOL_VERSION);
        assert_eq!(ack.worker_id.as_deref(), Some("python-worker-1"));
        ack.validate_protocol().expect("ack protocol");

        let json = serde_json::to_string(&hello).expect("serialize hello");
        let decoded: WorkerHello = serde_json::from_str(&json).expect("deserialize hello");
        assert_eq!(decoded, hello);
    }

    #[test]
    fn worker_protocol_rejects_unsupported_versions() {
        let request = InvokeRequest {
            protocol_version: WORKER_PROTOCOL_VERSION + 1,
            node_id: "demo.add".into(),
            correlation_id: Some("req-unsupported".into()),
            args: BTreeMap::new(),
            state: None,
            context: BTreeMap::new(),
        };
        assert!(matches!(
            request.validate_protocol(),
            Err(WorkerProtocolError::UnsupportedVersion { found, .. })
                if found == WORKER_PROTOCOL_VERSION + 1
        ));

        let hello = WorkerHello {
            protocol_version: WORKER_PROTOCOL_VERSION + 2,
            min_protocol_version: WORKER_PROTOCOL_VERSION + 1,
            worker_id: None,
            backend: Some(BackendKind::Node),
            supported_nodes: Vec::new(),
            capabilities: Vec::new(),
            metadata: BTreeMap::new(),
        };
        assert!(matches!(
            hello.negotiated_protocol_version(),
            Err(WorkerProtocolError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn worker_messages_wrap_every_payload_with_protocol_and_correlation_id() {
        let hello = WorkerMessage::new(
            WorkerMessagePayload::Hello(WorkerHello {
                protocol_version: WORKER_PROTOCOL_VERSION,
                min_protocol_version: WORKER_PROTOCOL_VERSION,
                worker_id: Some("node-worker-1".into()),
                backend: Some(BackendKind::Node),
                supported_nodes: vec!["demo.add".into()],
                capabilities: vec!["persistent_worker".into()],
                metadata: BTreeMap::new(),
            }),
            Some("startup-1".into()),
        );
        let request = WorkerMessage::new(
            WorkerMessagePayload::Invoke(InvokeRequest {
                protocol_version: WORKER_PROTOCOL_VERSION,
                node_id: "demo.add".into(),
                correlation_id: Some("invoke-1".into()),
                args: BTreeMap::new(),
                state: None,
                context: BTreeMap::new(),
            }),
            Some("invoke-1".into()),
        );
        let event = WorkerMessage::new(
            WorkerMessagePayload::Event(InvokeEvent {
                level: InvokeEventLevel::Info,
                message: "loaded".into(),
                metadata: BTreeMap::new(),
            }),
            Some("invoke-1".into()),
        );
        let error = WorkerMessage::new(
            WorkerMessagePayload::Error(WorkerError {
                code: "method_not_found".into(),
                message: "missing demo.add".into(),
                metadata: BTreeMap::new(),
            }),
            Some("invoke-1".into()),
        );

        for message in [hello, request, event, error] {
            message.validate_protocol().expect("message protocol");
            assert!(message.correlation_id.is_some());
            let json = serde_json::to_string(&message).expect("serialize message");
            let decoded: WorkerMessage = serde_json::from_str(&json).expect("deserialize message");
            assert_eq!(decoded, message);
        }
    }

    #[test]
    fn layer_and_runtime_model_are_explicit() {
        let layers = [
            FfiLayer::PackageDiscovery,
            FfiLayer::Schema,
            FfiLayer::HostCore,
            FfiLayer::BackendRuntime,
            FfiLayer::Transport,
        ];

        let json = serde_json::to_string(&layers).expect("serialize layers");
        assert!(json.contains("package_discovery"));
        assert!(json.contains("backend_runtime"));

        let model_json =
            serde_json::to_string(&BackendRuntimeModel::PersistentWorker).expect("serialize model");
        assert_eq!(model_json, "\"persistent_worker\"");
    }

    #[test]
    fn canonical_fixture_specs_cover_declared_matrix_and_validate_contracts() {
        let specs = canonical_fixture_specs();
        let kinds = specs
            .iter()
            .map(|spec| spec.kind)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            kinds,
            [
                CanonicalFixtureKind::Scalar,
                CanonicalFixtureKind::Bytes,
                CanonicalFixtureKind::Image,
                CanonicalFixtureKind::Struct,
                CanonicalFixtureKind::Enum,
                CanonicalFixtureKind::Optional,
                CanonicalFixtureKind::List,
                CanonicalFixtureKind::Tuple,
                CanonicalFixtureKind::Map,
                CanonicalFixtureKind::MultiOutput,
                CanonicalFixtureKind::RawIo,
                CanonicalFixtureKind::Stateful,
                CanonicalFixtureKind::Shader,
                CanonicalFixtureKind::CapabilityBacked,
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(specs.len(), 14);

        let mut names = std::collections::BTreeSet::new();
        let mut node_ids = std::collections::BTreeSet::new();
        for spec in &specs {
            assert!(names.insert(spec.name.clone()), "duplicate fixture name");
            assert!(node_ids.insert(spec.node_id.clone()), "duplicate node id");
            assert!(!spec.inputs.is_empty(), "{} missing inputs", spec.name);
            assert!(!spec.outputs.is_empty(), "{} missing outputs", spec.name);

            let node = NodeSchema {
                id: spec.node_id.clone(),
                backend: spec.backend.clone().unwrap_or(BackendKind::Python),
                entrypoint: "run".into(),
                label: None,
                stateful: spec.stateful,
                feature_flags: Vec::new(),
                inputs: spec.inputs.clone(),
                outputs: spec.outputs.clone(),
                metadata: BTreeMap::new(),
            };
            let schema = PluginSchema {
                schema_version: SCHEMA_VERSION,
                plugin: PluginSchemaInfo {
                    name: format!("ffi.conformance.{}", spec.name),
                    version: Some("1.0.0".into()),
                    description: None,
                    metadata: BTreeMap::new(),
                },
                dependencies: Vec::new(),
                required_host_capabilities: spec.required_host_capabilities.clone(),
                feature_flags: Vec::new(),
                boundary_contracts: Vec::new(),
                nodes: vec![node.clone()],
            };
            schema.validate().expect("fixture schema validates");

            let request = InvokeRequest {
                protocol_version: WORKER_PROTOCOL_VERSION,
                node_id: spec.node_id.clone(),
                correlation_id: Some(DEFAULT_CORRELATION_ID.into()),
                args: spec.request_inputs.clone(),
                state: spec.request_state.clone(),
                context: BTreeMap::new(),
            };
            request
                .validate_against_node(&node)
                .expect("fixture request validates");

            let response = InvokeResponse {
                protocol_version: WORKER_PROTOCOL_VERSION,
                correlation_id: Some(DEFAULT_CORRELATION_ID.into()),
                outputs: spec.expected_outputs.clone(),
                state: spec.expected_state.clone(),
                events: spec.expected_events.clone(),
            };
            response
                .validate_against_node(&node)
                .expect("fixture response validates");
        }

        let raw_io = specs
            .iter()
            .find(|spec| spec.kind == CanonicalFixtureKind::RawIo)
            .expect("raw io spec");
        assert_eq!(raw_io.expected_events.len(), 1);

        let stateful = specs
            .iter()
            .find(|spec| spec.kind == CanonicalFixtureKind::Stateful)
            .expect("stateful spec");
        assert!(stateful.stateful);
        assert!(stateful.request_state.is_some());
        assert!(stateful.expected_state.is_some());

        let capability = specs
            .iter()
            .find(|spec| spec.kind == CanonicalFixtureKind::CapabilityBacked)
            .expect("capability spec");
        assert_eq!(
            capability.required_host_capabilities,
            vec![String::from("camera.read")]
        );

        let shader = specs
            .iter()
            .find(|spec| spec.kind == CanonicalFixtureKind::Shader)
            .expect("shader spec");
        assert_eq!(shader.backend, Some(BackendKind::Shader));
    }

    #[test]
    fn generates_scalar_add_conformance_fixtures_for_all_languages() {
        let fixtures = generate_scalar_add_fixtures().expect("fixtures");
        assert_eq!(fixtures.len(), 5);

        let languages = fixtures
            .iter()
            .map(|fixture| fixture.language)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            languages,
            [
                FixtureLanguage::Rust,
                FixtureLanguage::Python,
                FixtureLanguage::Node,
                FixtureLanguage::Java,
                FixtureLanguage::CCpp,
            ]
            .into_iter()
            .collect()
        );

        for fixture in &fixtures {
            fixture.schema.validate().expect("schema validates");
            assert_eq!(fixture.schema.nodes.len(), 1);
            let node = &fixture.schema.nodes[0];
            assert_eq!(node.id, "ffi.conformance.scalar_add:add");
            assert_eq!(node.backend, fixture.language.backend());
            fixture
                .backends
                .get(&node.id)
                .expect("backend")
                .validate_for_node(&node.id)
                .expect("backend validates");
            assert_eq!(fixture.request.args.get("a"), Some(&WireValue::Int(2)));
            assert_eq!(fixture.request.args.get("b"), Some(&WireValue::Int(40)));
            assert_eq!(
                fixture.expected_response.outputs.get("out"),
                Some(&WireValue::Int(42))
            );
            assert_eq!(fixture.files.len(), 1);
        }
    }

    #[test]
    fn fixture_schema_and_backend_snapshots_are_stable() {
        let fixtures = generate_scalar_add_fixtures().expect("fixtures");
        let text = serde_json::to_string_pretty(&fixtures).expect("snapshot");
        for needle in [
            "ffi.conformance.python.scalar_add",
            "ffi.conformance.node.scalar_add",
            "ffi.conformance.java.scalar_add",
            "ffi.conformance.c_cpp.scalar_add",
            "ffi.conformance.rust.scalar_add",
            "scalar_add.py",
            "scalar_add.mjs",
            "ScalarAdd.java",
            "scalar_add.cpp",
        ] {
            assert!(text.contains(needle), "snapshot missing {needle}");
        }
    }

    #[test]
    fn generated_package_fixtures_validate_and_stamp_integrity_for_all_languages() {
        let fixtures = generate_scalar_add_package_fixtures().expect("package fixtures");
        assert_eq!(fixtures.len(), 5);

        for mut fixture in fixtures {
            let root = tempfile::tempdir().expect("temp package root");
            for file in &fixture.files {
                let path = root.path().join(&file.path);
                fs::create_dir_all(path.parent().expect("artifact parent")).expect("mkdir");
                fs::write(path, &file.contents).expect("write artifact");
            }
            fixture
                .package
                .validate_artifact_files(root.path())
                .expect("artifact files");
            fixture
                .package
                .stamp_integrity(root.path())
                .expect("stamp integrity");
            fixture
                .package
                .verify_integrity(root.path())
                .expect("verify integrity");
            assert!(fixture.package.manifest_hash.is_some());
            assert!(
                fixture
                    .package
                    .artifacts
                    .iter()
                    .all(|artifact| artifact.path.starts_with("_bundle/src/"))
            );
        }
    }

    #[test]
    fn generated_failure_fixtures_cover_bad_input_missing_node_and_schema_validation() {
        let fixture = generate_scalar_add_fixtures()
            .expect("fixtures")
            .into_iter()
            .find(|fixture| fixture.language == FixtureLanguage::Python)
            .expect("python fixture");

        let mut bad_input = fixture.request.clone();
        bad_input
            .args
            .insert("a".into(), WireValue::String("wrong".into()));
        assert!(matches!(
            bad_input.args.get("a"),
            Some(WireValue::String(value)) if value == "wrong"
        ));

        let mut missing_node_package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: Some(fixture.schema.clone()),
            backends: fixture.backends.clone(),
            artifacts: Vec::new(),
            lockfile: None,
            manifest_hash: None,
            signature: None,
            metadata: BTreeMap::new(),
        };
        missing_node_package.backends.clear();
        assert!(matches!(
            missing_node_package.validate(),
            Err(FfiContractError::MissingBackendConfig { .. })
        ));

        let mut invalid_schema = fixture.schema;
        invalid_schema.nodes[0].inputs[0].name.clear();
        assert!(matches!(
            invalid_schema.validate(),
            Err(FfiContractError::EmptyField { field: "port.name" })
        ));
    }
}
