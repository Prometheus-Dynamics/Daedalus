use std::collections::BTreeMap;

use daedalus_data::model::{EnumVariant, StructField, TypeExpr, ValueType};
use daedalus_transport::{
    AccessMode, BoundaryCapabilities, BoundaryTypeContract, Layout, LayoutHash, Residency, TypeKey,
};
use serde::{Deserialize, Serialize};

use super::*;
use crate::{
    ByteEncoding, BytePayload, ImageLayout, ImagePayload, InvokeEvent, InvokeEventLevel,
    InvokeRequest, InvokeResponse, ScalarDType, WireEnumValue, WirePayloadHandle, WireValue,
};

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
    CustomTypeKey,
    BoundaryContract,
    PackageArtifact,
    FailureDiagnostic,
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
    pub boundary_contracts: Vec<BoundaryTypeContract>,
    #[serde(default)]
    pub package_artifacts: Vec<PackageArtifact>,
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
        boundary_contracts: Vec::new(),
        package_artifacts: Vec::new(),
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
        custom_type_key_fixture_spec(),
        boundary_contract_fixture_spec(),
        package_artifact_fixture_spec(),
        failure_diagnostic_fixture_spec(),
    ]
}

fn bytes_fixture_spec() -> CanonicalFixtureSpec {
    let ty = TypeExpr::scalar(ValueType::Bytes);
    unary_fixture_spec(
        CanonicalFixtureKind::Bytes,
        "bytes_echo",
        unary_fixture_value(
            "payload",
            ty.clone(),
            WireValue::Bytes(BytePayload {
                data: vec![1, 2, 3, 4],
                encoding: ByteEncoding::Raw,
            }),
        ),
        unary_fixture_value(
            "out",
            ty,
            WireValue::Bytes(BytePayload {
                data: vec![1, 2, 3, 4],
                encoding: ByteEncoding::Raw,
            }),
        ),
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
        unary_fixture_value("image", TypeExpr::opaque("ffi:image:u8"), image.clone()),
        unary_fixture_value("image", TypeExpr::opaque("ffi:image:u8"), image),
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
        unary_fixture_value("input", ty.clone(), input),
        unary_fixture_value("out", ty, output),
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
        unary_fixture_value(
            "input",
            ty.clone(),
            WireValue::Enum(WireEnumValue {
                name: "Value".into(),
                value: Some(Box::new(WireValue::Int(7))),
            }),
        ),
        unary_fixture_value(
            "out",
            ty,
            WireValue::Enum(WireEnumValue {
                name: "Value".into(),
                value: Some(Box::new(WireValue::Int(7))),
            }),
        ),
    )
}

fn optional_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::Optional,
        "optional_default",
        unary_fixture_value(
            "maybe",
            TypeExpr::optional(TypeExpr::scalar(ValueType::Int)),
            WireValue::Unit,
        ),
        unary_fixture_value("out", TypeExpr::scalar(ValueType::Int), WireValue::Int(5)),
    );
    spec.inputs[0].optional = true;
    spec
}

fn list_fixture_spec() -> CanonicalFixtureSpec {
    let ty = TypeExpr::list(TypeExpr::scalar(ValueType::Int));
    unary_fixture_spec(
        CanonicalFixtureKind::List,
        "list_sum",
        unary_fixture_value(
            "items",
            ty,
            WireValue::List(vec![
                WireValue::Int(1),
                WireValue::Int(2),
                WireValue::Int(3),
            ]),
        ),
        unary_fixture_value("out", TypeExpr::scalar(ValueType::Int), WireValue::Int(6)),
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
        unary_fixture_value(
            "pair",
            ty,
            WireValue::List(vec![
                WireValue::String("left".into()),
                WireValue::Bool(true),
            ]),
        ),
        unary_fixture_value(
            "out",
            TypeExpr::scalar(ValueType::String),
            WireValue::String("left:true".into()),
        ),
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
        unary_fixture_value(
            "items",
            ty,
            WireValue::Record(BTreeMap::from([
                ("a".into(), WireValue::Int(1)),
                ("b".into(), WireValue::Int(2)),
            ])),
        ),
        unary_fixture_value("out", TypeExpr::scalar(ValueType::Int), WireValue::Int(2)),
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
        boundary_contracts: Vec::new(),
        package_artifacts: Vec::new(),
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
        unary_fixture_value("delta", TypeExpr::scalar(ValueType::Int), WireValue::Int(3)),
        unary_fixture_value(
            "count",
            TypeExpr::scalar(ValueType::Int),
            WireValue::Int(10),
        ),
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
        unary_fixture_value(
            "input",
            TypeExpr::scalar(ValueType::U32),
            WireValue::Int(41),
        ),
        unary_fixture_value("out", TypeExpr::scalar(ValueType::U32), WireValue::Int(42)),
    );
    spec.backend = Some(BackendKind::Shader);
    spec
}

fn capability_backed_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::CapabilityBacked,
        "capability_camera_frame",
        unary_fixture_value(
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
        ),
        unary_fixture_value(
            "ok",
            TypeExpr::scalar(ValueType::Bool),
            WireValue::Bool(true),
        ),
    );
    spec.required_host_capabilities = vec!["camera.read".into()];
    spec
}

fn custom_type_key_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::CustomTypeKey,
        "custom_point_type",
        unary_fixture_value(
            "point",
            TypeExpr::opaque("example.Point"),
            WireValue::Record(BTreeMap::from([
                ("x".into(), WireValue::Float(1.5)),
                ("y".into(), WireValue::Float(2.5)),
            ])),
        ),
        unary_fixture_value(
            "magnitude",
            TypeExpr::scalar(ValueType::Float),
            WireValue::Float(4.0),
        ),
    );
    spec.inputs[0].type_key = Some(TypeKey::new("example.Point"));
    spec.outputs[0].type_key = Some(TypeKey::new("example.PointMagnitude"));
    spec.expected_events = vec![InvokeEvent {
        level: InvokeEventLevel::Info,
        message: "custom type key resolved".into(),
        metadata: BTreeMap::from([("type_key".into(), serde_json::json!("example.Point"))]),
    }];
    spec
}

pub(crate) fn boundary_contract_fixture_spec() -> CanonicalFixtureSpec {
    let handle = WireValue::Handle(WirePayloadHandle {
        id: "frame-boundary-0".into(),
        type_key: TypeKey::new("example.Frame"),
        access: AccessMode::Modify,
        residency: Some(Residency::Gpu),
        layout: Some(Layout::new("rgba8-hwc")),
        capabilities: vec!["frame.modify".into()],
        metadata: BTreeMap::from([("lease".into(), serde_json::json!("invoke"))]),
    });
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::BoundaryContract,
        "boundary_frame_modify",
        unary_fixture_value("frame", TypeExpr::opaque("example.Frame"), handle.clone()),
        unary_fixture_value("frame", TypeExpr::opaque("example.Frame"), handle),
    );
    for port in spec.inputs.iter_mut().chain(spec.outputs.iter_mut()) {
        port.type_key = Some(TypeKey::new("example.Frame"));
        port.access = AccessMode::Modify;
        port.residency = Some(Residency::Gpu);
        port.layout = Some(Layout::new("rgba8-hwc"));
    }
    spec.required_host_capabilities = vec!["frame.modify".into()];
    spec.boundary_contracts = vec![BoundaryTypeContract {
        type_key: TypeKey::new("example.Frame"),
        rust_type_name: Some("example::Frame".into()),
        abi_version: BoundaryTypeContract::ABI_VERSION,
        layout_hash: LayoutHash::new("example.Frame:rgba8-hwc"),
        capabilities: BoundaryCapabilities::frame_like(),
    }];
    spec
}

fn package_artifact_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::PackageArtifact,
        "package_shader_asset",
        unary_fixture_value("value", TypeExpr::scalar(ValueType::U32), WireValue::Int(1)),
        unary_fixture_value("out", TypeExpr::scalar(ValueType::U32), WireValue::Int(2)),
    );
    spec.package_artifacts = vec![PackageArtifact {
        path: "_bundle/shaders/package_shader_asset.wgsl".into(),
        kind: PackageArtifactKind::ShaderAsset,
        backend: Some(BackendKind::Shader),
        platform: None,
        sha256: None,
        metadata: BTreeMap::from([("entrypoint".into(), serde_json::json!("main"))]),
    }];
    spec
}

fn failure_diagnostic_fixture_spec() -> CanonicalFixtureSpec {
    let mut spec = unary_fixture_spec(
        CanonicalFixtureKind::FailureDiagnostic,
        "typed_failure",
        unary_fixture_value(
            "value",
            TypeExpr::scalar(ValueType::Int),
            WireValue::Int(-1),
        ),
        unary_fixture_value(
            "ok",
            TypeExpr::scalar(ValueType::Bool),
            WireValue::Bool(false),
        ),
    );
    spec.expected_events = vec![InvokeEvent {
        level: InvokeEventLevel::Error,
        message: "value must be non-negative".into(),
        metadata: BTreeMap::from([
            ("code".into(), serde_json::json!("invalid_argument")),
            ("field".into(), serde_json::json!("value")),
        ]),
    }];
    spec
}

struct UnaryFixtureValue {
    name: &'static str,
    ty: TypeExpr,
    value: WireValue,
}

fn unary_fixture_value(name: &'static str, ty: TypeExpr, value: WireValue) -> UnaryFixtureValue {
    UnaryFixtureValue { name, ty, value }
}

fn unary_fixture_spec(
    kind: CanonicalFixtureKind,
    name: &str,
    input: UnaryFixtureValue,
    output: UnaryFixtureValue,
) -> CanonicalFixtureSpec {
    CanonicalFixtureSpec {
        kind,
        name: name.into(),
        node_id: format!("ffi.conformance.{name}:run"),
        inputs: vec![wire_port(input.name, input.ty)],
        outputs: vec![wire_port(output.name, output.ty)],
        request_inputs: BTreeMap::from([(input.name.into(), input.value)]),
        expected_outputs: BTreeMap::from([(output.name.into(), output.value)]),
        request_state: None,
        expected_state: None,
        expected_events: Vec::new(),
        required_host_capabilities: Vec::new(),
        boundary_contracts: Vec::new(),
        package_artifacts: Vec::new(),
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
