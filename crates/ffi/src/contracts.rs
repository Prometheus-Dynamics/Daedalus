use std::collections::BTreeMap;

use daedalus_data::model::TypeExpr;
use serde::{Deserialize, Serialize};

/// Explicit layer map for the FFI rewrite.
///
/// This exists to make the intended architecture concrete inside the crate instead of leaving it
/// implicit in the current manifest-first implementation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FfiLayer {
    PackageDiscovery,
    Schema,
    HostCore,
    BackendRuntime,
    Transport,
}

/// The only backend execution models the rewrite intends to support as first-class paths.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    Other(String),
}

/// Smaller core schema boundary for plugin metadata and node shape.
///
/// Runtime process details and transport behavior are intentionally excluded from this type.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginSchema {
    pub plugin: PluginSchemaInfo,
    #[serde(default)]
    pub nodes: Vec<NodeSchema>,
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
    #[serde(default)]
    pub optional: bool,
}

/// Backend runtime config is deliberately separate from the schema surface.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendConfig {
    pub backend: BackendKind,
    pub runtime_model: BackendRuntimeModel,
    #[serde(default)]
    pub entry_module: Option<String>,
    #[serde(default)]
    pub entry_symbol: Option<String>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub options: BTreeMap<String, serde_json::Value>,
}

/// Typed transport contract for host/backend invocation.
///
/// This is intentionally small and explicit. It should replace the current best-effort `Any`/JSON
/// conversion path over time.
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
    List(Vec<WireValue>),
    Record(BTreeMap<String, WireValue>),
    Enum(WireEnumValue),
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarDType {
    #[default]
    U8,
    U16,
    F32,
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
    pub node_id: String,
    #[serde(default)]
    pub args: BTreeMap<String, WireValue>,
    #[serde(default)]
    pub state: Option<WireValue>,
    #[serde(default)]
    pub context: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InvokeResponse {
    #[serde(default)]
    pub outputs: BTreeMap<String, WireValue>,
    #[serde(default)]
    pub state: Option<WireValue>,
    #[serde(default)]
    pub events: Vec<InvokeEvent>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvokeEvent {
    pub level: InvokeEventLevel,
    pub message: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
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
    use daedalus_data::model::TypeExpr;

    #[test]
    fn plugin_schema_stays_separate_from_backend_config() {
        let schema = PluginSchema {
            plugin: PluginSchemaInfo {
                name: "demo".into(),
                version: Some("0.1.0".into()),
                description: Some("demo plugin".into()),
                metadata: BTreeMap::new(),
            },
            nodes: vec![NodeSchema {
                id: "demo.blur".into(),
                backend: BackendKind::Python,
                entrypoint: "blur".into(),
                label: Some("Blur".into()),
                stateful: true,
                inputs: vec![WirePort {
                    name: "image".into(),
                    ty: TypeExpr::opaque("image"),
                    optional: false,
                }],
                outputs: vec![WirePort {
                    name: "image".into(),
                    ty: TypeExpr::opaque("image"),
                    optional: false,
                }],
                metadata: BTreeMap::new(),
            }],
        };

        let backend = BackendConfig {
            backend: BackendKind::Python,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some("plugin".into()),
            entry_symbol: Some("blur".into()),
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

    #[test]
    fn invoke_contract_preserves_structured_state_and_outputs() {
        let request = InvokeRequest {
            node_id: String::from("demo.normalize"),
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
}
