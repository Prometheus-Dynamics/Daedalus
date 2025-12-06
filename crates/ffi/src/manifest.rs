use std::collections::BTreeMap;

use daedalus_core::compute::ComputeAffinity;
use daedalus_core::policy::BackpressureStrategy;
use daedalus_core::sync::SyncPolicy;
use daedalus_data::model::TypeExpr;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize)]
pub struct Manifest {
    #[serde(default)]
    pub manifest_version: Option<String>,
    #[serde(default)]
    pub manifest_hash: Option<String>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub lockfile: Option<String>,
    #[serde(default)]
    pub bundle: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    pub plugin: PluginInfo,
    pub nodes: Vec<NodeManifest>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PluginInfo {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeManifest {
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub py_module: Option<String>,
    /// Optional path to a Python source file to load the module from (relative to manifest dir).
    ///
    /// When present, this avoids requiring `py_module` to be importable on `PYTHONPATH` and
    /// provides a more Rust-like "plugin = one file" authoring flow.
    #[serde(default)]
    pub py_path: Option<String>,
    #[serde(default)]
    pub py_function: Option<String>,
    #[serde(default)]
    pub js_module: Option<String>,
    /// Optional path to a JS/TS transpiled module file to load (relative to manifest dir).
    #[serde(default)]
    pub js_path: Option<String>,
    #[serde(default)]
    pub js_function: Option<String>,
    /// Java entrypoint classpath (directory or jar), relative to manifest directory.
    ///
    /// When present, this node is executed via the Java subprocess bridge.
    #[serde(default)]
    pub java_classpath: Option<String>,
    /// Java class name to call (e.g. `"com.example.Nodes"`).
    #[serde(default)]
    pub java_class: Option<String>,
    /// Java static method name to call (e.g. `"add_defaults"`).
    #[serde(default)]
    pub java_method: Option<String>,
    /// C/C++ shared library path, relative to the manifest directory.
    ///
    /// When present, this node is executed by loading the dylib and calling `cc_function`.
    #[serde(default)]
    pub cc_path: Option<String>,
    /// C ABI symbol to call (e.g. `"add"`).
    ///
    /// The function receives a JSON payload string and returns a JSON result string via a C ABI.
    #[serde(default)]
    pub cc_function: Option<String>,
    /// Optional C ABI symbol used to free strings returned by `cc_function`.
    ///
    /// Defaults to `"daedalus_free"` when absent.
    #[serde(default)]
    pub cc_free: Option<String>,
    /// If true, the language bridge receives a mutable `io` object that can push multiple
    /// outputs/events per tick (Rust `NodeIo`-style), instead of only returning a single output.
    #[serde(default)]
    pub raw_io: bool,
    #[serde(default)]
    pub stateful: bool,
    #[serde(default)]
    pub state: Option<NodeState>,
    /// Optional runtime capability dispatch key (mirrors Rust `#[node(capability = "...")]`).
    ///
    /// When set, this node is executed in Rust by dispatching to the global capability registry,
    /// and does not require language bridge fields (`py_*`/`js_*`).
    #[serde(default)]
    pub capability: Option<String>,
    /// If present, this node is executed by the Rust GPU shader runner instead of a language bridge.
    ///
    /// This is intentionally minimal (texture2d rgba8 input -> storage texture rgba8 output) to
    /// make it ergonomic for Python/Node manifests.
    #[serde(default)]
    pub shader: Option<ManifestShader>,
    #[serde(default)]
    pub feature_flags: Vec<String>,
    #[serde(default)]
    pub default_compute: ComputeAffinity,
    #[serde(default)]
    pub sync_groups: Vec<ManifestSyncGroup>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub inputs: Vec<ManifestPort>,
    #[serde(default)]
    pub outputs: Vec<ManifestPort>,
}

fn default_shader_entry() -> String {
    "main".to_string()
}

fn default_shader_input_binding() -> u32 {
    0
}

fn default_shader_output_binding() -> u32 {
    1
}

fn default_shader_entry_name() -> String {
    default_shader_entry()
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManifestShader {
    /// Optional logical shader name (for debugging/logging).
    #[serde(default)]
    pub name: Option<String>,
    /// WGSL source code (inline).
    #[serde(default)]
    pub src: Option<String>,
    /// Path to a WGSL file, relative to the manifest directory.
    #[serde(default)]
    pub src_path: Option<String>,
    /// Entry point function name (defaults to `"main"` like the Rust macro).
    #[serde(default = "default_shader_entry")]
    pub entry: String,
    /// Optional override; if absent the runner will infer from `@workgroup_size` when possible.
    #[serde(default)]
    pub workgroup_size: Option<[u32; 3]>,
    /// Optional multi-shader pipeline, mirroring Rust `shaders(...)`.
    ///
    /// When present and non-empty, this node can dispatch by name via `dispatch` or
    /// `dispatch_from_port`.
    #[serde(default)]
    pub shaders: Vec<ManifestNamedShader>,
    /// Optional shader name to dispatch (defaults to the first shader when `shaders` is set).
    #[serde(default)]
    pub dispatch: Option<String>,
    /// Optional node input port containing the shader name to dispatch.
    #[serde(default)]
    pub dispatch_from_port: Option<String>,
    /// Binding slot for the input `texture_2d<f32>` (defaults to 0).
    #[serde(default = "default_shader_input_binding")]
    pub input_binding: u32,
    /// Binding slot for the output `texture_storage_2d<rgba8unorm, write>` (defaults to 1).
    #[serde(default = "default_shader_output_binding")]
    pub output_binding: u32,
    /// Optional explicit bindings for a single dispatch (a higher-level mirror of `ShaderBinding`).
    ///
    /// If present and non-empty, `input_binding`/`output_binding` are ignored.
    #[serde(default)]
    pub bindings: Vec<ManifestShaderBinding>,
    /// Optional override for dispatch invocation count (defaults to `[width,height,1]`).
    #[serde(default)]
    pub invocations: Option<[u32; 3]>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManifestShaderBinding {
    pub binding: u32,
    pub kind: ManifestShaderBindingKind,
    pub access: ManifestShaderAccess,
    #[serde(default)]
    pub readback: bool,
    /// State backend for `from_state`/`to_state` (defaults to CPU-side bytes).
    #[serde(default)]
    pub state_backend: Option<ManifestShaderStateBackend>,
    /// For read-only bindings, pull data from this node input port.
    #[serde(default)]
    pub from_port: Option<String>,
    /// For buffer bindings, pull initial bytes from shader node state (keyed by this string).
    #[serde(default)]
    pub from_state: Option<String>,
    /// For readback bindings, push bytes/image payload to this node output port.
    #[serde(default)]
    pub to_port: Option<String>,
    /// For readback bindings, persist bytes into shader node state (keyed by this string).
    #[serde(default)]
    pub to_state: Option<String>,
    /// For buffer allocations, number of bytes.
    #[serde(default)]
    pub size_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestShaderBindingKind {
    Texture2dRgba8,
    StorageTexture2dRgba8,
    UniformBuffer,
    StorageBuffer,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestShaderStateBackend {
    Cpu,
    Gpu,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestShaderAccess {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ManifestSyncGroup {
    /// Backward-compatible shorthand: `["a","b"]` implies an auto-named group with
    /// `policy=AllReady` and default capacity/backpressure.
    Ports(Vec<String>),
    /// Full spec matching `daedalus_core::sync::SyncGroup` fields.
    Group(ManifestSyncGroupSpec),
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManifestSyncGroupSpec {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub policy: SyncPolicy,
    #[serde(default)]
    pub backpressure: Option<BackpressureStrategy>,
    #[serde(default)]
    pub capacity: Option<usize>,
    pub ports: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManifestNamedShader {
    pub name: String,
    /// WGSL source code (inline).
    #[serde(default)]
    pub src: Option<String>,
    /// Path to a WGSL file, relative to the manifest directory.
    #[serde(default)]
    pub src_path: Option<String>,
    /// Entry point function name (defaults to `"main"` like the Rust macro).
    #[serde(default = "default_shader_entry_name")]
    pub entry: String,
    /// Optional override; if absent the runner will infer from `@workgroup_size` when possible.
    #[serde(default)]
    pub workgroup_size: Option<[u32; 3]>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManifestPort {
    pub name: String,
    pub ty: TypeExpr,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub const_value: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeState {
    #[serde(default)]
    pub ty: Option<TypeExpr>,
    #[serde(default)]
    pub py_dataclass: Option<PyDataclass>,
    #[serde(default)]
    pub init: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PyDataclass {
    #[serde(default)]
    pub module: Option<String>,
    /// Optional path to a Python source file that defines this dataclass (relative to manifest dir).
    #[serde(default)]
    pub path: Option<String>,
    pub name: String,
}
