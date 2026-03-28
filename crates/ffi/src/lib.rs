//! FFI helpers and plugin loading.
//!
//! Rust-authored plugins can be loaded via `PluginLibrary`.
//! Language manifests (Python, Node.js) can be ingested and installed as plugins.

mod bridge;
mod c_cpp;
mod cpp_pack;
mod dispatch;
mod java;
mod manifest;
mod node;
mod plugin;
mod python;
#[cfg(feature = "gpu-wgpu")]
mod shader_manifest;

pub use c_cpp::{
    CppManifest, CppManifestError, CppManifestPlugin, load_cpp_library_plugin, load_cpp_manifest,
};
pub use cpp_pack::{CppPackError, CppPackOptions, pack_cpp_library_plugin};
pub use dispatch::{ManifestDispatchError, ManifestPlugin, load_manifest_plugin};
pub use java::{JavaManifest, JavaManifestError, JavaManifestPlugin, load_java_manifest};
pub use manifest::Manifest;
pub use node::{NodeManifest, NodeManifestError, NodeManifestPlugin, load_node_manifest};
pub use plugin::{
    FFI_VERSION, FfiPluginError, PLUGIN_ABI_SYMBOL, PLUGIN_ABI_VERSION, PLUGIN_INFO_SYMBOL,
    PluginInfo, PluginLibrary, REGISTER_SYMBOL, StrView,
};
pub use python::{
    ImageCompute, PythonManifest, PythonManifestError, PythonManifestPlugin, load_python_manifest,
};
