//! Daedalus Rust FFI example plugin (cdylib).
//!
//! This is the “native” baseline:
//! - Use `#[node(...)]` for ID/ports/defaults/config/state.
//! - Use `declare_plugin!` to define a plugin that installs node descriptors + handlers.
//! - Use `export_plugin!` to expose the plugin as a dynamic library for the host loader.

#![crate_type = "cdylib"]

use daedalus::ffi::export_plugin;
use daedalus::macros::{NodeConfig, node};
use daedalus::runtime::NodeError;
use daedalus::{PluginRegistry, declare_plugin};

// --- Typed stateless nodes --------------------------------------------------

#[node(id = "example_rust:add", inputs("a", "b"), outputs("out"))]
fn add(a: i32, b: i32) -> Result<i32, NodeError> {
    Ok(a + b)
}

#[node(id = "example_rust:split", inputs("value"), outputs("out0", "out1"))]
fn split(value: i32) -> Result<(i32, i32), NodeError> {
    Ok((value, -value))
}

// --- Config-backed input ----------------------------------------------------

#[derive(Clone, Debug, NodeConfig)]
struct ScaleCfg {
    #[port(default = 2, min = 1, max = 16, policy = "clamp")]
    factor: i32,
}

#[node(
    id = "example_rust:scale_cfg",
    inputs("value", config = ScaleCfg),
    outputs("out")
)]
fn scale_cfg(value: i32, cfg: ScaleCfg) -> Result<i32, NodeError> {
    Ok(value * cfg.factor)
}

// --- Stateful node ----------------------------------------------------------

#[derive(Default)]
struct CounterState {
    v: i32,
}

#[node(
    id = "example_rust:counter",
    inputs("inc"),
    outputs("out"),
    state(CounterState)
)]
fn counter(inc: i32, state: &mut CounterState) -> Result<i32, NodeError> {
    state.v += inc;
    Ok(state.v)
}

// --- Capability node (dispatch through registry) ---------------------------

#[node(
    id = "example_rust:cap_add",
    capability = "Add",
    inputs("a", "b"),
    outputs("out")
)]
fn cap_add<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: std::ops::Add<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a + b)
}

fn register_capabilities(registry: &mut PluginRegistry) {
    registry.register_capability_typed::<i32, _>("Add", |a, b| Ok(*a + *b));
}

// --- GPU shader example (feature-gated) ------------------------------------
//
// Enable with:
//   cargo build -p daedalus_rust_ffi_example --features gpu-wgpu
//
// This shows the native Rust shader path (no manifest JSON needed):
// - `shaders("../assets/write_u32.wgsl")` embeds WGSL via include_str! at compile time
// - `ShaderContext` gives you GPU access and readback helpers
#[cfg(feature = "gpu-wgpu")]
#[node(
    id = "example_rust:write_u32_gpu",
    outputs("out"),
    compute(::daedalus::ComputeAffinity::GpuPreferred),
    shaders("../assets/write_u32.wgsl")
)]
fn write_u32_gpu(ctx: daedalus::gpu::shader::ShaderContext) -> Result<u32, NodeError> {
    use daedalus::gpu::shader::{Access, BindingData, BindingKind, BufferInit, ShaderBinding};

    let bindings = [ShaderBinding {
        binding: 0,
        kind: BindingKind::Storage,
        access: Access::ReadWrite,
        data: BindingData::Buffer(BufferInit::Zeroed(4)),
        readback: true,
    }];

    let out = ctx
        .dispatch_first(&bindings, None, None, Some([1, 1, 1]))
        .map_err(|e| NodeError::Handler(e.to_string()))?;

    let bytes = out
        .buffers
        .get(&0)
        .ok_or_else(|| NodeError::Handler("missing buffer readback".into()))?;
    if bytes.len() < 4 {
        return Err(NodeError::Handler("buffer readback too small".into()));
    }
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

declare_plugin!(
    ExampleRustFfiPlugin,
    "example_rust",
    [add, split, scale_cfg, counter, cap_add],
    install = |registry| {
        register_capabilities(registry);
    }
);

#[cfg(feature = "gpu-wgpu")]
declare_plugin!(
    ExampleRustFfiGpuPlugin,
    "example_rust",
    [add, split, scale_cfg, counter, cap_add, write_u32_gpu],
    install = |registry| {
        register_capabilities(registry);
    }
);

// Export the plugin symbol the host loader expects.
#[cfg(feature = "gpu-wgpu")]
type ExportedPlugin = ExampleRustFfiGpuPlugin;
#[cfg(not(feature = "gpu-wgpu"))]
type ExportedPlugin = ExampleRustFfiPlugin;
export_plugin!(ExportedPlugin);
