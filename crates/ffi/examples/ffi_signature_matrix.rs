//! FFI plugin that demonstrates the broadest set of Rust `#[node]` signature shapes.
//!
//! Build:
//! - `cargo build -p daedalus-ffi --example ffi_signature_matrix`
//! - Optional shader signature (compiles only): `cargo build -p daedalus-ffi --features gpu-wgpu --example ffi_signature_matrix`

#![crate_type = "cdylib"]

use daedalus::ffi::export_plugin;
use daedalus::macros::{NodeConfig, node};
use daedalus::runtime::io::NodeIo;
use daedalus::runtime::plugins::{Plugin, PluginRegistry, RegistryPluginExt};
use daedalus::runtime::state::ExecutionContext;
use daedalus::runtime::{NodeError, RuntimeNode};

// --- Simple typed nodes -----------------------------------------------------

#[node(id = "matrix.source", outputs("out"))]
fn source() -> Result<i32, NodeError> {
    Ok(1)
}

#[node(id = "matrix.source_pair", outputs("a", "b"))]
fn source_pair() -> Result<(i32, i32), NodeError> {
    Ok((2, 3))
}

#[node(id = "matrix.add_typed", inputs("a", "b"), outputs("sum"))]
fn add_typed(a: i32, b: i32) -> Result<i32, NodeError> {
    Ok(a + b)
}

#[node(id = "matrix.sink", inputs("value"))]
fn sink(_value: i32) -> Result<(), NodeError> {
    Ok(())
}

// --- Optional / collection typing ------------------------------------------

#[node(id = "matrix.option_passthrough", inputs("value"), outputs("out"))]
fn option_passthrough(value: Option<i32>) -> Result<Option<i32>, NodeError> {
    Ok(value)
}

#[node(id = "matrix.vec_sum", inputs("values"), outputs("out"))]
fn vec_sum(values: Vec<i32>) -> Result<i32, NodeError> {
    Ok(values.into_iter().sum())
}

// --- ExecutionContext / RuntimeNode injection (not ports) ------------------

#[node(id = "matrix.ctx_sink", inputs("text"))]
fn ctx_sink(_text: String, _ctx: &ExecutionContext) -> Result<(), NodeError> {
    Ok(())
}

#[node(id = "matrix.node_inspect", inputs("value"), outputs("out"))]
fn node_inspect(_node: &RuntimeNode, value: i32) -> Result<i32, NodeError> {
    Ok(value)
}

// --- Config-backed input ----------------------------------------------------

#[derive(Clone, Debug, NodeConfig)]
struct ScaleConfig {
    #[port(default = 2, min = 1, max = 16, policy = "clamp")]
    factor: i32,
}

#[node(
    id = "matrix.scale_cfg",
    inputs("value", config = ScaleConfig),
    outputs("out")
)]
fn scale_cfg(value: i32, cfg: ScaleConfig) -> Result<i32, NodeError> {
    Ok(value * cfg.factor)
}

// --- Stateful node ----------------------------------------------------------

#[derive(Default)]
struct AccumState {
    sum: i32,
}

#[node(
    id = "matrix.accum_state",
    inputs("value"),
    outputs("sum"),
    state(AccumState)
)]
fn accum_state(value: i32, state: &mut AccumState) -> Result<i32, NodeError> {
    state.sum += value;
    Ok(state.sum)
}

// --- Sync groups metadata (typed signature) --------------------------------

#[node(
    id = "matrix.sync_latest",
    inputs("a", "b"),
    outputs("out"),
    sync_groups(vec![daedalus::SyncGroup {
        name: "ab".into(),
        policy: daedalus::SyncPolicy::Latest,
        backpressure: None,
        capacity: None,
        ports: vec!["a".into(), "b".into()],
    }])
)]
fn sync_latest(a: i32, b: i32) -> Result<i32, NodeError> {
    Ok(a + b)
}

// --- Metadata-only ports (port source/default metadata) ---------------------

pub fn modes() -> Vec<&'static str> {
    vec!["fast", "balanced", "quality"]
}

#[node(
    id = "matrix.choose_mode_meta",
    inputs(port(name = "mode", source = "modes", default = "quality")),
    outputs("out")
)]
fn choose_mode_meta(mode: String) -> Result<String, NodeError> {
    Ok(format!("mode={mode}"))
}

// --- Fully low-level triad handler form ------------------------------------

#[node(id = "matrix.low_level", inputs("value"), outputs("out"))]
fn low_level(
    _node: &RuntimeNode,
    _ctx: &ExecutionContext,
    io: &mut NodeIo,
) -> Result<(), NodeError> {
    let v = io
        .get_any::<i32>("value")
        .ok_or_else(|| NodeError::InvalidInput("missing value".into()))?;
    io.push_any(Some("out"), v);
    Ok(())
}

#[node(id = "matrix.low_level_source", outputs("a", "b"))]
fn low_level_source(
    _node: &RuntimeNode,
    _ctx: &ExecutionContext,
    io: &mut NodeIo,
) -> Result<(), NodeError> {
    io.push_any(Some("a"), 10i32);
    io.push_any(Some("b"), 20i32);
    Ok(())
}

// --- Capability-dispatched generic node ------------------------------------

#[node(
    id = "matrix.add_cap",
    capability = "Add",
    inputs("a", "b"),
    outputs("out")
)]
fn add_cap<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: std::ops::Add<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a + b)
}

// --- ShaderContext signature (compile-time only unless gpu-wgpu enabled) ----

#[cfg(feature = "gpu-wgpu")]
mod shader_sig {
    use super::*;
    use daedalus_gpu::shader::ShaderContext;

    #[node(
        id = "matrix.shader_passthrough",
        inputs("value"),
        outputs("out"),
        shaders("assets/passthrough.wgsl")
    )]
    fn shader_passthrough(value: i32, _ctx: ShaderContext) -> Result<i32, NodeError> {
        Ok(value)
    }

    pub(super) fn install(reg: &mut PluginRegistry) -> Result<(), &'static str> {
        reg.merge::<shader_passthrough>()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct SignatureMatrixPlugin;

impl Plugin for SignatureMatrixPlugin {
    fn id(&self) -> &'static str {
        "ffi.signature_matrix"
    }

    fn install(&self, reg: &mut PluginRegistry) -> Result<(), &'static str> {
        reg.merge::<source>()?;
        reg.merge::<source_pair>()?;
        reg.merge::<add_typed>()?;
        reg.merge::<sink>()?;
        reg.merge::<option_passthrough>()?;
        reg.merge::<vec_sum>()?;
        reg.merge::<ctx_sink>()?;
        reg.merge::<node_inspect>()?;
        reg.merge::<scale_cfg>()?;
        reg.merge::<accum_state>()?;
        reg.merge::<sync_latest>()?;
        reg.merge::<choose_mode_meta>()?;
        reg.merge::<low_level>()?;
        reg.merge::<low_level_source>()?;
        reg.merge::<add_cap>()?;

        // Capability registration (mirrors what the MathPlugin does).
        add_cap::register_capability::<i32>(reg);
        add_cap::register_capability::<f64>(reg);

        #[cfg(feature = "gpu-wgpu")]
        shader_sig::install(reg)?;

        // Demonstrate nested plugin install works in FFI contexts too.
        // (no-op plugin, but exercises prefix logic)
        reg.install_plugin(&NoopChildPlugin)?;

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct NoopChildPlugin;

impl Plugin for NoopChildPlugin {
    fn id(&self) -> &'static str {
        "child"
    }

    fn install(&self, _registry: &mut PluginRegistry) -> Result<(), &'static str> {
        Ok(())
    }
}

export_plugin!(SignatureMatrixPlugin);
