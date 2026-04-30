//! Minimal Rust plugin crate showing the “native” node authoring experience.
//!
//! This is the baseline that other languages try to match:
//! - `#[node(...)]` macro attaches IDs/ports/defaults/metadata.
//! - `#[derive(NodeConfig)]` makes config inputs first-class (defaults/validation).
//! - `state(T)` wires up persistent state for stateful nodes.
//! - Capability nodes can dispatch via the global capability registry.

use daedalus::macros::NodeConfig;
use daedalus::{PluginRegistry, declare_plugin, macros::node, runtime::NodeError};

// --- Stateless typed nodes --------------------------------------------------

#[node(id = "add", inputs("a", "b"), outputs("out"))]
fn add(a: i32, b: i32) -> Result<i32, NodeError> {
    Ok(a + b)
}

#[node(id = "split", inputs("value"), outputs("out0", "out1"))]
fn split(value: i32) -> Result<(i32, i32), NodeError> {
    Ok((value, -value))
}

// --- Config-backed input ----------------------------------------------------

#[derive(Clone, Debug, NodeConfig)]
struct ScaleConfig {
    // Config fields are also “ports”, with defaults + validation rules.
    #[port(default = 2, min = 1, max = 16, policy = "clamp")]
    factor: i32,
}

#[node(id = "scale_cfg", inputs("value", config = ScaleConfig), outputs("out"))]
fn scale_cfg(value: i32, cfg: ScaleConfig) -> Result<i32, NodeError> {
    Ok(value * cfg.factor)
}

// --- Stateful node ----------------------------------------------------------

#[derive(Default)]
struct AccumState {
    sum: i32,
}

#[node(id = "accum", inputs("value"), outputs("sum"), state(AccumState))]
fn accum(value: i32, state: &mut AccumState) -> Result<i32, NodeError> {
    state.sum += value;
    Ok(state.sum)
}

// --- Metadata-only ports (port source/default metadata) ---------------------

pub fn modes() -> Vec<&'static str> {
    vec!["fast", "balanced", "quality"]
}

#[node(
    id = "choose_mode_meta",
    inputs(port(name = "mode", source = "modes", default = "quality")),
    outputs("out")
)]
fn choose_mode_meta(mode: String) -> Result<String, NodeError> {
    Ok(format!("mode={mode}"))
}

// --- Capability-dispatch node ----------------------------------------------

#[node(id = "cap_add", capability = "Add", inputs("a", "b"), outputs("out"))]
fn cap_add<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: std::ops::Add<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a + b)
}

fn register_capabilities(registry: &mut PluginRegistry) {
    registry.register_capability_typed::<i32, _>("Add", |a, b| Ok(*a + *b));
}

declare_plugin!(
    ExampleProjectPlugin,
    "example_rust",
    [add, split, scale_cfg, accum, choose_mode_meta, cap_add],
    install = |registry| {
        register_capabilities(registry);
    }
);

daedalus::export_plugin!(ExampleProjectPlugin);
