//! A Rust plugin written with the in-process authoring model and served out of process.
//!
//! `worker()` is the plugin as the host sees it: `#[node]` functions, a `NodeConfig` port, a
//! `state(T)` node, a node that panics, and a node that emits an event. `rust_worker` (the binary)
//! serves it; `tests/e2e.rs` installs it through the host and runs a graph of invokes.

use daedalus::data::model::{StructFieldValue, Value};
use daedalus::macros::{DaedalusToValue, NodeConfig, node};
use daedalus::runtime::NodeError;
use daedalus::{PluginRegistry, declare_plugin};
use daedalus_ffi_core::PluginPackage;
use daedalus_ffi_core::PluginSchema;
use daedalus_ffi_rust::{Worker, WorkerBuildError, event_info, rust_worker_package};

pub const PLUGIN_ID: &str = "rust_worker.showcase";
/// The stateful node's id, for the host.
pub const ACCUMULATE: &str = "rust_worker.showcase:accumulate";

#[derive(Clone, Debug, NodeConfig)]
pub struct ScaleConfig {
    #[port(default = 2, min = 1, max = 16, policy = "clamp")]
    pub factor: i64,
}

#[node(id = "add", inputs("a", "b"), outputs("out"))]
fn add(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a + b)
}

#[node(id = "scale", inputs("value", config = ScaleConfig), outputs("out"))]
fn scale(value: i64, config: ScaleConfig) -> Result<i64, NodeError> {
    Ok(value * config.factor)
}

#[derive(Default, DaedalusToValue)]
pub struct AccumState {
    pub sum: i64,
}

#[node(id = "accumulate", inputs("value"), outputs("sum"), state(AccumState))]
fn accumulate(value: i64, state: &mut AccumState) -> Result<i64, NodeError> {
    state.sum += value;
    Ok(state.sum)
}

#[node(id = "boom", inputs("value"), outputs("out"))]
fn boom(value: i64) -> Result<i64, NodeError> {
    if value < 0 {
        panic!("negative input");
    }
    Ok(value)
}

#[node(id = "describe", inputs("value"), outputs("text"))]
fn describe(value: i64) -> Result<String, NodeError> {
    event_info(format!("describing {value}"));
    Ok(format!("value is {value}"))
}

declare_plugin!(
    ShowcasePlugin,
    "rust_worker.showcase",
    [add, scale, accumulate, boom, describe]
);

fn decode_accumulate(value: &Value) -> Option<AccumState> {
    let Value::Struct(fields) = value else {
        return None;
    };
    let StructFieldValue {
        value: Value::Int(sum),
        ..
    } = fields.iter().find(|f| f.name == "sum")?
    else {
        return None;
    };
    Some(AccumState { sum: *sum })
}

/// The worker for this plugin, with the stateful node's state declared.
pub fn worker() -> Result<Worker, WorkerBuildError> {
    Ok(Worker::from_plugin(&ShowcasePlugin::new())?.with_state(ACCUMULATE, decode_accumulate))
}

/// The plugin's schema, as the host installs it.
pub fn schema() -> Result<PluginSchema, WorkerBuildError> {
    Ok(worker()?.schemas()[0].clone())
}

/// The package that runs this plugin from the worker binary at `executable`.
pub fn package(executable: &str) -> Result<PluginPackage, Box<dyn std::error::Error>> {
    Ok(rust_worker_package(
        schema()?,
        executable,
        Vec::<String>::new(),
    )?)
}

/// Installs the plugin into a registry directly (no worker), for in-process comparison.
pub fn install(registry: &mut PluginRegistry) -> Result<(), String> {
    registry
        .install(&ShowcasePlugin::new())
        .map_err(|err| err.to_string())
}
