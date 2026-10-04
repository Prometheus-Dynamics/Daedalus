//! `wasm32-unknown-unknown` runtime smoke module for the `embedded` preset without `threads`
//! (`scripts/ci.sh wasm`).
//!
//! Built as a `cdylib` with no imports; `scripts/wasm-smoke.mjs` instantiates it and calls
//! [`smoke`]. A fan-out graph (`x + 1` and `x * 2`, summed) runs a few ticks in each runtime mode:
//! `Parallel` and `Adaptive` must degrade to serial there, and timing must not touch the missing
//! OS clock. Any panic traps, failing the run.

use daedalus::{
    engine::{Engine, EngineConfig, RuntimeMode},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "smoke.inc", inputs("x"), outputs("y"))]
fn inc(x: i64) -> Result<i64, NodeError> {
    Ok(x + 1)
}

#[node(id = "smoke.double", inputs("x"), outputs("y"))]
fn double(x: i64) -> Result<i64, NodeError> {
    Ok(x * 2)
}

#[node(id = "smoke.add", inputs("a", "b"), outputs("sum"))]
fn add(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a + b)
}

#[plugin(id = "smoke", nodes(inc, double, add))]
struct Smoke;

/// Sum of the graph outputs over `ticks` ticks fed `0..ticks`.
fn run(mode: RuntimeMode, ticks: i64) -> i64 {
    let mut registry = PluginRegistry::new();
    let plugin = Smoke::new();
    registry.install(&plugin).expect("install");
    let inc = plugin.inc.alias("inc");
    let double = plugin.double.alias("double");
    let add = plugin.add.alias("add");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("x")
        .and_then(|b| b.try_node(&inc))
        .and_then(|b| b.try_node(&double))
        .and_then(|b| b.try_node(&add))
        .and_then(|b| b.try_connect("x", &inc.inputs.x))
        .and_then(|b| b.try_connect("x", &double.inputs.x))
        .and_then(|b| b.try_connect(&inc.outputs.y, &add.inputs.a))
        .and_then(|b| b.try_connect(&double.outputs.y, &add.inputs.b))
        .and_then(|b| b.try_connect(&add.outputs.sum, "sum"))
        .expect("wire")
        .build();
    let config = EngineConfig::default()
        .with_runtime_mode(mode)
        .with_pool_size(4);
    let mut host = Engine::new(config)
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");
    (0..ticks)
        .map(|x| {
            host.push("x", x);
            host.tick().expect("tick");
            host.take::<i64>("sum").expect("sum")
        })
        .sum()
}

/// 0 on success, else the 1-based index of the first mode with a wrong result.
#[unsafe(no_mangle)]
pub extern "C" fn smoke() -> i32 {
    const TICKS: i64 = 32;
    // sum of 3x + 1 over 0..TICKS
    let expected = 3 * TICKS * (TICKS - 1) / 2 + TICKS;
    [
        RuntimeMode::Serial,
        RuntimeMode::Parallel,
        RuntimeMode::Adaptive,
    ]
    .into_iter()
    .position(|mode| run(mode, TICKS) != expected)
    .map_or(0, |idx| idx as i32 + 1)
}
