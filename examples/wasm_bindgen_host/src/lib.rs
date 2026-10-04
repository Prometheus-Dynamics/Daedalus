//! `wasm-bindgen` host glue for the `embedded` preset without `threads` on
//! `wasm32-unknown-unknown`: the engine's [`Clock`] reads the host's `performance.now()`, and
//! JavaScript drives a small graph (`y = gain * x + offset`) through [`Pipeline`]'s
//! `push`/`tick`/`take`.
//!
//! ```js
//! const { Pipeline } = require("./daedalus_wasm_bindgen_host.js"); // wasm-bindgen --target nodejs
//! const pipeline = new Pipeline(2, 1);
//! pipeline.push(20);
//! const ms = pipeline.tick(); // graph duration on the performance.now() clock
//! pipeline.take(); // 41
//! ```
//!
//! `scripts/wasm-bindgen-host.mjs` is the Node driver `scripts/ci.sh wasm` runs.

use core::time::Duration;

use daedalus::{
    data::model::Value,
    engine::{Clock, Engine, EngineConfig, HostGraph, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
    transport::FeedOutcome,
};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    /// The host's monotonic clock, in milliseconds (`performance.now()` in browsers and Node).
    #[wasm_bindgen(js_namespace = performance, js_name = now)]
    fn performance_now() -> f64;
}

/// Time since the host clock's origin.
fn host_now() -> Duration {
    Duration::from_secs_f64(performance_now() / 1e3)
}

#[node(id = "host.scale", inputs("x", "gain"), outputs("y"))]
fn scale(x: f64, gain: f64) -> Result<f64, NodeError> {
    Ok(x * gain)
}

#[node(id = "host.offset", inputs("x", "offset"), outputs("y"))]
fn offset(x: f64, offset: f64) -> Result<f64, NodeError> {
    Ok(x + offset)
}

#[plugin(id = "host", nodes(scale, offset))]
struct HostNodes;

fn js_error(error: impl core::fmt::Display) -> JsError {
    JsError::new(&error.to_string())
}

/// A compiled `x -> scale -> offset -> y` graph, driven from JavaScript.
#[wasm_bindgen]
pub struct Pipeline {
    host: HostGraph<HandlerRegistry>,
}

#[wasm_bindgen]
impl Pipeline {
    /// Compiles the graph with the given constants, timed by `performance.now()`.
    #[wasm_bindgen(constructor)]
    pub fn new(gain: f64, offset: f64) -> Result<Pipeline, JsError> {
        // Payload lineage and host-bridge events have no engine at hand: they read the
        // process-wide fallback clock (only the first call installs it), which exists where the
        // target has no OS clock.
        #[cfg(all(target_family = "wasm", target_os = "unknown"))]
        daedalus::core::platform::set_clock(host_now);
        let mut registry = PluginRegistry::new();
        let plugin = HostNodes::new();
        registry.install(&plugin).map_err(js_error)?;
        let scale = plugin.scale.alias("scale");
        let shift = plugin.offset.alias("offset");
        let graph = registry
            .graph_builder()
            .map_err(js_error)?
            .input_typed::<f64>("x")
            .and_then(|b| b.try_node(&scale))
            .and_then(|b| b.try_node(&shift))
            .and_then(|b| b.try_connect("x", &scale.inputs.x))
            .and_then(|b| b.try_connect(&scale.outputs.y, &shift.inputs.x))
            .and_then(|b| b.try_connect(&shift.outputs.y, "y"))
            .map_err(js_error)?
            .const_input(&scale.inputs.gain, Some(Value::Float(gain)))
            .const_input(&shift.inputs.offset, Some(Value::Float(offset)))
            .build();
        let config = EngineConfig::default()
            .with_metrics_level(MetricsLevel::Basic)
            .with_clock(Clock::new(host_now));
        let host = Engine::new(config)
            .map_err(js_error)?
            .compile_registry(&registry, graph)
            .map_err(js_error)?;
        Ok(Self { host })
    }

    /// Queues `x` on the graph input; `false` if the input refused it.
    pub fn push(&self, x: f64) -> bool {
        matches!(
            self.host.push("x", x),
            FeedOutcome::Accepted { .. } | FeedOutcome::Replaced { .. }
        )
    }

    /// Runs one tick; returns its graph duration in milliseconds on the host clock.
    pub fn tick(&mut self) -> Result<f64, JsError> {
        let telemetry = self.host.tick().map_err(js_error)?;
        Ok(telemetry.graph_duration.as_secs_f64() * 1e3)
    }

    /// The next output value, if a tick produced one.
    pub fn take(&self) -> Option<f64> {
        self.host.take::<f64>("y")
    }
}
