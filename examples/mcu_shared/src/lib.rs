//! Shared nodes (docs/mcu.md, "Shared Nodes"): each function is a device node for `daedalus-mcu`
//! and, with the `runtime` feature, a full-runtime `#[node]` with the same id, ports, types, state
//! and fire mode, from the same function. The device build is `no_std` and does not name the
//! runtime.
//!
//! Ids start with the plugin id and a `:` (`shared:scale`): the full runtime prefixes a plugin's
//! node ids with its id unless they already start with it, so the same id names the node in both
//! planners.
//!
//! `graph.json` wires them the same way in both: `host.sample` (u16) -> `scale` -> `lowpass` ->
//! `clamp` -> `threshold` -> `rising`, with the host outputs `level`, `active` and `rises`.

#![cfg_attr(not(feature = "runtime"), no_std)]

use daedalus_mcu::{NodeState, node};

/// ADC counts to volts.
#[node(
    id = "shared:scale",
    inputs("raw", "volts_per_count"),
    outputs("volts"),
    runtime = "runtime"
)]
pub fn scale(raw: f32, volts_per_count: f32) -> f32 {
    raw * volts_per_count
}

/// Exponential smoothing state: the output and whether a first sample arrived.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Lowpass {
    y: f32,
    primed: bool,
}

impl NodeState for Lowpass {
    const INIT: Self = Lowpass {
        y: 0.0,
        primed: false,
    };
}

/// `y += alpha * (x - y)`, starting at the first sample.
#[node(
    id = "shared:lowpass",
    inputs("x", "alpha"),
    outputs("y"),
    state(Lowpass),
    runtime = "runtime"
)]
pub fn lowpass(x: f32, alpha: f32, state: &mut Lowpass) -> f32 {
    if !state.primed {
        state.y = x;
        state.primed = true;
    }
    state.y += alpha * (x - state.y);
    state.y
}

/// Caps `value` at `ceiling` when one is connected; an unconnected optional input never blocks.
#[node(
    id = "shared:clamp",
    inputs("value", "ceiling"),
    outputs("y"),
    runtime = "runtime"
)]
pub fn clamp(value: f32, ceiling: Option<f32>) -> f32 {
    match ceiling {
        Some(ceiling) if value > ceiling => ceiling,
        _ => value,
    }
}

/// Hysteresis comparator: on above `on`, off below `off`; the state is the current output.
#[node(
    id = "shared:threshold",
    inputs("level", "on", "off"),
    outputs("active"),
    state(bool),
    runtime = "runtime"
)]
pub fn threshold(level: &f32, on: f32, off: f32, state: &mut bool) -> bool {
    if *level >= on {
        *state = true;
    } else if *level <= off {
        *state = false;
    }
    *state
}

/// Rising-edge counter state: the previous input and the count so far.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Edges {
    last: bool,
    count: u32,
}

impl NodeState for Edges {
    const INIT: Self = Edges {
        last: false,
        count: 0,
    };
}

/// The number of rising edges so far, emitted only on a rising edge (a conditional output).
#[node(
    id = "shared:rising",
    inputs("active"),
    outputs("count"),
    state(Edges),
    runtime = "runtime"
)]
pub fn rising(active: bool, state: &mut Edges) -> Option<u32> {
    let rose = active && !state.last;
    state.last = active;
    rose.then(|| {
        state.count += 1;
        state.count
    })
}

/// The full runtime's plugin of the shared nodes.
#[cfg(feature = "runtime")]
#[daedalus::macros::plugin(id = "shared", nodes(scale, lowpass, clamp, threshold, rising))]
pub struct SharedPlugin;
