//! Device nodes of the MCU blink example (`#![no_std]`, no `alloc`).
//!
//! The firmware crate depends on this crate twice: as a regular dependency (the node functions
//! run on the device) and as a build dependency (its build script reads each `<node>::NODE`
//! descriptor to plan the graph on the host).
#![no_std]

use daedalus_mcu::{Ctx, NodeState, node};

/// ADC counts to volts.
#[node(id = "blink.scale", inputs("raw", "volts_per_count"), outputs("volts"))]
pub fn scale(raw: f32, volts_per_count: f32) -> f32 {
    raw * volts_per_count
}

/// One-pole low-pass filter state.
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

/// Exponential smoothing: `y += alpha * (x - y)`, starting at the first sample.
#[node(
    id = "blink.lowpass",
    inputs("x", "alpha"),
    outputs("y"),
    state(Lowpass)
)]
pub fn lowpass(x: f32, alpha: f32, state: &mut Lowpass) -> f32 {
    if !state.primed {
        state.y = x;
        state.primed = true;
    }
    state.y += alpha * (x - state.y);
    state.y
}

/// Hysteresis comparator: on above `on`, off below `off`; the state is the current output.
#[node(
    id = "blink.threshold",
    inputs("level", "on", "off"),
    outputs("active"),
    state(bool)
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

/// Emits the number of rising edges so far, only on a rising edge (a conditional output).
#[node(id = "blink.rising", inputs("active"), outputs("count"), state(Edges))]
pub fn rising(active: bool, state: &mut Edges) -> Option<u32> {
    let rose = active && !state.last;
    state.last = active;
    rose.then(|| {
        state.count += 1;
        state.count
    })
}

/// Blinks while active: on for `period` ticks, off for `period` ticks.
#[node(id = "blink.blink", inputs("active", "period"), outputs("led"))]
pub fn blink(ctx: &Ctx, active: bool, period: u32) -> bool {
    active && (ctx.tick / period.max(1)).is_multiple_of(2)
}
