//! The chip blink nodes. No `std`, no allocator.

#![no_std]
#![forbid(unsafe_code)]

use daedalus_cpu_author::Ctx;
use daedalus_macros::node;
use daedalus_registry::NodeRecord;

/// ADC counts to volts.
#[node(id = "blink.scale", inputs(raw, volts_per_count), outputs(volts))]
pub fn scale(raw: u16, volts_per_count: f32) -> f32 {
    f32::from(raw) * volts_per_count
}

#[derive(Default)]
pub struct Lowpass {
    y: f32,
    primed: bool,
}

/// Exponential smoothing, starting at the first sample.
#[node(id = "blink.lowpass", inputs(x, alpha), outputs(y), state(Lowpass))]
pub fn lowpass(x: f32, alpha: f32, state: &mut Lowpass) -> f32 {
    if !state.primed {
        state.y = x;
        state.primed = true;
    }
    state.y += alpha * (x - state.y);
    state.y
}

/// On above `on`, off below `off`.
#[node(
    id = "blink.threshold",
    inputs(level, on, off),
    outputs(active),
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

#[derive(Default)]
pub struct Edges {
    last: bool,
    count: u32,
}

/// The number of rising edges so far, sent only on a rising edge.
#[node(id = "blink.rising", inputs(active), outputs(count), state(Edges))]
pub fn rising(active: bool, state: &mut Edges) -> Option<u32> {
    let rose = active && !state.last;
    state.last = active;
    rose.then(|| {
        state.count += 1;
        state.count
    })
}

/// While active: on for `period` time units, off for `period`. The firmware's tick is the unit.
#[node(id = "blink.blink", inputs(active, period), outputs(led))]
pub fn blink(ctx: &Ctx, active: bool, period: u32) -> bool {
    active && (ctx.now_ns as u32 / period.max(1)).is_multiple_of(2)
}

/// Every node, for planning on a host.
pub const NODES: [&NodeRecord; 5] = [
    &SCALE_NODE,
    &LOWPASS_NODE,
    &THRESHOLD_NODE,
    &RISING_NODE,
    &BLINK_NODE,
];
