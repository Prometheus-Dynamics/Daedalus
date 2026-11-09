//! One function per node, declared once, for a full host and for a chip. Nothing here changes
//! between the two: the `host` feature only adds what the tests need to run it under a full host,
//! and without it the crate is `no_std` and doesn't name the facade.
//!
//! `graph.json` wires them: `host.sample` -> `scale` -> `lowpass` -> `clamp` -> `threshold` ->
//! `rising`, with the host outputs `level`, `active` and `rises`.

#![cfg_attr(not(feature = "host"), no_std)]
#![forbid(unsafe_code)]

use daedalus_cpu::blob::{Entry, Library};
use daedalus_macros::node;
use daedalus_registry::NodeRecord;

/// ADC counts to volts.
#[node(id = "shared.scale", inputs(raw, volts_per_count), outputs(volts))]
pub fn scale(raw: u16, volts_per_count: f32) -> f32 {
    f32::from(raw) * volts_per_count
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Lowpass {
    y: f32,
    primed: bool,
}

/// Exponential smoothing, starting at the first sample.
#[node(id = "shared.lowpass", inputs(x, alpha), outputs(y), state(Lowpass))]
pub fn lowpass(x: f32, alpha: f32, state: &mut Lowpass) -> f32 {
    if !state.primed {
        state.y = x;
        state.primed = true;
    }
    state.y += alpha * (x - state.y);
    state.y
}

/// Caps `value` at `ceiling`, if one is connected.
#[node(id = "shared.clamp", inputs(value, ceiling), outputs(y))]
pub fn clamp(value: f32, ceiling: Option<f32>) -> f32 {
    match ceiling {
        Some(ceiling) if value > ceiling => ceiling,
        _ => value,
    }
}

/// On above `on`, off below `off`.
#[node(
    id = "shared.threshold",
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

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Edges {
    last: bool,
    count: u32,
}

/// The number of rising edges so far, sent only on a rising edge.
#[node(id = "shared.rising", inputs(active), outputs(count), state(Edges))]
pub fn rising(active: bool, state: &mut Edges) -> Option<u32> {
    let rose = active && !state.last;
    state.last = active;
    rose.then(|| {
        state.count += 1;
        state.count
    })
}

/// Every node: for a host's registry, for planning, and for a chip's stored plans.
pub const NODES: [&NodeRecord; 5] = [
    &SCALE_NODE,
    &LOWPASS_NODE,
    &CLAMP_NODE,
    &THRESHOLD_NODE,
    &RISING_NODE,
];

/// What a stored plan names its nodes in.
pub static LIBRARY: Library<[Entry; 5]> = Library::new(&NODES);
