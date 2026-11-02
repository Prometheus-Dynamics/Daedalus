//! The shared graph's expected outputs, computed by calling the shared node functions directly.
//! Every mode (the full runtime, compiled, loaded) must match them tick for tick.
#![allow(dead_code)]

use daedalus_mcu::NodeState;
use daedalus_mcu_shared::{Edges, Lowpass, clamp, lowpass, rising, scale, threshold};

/// The constants of `graph.json`.
pub const VOLTS_PER_COUNT: f32 = 0.0008;
pub const ALPHA: f32 = 0.25;
pub const ON: f32 = 2.0;
pub const OFF: f32 = 1.0;

/// The host outputs of one tick.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Frame {
    pub level: f32,
    pub active: bool,
    pub rises: Option<u32>,
}

/// The graph's node states, stepped by hand.
pub struct Reference {
    lowpass: Lowpass,
    active: bool,
    edges: Edges,
}

impl Reference {
    pub fn new() -> Self {
        Self {
            lowpass: Lowpass::INIT,
            active: <bool as NodeState>::INIT,
            edges: Edges::INIT,
        }
    }

    /// One tick with the raw ADC count `raw`.
    pub fn step(&mut self, raw: u16) -> Frame {
        let volts = scale(f32::from(raw), VOLTS_PER_COUNT);
        let level = clamp(lowpass(volts, ALPHA, &mut self.lowpass), None);
        let active = threshold(&level, ON, OFF, &mut self.active);
        Frame {
            level,
            active,
            rises: rising(active, &mut self.edges),
        }
    }
}

/// Low, a ramp above the `on` threshold, back down below `off`, and up again: two rising edges
/// in 200 ticks.
pub fn signal() -> impl Iterator<Item = u16> {
    (0..200u32).map(|t| match t % 100 {
        0..20 => 100,
        20..60 => 3500,
        _ => 400,
    })
}
