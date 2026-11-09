//! The graph's expected outputs, from the node functions called by hand. Every mode has to match
//! them tick for tick.
#![allow(dead_code)]

use daedalus_chip_shared::{Edges, Lowpass, clamp, lowpass, rising, scale, threshold};

/// What one tick puts on `level`, `active` and `rises`.
pub type Out = (Option<f32>, Option<bool>, Option<u32>);

#[derive(Default)]
pub struct Reference {
    lowpass: Lowpass,
    active: bool,
    edges: Edges,
}

impl Reference {
    pub fn step(&mut self, raw: u16) -> Out {
        let volts = scale(raw, 0.0008);
        let level = clamp(lowpass(volts, 0.25, &mut self.lowpass), None);
        let active = threshold(&level, 2.0, 1.0, &mut self.active);
        (Some(level), Some(active), rising(active, &mut self.edges))
    }
}

/// Low, a step above the `on` threshold, down below `off`, and up again: two rising edges.
pub fn signal() -> impl Iterator<Item = u16> {
    (0..200_u32).map(|t| match t % 100 {
        0..20 => 100,
        20..60 => 3500,
        _ => 400,
    })
}
