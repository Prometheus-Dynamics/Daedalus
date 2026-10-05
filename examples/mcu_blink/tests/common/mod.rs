//! Shared by the native tests: an allocation counter and the blink graph computed by calling
//! the node functions by hand.
#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use daedalus_mcu::{Ctx, NodeState};
use daedalus_mcu_blink::nodes;

/// Counts this thread's allocations (tests run on separate threads).
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System.alloc` with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

pub fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// Low, a ramp above the `on` threshold, back down below `off`, and up again.
pub fn signal() -> impl Iterator<Item = u16> {
    (0..200u32).map(|t| match t % 100 {
        0..20 => 100,
        20..60 => 3500,
        _ => 400,
    })
}

/// The tunable constants of the blink graph.
#[derive(Clone, Copy)]
pub struct Params {
    pub alpha: f32,
    pub on: f32,
    pub off: f32,
    pub period: u32,
}

impl Params {
    /// The constants of `graph.json`.
    pub const A: Self = Self {
        alpha: 0.25,
        on: 2.0,
        off: 1.0,
        period: 4,
    };
}

/// What one tick outputs, `(level, led, rises)`, from the node functions called by hand.
pub struct Reference {
    tick: u32,
    lowpass: nodes::Lowpass,
    active: bool,
    edges: nodes::Edges,
    pub params: Params,
    /// `graph.json` (low-pass filtered) or `graph_b.json` (unfiltered, period 2).
    filtered: bool,
}

impl Reference {
    /// `graph.json`.
    pub fn a() -> Self {
        Self::new(Params::A, true)
    }

    /// `graph_b.json`.
    pub fn b() -> Self {
        Self::new(
            Params {
                period: 2,
                ..Params::A
            },
            false,
        )
    }

    fn new(params: Params, filtered: bool) -> Self {
        Self {
            tick: 0,
            lowpass: NodeState::INIT,
            active: false,
            edges: NodeState::INIT,
            params,
            filtered,
        }
    }

    pub fn step(&mut self, raw: u16) -> (f32, bool, Option<u32>) {
        let p = self.params;
        let ctx = Ctx {
            tick: self.tick,
            now_micros: 0,
        };
        self.tick += 1;
        let volts = nodes::scale(f32::from(raw), 0.0008);
        let level = if self.filtered {
            nodes::lowpass(volts, p.alpha, &mut self.lowpass)
        } else {
            volts
        };
        let active = nodes::threshold(&level, p.on, p.off, &mut self.active);
        let led = nodes::blink(&ctx, active, p.period);
        (level, led, nodes::rising(active, &mut self.edges))
    }
}
