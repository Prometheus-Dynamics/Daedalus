//! Runs the generated blink plan natively (the same code the firmware runs) and checks its
//! outputs against the node functions called by hand, and that pushes, ticks and pops never
//! allocate.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use daedalus_mcu::{Ctx, NodeState};
use daedalus_mcu_blink::graph::{EDGES, NODE_IDS};
use daedalus_mcu_blink::{Graph, nodes};

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

fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// What one tick should output, computed by calling the node functions directly.
struct Reference {
    tick: u32,
    lowpass: nodes::Lowpass,
    active: bool,
    edges: nodes::Edges,
}

impl Reference {
    fn new() -> Self {
        Self {
            tick: 0,
            lowpass: NodeState::INIT,
            active: false,
            edges: NodeState::INIT,
        }
    }

    fn step(&mut self, raw: u16) -> (f32, bool, Option<u32>) {
        let ctx = Ctx {
            tick: self.tick,
            now_micros: 0,
        };
        self.tick += 1;
        let volts = nodes::scale(f32::from(raw), 0.0008);
        let level = nodes::lowpass(volts, 0.25, &mut self.lowpass);
        let active = nodes::threshold(&level, 2.0, 1.0, &mut self.active);
        let led = nodes::blink(&ctx, active, 4);
        (level, led, nodes::rising(active, &mut self.edges))
    }
}

#[test]
fn plan_follows_the_graph_document() {
    assert_eq!(
        NODE_IDS,
        ["scale", "lowpass", "threshold", "blink", "rising"]
    );
    assert_eq!(EDGES.len(), 8);
}

#[test]
fn graph_matches_the_node_functions_without_allocating() {
    let mut graph = Graph::new();
    let mut reference = Reference::new();
    // Low, a ramp above the `on` threshold, back down below `off`, and up again.
    let signal = (0..200u32).map(|t| match t % 100 {
        0..20 => 100,
        20..60 => 3500,
        _ => 400,
    });
    let before = allocations();
    let (mut rises, mut led_on) = (0, 0);
    for raw in signal {
        graph.push_sample(raw as u16).unwrap();
        graph.tick().unwrap();
        let (level, led, rose) = reference.step(raw as u16);
        assert_eq!(graph.pop_level(), Some(level));
        assert_eq!(graph.pop_led(), Some(led));
        assert_eq!(graph.pop_rises(), rose);
        rises += usize::from(rose.is_some());
        led_on += usize::from(led);
    }
    assert_eq!(
        allocations(),
        before,
        "pushes, ticks and pops allocate nothing"
    );
    assert_eq!(rises, 2);
    assert!(led_on > 0);
}

#[test]
fn edge_policies_shape_the_queues() {
    let mut graph = Graph::new();
    // `sample` is latest-only: the second push replaces the first, which primes the filter.
    graph.push_sample(4000).unwrap();
    graph.push_sample(0).unwrap();
    graph.tick().unwrap();
    assert_eq!(graph.pop_level(), Some(0.0));

    // `rises` is bounded to 8 (drop oldest): toggle 10 times without popping it.
    for _ in 0..10 {
        for raw in [4000, 0] {
            for _ in 0..8 {
                graph.push_sample(raw).unwrap();
                graph.tick().unwrap();
            }
        }
    }
    let kept: Vec<u32> = std::iter::from_fn(|| graph.pop_rises()).collect();
    assert_eq!(kept, (3..=10).collect::<Vec<_>>());
    // With no new sample, nothing runs: the filter output stays where it was.
    let _ = graph.pop_level();
    graph.tick().unwrap();
    assert_eq!(graph.pop_level(), None);
}
