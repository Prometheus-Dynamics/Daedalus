//! Generated Rust checked against the node functions it calls.

#![cfg(feature = "source")]

#[path = "shared/source_fixture.rs"]
mod nodes;

#[allow(dead_code)]
mod generated {
    include!("fixtures/blink.rs");
}

#[allow(dead_code)]
mod mixed {
    include!("fixtures/mixed.rs");
}

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_cpu::Ctx;
use daedalus_cpu::chip::ChipError;
use generated::{EDGES, Graph, NODE_IDS};
use nodes::{ALPHA, Edges, Lowpass, OFF, ON, PER_COUNT, PERIOD};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// The module's type works in a `static`.
#[allow(dead_code)]
static IN_A_STATIC: Graph = Graph::new();

fn signal() -> impl Iterator<Item = u16> {
    (0..200_u32).map(|t| match t % 100 {
        0..20 => 100,
        20..60 => 3500,
        _ => 400,
    })
}

/// What one tick outputs, from the node functions called by hand.
#[derive(Default)]
struct Reference {
    lowpass: Lowpass,
    active: bool,
    edges: Edges,
}

impl Reference {
    fn step(&mut self, raw: u16, now: u64) -> (f32, bool, Option<u32>) {
        let volts = nodes::scale(raw, PER_COUNT);
        let level = nodes::lowpass(volts, ALPHA, &mut self.lowpass);
        let active = nodes::threshold(&level, ON, OFF, &mut self.active);
        let rose = nodes::rising(active, &mut self.edges);
        let mut cx = Ctx::new();
        cx.now_ns = now;
        (level, nodes::blink(&cx, active, PERIOD), rose)
    }
}

#[test]
fn the_module_follows_the_graph() {
    assert_eq!(
        NODE_IDS,
        [
            "blink.scale",
            "blink.lowpass",
            "blink.threshold",
            "blink.rising",
            "blink.blink"
        ]
    );
    let source = include_str!("fixtures/blink.rs");
    assert!(source.contains("0.25_f32"), "constants are literals");
    assert!(
        source.contains("let mut l0: Option<f32> = None;"),
        "a merged edge is a local"
    );
    assert_eq!(EDGES.len(), 7, "seven queues, and one edge merged away");
}

#[test]
fn the_module_matches_the_node_functions_without_allocating() {
    let mut graph = Box::new(Graph::new());
    let mut reference = Reference::default();
    let (mut rises, mut led_on) = (0, 0);
    for (tick, raw) in signal().enumerate() {
        let now = tick as u64;
        let (outputs, allocated) = counted(|| {
            graph.push_sample(raw).unwrap();
            graph.tick_at(now).unwrap();
            (graph.pop_level(), graph.pop_led(), graph.pop_rises())
        });
        assert_eq!(allocated, 0, "tick {tick} allocated");
        let (level, led, rose) = reference.step(raw, now);
        assert_eq!(outputs, (Some(level), Some(led), rose), "tick {tick}");
        rises += usize::from(rose.is_some());
        led_on += usize::from(led);
    }
    assert_eq!(rises, 2);
    assert!(led_on > 0);
}

#[test]
fn edge_policies_shape_the_queues() {
    let mut graph = Box::new(Graph::new());
    // `sample` keeps only the latest, so the second push replaces the first.
    graph.push_sample(4000).unwrap();
    graph.push_sample(0).unwrap();
    graph.tick().unwrap();
    assert_eq!(graph.pop_level(), Some(0.0));

    // `rises` holds 8 and drops the oldest. Toggle 10 times without popping it.
    for _ in 0..10 {
        for raw in [4000, 0] {
            for _ in 0..8 {
                graph.push_sample(raw).unwrap();
                graph.tick().unwrap();
                graph.pop_led();
            }
        }
    }
    let kept: Vec<u32> = std::iter::from_fn(|| graph.pop_rises()).collect();
    assert_eq!(kept, (3..=10).collect::<Vec<_>>());

    // With no new sample nothing runs and the level stays where it was.
    let _ = graph.pop_level();
    graph.tick().unwrap();
    assert_eq!(graph.pop_level(), None);
}

#[test]
fn two_port_names_that_become_one_identifier_fail_by_name() {
    let error = nodes::generate(&nodes::clashing()).unwrap_err().to_string();
    assert!(
        error.contains("`a_b`") && error.contains("`a-b`"),
        "{error}"
    );
}

#[test]
fn the_output_is_repeatable_and_plain() {
    let graph = nodes::graph();
    let (first, second) = (
        nodes::generate(&graph).unwrap(),
        nodes::generate(&graph).unwrap(),
    );
    assert_eq!(first, second);
    assert!(!first.contains('\r'));
    assert!(!first.contains(env!("CARGO_MANIFEST_DIR")), "no host paths");
}

#[test]
fn every_kind_of_parameter_is_called_correctly() {
    let mut graph = Box::new(mixed::Graph::new());
    graph.push_a(1).unwrap();
    graph.push_b(1).unwrap();
    graph.tick().unwrap();
    // Host input `a` feeds both `wrap`s, `bump` gives 2, the optional input has no value so
    // `fallback` gives 9, and the fan-in adds all four.
    assert_eq!((graph.pop_low(), graph.pop_high()), (Some(13), Some(14)));
    // The optional input alone is enough for `fallback`, and any input is enough for the fan-in.
    graph.push_maybe(6).unwrap();
    graph.tick().unwrap();
    assert_eq!((graph.pop_low(), graph.pop_high()), (Some(6), Some(7)));
}

#[test]
fn a_failing_node_stops_the_tick_and_is_named() {
    let mut graph = Box::new(mixed::Graph::new());
    graph.push_a(100).unwrap();
    graph.push_b(100).unwrap();
    let error = graph.tick().unwrap_err();
    let ChipError::Node { node, code } = error else {
        panic!("{error:?}");
    };
    assert_eq!(mixed::NODE_IDS[usize::from(node)], "mix.check");
    assert_eq!(code, 310);
}

#[test]
fn a_full_error_queue_is_reported_with_its_edge() {
    let mut graph = Box::new(Graph::new());
    let mut stopped = None;
    for tick in 0..8 {
        graph.push_sample(100).unwrap();
        // Nothing pops `led`, whose edge holds the default of four.
        if let Err(error) = graph.tick_at(tick) {
            stopped = Some(error);
            break;
        }
    }
    let Some(ChipError::QueueFull { edge }) = stopped else {
        panic!("{stopped:?}");
    };
    assert!(
        EDGES[usize::from(edge)].ends_with("host.led"),
        "{}",
        EDGES[usize::from(edge)]
    );
}

#[test]
fn the_plan_call_sets_the_fifo_capacity() {
    let text = nodes::generate_with(&nodes::graph(), 2).unwrap();
    assert!(text.contains("Queue<f32, 2>"), "{text}");
}

#[test]
fn separate_graphs_cant_share_a_module() {
    let error = nodes::generate(&nodes::disconnected()).unwrap_err();
    assert!(error.to_string().contains("Source"), "{error}");
}
