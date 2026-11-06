//! A node whose three outputs are fused edges into the next node (the shape of Eidos's
//! `tracked_detect`): on the Serial executor a warm tick allocates nothing. The fused payloads
//! wait in the run's carry until the consumer runs; a carry that spilled its inline capacity
//! allocated on every such tick.
//!
//! Counts heap allocations made on the calling thread, so it is robust against other tests running
//! in parallel. The Serial executor runs there; a Parallel tick of a graph this small does too.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use daedalus::{
    engine::{Engine, EngineConfig, HostGraph, MetricsLevel, RuntimeMode},
    macros::{node, plugin},
    runtime::{
        NodeError,
        handler_registry::HandlerRegistry,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

struct CountingAlloc;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

#[node(id = "split", inputs("x"), outputs("a", "b", "c"))]
fn split(x: i64) -> Result<(i64, i64, i64), NodeError> {
    Ok((x, x * 2, x * 3))
}

#[node(id = "join", inputs("a", "b", "c"), outputs("sum"))]
fn join(a: i64, b: i64, c: i64) -> Result<i64, NodeError> {
    Ok(a + b + c)
}

#[plugin(id = "test.carry", nodes(split, join))]
struct CarryPlugin;

fn compile(mode: RuntimeMode, metrics: MetricsLevel) -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&CarryPlugin::new()).unwrap();
    let graph = registry
        .graph_builder()
        .unwrap()
        .node_id("test.carry:split", "split")
        .node_id("test.carry:join", "join")
        .connect("x", "split.x")
        .connect("split.a", "join.a")
        .connect("split.b", "join.b")
        .connect("split.c", "join.c")
        .connect("join.sum", "sum")
        .build();
    let config = EngineConfig::default()
        .with_runtime_mode(mode)
        .with_metrics_level(metrics)
        .with_host_event_recording(false);
    let mut host = Engine::new(config)
        .unwrap()
        .compile_registry(&registry, graph)
        .unwrap();
    host.prepare().unwrap();
    host
}

fn tick(host: &mut HostGraph<HandlerRegistry>, x: i64) -> Option<i64> {
    host.push("x", x);
    let telemetry = host.tick().expect("tick");
    host.recycle_telemetry(telemetry);
    host.take::<i64>("sum")
}

#[test]
fn three_fused_edges_into_one_node_allocate_nothing_per_tick() {
    for (mode, metrics) in [
        (RuntimeMode::Serial, MetricsLevel::Off),
        (RuntimeMode::Serial, MetricsLevel::Basic),
        (RuntimeMode::Serial, MetricsLevel::Detailed),
        (RuntimeMode::Parallel, MetricsLevel::Off),
    ] {
        let mut host = compile(mode.clone(), metrics);
        let fused = host
            .explain_plan()
            .edges
            .iter()
            .filter(|edge| edge.fused)
            .count();
        assert_eq!(fused, 3, "split's three outputs fuse into join");
        for x in 0..8 {
            assert_eq!(tick(&mut host, x), Some(x * 6));
        }
        let before = ALLOCATIONS.with(Cell::get);
        for x in 0..32 {
            assert_eq!(tick(&mut host, x), Some(x * 6));
        }
        let allocations = ALLOCATIONS.with(Cell::get) - before;
        assert_eq!(
            allocations, 0,
            "{mode:?}/{metrics:?}: allocations in 32 warm ticks"
        );
    }
}
