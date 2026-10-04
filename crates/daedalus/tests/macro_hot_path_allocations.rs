//! Macro-generated handlers resolve their output keys once per registry, not per push: a typed
//! `#[node]` allocates no more per round trip than a low-level node pushing prebuilt static keys.
//!
//! Counts heap allocations made on the calling thread, so it is robust against other tests
//! running in parallel.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use daedalus::{
    data::model::{TypeExpr, ValueType},
    engine::{Engine, EngineConfig, HostGraph},
    macros::{node, plugin},
    runtime::{
        NodeError, RuntimeNode,
        executor::MetricsLevel,
        handler_registry::HandlerRegistry,
        io::NodeIo,
        plugins::{PluginRegistry, RegistryPluginExt},
        state::ExecutionContext,
    },
    transport::{Payload, TypeKey},
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

fn allocations_during(f: impl FnOnce()) -> usize {
    let before = ALLOCATIONS.with(Cell::get);
    f();
    ALLOCATIONS.with(Cell::get) - before
}

const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";
const GRAY_KEY: &str = "test:alloc:gray";

/// A mapped foreign type (no key of its own) and a builtin, through the regular handler path.
#[node(id = "typed", inputs("x"), outputs("frame", "next"))]
fn typed(x: i64) -> Result<(image::GrayImage, i64), NodeError> {
    Ok((image::GrayImage::new(0, 0), x + 1))
}

/// The same node written by hand with static keys and ports.
#[node(
    id = "manual",
    inputs(port(name = "x", ty = TypeExpr::Scalar(ValueType::Int))),
    outputs(
        port(name = "frame", type_key = "test:alloc:gray"),
        port(name = "next", ty = TypeExpr::Scalar(ValueType::Int))
    )
)]
fn manual(_node: &RuntimeNode, _ctx: &ExecutionContext, io: &mut NodeIo) -> Result<(), NodeError> {
    let x = io
        .take_owned::<i64>("x")
        .ok_or_else(|| NodeError::InvalidInput("missing x".into()))?;
    io.push_as_to(
        "frame",
        TypeKey::from_static(GRAY_KEY),
        image::GrayImage::new(0, 0),
    );
    io.push_as_to("next", TypeKey::from_static(INT_KEY), x + 1);
    Ok(())
}

#[plugin(
    id = "test.alloc",
    foreign_types(image::GrayImage = "test:alloc:gray"),
    nodes(typed, manual)
)]
struct AllocPlugin;

fn compile(node_id: &str) -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&AllocPlugin::new()).unwrap();
    let node = daedalus::NodeHandle::new(format!("test.alloc:{node_id}")).alias("node");
    let graph = registry
        .graph_builder()
        .unwrap()
        .try_node(&node)
        .and_then(|b| b.try_connect("x", &node.input("x")))
        .and_then(|b| b.try_connect(&node.output("frame"), "frame"))
        .and_then(|b| b.try_connect(&node.output("next"), "next"))
        .unwrap()
        .build();
    let host = Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Off))
        .unwrap()
        .compile_registry(&registry, graph)
        .unwrap();
    host.host().set_event_recording(false);
    host
}

fn round_trip(host: &mut HostGraph<HandlerRegistry>, x: i64) -> Option<i64> {
    host.push_payload("x", Payload::owned(INT_KEY, x));
    host.tick().expect("tick");
    let frame = host.take_payload("frame")?;
    assert_eq!(frame.type_key().as_str(), GRAY_KEY);
    host.take::<i64>("next")
}

/// Allocations of one warmed-up round trip through `node_id`.
fn round_trip_allocations(node_id: &str) -> usize {
    let mut host = compile(node_id);
    for x in 0..4 {
        assert_eq!(round_trip(&mut host, x), Some(x + 1));
    }
    allocations_during(|| {
        assert_eq!(round_trip(&mut host, 41), Some(42));
    })
}

#[test]
fn typed_macro_nodes_allocate_no_more_than_hand_written_ones() {
    let manual = round_trip_allocations("manual");
    let typed = round_trip_allocations("typed");
    assert!(
        typed <= manual,
        "typed #[node] round trip allocated {typed} times, a hand-written node {manual}"
    );
}
