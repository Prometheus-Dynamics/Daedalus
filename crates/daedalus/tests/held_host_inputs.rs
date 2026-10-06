//! Context inputs: held host ports keep their last value across ticks, and batched pushes land
//! in one tick whole, so a frame and its context never split across ticks.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::thread;
use std::time::Duration;

use daedalus::{
    GraphDocument,
    engine::{Engine, EngineConfig, HostGraph, HostGraphDriveExit, InboundWait, MetricsLevel},
    macros::{node, plugin},
    planner::Graph,
    runtime::{
        HOST_HELD_INPUTS_KEY, NodeError, PortId, handler_registry::HandlerRegistry,
        plugins::PluginRegistry,
    },
    transport::{FeedOutcome, Payload, TypeKeyError},
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

/// Heap allocations made on this thread while `f` runs.
fn allocations_during<R>(f: impl FnOnce() -> R) -> (usize, R) {
    let before = ALLOCATIONS.with(Cell::get);
    let result = f();
    (ALLOCATIONS.with(Cell::get) - before, result)
}

/// Takes the context by value: the planner branches (copies) a held input for it.
#[node(id = "held.fuse", inputs("frame", "imu"), outputs("out"))]
fn fuse(frame: i64, imu: i64) -> Result<i64, NodeError> {
    Ok(frame * 1000 + imu)
}

/// Borrows the context: the held value reaches it as an `Arc` clone.
#[node(id = "held.join", inputs("frame", "imu"), outputs("out"), fire = "all")]
fn join(frame: i64, imu: &i64) -> Result<i64, NodeError> {
    Ok(frame * 1000 + imu)
}

/// `frame` when the context pushed with it arrived in the same tick, else `-frame`.
#[node(id = "held.pair", inputs("frame", "imu"), outputs("out"))]
fn pair(frame: i64, imu: Option<i64>) -> Result<i64, NodeError> {
    Ok(if imu == Some(frame) { frame } else { -frame })
}

const TAG_KEY: &str = "test:held:tag";

#[daedalus::type_key(TAG_KEY)]
struct Tag;

/// Registers `Tag`'s Rust type under `TAG_KEY`, so payloads under that key are type checked.
#[node(id = "held.tag", inputs("tag"), outputs("out"))]
fn tag(_tag: &Tag) -> Result<i64, NodeError> {
    Ok(0)
}

#[plugin(id = "held", nodes(fuse, join, pair, tag))]
struct HeldPlugin;

/// `frame` and `ctx` host inputs into `node` (`fuse`, `join` or `pair`), its output to `out`;
/// `held` declares `ctx` held in the graph.
fn graph(node: &str, held: bool) -> (PluginRegistry, Graph) {
    let mut registry = PluginRegistry::new();
    let plugin = HeldPlugin::new();
    registry.install(&plugin).expect("install");
    let (fuse, join, pair) = (
        plugin.fuse.alias("node"),
        plugin.join.alias("node"),
        plugin.pair.alias("node"),
    );
    let (frame, ctx, out) = match node {
        "fuse" => (&fuse.inputs.frame, &fuse.inputs.imu, &fuse.outputs.out),
        "join" => (&join.inputs.frame, &join.inputs.imu, &join.outputs.out),
        _ => (&pair.inputs.frame, &pair.inputs.imu, &pair.outputs.out),
    };
    let mut builder = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("frame")
        .and_then(|b| b.input_typed::<i64>("ctx"))
        .and_then(|b| match node {
            "fuse" => b.try_node(&fuse),
            "join" => b.try_node(&join),
            _ => b.try_node(&pair),
        })
        .and_then(|b| b.try_connect("frame", frame))
        .and_then(|b| b.try_connect("ctx", ctx))
        .and_then(|b| b.try_connect(out, "out"))
        .expect("wire");
    if held {
        builder = builder.held_input("ctx");
    }
    (registry, builder.build())
}

fn compile(node: &str, held: bool) -> HostGraph<HandlerRegistry> {
    let (registry, graph) = graph(node, held);
    Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Off))
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile")
}

fn frame_out(host: &mut HostGraph<HandlerRegistry>, frame: i64) -> Option<i64> {
    host.push("frame", frame);
    host.tick().expect("tick");
    host.take::<i64>("out")
}

#[test]
fn held_context_reaches_every_tick_until_replaced_or_cleared() {
    let mut host = compile("fuse", true);
    assert!(host.host().is_input_held("ctx"));
    let ctx = host.bind_input::<i64>("ctx").expect("typed ctx");
    assert!(matches!(ctx.push(7), FeedOutcome::Accepted { .. }));
    assert_eq!(frame_out(&mut host, 1), Some(1007));
    assert_eq!(frame_out(&mut host, 2), Some(2007), "no re-push needed");
    assert!(matches!(ctx.push(8), FeedOutcome::Replaced { .. }));
    assert_eq!(
        frame_out(&mut host, 3),
        Some(3008),
        "replacement is seen next tick"
    );
    host.clear_input("ctx");
    assert_eq!(
        frame_out(&mut host, 4),
        None,
        "cleared: the required input is missing"
    );
    ctx.push(9);
    assert_eq!(frame_out(&mut host, 5), Some(5009));
    let stats = host.host().input_port_stats("ctx").expect("ctx stats");
    assert_eq!((stats.accepted, stats.replaced, stats.pending), (3, 1, 0));
}

#[test]
fn held_input_never_triggers_a_tick_by_itself() {
    let mut host = compile("fuse", true);
    host.push("ctx", 1_i64);
    host.push("ctx", 2_i64);
    assert!(!host.host().has_pending_inbound());
    assert_eq!(host.host().pending_inbound(), 0);
    assert!(host.tick_if_ready().expect("tick").is_none());
    assert_eq!(
        host.wait_for_input(Some(Duration::from_millis(5))),
        InboundWait::TimedOut
    );
    host.push("frame", 3_i64);
    assert!(host.tick_if_ready().expect("tick").is_some());
    assert_eq!(host.take::<i64>("out"), Some(3002));
}

#[test]
fn held_context_counts_as_present_for_fire_all() {
    // Declared at runtime instead of in the graph; the join's edges are FIFO.
    let mut host = compile("join", false);
    host.set_held_input("ctx");
    host.push("ctx", 1_i64);
    for _ in 0..3 {
        // The join waits for a frame; the held context is refreshed, not queued up.
        let telemetry = host.tick().expect("tick");
        assert_eq!(telemetry.nodes_executed, 0);
    }
    assert_eq!(frame_out(&mut host, 10), Some(10_001));
    assert_eq!(
        frame_out(&mut host, 11),
        Some(11_001),
        "context stays present"
    );
    host.push("ctx", 2_i64);
    host.tick().expect("idle tick");
    assert_eq!(
        frame_out(&mut host, 12),
        Some(12_002),
        "no stale context left"
    );
}

#[test]
fn every_frame_tick_sees_held_context_under_drive_blocking() {
    const FRAMES: i64 = 500;
    let mut host = compile("fuse", true);
    host.set_latest_input("frame").expect("latest frame");
    let stop = host.stop_handle();
    let feeder = host.host().clone();
    let producer = thread::spawn(move || {
        feeder.push("ctx", 7_i64);
        for frame in 1..=FRAMES {
            feeder.push("frame", frame);
            if frame % 50 == 0 {
                thread::yield_now();
            }
        }
    });
    let mut outputs = 0;
    let exit = host
        .drive_blocking(&stop, |graph, _turn| {
            for out in graph.drain_owned::<i64>("out")? {
                assert_eq!(out % 1000, 7, "frame {} saw no context", out / 1000);
                outputs += 1;
                if out / 1000 == FRAMES {
                    stop.stop();
                }
            }
            Ok(())
        })
        .expect("drive");
    producer.join().expect("producer");
    assert_eq!(exit, HostGraphDriveExit::Stopped);
    assert!(outputs > 0);
}

#[test]
fn batched_pushes_never_split_across_ticks() {
    const BATCHES: i64 = 2000;
    let mut host = compile("pair", false);
    host.set_latest_input("frame").expect("latest frame");
    host.set_latest_input("ctx").expect("latest ctx");
    let stop = host.stop_handle();
    let feeder = host.host().clone();
    let producer = thread::spawn(move || {
        for frame in 1..=BATCHES {
            feeder
                .batch()
                .push("frame", frame)
                .push("ctx", frame)
                .commit()
                .expect("batch");
        }
    });
    let mut outputs = 0;
    host.drive_blocking(&stop, |graph, _turn| {
        for out in graph.drain_owned::<i64>("out")? {
            assert!(out > 0, "frame {} ticked without its context", -out);
            outputs += 1;
            if out == BATCHES {
                stop.stop();
            }
        }
        Ok(())
    })
    .expect("drive");
    producer.join().expect("producer");
    assert!(outputs > 0);
}

#[test]
fn a_batch_failing_the_type_check_pushes_nothing() {
    let mut host = compile("fuse", true);
    let int_key = host.type_index().key_of::<i64>().expect("i64 key");
    let rejected = host
        .push_batch([
            (PortId::from("frame"), Payload::owned(int_key, 1_i64)),
            (PortId::from("ctx"), Payload::owned(TAG_KEY, 5_u32)),
        ])
        .expect_err("a u32 under the Tag key");
    assert_eq!((rejected.index, rejected.port.as_str()), (1, "ctx"));
    assert!(matches!(
        rejected.error,
        TypeKeyError::RustTypeMismatch { .. }
    ));
    assert_eq!(host.host().pending_inbound(), 0, "frame was not pushed");

    struct Unkeyed;
    let rejected = host
        .batch()
        .push("ctx", 4_i64)
        .push("frame", Unkeyed)
        .commit()
        .expect_err("no key for Unkeyed");
    assert_eq!(rejected.index, 1);
    assert!(matches!(rejected.error, TypeKeyError::Unkeyed { .. }));
    assert!(host.tick_if_ready().expect("tick").is_none());

    let outcomes = host
        .batch()
        .push("frame", 2_i64)
        .push("ctx", 5_i64)
        .commit()
        .expect("batch");
    assert!(matches!(
        &outcomes[..],
        [FeedOutcome::Accepted { .. }, FeedOutcome::Accepted { .. }]
    ));
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("out"), Some(2005));
}

#[test]
fn held_inputs_round_trip_through_graph_documents() {
    let (registry, graph) = graph("fuse", true);
    let json = registry.graph_document(graph).to_json_pretty().unwrap();
    assert!(json.contains(HOST_HELD_INPUTS_KEY), "{json}");
    let document = GraphDocument::from_json(&json).expect("parse");
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_document(&registry, document)
        .expect("compile document");
    assert!(host.host().is_input_held("ctx"));
    host.push("ctx", 3_i64);
    assert_eq!(frame_out(&mut host, 1), Some(1003));
    assert_eq!(frame_out(&mut host, 2), Some(2003));
}

#[test]
fn held_delivery_and_batches_allocate_nothing_extra() {
    let mut host = compile("join", true);
    host.host().set_event_recording(false);
    let (frame, ctx) = (PortId::from("frame"), PortId::from("ctx"));
    host.push(ctx.clone(), 7_i64);
    for value in 0..8 {
        assert_eq!(frame_out(&mut host, value), Some(value * 1000 + 7));
    }
    // Two payloads per frame (the pushed frame and the node's output); re-delivering the held
    // context is an `Arc` clone.
    let (payload, _) = allocations_during(|| Payload::owned("i64", 1_i64));
    let (round, out) = allocations_during(|| {
        host.push(frame.clone(), 41_i64);
        host.tick().expect("tick");
        host.take::<i64>("out")
    });
    assert_eq!(out, Some(41_007));
    assert!(
        round <= 2 * payload,
        "frame round with held context allocated {round} times ({payload} per payload)"
    );

    // A batch allocates what the same single pushes do.
    let (singles, _) = allocations_during(|| {
        host.push(frame.clone(), 1_i64);
        host.push(ctx.clone(), 2_i64)
    });
    host.tick().expect("tick");
    let (batch, outcomes) = allocations_during(|| {
        host.batch()
            .push(frame.clone(), 3_i64)
            .push(ctx.clone(), 4_i64)
            .commit()
    });
    assert!(outcomes.is_ok());
    assert!(
        batch <= singles,
        "batch allocated {batch} times, single pushes {singles}"
    );
}
