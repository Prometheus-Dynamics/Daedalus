//! Per-frame cost of handing a 1920x1080 NV12 frame to a node and back, statically installed
//! (equivalently, through the Rust ABI) and through the stable path, against the byte-buffer
//! ways a frame crossed before `SharedFrame` (`SharedBytes` by reference, `Vec<u8>` by value).
//! Allocations are this binary's (the host's); a stable-ABI plugin allocates with its own
//! allocator, which is not counted here.
//!
//! `cargo test -p daedalus-rs --features engine,dylib-plugins --release --test
//! dylib_frames_overhead -- --ignored --nocapture`

#[path = "support/cdylib.rs"]
mod cdylib;
#[path = "support/frames.rs"]
mod frames;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::InstallPath;
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::transport::{ForeignHandle, FrameInterface, Payload};
use daedalus::{PluginLibrary, SharedBytes};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use frames::{COUNTED_KEY, Counted, FramesHost};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

// Safety: forwards to the system allocator, counting allocations.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // Safety: forwarded.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Safety: forwarded.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // Safety: forwarded.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const RUNS: u32 = 5_000;
const NV12_1080P: usize = 1920 * 1080 * 3 / 2;

/// Mean ns and host allocations per run of `once`, after a warm-up.
fn measure(mut once: impl FnMut()) -> (f64, f64) {
    (0..200).for_each(|_| once());
    let allocs = ALLOCS.load(Relaxed);
    let start = Instant::now();
    (0..RUNS).for_each(|_| once());
    let ns = start.elapsed().as_nanos() as f64 / f64::from(RUNS);
    let allocs = (ALLOCS.load(Relaxed) - allocs) as f64 / f64::from(RUNS);
    (ns, allocs)
}

fn registry(stable: bool) -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&FramesHost::new()).unwrap();
    if stable {
        let path = cdylib::build_cdylib(
            "daedalus-plugins-stable-abi",
            "daedalus_plugins_stable_abi",
            "dylib",
        );
        // Safety: a trusted test plugin, kept loaded for the process.
        let library = Box::leak(Box::new(unsafe { PluginLibrary::load(path) }.unwrap()));
        library
            .install_into_as(&mut registry, InstallPath::Stable)
            .unwrap();
    } else {
        registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    }
    registry
}

fn graph(registry: &PluginRegistry, node: &str, input: TypeExpr) -> HostGraph<HandlerRegistry> {
    let builder = registry
        .graph_builder()
        .unwrap()
        .input_as("in", input)
        .node_id(&format!("stable_abi:{node}"), node)
        .connect("in", format!("{node}.{}", port(node)).as_str())
        .connect(format!("{node}.{}", port(node)).as_str(), "out");
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

fn port(node: &str) -> &'static str {
    if node == "frame_pass" {
        "frame"
    } else {
        "bytes"
    }
}

#[test]
#[ignore = "timing; run manually"]
#[allow(clippy::print_stdout)]
fn frame_handoff_overhead() {
    let (frame, _) = Counted::nv12(1920, 1080, 1);
    let shared = SharedBytes::new(vec![7; NV12_1080P]);
    let vec = vec![7_u8; NV12_1080P];
    println!("1920x1080 NV12 ({NV12_1080P} bytes), in and back out, per tick:");
    for (install, stable) in [("static (Rust ABI)", false), ("stable ABI", true)] {
        let registry = registry(stable);
        let mut host = graph(&registry, "frame_pass", TypeExpr::opaque(COUNTED_KEY));
        let (ns, allocs) = measure(|| {
            host.push_payload("in", frame.payload());
            host.tick().unwrap();
            assert!(host.take_payload("out").is_some());
        });
        println!("  {install:17} SharedFrame:          {ns:>9.0} ns  {allocs:>5.1} allocs");

        let mut host = graph(
            &registry,
            "bytes_pass",
            TypeExpr::opaque(daedalus::dylib::SHARED_BYTES_KEY),
        );
        let (ns, allocs) = measure(|| {
            host.push("in", shared.clone());
            host.tick().unwrap();
            assert!(host.take_payload("out").is_some());
        });
        println!("  {install:17} SharedBytes:          {ns:>9.0} ns  {allocs:>5.1} allocs");

        let mut host = graph(&registry, "vec_pass", TypeExpr::Scalar(ValueType::Bytes));
        let (ns, allocs) = measure(|| {
            host.push("in", vec.clone());
            host.tick().unwrap();
            assert!(host.take_payload("out").is_some());
        });
        println!("  {install:17} Vec<u8> (by value):   {ns:>9.0} ns  {allocs:>5.1} allocs");
    }

    // Reading a frame's metadata costs one indirect call per field (the same calls a stable
    // plugin makes into the host's provider).
    let handle = ForeignHandle::from_arc::<_, FrameInterface>(frame.clone());
    let view = handle.view::<FrameInterface>().unwrap();
    let (ns, _) = measure(|| {
        let mut sum = u64::from(view.width()) + u64::from(view.height());
        sum += u64::from(view.format()) + view.modifier() + view.sequence();
        sum += view.timestamp_ns() + u64::from(view.colorimetry().range);
        for plane in view.planes() {
            sum += plane.offset + plane.stride + plane.len;
        }
        std::hint::black_box(sum);
    });
    println!("  every metadata field of a 2-plane frame (21 accessor calls): {ns:.1} ns");
    let payload = frame.payload();
    let (ns, allocs) = measure(|| {
        std::hint::black_box(Payload::clone(&payload));
    });
    println!("  for scale, one payload clone: {ns:.1} ns, {allocs:.1} allocs");
}
