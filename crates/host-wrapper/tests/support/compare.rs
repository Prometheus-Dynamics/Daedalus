//! The comparison both shared-mode tests run: one graph (the `stable_abi` plugin's `accumulate`,
//! a conditional `every`, and `shared_sum` on bytes) over N frames, statically (the core linked
//! into this process, the plugin's Rust handlers) and through `daedalus-host` and a surface
//! library. Asserts the outputs match frame by frame, that a warm scalar frame allocates nothing
//! in the surface, and that a bytes frame allocates no more there than the static graph does.
//! The shared side alternates the by-name calls with ports resolved once and one batch push per
//! frame. Prints the per-frame cost of both paths (no timing assertion).
//!
//! Each test binary that includes this module gets its counting global allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use daedalus::GraphDocument;
use daedalus::data::daedalus_type::DaedalusTypeExpr;
use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::SharedBytes;
use daedalus::engine::{Engine, EngineConfig};
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus_host::{Push, Surface};
use daedalus_plugins_stable_abi::StableAbiPlugin;

struct Counting;

static HOST_ALLOCS: AtomicU64 = AtomicU64::new(0);

// Safety: forwards to the system allocator, counting this test binary's allocations.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        HOST_ALLOCS.fetch_add(1, Relaxed);
        // Safety: forwarded.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Safety: forwarded.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        HOST_ALLOCS.fetch_add(1, Relaxed);
        // Safety: forwarded.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const FRAMES: u64 = 300;
/// Long enough that every payload length (1 to 37) has been pushed once, so the reused vectors have
/// grown to their largest size before the measured frames.
const WARM: u64 = 40;

/// Every third frame pushes bytes; the others push only scalars.
fn with_bytes(i: u64) -> bool {
    i.is_multiple_of(3)
}

/// One frame's outputs: the accumulated total, the optional `every` hit, and the tail bytes.
#[derive(Debug, PartialEq)]
struct Frame {
    total: Option<i64>,
    hits: Option<i64>,
    tail: Option<Vec<u8>>,
}

/// The payload pushed on frame `i`: its length and contents change, so a stale buffer shows.
fn payload(i: u64) -> Vec<u8> {
    (0..(i % 37 + 1)).map(|b| (b * 7 + i) as u8).collect()
}

/// The graph: `step` feeds the running total and the `every` node (its period is `period`), and
/// `bytes` feeds `shared_sum`, whose tail comes out.
fn document(registry: &PluginRegistry) -> String {
    let bytes = <SharedBytes as DaedalusTypeExpr>::type_expr();
    let int = TypeExpr::Scalar(ValueType::Int);
    let graph = registry
        .graph_builder()
        .unwrap()
        .node_id("stable_abi:accumulate", "accumulate")
        .node_id("stable_abi:every", "every")
        .node_id("stable_abi:shared_sum", "shared_sum")
        .input_as("step", int.clone())
        .input_as("period", int)
        .input_as("bytes", bytes)
        .connect("step", "accumulate.step")
        .connect("step", "every.tick")
        .connect("period", "every.every")
        .connect("bytes", "shared_sum.bytes")
        .connect("accumulate.total", "total")
        .connect("every.out", "hits")
        .connect("shared_sum.tail", "tail")
        .build();
    GraphDocument::new(graph).to_json().unwrap()
}

fn static_registry() -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    registry
}

/// The surface's allocation counter, read through its own library.
fn surface_allocations(path: &Path) -> impl Fn() -> u64 {
    // Safety: the library is already loaded by the shared path; this only looks up a function.
    let library = Box::leak(Box::new(unsafe { libloading::Library::new(path) }.unwrap()));
    let counter: libloading::Symbol<'static, extern "C" fn() -> u64> =
        unsafe { library.get(b"daedalus_surface_allocations\0") }.unwrap();
    let counter = *counter;
    move || counter()
}

/// Runs the graph over [`FRAMES`] frames statically and through the surface at `surface_path`,
/// and asserts the outputs and allocations (see the test files). `plugin` is the `stable_abi`
/// plugin library to install by path, or `None` for an image surface whose hosts already have
/// the nodes. `shared` names the shared side in the report.
#[allow(clippy::print_stdout)] // The per-frame cost is the test's report.
pub fn compare(surface_path: &Path, plugin: Option<&Path>, shared_name: &str) {
    // Static: the core in this process, the plugin's handlers in Rust.
    let registry = static_registry();
    let json = document(&registry);
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let mut graph = engine
        .compile_document(&registry, GraphDocument::from_json(&json).unwrap())
        .unwrap();
    graph.prepare().unwrap();

    // Shared: the surface, which installs the same plugin through the stable path, or an image
    // surface whose hosts have it in-image.
    let surface = Surface::load(surface_path).unwrap();
    let host = surface.create_host().unwrap();
    if let Some(plugin) = plugin {
        host.install(plugin).unwrap();
    }
    let shared = host.compile(&json).unwrap();
    let surface_count = surface_allocations(surface_path);
    let (step_in, period_in, bytes_in) = (
        shared.input("step").unwrap(),
        shared.input("period").unwrap(),
        shared.input("bytes").unwrap(),
    );
    let (total_out, hits_out, tail_out) = (
        shared.output("total").unwrap(),
        shared.output("hits").unwrap(),
        shared.output("tail").unwrap(),
    );

    let mut bytes_out = Vec::new();
    let mut static_frames = Vec::new();
    let mut shared_frames = Vec::new();
    // Per path: time and allocations, for scalar-only and bytes frames (warm frames only).
    let mut static_time = [std::time::Duration::ZERO; 2];
    let mut shared_time = [std::time::Duration::ZERO; 2];
    let mut static_allocs = [0u64; 2];
    let mut shared_allocs = [0u64; 2];
    let mut surface_allocs = [0u64; 2];
    let mut frames_of = [0u64; 2];

    for i in 0..FRAMES {
        let step = (i as i64 * 7) % 11 - 5;
        let bytes = payload(i);
        let kind = usize::from(with_bytes(i));
        let warm = i >= WARM;
        // Built before any window opens, so the static path's window holds only engine work.
        let prebuilt = with_bytes(i).then(|| SharedBytes::new(bytes.clone()));

        // Static.
        let before = HOST_ALLOCS.load(Relaxed);
        let start = Instant::now();
        graph.push("step", step);
        graph.push("period", 3_i64);
        if let Some(bytes) = prebuilt {
            graph.push("bytes", bytes);
        }
        graph.tick().unwrap();
        let total = graph.take::<i64>("total");
        let hits = graph.take::<i64>("hits");
        let tail = graph.take::<SharedBytes>("tail");
        let elapsed = start.elapsed();
        let allocs = HOST_ALLOCS.load(Relaxed) - before;
        if warm {
            static_time[kind] += elapsed;
            static_allocs[kind] += allocs;
            frames_of[kind] += 1;
        }
        static_frames.push(Frame {
            total,
            hits,
            tail: tail.map(|b| b.to_vec()),
        });

        // Shared: even frames call per push and take by name; odd frames push in one batch and
        // take by resolved port.
        let (surface_before, host_before) = (surface_count(), HOST_ALLOCS.load(Relaxed));
        let start = Instant::now();
        let (total, hits, has_tail) = if i % 2 == 0 {
            shared.push_i64("step", step).unwrap();
            shared.push_i64("period", 3).unwrap();
            if with_bytes(i) {
                shared.push_bytes("bytes", &bytes).unwrap();
            }
            shared.tick().unwrap();
            (
                shared.take_i64("total").unwrap(),
                shared.take_i64("hits").unwrap(),
                shared.take_bytes("tail", &mut bytes_out).unwrap(),
            )
        } else {
            let pushes = [
                Push::i64(step_in, step),
                Push::i64(period_in, 3),
                Push::bytes(bytes_in, &bytes),
            ];
            let count = if with_bytes(i) { 3 } else { 2 };
            shared.push_batch(&pushes[..count]).unwrap();
            shared.tick().unwrap();
            (
                shared.take_i64_at(total_out).unwrap(),
                shared.take_i64_at(hits_out).unwrap(),
                shared.take_bytes_at(tail_out, &mut bytes_out).unwrap(),
            )
        };
        let elapsed = start.elapsed();
        let (surface_now, host_now) = (surface_count(), HOST_ALLOCS.load(Relaxed));
        if warm {
            shared_time[kind] += elapsed;
            surface_allocs[kind] += surface_now - surface_before;
            shared_allocs[kind] += host_now - host_before;
        }
        shared_frames.push(Frame {
            total,
            hits,
            tail: has_tail.then(|| bytes_out.clone()),
        });
    }

    assert_eq!(static_frames.len(), shared_frames.len());
    for (i, (s, d)) in static_frames.iter().zip(&shared_frames).enumerate() {
        assert_eq!(s, d, "frame {i} differs between static and shared mode");
    }
    assert!(
        static_frames.iter().any(|f| f.hits.is_some())
            && static_frames.iter().any(|f| f.hits.is_none()),
        "the optional output should be present on some frames and absent on others"
    );
    assert!(static_frames.iter().any(|f| f.tail.is_some()));

    let per = |t: std::time::Duration, n: u64| t.as_secs_f64() * 1e6 / n as f64;
    let names = ["scalar-only", "with bytes"];
    println!("static vs {shared_name} over {FRAMES} frames ({WARM} warm-up): outputs identical");
    for kind in 0..2 {
        let n = frames_of[kind] as f64;
        println!(
            "{:>11} frames: per frame, static {:.2} us, {shared_name} {:.2} us; allocations: \
             static {:.2}, surface {:.2}, wrapper (this binary) {:.2}",
            names[kind],
            per(static_time[kind], frames_of[kind]),
            per(shared_time[kind], frames_of[kind]),
            static_allocs[kind] as f64 / n,
            surface_allocs[kind] as f64 / n,
            shared_allocs[kind] as f64 / n,
        );
    }
    // Scalars: a warm frame allocates nothing in the surface.
    assert_eq!(
        surface_allocs[0], 0,
        "a warm scalar frame through the surface must allocate nothing in the surface"
    );
    // Bytes: the surface adds no allocation to the engine's own (the port's vector and payload
    // are reused): at most the static path's count for the same window. Not equal: this binary's static engine is built with whatever features
    // the workspace unifies for the test (metrics, for one), while the surface cdylib has its own.
    assert!(
        surface_allocs[1] <= static_allocs[1],
        "the surface's bytes frames allocate more than the static graph's: {} > {}",
        surface_allocs[1],
        static_allocs[1]
    );
    drop(shared);
    drop(host);
}
