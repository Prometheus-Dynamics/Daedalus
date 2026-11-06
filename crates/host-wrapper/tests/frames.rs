//! Dma-buf frames through the surface library, as a Rust caller uses them: each frame is pushed to
//! the `stable_abi` plugin's `frame_pass` node (installed through the stable handler path) and
//! taken back. A taken frame is the same dma-buf as the pushed one (same device and inode), with
//! the same pixels, and no descriptor is left open over the run.
//!
//! Allocations: a warm frame allocates nothing, through the surface (push, tick, take and
//! release) and through a statically installed graph (push, tick, take, with the tick's telemetry
//! handed back). Both are measured here (the surface through its own counter, the static graph
//! through this binary's counting allocator). A payload holds a frame's handle in place, and the
//! surface reuses its frame objects and the tick's telemetry.
//!
//! Linux only, and it needs `/dev/dma_heap/system`: without it the test says so and passes.

#![cfg(target_os = "linux")]

#[path = "support/cdylib.rs"]
mod cdylib;

use std::alloc::{GlobalAlloc, Layout, System};
use std::os::fd::{AsFd, BorrowedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use daedalus::GraphDocument;
use daedalus::data::model::TypeExpr;
use daedalus::engine::{Engine, EngineConfig};
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::transport::{
    FRAME_INTERFACE_KEY, FrameFormatKind, FramePlane, FrameResidency, FrameSource, SharedFrame,
    fourcc,
};
use daedalus_host::{Frame, Surface};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use lemnos_abi::Status;
use lemnos_linux::dma::{Access, DmaBuf, DmaBufRef, DmaHeap};

/// Counts this binary's allocations (the static graph's, measured here).
struct Counting;

static HOST_ALLOCS: AtomicU64 = AtomicU64::new(0);

// Safety: forwards to the system allocator, counting allocations.
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

/// A CPU frame for the static graph: the same handoff, with no dma-buf behind it.
struct CpuFrame {
    pixels: Vec<u8>,
}

impl FrameSource for CpuFrame {
    fn width(&self) -> u32 {
        4
    }
    fn height(&self) -> u32 {
        4
    }
    fn format(&self) -> u32 {
        fourcc(b"R8  ")
    }
    fn format_kind(&self) -> FrameFormatKind {
        FrameFormatKind::Pixel
    }
    fn residency(&self) -> FrameResidency {
        FrameResidency::Cpu
    }
    fn plane_count(&self) -> u32 {
        1
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        (index == 0).then(|| FramePlane::cpu(&self.pixels, 4))
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        (index == 0).then_some(&self.pixels[..])
    }
}

const WIDTH: u32 = 256;
const HEIGHT: u32 = 256;
const NV12: u32 = fourcc(b"NV12");
const LUMA: usize = (WIDTH * HEIGHT) as usize;
/// Luma plus half-height interleaved chroma.
const FRAME: usize = LUMA * 3 / 2;
const FRAMES: u64 = 120;
/// Frames before the measured ones: every port's frame object has been made and reused.
const WARM: u64 = 20;
/// The frames rotate over this many dma-bufs, so the same buffer comes back and others do too.
const BUFFERS: u64 = 3;

/// The graph `frame` -> `pass` -> `out`, over the `stable_abi` plugin's `frame_pass` node.
fn document() -> String {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    let frame = TypeExpr::opaque(FRAME_INTERFACE_KEY);
    let graph = registry
        .graph_builder()
        .unwrap()
        .node_id("stable_abi:frame_pass", "pass")
        .input_as("frame", frame.clone())
        .output_as("out", frame)
        .connect("frame", "pass.frame")
        .connect("pass.frame", "out")
        .build();
    GraphDocument::new(graph).to_json().unwrap()
}

/// The surface's allocation counter, read through its own library.
fn surface_allocations(path: &std::path::Path) -> impl Fn() -> u64 {
    // Safety: the library is already loaded by the surface; this only looks up a function.
    let library = Box::leak(Box::new(unsafe { libloading::Library::new(path) }.unwrap()));
    let counter: libloading::Symbol<'static, extern "C" fn() -> u64> =
        unsafe { library.get(b"daedalus_surface_allocations\0") }.unwrap();
    let counter = *counter;
    move || counter()
}

/// The number of open descriptors of this process.
fn open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

/// A dma-buf holding `FRAME` bytes derived from `seed`, written through a mapping.
fn buffer(heap: &DmaHeap, seed: u64) -> DmaBuf {
    let buf = heap.allocate(FRAME).unwrap();
    let mut map = buf.map().unwrap();
    buf.begin_cpu_access(Access::ReadWrite).unwrap();
    for (i, byte) in map.as_mut_slice().iter_mut().enumerate() {
        *byte = (i as u64).wrapping_mul(31).wrapping_add(seed) as u8;
    }
    buf.end_cpu_access(Access::ReadWrite).unwrap();
    buf
}

/// The bytes at `offset` of `fd`, read through a mapping of its dma-buf.
fn read(fd: BorrowedFd<'_>, offset: usize, len: usize) -> Vec<u8> {
    let buf = DmaBufRef::from_fd(fd).unwrap();
    let map = buf.map_read_only().unwrap();
    buf.begin_cpu_access(Access::Read).unwrap();
    let bytes = map.as_slice()[offset..offset + len].to_vec();
    buf.end_cpu_access(Access::Read).unwrap();
    bytes
}

fn identity(fd: BorrowedFd<'_>) -> (u64, u64) {
    DmaBufRef::from_fd(fd).unwrap().identity().unwrap()
}

/// Allocations per warm frame (push, tick, take) through a statically installed graph: the
/// frame handoff with the Rust-ABI `frame_pass` node, measured with this binary's counter.
fn static_allocations_per_frame() -> f64 {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    let frame = TypeExpr::opaque(FRAME_INTERFACE_KEY);
    let graph = registry
        .graph_builder()
        .unwrap()
        .node_id("stable_abi:frame_pass", "pass")
        .input_as("frame", frame.clone())
        .output_as("out", frame)
        .connect("frame", "pass.frame")
        .connect("pass.frame", "out")
        .build();
    let json = GraphDocument::new(graph).to_json().unwrap();
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let mut host = engine
        .compile_document(&registry, GraphDocument::from_json(&json).unwrap())
        .unwrap();
    host.prepare().unwrap();
    let object = Arc::new(CpuFrame {
        pixels: vec![1; 16],
    });
    let mut total = 0u64;
    for i in 0..FRAMES {
        let shared = SharedFrame::new(Arc::clone(&object));
        let before = HOST_ALLOCS.load(Relaxed);
        host.push_payload("frame", shared.into_payload());
        let telemetry = host.tick().unwrap();
        host.recycle_telemetry(telemetry);
        let out = host.take_payload("out");
        let allocs = HOST_ALLOCS.load(Relaxed) - before;
        drop(out);
        if i >= WARM {
            total += allocs;
        }
    }
    total as f64 / (FRAMES - WARM) as f64
}

#[test]
#[allow(clippy::print_stdout, clippy::print_stderr)]
fn frames_round_trip_as_the_same_dma_bufs_with_the_same_pixels() {
    let heap = match DmaHeap::open("system") {
        Ok(heap) => heap,
        Err(error) => {
            eprintln!("SKIP frames: /dev/dma_heap/system is not accessible: {error}");
            return;
        }
    };
    let surface_path = cdylib::build_cdylib("daedalus-surface", "daedalus_surface", "alloc-count");
    let plugin_path = cdylib::build_cdylib(
        "daedalus-plugins-stable-abi",
        "daedalus_plugins_stable_abi",
        "dylib",
    );
    let surface = Surface::load(&surface_path).unwrap();
    let host = surface.create_host().unwrap();
    host.install(&plugin_path).unwrap();
    let graph = host.compile(&document()).unwrap();
    let surface_count = surface_allocations(&surface_path);

    let static_per_frame = static_allocations_per_frame();

    let buffers: Vec<DmaBuf> = (0..BUFFERS).map(|seed| buffer(&heap, seed)).collect();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = buffers
        .iter()
        .map(|buf| {
            (
                read(buf.as_fd(), 0, LUMA),
                read(buf.as_fd(), LUMA, LUMA / 2),
            )
        })
        .collect();

    let mut fds_before = 0;
    let (mut warm_allocs, mut warm_time, mut warm_frames) = (0u64, std::time::Duration::ZERO, 0u64);
    for i in 0..FRAMES {
        if i == WARM {
            fds_before = open_fds();
        }
        let index = (i % BUFFERS) as usize;
        let buf = &buffers[index];
        let frame = Frame::image(WIDTH, HEIGHT, NV12, 0)
            .plane(buf.as_fd(), 0, u64::from(WIDTH), LUMA as u64)
            .unwrap()
            .plane(
                buf.as_fd(),
                LUMA as u64,
                u64::from(WIDTH),
                (LUMA / 2) as u64,
            )
            .unwrap();
        // The buffer's own work, as an acquire fence (idle here, so already signalled).
        let acquire = buf.export_fence(Access::Write).unwrap();

        let before = surface_count();
        let start = Instant::now();
        graph
            .push_frame("frame", &frame, Some(acquire.as_fd()))
            .unwrap();
        graph.tick().unwrap();
        let lease = graph
            .take_frame("out")
            .unwrap()
            .expect("the frame comes back");
        let elapsed = start.elapsed();
        let allocs = surface_count() - before;
        if i >= WARM {
            warm_allocs += allocs;
            warm_time += elapsed;
            warm_frames += 1;
        }

        assert_eq!(lease.width(), WIDTH, "frame {i}");
        assert_eq!(lease.height(), HEIGHT);
        assert_eq!(lease.fourcc(), NV12);
        assert_eq!(lease.planes().len(), 2);
        assert_eq!(lease.planes()[1].offset, LUMA as u64);
        for plane in 0..2 {
            let fd = lease.plane_fd(plane).unwrap();
            assert_eq!(
                identity(fd),
                buf.identity().unwrap(),
                "frame {i}: plane {plane} is the same dma-buf"
            );
        }
        let (luma, chroma) = &expected[index];
        assert_eq!(
            read(lease.plane_fd(0).unwrap(), 0, LUMA),
            *luma,
            "frame {i}: luma bytes"
        );
        assert_eq!(
            read(lease.plane_fd(1).unwrap(), LUMA, LUMA / 2),
            *chroma,
            "frame {i}: chroma bytes"
        );
        let before = surface_count();
        drop(lease);
        if i >= WARM {
            warm_allocs += surface_count() - before;
        }
        drop(acquire);
    }
    assert_eq!(
        open_fds(),
        fds_before,
        "no descriptor is left open after {FRAMES} frames"
    );

    // The output is empty once the lease is gone: nothing is queued.
    assert!(graph.take_frame("out").unwrap().is_none());

    let per_frame = warm_time.as_secs_f64() * 1e6 / warm_frames as f64;
    let surface_per_frame = warm_allocs as f64 / warm_frames as f64;
    println!(
        "dma-buf frames over {FRAMES} frames ({WARM} warm-up): identical dma-bufs and pixels; \
         allocations per warm frame: surface {surface_per_frame:.2}, static graph \
         {static_per_frame:.2}; {per_frame:.1} us per frame (debug build)"
    );
    assert_eq!(
        static_per_frame, 0.0,
        "a warm frame through the static graph allocates nothing"
    );
    assert_eq!(
        surface_per_frame, 0.0,
        "a warm frame through the surface allocates nothing"
    );

    // A plane that is not a frame's: an invalid descriptor is refused, and the port keeps working.
    let bad = Frame::image(WIDTH, HEIGHT, NV12, 0);
    let refused = graph.push_frame("frame", &bad, None).unwrap_err();
    assert_eq!(refused.status(), Some(Status::INVALID_ARGUMENT));
    drop(graph);
    drop(host);
}
