//! One graph fed by four cameras: synchronized groups tick whole (with timeouts serviced by the
//! drive loop), independent-latest cameras tick per arrival on the newest frames, and both feed
//! frames without copies or per-tick allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use daedalus::{
    engine::{
        CameraFeed, CameraSet, Engine, EngineConfig, HostGraph, HostGraphDriveExit,
        IndependentConfig, MetricsLevel, MultiCamera, PartialPolicy, SyncConfig,
    },
    macros::{node, plugin},
    runtime::{
        NodeError,
        handler_registry::HandlerRegistry,
        host_bridge::multicam::{frame_sequence, frame_timestamp, source_timestamp},
        plugins::PluginRegistry,
    },
    transport::{
        FrameInterface, FramePlane, FrameResidency, FrameSource, Payload, TypeKey, fourcc,
    },
    type_key,
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

const FRAME_KEY: &str = "test:multicam:frame";

/// Stand-in for a camera library's frame.
#[type_key(FRAME_KEY)]
struct CamFrame {
    pixels: Vec<u8>,
    timestamp_ns: u64,
    sequence: u64,
}

impl FrameSource for CamFrame {
    fn width(&self) -> u32 {
        self.pixels.len() as u32
    }
    fn height(&self) -> u32 {
        1
    }
    fn format(&self) -> u32 {
        fourcc(b"R8  ")
    }
    fn timestamp_ns(&self) -> u64 {
        self.timestamp_ns
    }
    fn sequence(&self) -> u64 {
        self.sequence
    }
    fn residency(&self) -> FrameResidency {
        FrameResidency::Cpu
    }
    fn plane_count(&self) -> u32 {
        1
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        (index == 0).then(|| FramePlane::cpu(&self.pixels, self.pixels.len() as u64))
    }
}

/// `sequence * 10 + cameras present` when every present frame has camera 0's sequence, else -1;
/// plus the address of camera 0's pixels (to prove frames arrive uncopied).
#[node(
    id = "multicam.fuse",
    inputs("a", "b", "c", "d"),
    outputs("out", "ptr")
)]
fn fuse(
    a: &CamFrame,
    b: &CamFrame,
    c: Option<&CamFrame>,
    d: Option<&CamFrame>,
) -> Result<(i64, i64), NodeError> {
    let frames = [Some(a), Some(b), c, d];
    let present = frames.iter().flatten().count() as i64;
    let same = frames.iter().flatten().all(|f| f.sequence == a.sequence);
    let out = if same {
        a.sequence as i64 * 10 + present
    } else {
        -1
    };
    Ok((out, a.pixels.as_ptr() as i64))
}

#[plugin(id = "multicam", types(CamFrame), nodes(fuse))]
struct MultiCamPlugin;

const PORTS: [&str; 4] = ["cam0", "cam1", "cam2", "cam3"];

fn compile() -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    let plugin = MultiCamPlugin::new();
    registry.install(&plugin).expect("install");
    let fuse = plugin.fuse.alias("fuse");
    let mut builder = registry
        .graph_builder()
        .expect("builder")
        .try_node(&fuse)
        .expect("node");
    let inputs = [
        &fuse.inputs.a,
        &fuse.inputs.b,
        &fuse.inputs.c,
        &fuse.inputs.d,
    ];
    for (port, input) in PORTS.into_iter().zip(inputs) {
        builder = builder
            .input_typed::<CamFrame>(port)
            .and_then(|b| b.try_connect(port, input))
            .expect("camera input");
    }
    let graph = builder
        .try_connect(&fuse.outputs.out, "out")
        .and_then(|b| b.try_connect(&fuse.outputs.ptr, "ptr"))
        .expect("wire")
        .build();
    Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Off))
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile")
}

const PERIOD_NS: u64 = 33_000_000;

fn cam_frame(sequence: u64, jitter_ns: u64) -> Arc<CamFrame> {
    Arc::new(CamFrame {
        pixels: vec![sequence as u8; 16],
        timestamp_ns: sequence * PERIOD_NS + jitter_ns,
        sequence,
    })
}

fn payload(frame: &Arc<CamFrame>) -> Payload {
    Payload::shared(FRAME_KEY, frame.clone())
}

fn sync_config(partial: PartialPolicy, timeout: Duration) -> SyncConfig {
    SyncConfig {
        partial,
        stamp: source_timestamp::<CamFrame>,
        ..SyncConfig::new(Duration::from_millis(8), timeout)
    }
}

#[test]
fn four_camera_threads_tick_whole_groups_under_drive_blocking() {
    const FRAMES: u64 = 300;
    let mut graph = compile();
    let cameras = MultiCamera::synchronized(
        graph.host(),
        PORTS,
        sync_config(PartialPolicy::Drop, Duration::from_secs(10)),
    );
    let stop = graph.stop_handle();
    let producers: Vec<_> = (0..4)
        .map(|camera| {
            let cameras = cameras.clone();
            thread::spawn(move || {
                for sequence in 0..FRAMES {
                    let jitter = (sequence * 7 + camera as u64 * 3) % 5 * 1_000_000;
                    cameras.push(camera, payload(&cam_frame(sequence, jitter)));
                    if sequence % 16 == camera as u64 {
                        thread::yield_now();
                    }
                }
            })
        })
        .collect();
    let mut ticks = 0;
    let exit = graph
        .drive_cameras_blocking(&stop, &cameras, |graph, _turn| {
            for out in graph.drain_owned::<i64>("out")? {
                assert!(out > 0, "a tick mixed frames of different groups");
                assert_eq!(out % 10, 4, "a dropped-policy tick is always complete");
                ticks += 1;
                if out / 10 == FRAMES as i64 - 1 {
                    stop.stop();
                }
            }
            Ok(())
        })
        .expect("drive");
    for producer in producers {
        producer.join().expect("producer");
    }
    assert_eq!(exit, HostGraphDriveExit::Stopped);
    let stats = cameras.stats();
    assert!(ticks > 0 && stats.complete_groups >= ticks);
    assert!(stats.max_skew <= 8_000_000);
    for camera in 0..4 {
        assert!(cameras.buffered(camera) <= 4, "buffers stay bounded");
    }
    assert_eq!(
        stats.frames,
        stats.complete_groups * 4 + stats.dropped_frames + buffered(&cameras),
        "every accepted frame was grouped, dropped or is still buffered: {stats:?}"
    );
}

fn buffered(cameras: &daedalus::engine::SynchronizedCameras) -> u64 {
    (0..4).map(|camera| cameras.buffered(camera) as u64).sum()
}

#[test]
fn a_stalled_camera_ticks_partially_once_the_drive_loop_times_out() {
    let mut graph = compile();
    let cameras = MultiCamera::synchronized(
        graph.host(),
        PORTS,
        sync_config(PartialPolicy::TickPartial, Duration::from_millis(30)),
    );
    // Camera 3 never delivers; nothing else arrives after these three frames.
    for camera in 0..3 {
        cameras.push(camera, payload(&cam_frame(5, 0)));
    }
    assert!(!graph.host().has_pending_inbound(), "the group waits");
    assert!(cameras.poll_timeout().is_some());
    let stop = graph.stop_handle();
    let mut seen = Vec::new();
    graph
        .drive_cameras_blocking(&stop, &cameras, |graph, _turn| {
            seen.extend(graph.drain_owned::<i64>("out")?);
            stop.stop();
            Ok(())
        })
        .expect("drive");
    assert_eq!(
        seen,
        [53],
        "frame 5 from three cameras; camera 3's input is None"
    );
    let stats = cameras.stats();
    assert_eq!((stats.timeouts, stats.partial_groups), (1, 1));
    let group = stats.last_group.expect("group");
    assert_eq!(group.cameras, CameraSet::all(3));
}

#[test]
fn synchronized_steady_state_ticks_copy_and_allocate_nothing() {
    let mut graph = compile();
    graph.host().set_event_recording(false);
    let cameras = MultiCamera::synchronized(
        graph.host(),
        PORTS,
        sync_config(PartialPolicy::HoldLast, Duration::from_secs(1)),
    );
    let frames: Vec<Vec<Arc<CamFrame>>> = (0..40)
        .map(|sequence| (0..4).map(|camera| cam_frame(sequence, camera)).collect())
        .collect();
    let mut round = |payloads: Vec<Payload>| {
        let (pushes, ()) = allocations_during(|| {
            for (camera, payload) in payloads.into_iter().enumerate() {
                cameras.push(camera, payload);
            }
        });
        let (tick, outputs) = allocations_during(|| {
            graph
                .tick_if_ready()
                .expect("tick")
                .expect("a group ticked");
            (graph.take::<i64>("out"), graph.take::<i64>("ptr"))
        });
        (pushes, tick, outputs)
    };
    let payloads = |sequence: usize| frames[sequence].iter().map(payload).collect::<Vec<_>>();
    for sequence in 0..8 {
        round(payloads(sequence));
    }
    let (output, _) = allocations_during(|| Payload::owned("i64", 1_i64));
    for sequence in 8..40 {
        let (pushes, tick, (out, ptr)) = round(payloads(sequence));
        assert_eq!(out, Some(sequence as i64 * 10 + 4));
        assert_eq!(
            ptr,
            Some(frames[sequence][0].pixels.as_ptr() as i64),
            "the node read the camera's buffer in place"
        );
        // Buffering, grouping and committing four frames allocate nothing; a tick allocates only
        // the node's two output payloads.
        assert_eq!(pushes, 0, "round {sequence}: pushes allocated");
        assert!(
            tick <= 2 * output,
            "round {sequence}: tick allocated {tick} times ({output} per output)"
        );
    }
}

#[test]
fn independent_cameras_tick_per_arrival_on_the_newest_frames() {
    let mut graph = compile();
    let cameras = MultiCamera::independent(
        graph.host(),
        PORTS,
        IndependentConfig {
            trigger: Some(CameraSet::only(0)),
            max_age: None,
        },
    );
    cameras.push(1, payload(&cam_frame(3, 0)));
    assert!(
        graph.tick_if_ready().expect("tick").is_none(),
        "camera 1 does not trigger"
    );
    // A burst on the trigger camera: one tick, on its newest frame, and no queue growth.
    for sequence in 0..10 {
        cameras.push(0, payload(&cam_frame(sequence, 0)));
    }
    assert_eq!(graph.host().pending_inbound(), 1);
    graph.tick_if_ready().expect("tick").expect("ticked");
    assert_eq!(
        graph.take::<i64>("out"),
        Some(-1),
        "frame 9 with camera 1's frame 3"
    );
    assert!(
        graph.tick_if_ready().expect("tick").is_none(),
        "no stale re-tick"
    );
    cameras.push(1, payload(&cam_frame(9, 0)));
    cameras.push(2, payload(&cam_frame(9, 0)));
    cameras.push(0, payload(&cam_frame(9, 0)));
    graph.tick_if_ready().expect("tick").expect("ticked");
    assert_eq!(graph.take::<i64>("out"), Some(93));
    // Camera 0 alone again: the others' latest frames are held.
    cameras.push(0, payload(&cam_frame(9, 1)));
    graph.tick_if_ready().expect("tick").expect("ticked");
    assert_eq!(graph.take::<i64>("out"), Some(93));
    assert_eq!(cameras.stats().frames, 15);

    // Steady state: an arrival allocates nothing, its tick only the node's outputs.
    let next = cam_frame(10, 0);
    let (output, _) = allocations_during(|| Payload::owned("i64", 1_i64));
    for _ in 0..4 {
        let frame = payload(&next);
        let (push, _) = allocations_during(|| cameras.push(0, frame));
        let (tick, _) = allocations_during(|| {
            graph.tick_if_ready().expect("tick").expect("ticked");
            graph.take::<i64>("out")
        });
        assert_eq!(push, 0, "a push allocated");
        assert!(tick <= 2 * output, "a tick allocated {tick} times");
    }
}

#[test]
fn frame_stamps_read_frame_metadata_only() {
    let frame = cam_frame(7, 5);
    let owner = payload(&frame);
    assert_eq!(
        source_timestamp::<CamFrame>(&owner),
        Some(7 * PERIOD_NS + 5)
    );
    assert_eq!(
        frame_timestamp(&owner),
        None,
        "an owner payload is not a frame view"
    );
    let view = owner
        .provide_foreign::<CamFrame, FrameInterface>()
        .expect("provider");
    assert_eq!(view.type_key(), &TypeKey::new("daedalus:frame"));
    assert_eq!(frame_timestamp(&view), Some(7 * PERIOD_NS + 5));
    assert_eq!(frame_sequence(&view), Some(7));
}

#[test]
fn feeders_report_their_deadlines() {
    let graph = compile();
    let cameras = MultiCamera::independent(
        graph.host(),
        PORTS,
        IndependentConfig {
            trigger: None,
            max_age: Some(Duration::from_millis(1)),
        },
    );
    assert_eq!(cameras.next_deadline(), None);
    cameras.push(2, payload(&cam_frame(1, 0)));
    assert!(cameras.next_deadline().is_some());
    thread::sleep(Duration::from_millis(2));
    assert!(cameras.expire());
    assert_eq!(cameras.present(), CameraSet::EMPTY);
}
