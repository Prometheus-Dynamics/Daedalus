//! Steady-state frame-path overhead of the no-op `daedalus:frame` chain: no copies, no runtime or
//! node allocations, and the overhead report accounts for every stage.

use std::sync::Mutex;

use daedalus::{
    data::model::{TypeExpr, ValueType},
    engine::{Engine, EngineConfig, FrameOverheadReport, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, PortId, RuntimeNode, io::NodeIo, state::ExecutionContext},
    transport::FrameView,
};
use daedalus_frame_bench::{
    CHAIN_OUTPUT, CountingAllocator, FRAME_INTERFACE_KEY, FrameBenchConfig, FrameBenchRun,
    FrameFeed, FrameSourceConfig, SyntheticFrame, SyntheticFrameSource, compile_frame_chain,
    compile_frame_fanout, frame_bench_registry, run_frame_bench,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

/// Allocation counters are process-wide: one measurement at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn run(stages: usize, feed: FrameFeed, metrics: MetricsLevel) -> FrameBenchRun {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let config = EngineConfig::default()
        .with_metrics_level(metrics)
        .with_frame_overhead(256);
    let mut host = compile_frame_chain(stages, feed, config).expect("compile chain");
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        width: 64,
        height: 48,
        feed,
        ..FrameSourceConfig::default()
    })
    .expect("frame source");
    run_frame_bench(
        &mut host,
        &mut source,
        &FrameBenchConfig::new([CHAIN_OUTPUT]).with_ticks(64, 256),
    )
    .expect("frame bench")
}

fn max(report: &FrameOverheadReport, counter: &str) -> u64 {
    report.counter(counter).expect(counter).max
}

#[test]
fn external_frame_chain_has_no_copies_and_no_runtime_allocations() {
    for stages in [1, 4, 16] {
        let run = run(stages, FrameFeed::Interface, MetricsLevel::Off);
        let report = run.overhead.as_ref().expect("overhead report");
        assert!(report.alloc_probe, "the counting allocator is installed");
        assert_eq!(report.ticks, 256);
        assert_eq!(report.counter("nodes").expect("nodes").p50, stages as u64);
        for counter in [
            "copies",
            "copied_bytes",
            "zero_copy_adapts",
            "gpu_uploads",
            "gpu_downloads",
            "runtime_allocs",
            "node_allocs",
            "host_allocs",
        ] {
            assert_eq!(max(report, counter), 0, "{stages} stages: {counter}\n{run}");
        }
        let [runtime, node, host, _other] = run.allocs_per_frame.expect("alloc probe");
        assert_eq!((runtime, node, host), (0.0, 0.0, 0.0), "{run}");
    }
}

/// The owner feed's provider `View` adapter on each consumer edge: zero-copy and, since the
/// fetch borrows the owner value through the provider, allocation-free.
fn assert_owner_views(run: &FrameBenchRun, adapting_edges: usize) {
    let report = run.overhead.as_ref().expect("overhead report");
    for counter in [
        "copies",
        "copied_bytes",
        "runtime_allocs",
        "node_allocs",
        "host_allocs",
    ] {
        assert_eq!(max(report, counter), 0, "{counter}\n{run}");
    }
    let [runtime, node, host, _other] = run.allocs_per_frame.expect("alloc probe");
    assert_eq!((runtime, node, host), (0.0, 0.0, 0.0), "{run}");
    assert_eq!(
        max(report, "zero_copy_adapts"),
        adapting_edges as u64,
        "{run}"
    );
    let adapting: Vec<_> = report
        .edges
        .iter()
        .filter(|edge| edge.adapts_per_tick > 0.0)
        .collect();
    assert_eq!(adapting.len(), adapting_edges, "{run}");
    for edge in adapting {
        assert_eq!(edge.adapter.as_str(), "zero_copy");
        assert!(
            edge.label.starts_with("host.frame -> stage_"),
            "{}",
            edge.label
        );
    }
}

#[test]
fn owner_feed_views_frames_without_allocating() {
    for stages in [1, 4, 16] {
        assert_owner_views(&run(stages, FrameFeed::Owner, MetricsLevel::Off), 1);
    }
}

#[test]
fn owner_feed_fanout_views_frames_without_allocating() {
    for consumers in [1, 4, 16] {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let config = EngineConfig::default()
            .with_metrics_level(MetricsLevel::Off)
            .with_frame_overhead(256);
        let mut host =
            compile_frame_fanout(consumers, FrameFeed::Owner, config).expect("compile fanout");
        let mut source = SyntheticFrameSource::new(FrameSourceConfig {
            width: 64,
            height: 48,
            feed: FrameFeed::Owner,
            ..FrameSourceConfig::default()
        })
        .expect("frame source");
        let outputs = (0..consumers).map(|idx| format!("{CHAIN_OUTPUT}_{idx}"));
        let run = run_frame_bench(
            &mut host,
            &mut source,
            &FrameBenchConfig::new(outputs).with_ticks(64, 256),
        )
        .expect("frame bench");
        assert_owner_views(&run, consumers);
    }
}

#[test]
fn stages_add_up_to_the_tick() {
    let run = run(4, FrameFeed::Interface, MetricsLevel::Off);
    let report = run.overhead.as_ref().expect("overhead report");
    let mean = |name: &str| report.stage(name).expect(name).mean;
    let parts: f64 = [
        "inject",
        "inputs",
        "adapters_zero_copy",
        "adapters_copying",
        "handlers",
        "node_io",
        "drain",
        "dispatch",
    ]
    .into_iter()
    .map(mean)
    .sum();
    let tick = mean("tick");
    assert!(
        (parts - tick).abs() <= tick * 0.01,
        "{parts} vs {tick}\n{run}"
    );
    assert!(mean("graph_overhead") <= tick);
    assert!(mean("push") > 0.0 && mean("take") > 0.0, "{run}");
    // Every edge, the host output included, delivered the frame each tick.
    assert_eq!(report.edges.len(), 5, "{run}");
}

#[test]
fn detailed_metrics_keep_the_chain_copy_free() {
    let run = run(4, FrameFeed::Interface, MetricsLevel::Detailed);
    let report = run.overhead.as_ref().expect("overhead report");
    assert_eq!(max(report, "copies"), 0);
    assert_eq!(max(report, "node_allocs"), 0);
}

/// Reads the frame and the held IMU sample it is fused with, allocating nothing.
#[node(
    id = "fuse",
    inputs(
        port(name = "frame", type_key = "daedalus:frame"),
        port(name = "imu", ty = TypeExpr::Scalar(ValueType::Int))
    )
)]
fn fuse(_node: &RuntimeNode, _ctx: &ExecutionContext, io: &mut NodeIo) -> Result<(), NodeError> {
    let frame: FrameView<'_> = io.get_foreign("frame")?;
    let imu = io.get_ref::<i64>("imu");
    std::hint::black_box((frame.width(), imu.copied()));
    Ok(())
}

/// [`fuse`] as a typed node, the way applications write it: the macro-generated fetch of a
/// `FrameView` and a borrowed context input.
#[node(id = "typed_fuse", inputs("frame", "imu"))]
fn typed_fuse(frame: FrameView<'_>, imu: &i64) -> Result<(), NodeError> {
    std::hint::black_box((frame.width(), *imu));
    Ok(())
}

#[plugin(id = "frame_bench_test", nodes(fuse, typed_fuse))]
struct ContextPlugin;

/// Frames (batched with an IMU sample every 16th tick, held in between) into `node`; asserts no
/// copies and no runtime, node or host allocations per tick.
fn assert_fused_without_allocating(node: &str, feed: FrameFeed) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut registry = frame_bench_registry().expect("registry");
    registry.install(&ContextPlugin::new()).expect("install");
    let fuse = daedalus::NodeHandle::new(format!("frame_bench_test:{node}")).alias("fuse");
    let builder = registry.graph_builder().expect("builder");
    let builder = match feed {
        FrameFeed::Interface => builder.input_as("frame", TypeExpr::opaque(FRAME_INTERFACE_KEY)),
        FrameFeed::Owner => builder
            .input_typed::<SyntheticFrame>("frame")
            .expect("frame"),
    };
    let graph = builder
        .input_typed::<i64>("imu")
        .and_then(|b| b.held_input("imu").try_node(&fuse))
        .and_then(|b| b.try_connect("frame", &fuse.input("frame")))
        .and_then(|b| b.try_connect("imu", &fuse.input("imu")))
        .expect("wire")
        .build();
    let config = EngineConfig::default()
        .with_metrics_level(MetricsLevel::Off)
        .with_frame_overhead(256)
        .with_host_event_recording(false);
    let mut host = Engine::new(config)
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");
    host.prepare().expect("prepare");
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        width: 64,
        height: 48,
        feed,
        ..FrameSourceConfig::default()
    })
    .expect("frame source");
    let (frame, imu) = (PortId::from("frame"), PortId::from("imu"));
    let mut before = daedalus::alloc_probe::counts();
    for tick in 0..(64 + 256_i64) {
        if tick == 64 {
            host.reset_frame_overhead();
            before = daedalus::alloc_probe::counts();
        }
        // The frame and, every 16th tick, a new IMU sample land in one tick; in between the
        // held sample rides along.
        let mut batch = host
            .batch()
            .push_payload(frame.clone(), source.next_payload());
        if tick % 16 == 0 {
            batch = batch.push(imu.clone(), tick);
        }
        batch.commit().expect("batch");
        host.tick().expect("tick");
    }
    let during = daedalus::alloc_probe::counts().since(&before);
    let report = host.frame_overhead().expect("overhead report");
    let context = format!("{node}, {} feed\n{report}", feed.as_str());
    assert_eq!(report.counter("nodes").expect("nodes").p50, 1, "{context}");
    for counter in [
        "copies",
        "copied_bytes",
        "runtime_allocs",
        "node_allocs",
        "host_allocs",
    ] {
        assert_eq!(max(&report, counter), 0, "{counter}: {context}");
    }
    assert_eq!(
        report.counter("shared_clones").expect("shared clones").p50,
        1,
        "the held sample is an Arc clone each tick: {context}"
    );
    let adapts = u64::from(feed == FrameFeed::Owner);
    assert_eq!(max(&report, "zero_copy_adapts"), adapts, "{context}");
    assert!(report.stage("push").expect("push").mean > 0.0, "{context}");
    assert_eq!(
        (during.runtime, during.node, during.host),
        (0, 0, 0),
        "{context}"
    );
}

#[test]
fn held_context_and_batched_frames_add_no_copies_or_runtime_allocations() {
    for feed in [FrameFeed::Interface, FrameFeed::Owner] {
        assert_fused_without_allocating("fuse", feed);
    }
}

#[test]
fn typed_frame_view_nodes_fetch_without_allocating() {
    for feed in [FrameFeed::Interface, FrameFeed::Owner] {
        assert_fused_without_allocating("typed_fuse", feed);
    }
}
