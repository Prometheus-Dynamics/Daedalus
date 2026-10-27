// The report is this example's output.
#![allow(clippy::print_stdout)]

mod graph;
mod stages;

use std::sync::Arc;
use std::time::Instant;

use daedalus::data::daedalus_type::DaedalusTypeExpr as _;
use daedalus::{
    engine::{Engine, EngineConfig, HostGraph, MetricsLevel, RuntimeMode},
    runtime::handler_registry::HandlerRegistry,
    transport::Payload,
};
use daedalus_frame_bench::{
    ALL_EVENTS, CacheCounters, CountingAllocator, FrameFeed, FrameSourceConfig,
    SyntheticFrameSource, env_usize,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Node id prefix of the mirror plugin.
pub(crate) const PLUGIN: &str = "eidos";
/// Host inputs and outputs, as the live graph names them (`edges.txt`).
pub(crate) const FRAME: &str = "frame";
pub(crate) const CAMERA: &str = "camera";
pub(crate) const EXTRINSICS: &str = "extrinsics";
pub(crate) const HOST_OUTPUTS: [&str; 6] = [
    "detections",
    "rejected",
    "refined_corners",
    "poses",
    "pose_solutions",
    "multi_tag_pose",
];
/// Frame-overhead window, as on the live engine (`window 512`).
pub(crate) const WINDOW: usize = 512;
/// The `Blob` value type's key (its `#[type_key]` below).
pub(crate) const BLOB_KEY: &str = "daedalus.frame_bench.mirror:blob";
/// Stub outputs carry this much data, refilled in place.
pub(crate) const STUB_ITEMS: [f32; 4] = [0.0; 4];

use graph::{mirror_graph, mirror_registry};
use stages::Blob;

/// Compile and prepare the mirror at `level`, with frame-overhead recording on or off.
fn compile(level: MetricsLevel, record: bool) -> Result<HostGraph<HandlerRegistry>, Error> {
    let registry = mirror_registry()?;
    let graph = mirror_graph(&registry)?;
    let mut config = EngineConfig::default()
        .with_metrics_level(level)
        .with_runtime_mode(RuntimeMode::Serial)
        .with_host_event_recording(false);
    if record {
        config = config.with_frame_overhead(WINDOW);
    }
    let mut host = Engine::new(config)?.compile_registry(&registry, graph)?;
    host.prepare()?;
    Ok(host)
}

/// One measured run: per-tick wall time, instructions and allocations, and the overhead report.
struct Measured {
    tick_p50_ns: u64,
    tick_mean_ns: f64,
    /// Per tick, per hardware event in `ALL_EVENTS` order; `None` where the kernel refuses it.
    hardware_per_tick: Vec<(&'static str, Option<f64>)>,
    allocs_per_tick: Option<[f64; 4]>,
    report: Option<daedalus::runtime::executor::FrameOverheadReport>,
}

fn measure(
    level: MetricsLevel,
    record: bool,
    ticks: usize,
    warmup: usize,
) -> Result<Measured, Error> {
    let mut host = compile(level, record)?;
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        feed: FrameFeed::Interface,
        ..FrameSourceConfig::default()
    })?;
    let camera = Payload::shared(Blob::TYPE_KEY, Arc::new(Blob::default()));
    let extrinsics = Payload::shared(Blob::TYPE_KEY, Arc::new(Blob::default()));
    let frame_port = daedalus::runtime::PortId::new(FRAME);
    let camera_port = daedalus::runtime::PortId::new(CAMERA);
    let extrinsics_port = daedalus::runtime::PortId::new(EXTRINSICS);
    let mut tick = |host: &mut HostGraph<HandlerRegistry>| -> Result<(), Error> {
        host.push_payload(frame_port.clone(), source.next_payload());
        host.push_payload(camera_port.clone(), camera.clone());
        host.push_payload(extrinsics_port.clone(), extrinsics.clone());
        host.tick()?;
        for output in HOST_OUTPUTS {
            while host.take_payload(output).is_some() {}
        }
        Ok(())
    };
    for _ in 0..warmup {
        tick(&mut host)?;
    }
    host.reset_frame_overhead();
    let mut wall = vec![0u64; ticks];
    let before = daedalus::alloc_probe::counts();
    let counters = CacheCounters::start(&ALL_EVENTS);
    for slot in wall.iter_mut() {
        let start = Instant::now();
        tick(&mut host)?;
        *slot = start.elapsed().as_nanos() as u64;
    }
    let hardware = counters.read();
    let during = daedalus::alloc_probe::counts().since(&before);
    let mean = wall.iter().sum::<u64>() as f64 / ticks as f64;
    wall.sort_unstable();
    let per = |count: u64| count as f64 / ticks as f64;
    Ok(Measured {
        tick_p50_ns: wall[(ticks / 2).min(ticks - 1)],
        tick_mean_ns: mean,
        hardware_per_tick: hardware
            .into_iter()
            .map(|(name, count)| (name, count.map(|count| count as f64 / ticks as f64)))
            .collect(),
        allocs_per_tick: daedalus::alloc_probe::is_installed().then(|| {
            [
                per(during.runtime),
                per(during.node),
                per(during.host),
                per(during.other),
            ]
        }),
        report: host.frame_overhead(),
    })
}

fn print_run(label: &str, run: &Measured) {
    println!("== {label} ==");
    println!(
        "tick (push + tick + take): p50 {} ns, mean {:.0} ns",
        run.tick_p50_ns, run.tick_mean_ns
    );
    for (name, per_tick) in &run.hardware_per_tick {
        match per_tick {
            Some(count) => println!("{name} per tick: {count:.1}"),
            None => println!("{name} per tick: n/a (the kernel refuses this event)"),
        }
    }
    if let Some([runtime, node, host, other]) = run.allocs_per_tick {
        println!(
            "allocations per tick: runtime {runtime:.3}, node {node:.3}, host {host:.3}, other {other:.3}"
        );
    }
    if let Some(report) = &run.report {
        println!("{report}");
    }
}

fn main() -> Result<(), Error> {
    if let Some(cpu) = daedalus_frame_bench::pin_from_env()? {
        println!("pinned to CPU {cpu}");
    }
    let ticks = env_usize("MIRROR_TICKS", 5000).max(1);
    let warmup = env_usize("MIRROR_WARMUP", ticks / 5);
    println!(
        "apriltag_mirror: {} {}, 21 stages, serial, {} measured ticks per run, window {WINDOW}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        ticks
    );
    // Untimed run so the first measured one does not pay for cold caches and clock ramp-up.
    measure(MetricsLevel::Off, false, ticks / 4, warmup)?;
    for (name, level) in [("Timing", MetricsLevel::Timing), ("Off", MetricsLevel::Off)] {
        let plain = measure(level, false, ticks, warmup)?;
        print_run(&format!("metrics {name}, no recording"), &plain);
        let recorded = measure(level, true, ticks, warmup)?;
        print_run(
            &format!("metrics {name}, frame overhead recorded"),
            &recorded,
        );
    }
    Ok(())
}
