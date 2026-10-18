//! Two cameras feeding one graph through the two multi-camera feeders.
//!
//! `MultiCamera::synchronized` groups frames by capture timestamp (within a window) and ticks
//! each complete group whole, so `fuse` never mixes frames of different instants.
//! `MultiCamera::independent` holds each camera's newest frame and ticks on every arrival, so
//! `fuse` always sees the latest frame of each camera. Both are driven on this thread: push, then
//! tick the graph when it is ready. Frames are `Arc` clones, not copies.

use std::sync::Arc;
use std::time::Duration;

use daedalus::{
    engine::{
        Engine, EngineConfig, HostGraph, IndependentConfig, MetricsLevel, MultiCamera,
        PartialPolicy, SyncConfig,
    },
    macros::{node, plugin},
    runtime::{
        NodeError, handler_registry::HandlerRegistry, host_bridge::multicam::source_timestamp,
        plugins::PluginRegistry,
    },
    transport::{FramePlane, FrameResidency, FrameSource, Payload, fourcc},
    type_key,
};

const FRAME_KEY: &str = "example:multicam:frame";
const PERIOD_NS: u64 = 33_000_000;

/// A camera frame: pixels, the capture timestamp (ns) and a sequence number.
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

/// `front * 100 + side`: the sequence of each camera's frame, so a tick shows which frames it saw.
#[node(id = "multicam.fuse", inputs("front", "side"), outputs("out"))]
fn fuse(front: &CamFrame, side: &CamFrame) -> Result<i64, NodeError> {
    Ok(front.sequence as i64 * 100 + side.sequence as i64)
}

#[plugin(id = "example.multicam", types(CamFrame), nodes(fuse))]
struct MultiCamPlugin;

fn compile() -> Result<HostGraph<HandlerRegistry>, Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = MultiCamPlugin::new();
    registry.install(&plugin)?;
    let fuse = plugin.fuse.alias("fuse");
    let graph = registry
        .graph_builder()?
        .input_typed::<CamFrame>("front")?
        .input_typed::<CamFrame>("side")?
        .try_node(&fuse)?
        .try_connect("front", &fuse.inputs.front)?
        .try_connect("side", &fuse.inputs.side)?
        .try_connect(&fuse.outputs.out, "out")?
        .build();
    let engine = Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Off))?;
    Ok(engine.compile_registry(&registry, graph)?)
}

fn frame(sequence: u64, jitter_ns: u64) -> Payload {
    Payload::shared(
        FRAME_KEY,
        Arc::new(CamFrame {
            pixels: vec![sequence as u8; 16],
            timestamp_ns: sequence * PERIOD_NS + jitter_ns,
            sequence,
        }),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PORTS: [&str; 2] = ["front", "side"];

    // Synchronized: the side camera's frames arrive 2 ms late; the 8 ms window groups them.
    let mut graph = compile()?;
    let cameras = MultiCamera::synchronized(
        graph.host(),
        PORTS,
        SyncConfig {
            partial: PartialPolicy::Drop,
            stamp: source_timestamp::<CamFrame>,
            ..SyncConfig::new(Duration::from_millis(8), Duration::from_secs(1))
        },
    );
    for sequence in 0..3 {
        // The front frame is held until its side frame arrives, then the group ticks whole.
        cameras.push(0, frame(sequence, 0));
        cameras.push(1, frame(sequence, 2_000_000));
        graph.tick_if_ready()?;
        println!(
            "synchronized group {sequence}: out={:?}",
            graph.take::<i64>("out")
        );
    }
    let stats = cameras.stats();
    println!(
        "synchronized stats: frames={} complete_groups={} max_skew={} ns",
        stats.frames, stats.complete_groups, stats.max_skew
    );

    // Independent: every arrival ticks on the newest frame of each camera.
    let mut graph = compile()?;
    let cameras = MultiCamera::independent(graph.host(), PORTS, IndependentConfig::default());
    cameras.push(0, frame(0, 0));
    let nodes = graph.tick_if_ready()?.map(|t| t.nodes_executed);
    println!("independent: front alone -> nodes run={nodes:?} (side has no frame yet)");
    cameras.push(1, frame(0, 0));
    graph.tick_if_ready()?;
    println!(
        "independent: side arrives -> out={:?}",
        graph.take::<i64>("out")
    );
    cameras.push(0, frame(1, 0));
    cameras.push(0, frame(2, 0));
    graph.tick_if_ready()?;
    println!(
        "independent: front bursts 1 and 2 -> one tick on the newest, out={:?}",
        graph.take::<i64>("out")
    );
    println!("independent cameras present: {:?}", cameras.present());
    Ok(())
}
