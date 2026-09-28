//! Template for integrating a camera-like frame source without any camera dependency.
//!
//! Follows "Integrating An External Frame Source" in `docs/node-authoring.md`:
//! a stable type key, a descriptor type, a plugin that registers everything once, zero-copy
//! payload wrapping, and a producer feeding a typed, latest-only host input while the host graph
//! is driven on input.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use daedalus::{
    DaedalusToValue, DaedalusTypeExpr, adapt,
    data::to_value::ToValue,
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::{Payload, Residency, TransportError},
    type_key,
};

/// Step 1: The stable key every frame payload carries. Treat it as a public contract.
pub const FRAME_TYPE_KEY: &str = "example:synthetic_frame";

/// Step 2: The frame carrier. `pixels` stands in for memory owned by the source (a dmabuf, a driver
/// ring buffer, ...). It is never cloned: payloads share it through an `Arc`.
#[type_key(FRAME_TYPE_KEY)]
struct SyntheticFrame {
    pixels: Arc<[u8]>,
    width: u32,
    height: u32,
    stride: u32,
    sequence: u64,
    timestamp_ns: u64,
}

/// Step 2: Plain descriptor: this is what graph documents, editors, and host inspection see.
#[derive(Clone, Debug, DaedalusTypeExpr, DaedalusToValue)]
#[daedalus(type_key = "example:frame_meta")]
struct FrameMeta {
    width: u32,
    height: u32,
    format: String,
    sequence: u64,
    timestamp_ns: u64,
    planes: Vec<PlaneMeta>,
    residency: String,
}

#[derive(Clone, Debug, DaedalusTypeExpr, DaedalusToValue)]
struct PlaneMeta {
    stride: u32,
    len: u64,
}

impl SyntheticFrame {
    fn meta(&self) -> FrameMeta {
        FrameMeta {
            width: self.width,
            height: self.height,
            format: "GRAY8".into(),
            sequence: self.sequence,
            timestamp_ns: self.timestamp_ns,
            planes: vec![PlaneMeta {
                stride: self.stride,
                len: self.pixels.len() as u64,
            }],
            residency: Residency::External.as_str().into(),
        }
    }
}

// Step 3: Adapter + nodes. `frame.meta` takes `FrameMeta`, so the planner inserts this adapter on
// the edge from a frame port; it reads metadata only and never touches the pixels.
#[adapt(id = "example.frame_to_meta", kind = daedalus::transport::AdapterKind::MetadataOnly)]
fn frame_to_meta(frame: &SyntheticFrame) -> Result<FrameMeta, TransportError> {
    Ok(frame.meta())
}

#[node(id = "frame.mean_luma", inputs("frame"), outputs("luma"))]
fn mean_luma(frame: &SyntheticFrame) -> Result<f64, NodeError> {
    let sum: u64 = frame.pixels.iter().map(|&p| u64::from(p)).sum();
    Ok(sum as f64 / frame.pixels.len().max(1) as f64)
}

#[node(id = "frame.meta", inputs("meta"), outputs("meta"))]
fn frame_meta(meta: &FrameMeta) -> Result<FrameMeta, NodeError> {
    Ok(meta.clone())
}

/// Frames inspect as their descriptor instead of an opaque summary.
fn install(registry: &mut PluginRegistry) -> daedalus::runtime::plugins::PluginResult<()> {
    registry.register_value_serializer::<SyntheticFrame, _>(|frame| frame.meta().to_value());
    Ok(())
}

/// Step 3: Register once: the frame type, the descriptor (its nested `PlaneMeta` comes along),
/// the frame serializer, the adapter and the nodes.
#[plugin(
    id = "example.external_frame_source",
    install = install,
    types(SyntheticFrame),
    values(FrameMeta),
    nodes(mean_luma, frame_meta),
    adapters(frame_to_meta)
)]
struct FrameSourcePlugin;

/// Step 4: Wrap without copying: the payload shares the frame `Arc`; `External` marks memory the
/// graph does not own.
fn wrap(frame: SyntheticFrame) -> Payload {
    let bytes = frame.pixels.len() as u64;
    Payload::shared_with(
        FRAME_TYPE_KEY,
        Arc::new(frame),
        Residency::External,
        None,
        Some(bytes),
    )
}

fn synthetic_frame(seq: u64, started: Instant) -> SyntheticFrame {
    let (width, height) = (320u32, 240u32);
    let pixels: Arc<[u8]> = (0..width * height)
        .map(|i| (i % width) as u8 / 2 + (seq % 64) as u8)
        .collect();
    SyntheticFrame {
        pixels,
        width,
        height,
        stride: width,
        sequence: seq,
        timestamp_ns: started.elapsed().as_nanos() as u64,
    }
}

const TICKS: u64 = 20;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = FrameSourcePlugin::new();
    registry.install(&plugin)?;
    let luma = plugin.mean_luma.alias("luma");
    let meta = plugin.frame_meta.alias("meta");
    // A typed host input fans out to ports of different types; the planner adapts per edge.
    let graph = registry
        .graph_builder()?
        .input_typed::<SyntheticFrame>("frame")
        .try_node(&luma)?
        .try_node(&meta)?
        .try_connect("frame", &luma.inputs.frame)?
        .try_connect("frame", &meta.inputs.meta)?
        .try_connect(&luma.outputs.luma, "luma")?
        .try_connect(&meta.outputs.meta, "meta")?
        .build();
    let mut host = Engine::new(EngineConfig::default())?.compile_registry(&registry, graph)?;

    for port in host.host_inputs().iter().chain(&host.host_outputs()) {
        println!(
            "{:?} {} : {:?}",
            port.direction,
            port.name(),
            port.type_expr
        );
    }
    for edge in host.explain_plan().edges {
        if !edge.adapter_steps.is_empty() {
            let (from, to) = (edge.from_port, edge.to_port);
            println!("edge {from} -> {to} adapters: {:?}", edge.adapter_steps);
        }
    }

    // Step 5: Latest-only input: a fast producer replaces stale frames instead of queueing them.
    host.set_latest_input("frame")?;
    let started = Instant::now();
    let first = wrap(synthetic_frame(0, started));
    println!("input frame: {}", host.inspect_payload(&first).to_json());

    let stop = host.stop_handle();
    let produced = Arc::new(AtomicU64::new(0));
    let producer = {
        let (input, stop, produced) = (
            host.bind_payload_input("frame"),
            stop.clone(),
            produced.clone(),
        );
        thread::spawn(move || {
            let mut seq = 0;
            while !stop.is_stopped() {
                input.push(wrap(synthetic_frame(seq, started)));
                seq = produced.fetch_add(1, Ordering::Relaxed) + 1;
                thread::sleep(Duration::from_millis(5)); // ~200 fps
            }
        })
    };

    // Each tick sees only the newest frame; `sequence` jumps show the frames that were replaced.
    let mut ticks = 0;
    host.drive_blocking(&stop, |host, _turn| {
        ticks += 1;
        let (mut luma, mut meta) = Default::default();
        for (port, inspection) in host.inspect_outputs() {
            match port.as_str() {
                "luma" => luma = inspection.to_json(),
                "meta" => meta = inspection.to_json(),
                _ => {}
            }
        }
        println!("tick {ticks:>2} sequence={} luma={luma}", meta["sequence"]);
        if ticks == TICKS {
            println!("last meta: {meta}");
        }
        thread::sleep(Duration::from_millis(20)); // simulate a graph slower than the source
        if ticks >= TICKS {
            stop.stop();
        }
        Ok(())
    })?;
    producer.join().expect("producer thread");

    let frame = host.host().input_port_stats("frame").unwrap_or_default();
    let luma = host.host().output_port_stats("luma").unwrap_or_default();
    println!(
        "produced={} ticks={ticks} frame: accepted={} replaced(stale)={} dropped={} delivered={} \
         luma: delivered={}",
        produced.load(Ordering::Relaxed),
        frame.accepted,
        frame.replaced,
        frame.dropped,
        frame.delivered,
        luma.delivered,
    );
    Ok(())
}
