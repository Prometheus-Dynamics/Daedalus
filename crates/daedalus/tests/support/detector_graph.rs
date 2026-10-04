//! A detector-like graph shared by the `graph_frame_allocations` test and the `graph_frame` bench.
//!
//! One host `frame` input fans out to 16 nodes: config-struct and const inputs (one node takes
//! five inputs; one config has a serde enum and a `String` field, one node takes them as owned
//! constants), a metadata-only adapter edge, connected and unconnected `Option<T>` inputs, two
//! conditional producers (one emits every frame, one never, so its required consumer is skipped),
//! `Arc`'d list and small-struct outputs, fan-in nodes, and four host outputs. Handlers allocate
//! nothing per frame (they reuse `Arc`'d inputs and state and return `Copy` values), so
//! allocations measured around a frame are the runtime's own plus the payloads pushed.

use std::sync::Arc;

use daedalus::{
    NodeHandleLike, PortHandle, adapt,
    data::model::Value,
    engine::{Engine, EngineConfig, EngineError, HostGraph, MetricsLevel, RuntimeMode},
    macros::{NodeConfig, node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
    transport::{Payload, TransportError},
    type_key,
};

pub const FRAME_KEY: &str = "bench:detector:frame";
const INFO_KEY: &str = "bench:detector:info";
pub const FRAME_PIXELS: usize = 64 * 48;

#[type_key(FRAME_KEY)]
pub struct Frame {
    pub pixels: Vec<u8>,
    pub width: u32,
    pub height: u32,
}

impl Frame {
    pub fn new(seed: u8) -> Self {
        Self {
            pixels: (0..FRAME_PIXELS)
                .map(|i| (i as u8).wrapping_add(seed))
                .collect(),
            width: 64,
            height: 48,
        }
    }
}

#[type_key(INFO_KEY)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrameInfo {
    pub width: u32,
    pub height: u32,
}

#[type_key("bench:detector:stats")]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Stats {
    pub mean: f64,
    pub max: u8,
}

#[type_key("bench:detector:roi")]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Roi {
    pub x: u32,
    pub y: u32,
    pub w: u32,
    pub h: u32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Detection {
    pub x: u32,
    pub y: u32,
    pub score: f32,
}

#[type_key("bench:detector:detections")]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Detections(pub Vec<Detection>);

#[type_key("bench:detector:histogram")]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Histogram(pub [u32; 8]);

#[type_key("bench:detector:track")]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Track {
    pub count: u32,
    pub best: f32,
    pub label_len: u32,
}

#[type_key("bench:detector:report")]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Report {
    pub tracks: u32,
    pub quality: f64,
    pub detections: i64,
    pub roi_area: u32,
}

#[adapt(id = "bench.detector.frame_info", from = FRAME_KEY, to = INFO_KEY, kind = "metadata_only")]
fn frame_info(frame: &Frame) -> Result<FrameInfo, TransportError> {
    Ok(FrameInfo {
        width: frame.width,
        height: frame.height,
    })
}

#[derive(Clone, Debug, NodeConfig)]
pub struct StatsConfig {
    #[port(default = 4, min = 1, max = 64, policy = "clamp")]
    stride: i64,
}

#[derive(Clone, Debug, NodeConfig)]
pub struct DetectConfig {
    #[port(default = 0.5, min = 0.0, max = 1.0, policy = "clamp")]
    threshold: f64,
    #[port(default = 8, min = 1, max = 64, policy = "clamp")]
    max_detections: i64,
    #[port(default = 0.25, min = 0.0, max = 1.0, policy = "clamp")]
    min_score: f64,
}

/// Decoded through serde (no `DaedalusTypeExpr`), the costliest const conversion.
#[derive(Clone, Copy, Debug, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackScore {
    Best,
    First,
}

#[derive(Clone, Debug, NodeConfig)]
pub struct TrackConfig {
    #[port(default = 3, min = 1, max = 16, policy = "clamp")]
    history: i64,
    #[port(default = "best")]
    score: TrackScore,
    #[port(default = "main")]
    label: String,
}

/// Per-node scratch so detector outputs are shared, not reallocated, every frame.
#[derive(Default)]
pub struct DetectionsState {
    dets: Arc<Detections>,
}

impl DetectionsState {
    /// Refresh the shared detections in place when no consumer still holds last frame's.
    fn publish(&mut self, fill: impl FnOnce(&mut Vec<Detection>)) -> Arc<Detections> {
        if let Some(dets) = Arc::get_mut(&mut self.dets) {
            dets.0.clear();
            fill(&mut dets.0);
        }
        self.dets.clone()
    }
}

#[node(id = "bench.detector.stats", inputs("frame", config = StatsConfig), outputs("stats"))]
fn stats(frame: &Frame, cfg: StatsConfig) -> Result<Stats, NodeError> {
    let step = cfg.stride.max(1) as usize;
    let (mut sum, mut count, mut max) = (0u64, 0u64, 0u8);
    for &p in frame.pixels.iter().step_by(step) {
        sum += u64::from(p);
        count += 1;
        max = max.max(p);
    }
    Ok(Stats {
        mean: sum as f64 / count.max(1) as f64,
        max,
    })
}

#[node(id = "bench.detector.roi", inputs("info"), outputs("roi"))]
fn roi(info: &FrameInfo) -> Result<Roi, NodeError> {
    Ok(Roi {
        x: info.width / 4,
        y: info.height / 4,
        w: info.width / 2,
        h: info.height / 2,
    })
}

#[node(
    id = "bench.detector.coarse",
    inputs("frame", "stats", config = DetectConfig),
    outputs("detections"),
    state(DetectionsState)
)]
fn coarse(
    frame: &Frame,
    stats: &Stats,
    cfg: DetectConfig,
    state: &mut DetectionsState,
) -> Result<Arc<Detections>, NodeError> {
    let cut = stats.mean * (1.0 + cfg.threshold);
    let score = cfg.min_score.max(0.5) as f32;
    let limit = cfg.max_detections as usize;
    Ok(state.publish(|dets| {
        let hits = frame.pixels.iter().enumerate();
        for (idx, _) in hits.filter(|(_, p)| f64::from(**p) > cut).take(limit) {
            let idx = idx as u32;
            dets.push(Detection {
                x: idx % frame.width,
                y: idx / frame.width,
                score,
            });
        }
    }))
}

#[node(
    id = "bench.detector.fine",
    inputs("frame", "roi", "scale", "gain"),
    outputs("detections"),
    state(DetectionsState)
)]
fn fine(
    frame: &Frame,
    roi: Option<&Roi>,
    scale: f64,
    gain: Option<f64>,
    state: &mut DetectionsState,
) -> Result<Arc<Detections>, NodeError> {
    let score = (scale * gain.unwrap_or(0.1)) as f32;
    let full = Roi {
        x: 0,
        y: 0,
        w: frame.width,
        h: frame.height,
    };
    let roi = roi.unwrap_or(&full);
    Ok(state.publish(|dets| {
        let n = (roi.w.min(frame.width) / 8).min(4);
        dets.extend((0..n).map(|i| Detection {
            x: roi.x + i,
            y: roi.y + i,
            score,
        }));
    }))
}

/// Conditional output: a tightened ROI for frames bright enough to refine (every test frame).
#[node(id = "bench.detector.refine", inputs("stats", "roi"), outputs("roi"))]
fn refine(stats: &Stats, roi: &Roi) -> Result<Option<Roi>, NodeError> {
    Ok((stats.mean > 64.0).then_some(Roi {
        x: roi.x + 1,
        y: roi.y + 1,
        w: roi.w.saturating_sub(2),
        h: roi.h.saturating_sub(2),
    }))
}

/// Conditional output: an alarm level for nearly black frames (never for test frames).
#[node(id = "bench.detector.alarm", inputs("stats"), outputs("level"))]
fn alarm(stats: &Stats) -> Result<Option<i64>, NodeError> {
    Ok((stats.mean < 8.0).then_some(i64::from(stats.max)))
}

/// Required input from `alarm`: skipped on every frame without an alarm.
#[node(id = "bench.detector.escalate", inputs("level"), outputs("escalation"))]
fn escalate(level: i64) -> Result<i64, NodeError> {
    Ok(level * 2)
}

#[node(id = "bench.detector.histogram", inputs("frame"), outputs("histogram"))]
fn histogram(frame: &Frame) -> Result<Histogram, NodeError> {
    let mut bins = [0u32; 8];
    for &p in &frame.pixels {
        bins[usize::from(p >> 5)] += 1;
    }
    Ok(Histogram(bins))
}

/// Owned constants of a serde enum and a `String`: decoded once, then cloned per call.
#[node(
    id = "bench.detector.exposure",
    inputs("histogram", "target", "weighting", "zone"),
    outputs("exposure")
)]
fn exposure(
    histogram: &Histogram,
    target: f64,
    weighting: TrackScore,
    zone: String,
) -> Result<f64, NodeError> {
    let bins = match weighting {
        TrackScore::Best => &histogram.0[4..],
        TrackScore::First => &histogram.0[..4],
    };
    let bright = bins.iter().sum::<u32>() as f64;
    let total = histogram.0.iter().sum::<u32>().max(1) as f64;
    Ok(bright / total - target - zone.len() as f64 / 1000.0)
}

#[node(id = "bench.detector.sharpness", inputs("frame"), outputs("sharpness"))]
fn sharpness(frame: Arc<Frame>) -> Result<f64, NodeError> {
    let diff: u64 = frame
        .pixels
        .windows(2)
        .map(|w| u64::from(w[0].abs_diff(w[1])))
        .sum();
    Ok(diff as f64 / frame.pixels.len().max(1) as f64)
}

#[node(id = "bench.detector.preview", inputs("frame"), outputs("preview"))]
fn preview(frame: Arc<Frame>) -> Result<Arc<Frame>, NodeError> {
    Ok(frame)
}

#[node(
    id = "bench.detector.merge",
    inputs("coarse_dets", "fine_dets"),
    outputs("detections"),
    state(DetectionsState)
)]
fn merge_detections(
    coarse_dets: Arc<Detections>,
    fine_dets: &Detections,
    state: &mut DetectionsState,
) -> Result<Arc<Detections>, NodeError> {
    Ok(state.publish(|dets| {
        dets.extend_from_slice(&coarse_dets.0);
        dets.extend_from_slice(&fine_dets.0);
    }))
}

#[node(id = "bench.detector.track", inputs("detections", "info", config = TrackConfig), outputs("track"))]
fn track(detections: &Detections, info: &FrameInfo, cfg: &TrackConfig) -> Result<Track, NodeError> {
    let mut scores = detections
        .0
        .iter()
        .filter(|det| det.x < info.width)
        .map(|det| det.score);
    let best = match cfg.score {
        TrackScore::Best => scores.fold(0.0f32, f32::max),
        TrackScore::First => scores.next().unwrap_or_default(),
    };
    Ok(Track {
        count: detections.0.len().min(cfg.history as usize * 8) as u32,
        best,
        label_len: cfg.label.len() as u32,
    })
}

#[node(id = "bench.detector.count", inputs("detections"), outputs("count"))]
fn count(detections: &Detections) -> Result<i64, NodeError> {
    Ok(detections.0.len() as i64)
}

#[node(
    id = "bench.detector.quality",
    inputs("exposure", "sharpness", "stats"),
    outputs("quality")
)]
fn quality(exposure: f64, sharpness: f64, stats: &Stats) -> Result<f64, NodeError> {
    Ok(sharpness - exposure.abs() + f64::from(stats.max) / 255.0)
}

#[node(
    id = "bench.detector.report",
    inputs("track", "quality", "count", "roi", "escalation"),
    outputs("report")
)]
fn report(
    track: &Track,
    quality: f64,
    count: i64,
    roi: &Roi,
    escalation: Option<i64>,
) -> Result<Report, NodeError> {
    Ok(Report {
        tracks: track.count,
        quality,
        detections: count + escalation.unwrap_or(0),
        roi_area: roi.w * roi.h,
    })
}

#[plugin(
    id = "bench.detector",
    types(Frame, FrameInfo, Stats, Roi, Detections, Histogram, Track, Report),
    nodes(
        stats,
        roi,
        coarse,
        fine,
        histogram,
        exposure,
        sharpness,
        preview,
        merge_detections,
        track,
        count,
        quality,
        report,
        refine,
        alarm,
        escalate
    ),
    adapters(frame_info)
)]
pub struct DetectorPlugin;

/// Host outputs the harness drains every frame.
pub const OUTPUTS: [&str; 4] = ["report", "detections", "count", "preview"];

/// Compile the detector graph for `mode` and `metrics`, ready to tick.
pub fn compile(
    mode: RuntimeMode,
    metrics: MetricsLevel,
) -> Result<HostGraph<HandlerRegistry>, EngineError> {
    let mut registry = PluginRegistry::new();
    let plugin = DetectorPlugin::new();
    registry.install(&plugin).expect("install detector plugin");
    let p = &plugin;
    let (stats, roi, coarse, fine) = (&p.stats, &p.roi, &p.coarse, &p.fine);
    let (histogram, exposure, sharpness) = (&p.histogram, &p.exposure, &p.sharpness);
    let (preview, merge, track, count) = (&p.preview, &p.merge_detections, &p.track, &p.count);
    let (quality, report) = (&p.quality, &p.report);
    let (refine, alarm, escalate) = (&p.refine, &p.alarm, &p.escalate);
    let mut builder = registry
        .graph_builder()
        .expect("graph builder")
        .input_typed::<Frame>("frame")
        .expect("frame key");
    let nodes: [&dyn NodeHandleLike; 16] = [
        stats, roi, coarse, fine, histogram, exposure, sharpness, preview, merge, track, count,
        quality, report, refine, alarm, escalate,
    ];
    for node in nodes {
        builder = builder.try_node_handle_like(node).expect("add node");
    }
    let host = |port: &str| PortHandle::new("", port);
    let edges = [
        (host("frame"), &stats.inputs.frame),
        (host("frame"), &roi.inputs.info),
        (host("frame"), &coarse.inputs.frame),
        (host("frame"), &fine.inputs.frame),
        (host("frame"), &histogram.inputs.frame),
        (host("frame"), &sharpness.inputs.frame),
        (host("frame"), &preview.inputs.frame),
        (host("frame"), &track.inputs.info),
        (stats.outputs.stats.clone(), &coarse.inputs.stats),
        (stats.outputs.stats.clone(), &quality.inputs.stats),
        (roi.outputs.roi.clone(), &refine.inputs.roi),
        (roi.outputs.roi.clone(), &report.inputs.roi),
        (stats.outputs.stats.clone(), &refine.inputs.stats),
        (refine.outputs.roi.clone(), &fine.inputs.roi),
        (stats.outputs.stats.clone(), &alarm.inputs.stats),
        (alarm.outputs.level.clone(), &escalate.inputs.level),
        (
            escalate.outputs.escalation.clone(),
            &report.inputs.escalation,
        ),
        (coarse.outputs.detections.clone(), &merge.inputs.coarse_dets),
        (fine.outputs.detections.clone(), &merge.inputs.fine_dets),
        (
            histogram.outputs.histogram.clone(),
            &exposure.inputs.histogram,
        ),
        (exposure.outputs.exposure.clone(), &quality.inputs.exposure),
        (
            sharpness.outputs.sharpness.clone(),
            &quality.inputs.sharpness,
        ),
        (merge.outputs.detections.clone(), &track.inputs.detections),
        (merge.outputs.detections.clone(), &count.inputs.detections),
        (track.outputs.track.clone(), &report.inputs.track),
        (quality.outputs.quality.clone(), &report.inputs.quality),
        (count.outputs.count.clone(), &report.inputs.count),
    ];
    for (from, to) in &edges {
        builder = builder.try_connect(from, *to).expect("connect");
    }
    let outputs = [
        (&report.outputs.report, OUTPUTS[0]),
        (&merge.outputs.detections, OUTPUTS[1]),
        (&count.outputs.count, OUTPUTS[2]),
        (&preview.outputs.preview, OUTPUTS[3]),
    ];
    for (from, to) in outputs {
        builder = builder
            .try_connect(from, &host(to))
            .expect("connect output");
    }
    let graph = builder
        .const_input(&fine.inputs.scale, Some(Value::Float(2.0)))
        .const_input(&exposure.inputs.target, Some(Value::Float(0.25)))
        .const_input(
            &exposure.inputs.weighting,
            Some(Value::String("best".into())),
        )
        .const_input(&exposure.inputs.zone, Some(Value::String("center".into())))
        .build();
    let config = EngineConfig::default()
        .with_runtime_mode(mode)
        .with_metrics_level(metrics)
        .with_host_event_recording(false);
    let mut host = Engine::new(config)?.compile_registry(&registry, graph)?;
    host.prepare()?;
    Ok(host)
}

/// One frame: push the shared frame, tick, drain every host output. Returns the report.
pub fn drive_frame(host: &mut HostGraph<HandlerRegistry>, frame: &Arc<Frame>) -> Option<Report> {
    host.push_payload("frame", Payload::shared(FRAME_KEY, frame.clone()));
    host.tick().expect("tick");
    let report = host
        .take_payload("report")
        .and_then(|payload| payload.get_ref::<Report>().copied());
    for port in &OUTPUTS[1..] {
        let _ = host.take_payload(port);
    }
    report
}
