#![allow(clippy::too_many_arguments, clippy::type_complexity)]
//! The stage handlers of the mirror: the value type, the config structs and the stub nodes.
//! Each stub refills a pooled `Arc`'d output and withholds some outputs on a fixed schedule.

use std::sync::Arc;

use daedalus::{
    DaedalusToValue, NodeConfig,
    macros::{node, plugin},
    runtime::{NodeError, PortId, RuntimeNode, io::NodeIo, state::ExecutionContext},
    transport::FrameView,
    type_key,
};

use crate::STUB_ITEMS;

/// One value type for every stage's data: a pooled `Arc`'d buffer.
#[type_key("daedalus.frame_bench.mirror:blob")]
#[derive(Clone, Debug, Default)]
pub struct Blob {
    pub items: Vec<f32>,
}

/// One output's pool: `Arc`s handed out and refilled once no payload holds them.
#[derive(Default)]
pub struct Pool(Vec<Arc<Blob>>);

impl Pool {
    fn fill(&mut self) -> Arc<Blob> {
        let index = match self.0.iter().position(|slot| Arc::strong_count(slot) == 1) {
            Some(index) => index,
            None => {
                self.0.push(Arc::default());
                self.0.len() - 1
            }
        };
        let slot = &mut self.0[index];
        if let Some(blob) = Arc::get_mut(slot) {
            blob.items.clear();
            blob.items.extend_from_slice(&STUB_ITEMS);
        }
        Arc::clone(slot)
    }
}

/// A stub's state: a pool per output (the widest stage has three).
#[derive(Default)]
pub struct Stub {
    outputs: [Pool; 3],
}

/// Tracking plan config: the plan node's const inputs (`pipe.json`'s tracking settings).
#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct PlanConfig {
    #[port(default = 5, min = 1, max = 4096)]
    pub full_search_every: u32,
    #[port(default = 6, min = 0, max = 64)]
    pub margin_growth_misses: u32,
    #[port(default = 0, min = 0, max = 64)]
    pub loss_full_search_after: u32,
    #[port(default = 1.5, min = 0.0, max = 16.0)]
    pub track_margin: f32,
    #[port(default = true)]
    pub tracked: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct MaskPrepConfig {
    #[port(default = 3, min = 1, max = 15)]
    pub radius: usize,
    #[port(default = 7, min = -255, max = 255)]
    pub offset: i16,
    #[port(default = true)]
    pub invert: bool,
    #[port(default = 1, min = 0, max = 3)]
    pub pyramid_level: u8,
    #[port(default = true)]
    pub downscale_missing: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct QuadConfig {
    #[port(default = 4, min = 1, max = 4096)]
    pub min_width: u32,
    #[port(default = 4, min = 1, max = 4096)]
    pub min_height: u32,
    #[port(default = 50, min = 1, max = 500)]
    pub dp_fraction_x1000: u32,
    #[port(default = 12, min = 0, max = 65535)]
    pub solid_max_side: u32,
    #[port(default = true)]
    pub subpixel: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct WindowsConfig {
    #[port(default = 3, min = 1, max = 15)]
    pub radius: usize,
    #[port(default = 7, min = -255, max = 255)]
    pub offset: i16,
    #[port(default = true)]
    pub invert: bool,
    #[port(default = true)]
    pub subpixel: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct DecodeConfig {
    #[port(default = 128, min = 0, max = 255)]
    pub threshold: u8,
    #[port(default = -1, min = -1, max = 64)]
    pub max_correction_bits: i16,
    #[port(default = false)]
    pub validate: bool,
    #[port(default = true)]
    pub refine_weak: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct ValidateConfig {
    #[port(default = true)]
    pub require_convex: bool,
    #[port(default = 30, min = 0, max = 4000)]
    pub min_perimeter_permille: u16,
    #[port(default = 3, min = 0, max = 1000)]
    pub min_distance_to_border: u16,
    #[port(default = 60, min = 0, max = 100)]
    pub error_correction_percent: u8,
    #[port(default = 20, min = 0, max = 255)]
    pub min_contrast: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct RefineConfig {
    #[port(default = 0.06, min = 0.0, max = 0.5)]
    pub search_rate: f32,
    #[port(default = 3.0, min = 0.5, max = 15.75)]
    pub min_radius: f32,
    #[port(default = 12.0, min = 0.5, max = 15.75)]
    pub max_radius: f32,
    #[port(default = 32, min = 4, max = 64)]
    pub max_side_samples: usize,
}

/// The tracked stages' settings (`detectors.tracked_detect`'s ports).
#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct TrackConfig {
    #[port(default = 0.06, min = 0.0, max = 0.5)]
    pub search_rate: f32,
    #[port(default = 3.0, min = 0.5, max = 15.75)]
    pub min_radius: f32,
    #[port(default = 12.0, min = 0.5, max = 15.75)]
    pub max_radius: f32,
    #[port(default = 1.5, min = 0.0, max = 15.75)]
    pub polish_radius: f32,
    #[port(default = 32, min = 4, max = 64)]
    pub max_side_samples: usize,
    #[port(default = true)]
    pub subpixel: bool,
}

/// Plan: full search every `full_search_every` frames (one in five), and the window and track
/// outputs every frame.
#[node(
    id = "detectors.track_plan",
    inputs("frame", config = PlanConfig),
    outputs("full_frame", "windows", "track"),
    state(Stub)
)]
fn track_plan(
    frame: FrameView<'_>,
    config: PlanConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<(Option<Arc<Blob>>, Arc<Blob>, Arc<Blob>), NodeError> {
    let _ = ctx;
    std::hint::black_box(config);
    let every = u64::from(config.full_search_every.max(1));
    let full = frame
        .sequence()
        .is_multiple_of(every)
        .then(|| state.outputs[0].fill());
    Ok((full, state.outputs[1].fill(), state.outputs[2].fill()))
}

/// The aruco mask prep (full search and same-frame): runs when the plan asks for a full search.
#[node(
    id = "aruco.mask_prep_runs",
    inputs("frame", config = MaskPrepConfig),
    outputs("runs"),
    state(Stub)
)]
fn mask_prep_runs(
    frame: &Blob,
    config: MaskPrepConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<Arc<Blob>, NodeError> {
    let _ = ctx;
    std::hint::black_box((frame, config));
    Ok(state.outputs[0].fill())
}

#[node(
    id = "aruco.quads_from_runs",
    inputs("runs", config = QuadConfig),
    outputs("quads"),
    state(Stub)
)]
fn quads_from_runs(
    runs: &Blob,
    config: QuadConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<Arc<Blob>, NodeError> {
    let _ = ctx;
    std::hint::black_box((runs, config));
    Ok(state.outputs[0].fill())
}

/// Windows around the tracked tags, or the full search's quads passed on (optional).
#[node(
    id = "detectors.track_windows",
    inputs("frame", "windows", "full_quads", config = WindowsConfig),
    outputs("quads"),
    state(Stub)
)]
fn track_windows(
    frame: FrameView<'_>,
    windows: &Blob,
    full_quads: Option<&Blob>,
    config: WindowsConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<Arc<Blob>, NodeError> {
    let _ = ctx;
    std::hint::black_box((frame.sequence(), windows, full_quads, config));
    Ok(state.outputs[0].fill())
}

#[node(
    id = "aruco.decode",
    inputs("frame", "quads", config = DecodeConfig),
    outputs("detections"),
    state(Stub)
)]
fn decode(
    frame: FrameView<'_>,
    quads: &Blob,
    config: DecodeConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<Arc<Blob>, NodeError> {
    let _ = ctx;
    std::hint::black_box((frame.sequence(), quads, config));
    Ok(state.outputs[0].fill())
}

#[node(
    id = "aruco.validate",
    inputs("frame", "detections", config = ValidateConfig),
    outputs("detections", "rejected"),
    state(Stub)
)]
fn validate(
    frame: FrameView<'_>,
    detections: &Blob,
    config: ValidateConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<(Arc<Blob>, Arc<Blob>), NodeError> {
    let _ = ctx;
    std::hint::black_box((frame.sequence(), detections, config));
    Ok((state.outputs[0].fill(), state.outputs[1].fill()))
}

#[node(
    id = "aruco.refine",
    inputs("frame", "detections", config = RefineConfig),
    outputs("refined_corners"),
    state(Stub)
)]
fn refine(
    frame: FrameView<'_>,
    detections: &Blob,
    config: RefineConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<Arc<Blob>, NodeError> {
    let _ = ctx;
    std::hint::black_box((frame.sequence(), detections, config));
    Ok(state.outputs[0].fill())
}

/// Fine windows: the tracked pass that fails about one frame in five (its outputs are withheld
/// on those frames, which is what makes loss run four frames in five).
#[node(
    id = "detectors.track_fine_windows",
    inputs(
        "frame",
        "track",
        "windows",
        "detections",
        "rejected",
        "refined_corners",
        "full_frame",
        config = TrackConfig
    ),
    outputs("detections", "rejected", "refined_corners"),
    state(Stub)
)]
fn track_fine_windows(
    frame: FrameView<'_>,
    track: &Blob,
    windows: &Blob,
    detections: &Blob,
    rejected: Option<&Blob>,
    refined_corners: Option<&Blob>,
    full_frame: Option<&Blob>,
    config: TrackConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<(Option<Arc<Blob>>, Option<Arc<Blob>>, Option<Arc<Blob>>), NodeError> {
    let _ = ctx;
    std::hint::black_box((
        track,
        windows,
        detections,
        rejected,
        refined_corners,
        full_frame,
        config,
    ));
    let found = !frame.sequence().is_multiple_of(5);
    if !found {
        return Ok((None, None, None));
    }
    Ok((
        Some(state.outputs[0].fill()),
        Some(state.outputs[1].fill()),
        Some(state.outputs[2].fill()),
    ))
}

/// Loss: runs when fine windows found detections (four frames in five). Its second-pass frame
/// is the frame itself (a `daedalus:frame` passed on, as the live node forwards it); its recovery
/// windows and same-frame full search never come, so the second pass almost never runs.
#[node(
    id = "detectors.track_loss",
    inputs(
        port(name = "frame", type_key = "daedalus:frame"),
        port(name = "track", type_key = "daedalus.frame_bench.mirror:blob"),
        port(name = "windows", type_key = "daedalus.frame_bench.mirror:blob"),
        port(name = "detections", type_key = "daedalus.frame_bench.mirror:blob"),
        port(
            name = "refined_corners",
            type_key = "daedalus.frame_bench.mirror:blob"
        )
    ),
    outputs(
        port(name = "full_frame", type_key = "daedalus.frame_bench.mirror:blob"),
        port(name = "second_frame", type_key = "daedalus:frame"),
        port(
            name = "recovery_windows",
            type_key = "daedalus.frame_bench.mirror:blob"
        )
    )
)]
fn track_loss(
    _node: &RuntimeNode,
    _ctx: &ExecutionContext,
    io: &mut NodeIo,
) -> Result<(), NodeError> {
    let frame = io
        .take_input_payload("frame")
        .ok_or_else(|| NodeError::InvalidInput("missing frame".into()))?;
    for port in ["track", "windows", "detections", "refined_corners"] {
        std::hint::black_box(io.take_input_payload(port));
    }
    io.push_correlated_payload(PortId::from_static("second_frame"), frame);
    Ok(())
}

#[node(
    id = "detectors.track_coarse",
    inputs("frame", "track", "windows", "second_frame", config = TrackConfig),
    outputs("detections", "rejected", "refined_corners"),
    state(Stub)
)]
fn track_coarse(
    frame: FrameView<'_>,
    track: &Blob,
    windows: &Blob,
    second_frame: Option<FrameView<'_>>,
    config: TrackConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<(Arc<Blob>, Arc<Blob>, Arc<Blob>), NodeError> {
    let _ = ctx;
    std::hint::black_box((
        frame.sequence(),
        track,
        windows,
        second_frame.map(|f| f.sequence()),
        config,
    ));
    Ok((
        state.outputs[0].fill(),
        state.outputs[1].fill(),
        state.outputs[2].fill(),
    ))
}

#[node(
    id = "detectors.track_fine_band",
    inputs("frame", "track", "windows", "second_frame", config = TrackConfig),
    outputs("detections", "rejected", "refined_corners"),
    state(Stub)
)]
fn track_fine_band(
    frame: FrameView<'_>,
    track: &Blob,
    windows: &Blob,
    second_frame: Option<FrameView<'_>>,
    config: TrackConfig,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<(Arc<Blob>, Arc<Blob>, Arc<Blob>), NodeError> {
    let _ = ctx;
    std::hint::black_box((
        frame.sequence(),
        track,
        windows,
        second_frame.map(|f| f.sequence()),
        config,
    ));
    Ok((
        state.outputs[0].fill(),
        state.outputs[1].fill(),
        state.outputs[2].fill(),
    ))
}

/// Update: merges the passes into the detections the host and the pose nodes read. Every pass's
/// input is optional, so it runs on the passes that produced results.
#[node(
    id = "detectors.track_update",
    inputs(
        "track",
        "detections",
        "rejected",
        "refined_corners",
        "second_quads",
        "second_detections",
        "second_rejected",
        "second_refined_corners",
        "coarse_detections",
        "coarse_rejected",
        "coarse_refined_corners",
        "band_detections",
        "band_rejected",
        "band_refined_corners"
    ),
    outputs("detections", "rejected", "refined_corners"),
    state(Stub)
)]
fn track_update(
    track: &Blob,
    detections: Option<&Blob>,
    rejected: Option<&Blob>,
    refined_corners: Option<&Blob>,
    second_quads: Option<&Blob>,
    second_detections: Option<&Blob>,
    second_rejected: Option<&Blob>,
    second_refined_corners: Option<&Blob>,
    coarse_detections: Option<&Blob>,
    coarse_rejected: Option<&Blob>,
    coarse_refined_corners: Option<&Blob>,
    band_detections: Option<&Blob>,
    band_rejected: Option<&Blob>,
    band_refined_corners: Option<&Blob>,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<(Arc<Blob>, Arc<Blob>, Arc<Blob>), NodeError> {
    let _ = ctx;
    std::hint::black_box((
        track,
        detections,
        rejected,
        refined_corners,
        second_quads,
        second_detections,
        second_rejected,
        second_refined_corners,
        coarse_detections,
        coarse_rejected,
        coarse_refined_corners,
        band_detections,
        band_rejected,
        band_refined_corners,
    ));
    Ok((
        state.outputs[0].fill(),
        state.outputs[1].fill(),
        state.outputs[2].fill(),
    ))
}

#[node(
    id = "aruco.pose",
    inputs("detections", "refined_corners", "camera"),
    outputs("poses", "solutions"),
    state(Stub)
)]
fn pose(
    detections: &Blob,
    refined_corners: &Blob,
    camera: &Blob,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<(Arc<Blob>, Arc<Blob>), NodeError> {
    let _ = ctx;
    std::hint::black_box((detections, refined_corners, camera));
    Ok((state.outputs[0].fill(), state.outputs[1].fill()))
}

#[node(
    id = "aruco.multi_tag_pose",
    inputs("detections", "refined_corners", "camera", "extrinsics"),
    outputs("camera_pose"),
    state(Stub)
)]
fn multi_tag_pose(
    detections: &Blob,
    refined_corners: &Blob,
    camera: &Blob,
    extrinsics: &Blob,
    state: &mut Stub,
    ctx: &ExecutionContext,
) -> Result<Arc<Blob>, NodeError> {
    let _ = ctx;
    std::hint::black_box((detections, refined_corners, camera, extrinsics));
    Ok(state.outputs[0].fill())
}

/// The mirror's stages, registered under the plugin id `eidos`.
#[plugin(
    id = "eidos",
    types(Blob),
    nodes(
        track_plan,
        mask_prep_runs,
        quads_from_runs,
        track_windows,
        decode,
        validate,
        refine,
        track_fine_windows,
        track_loss,
        track_coarse,
        track_fine_band,
        track_update,
        pose,
        multi_tag_pose
    )
)]
pub struct MirrorPlugin;
