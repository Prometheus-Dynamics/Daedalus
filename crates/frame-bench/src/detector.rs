//! A detector-shaped graph: the parameter shapes, wiring and node group of a staged marker
//! detector (mask prep into runs, quads from runs, decode, validate, refine), with handlers that
//! only touch their inputs and refill pooled outputs, so the graph's own per-frame cost is what
//! remains.
//!
//! Mask prep reads pixels through `FrameView::plane_bytes` (the owner feed's provider adapter
//! lends it the frame without a copy); the other stages take the owner type by reference
//! (`&SyntheticFrame`) and read only its metadata. Every stage takes a `Copy` config
//! struct by value (`#[derive(NodeConfig)]`, with enum, integer, float and bool fields fed by
//! graph constants), its state by `&mut` and the [`ExecutionContext`], and returns `Arc`'d
//! outputs it reuses across frames; validation returns two. The frame fans out to four stages.
//! [`compile_detector`] builds it as the flat per-stage graph or as one group node (an embedded
//! graph registered as a `NodeDecl` with [`EMBEDDED_GRAPH_KEY`], which the planner expands into
//! the same stages).

use std::collections::BTreeMap;
use std::sync::Arc;

use daedalus::{
    DaedalusToValue, DaedalusTypeExpr, NodeConfig, PortHandle,
    data::{
        model::{TypeExpr, Value, ValueType},
        to_value::ToValue,
        typing::TypeRegistry,
    },
    engine::{EngineConfig, HostGraph},
    macros::{node, plugin},
    planner::Graph,
    registry::{
        capability::{NodeDecl, PortDecl},
        ids::NodeId,
    },
    runtime::{
        EMBEDDED_GRAPH_KEY, EMBEDDED_HOST_KEY, NodeError,
        graph_builder::{GraphCtx, graph_to_json},
        handler_registry::HandlerRegistry,
        plugins::{PluginError, PluginRegistry},
        state::ExecutionContext,
    },
    transport::{FrameSource, FrameView},
    type_key,
};
use serde::{Deserialize, Serialize};

use crate::chain::{BenchError, frame_bench_registry, prepare};
use crate::source::{SYNTHETIC_FRAME_KEY, SyntheticFrame};

/// Node id of the detector group.
pub const DETECTOR_GROUP_ID: &str = "daedalus.frame_bench.detector:group";
/// Host outputs of a compiled detector.
pub const DETECTOR_OUTPUTS: [&str; 3] = ["detections", "rejected", "refined_corners"];

/// Which form of the detector [`compile_detector`] builds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetectorShape {
    /// The stage nodes wired in the host graph.
    Flat,
    /// One group node the planner expands into the same stages.
    Group,
}

impl DetectorShape {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::Group => "group",
        }
    }
}

/// Marker dictionary: a unit enum constant, named by its serde rename.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    DaedalusTypeExpr,
    DaedalusToValue,
    Serialize,
    Deserialize,
)]
#[daedalus(type_key = "daedalus.frame_bench:dictionary")]
pub enum Dictionary {
    #[serde(rename = "aruco_4x4_50")]
    Aruco4x4_50,
    #[serde(rename = "aruco_6x6_250")]
    Aruco6x6_250,
    #[serde(rename = "apriltag_16h5")]
    AprilTag16h5,
    #[serde(rename = "apriltag_36h11")]
    AprilTag36h11,
}

/// How bit cells are sampled.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    DaedalusTypeExpr,
    DaedalusToValue,
    Serialize,
    Deserialize,
)]
#[daedalus(type_key = "daedalus.frame_bench:sampler")]
pub enum Sampler {
    #[serde(rename = "point")]
    Point,
    #[serde(rename = "mean3x3")]
    Mean3x3,
    #[serde(rename = "mean3x3_otsu")]
    Mean3x3Otsu,
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
    #[port(default = 4, min = 4, max = 100000)]
    pub min_points: usize,
    #[port(default = 50, min = 1, max = 500)]
    pub dp_fraction_x1000: u32,
    #[port(default = 100, min = 0, max = 10000)]
    pub dp_min_x100: u32,
    #[port(default = 12, min = 0, max = 65535)]
    pub solid_max_side: u32,
    #[port(default = 16, min = 0, max = 65535)]
    pub hull_fallback_min_side: u32,
    #[port(default = true)]
    pub subpixel: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct DecodeConfig {
    #[port(default = "aruco_4x4_50")]
    pub dictionary: Dictionary,
    #[port(default = "mean3x3_otsu")]
    pub sampler: Sampler,
    #[port(default = 128, min = 0, max = 255)]
    pub threshold: u8,
    #[port(default = -1, min = -1, max = 64)]
    pub max_correction_bits: i16,
    #[port(default = true)]
    pub validate: bool,
    #[port(default = true)]
    pub refine_weak: bool,
    #[port(default = true)]
    pub require_dark_border: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, NodeConfig, DaedalusToValue)]
pub struct ValidateConfig {
    #[port(default = "aruco_4x4_50")]
    pub dictionary: Dictionary,
    #[port(default = true)]
    pub require_convex: bool,
    #[port(default = 30, min = 0, max = 4000)]
    pub min_perimeter_permille: u16,
    #[port(default = 50, min = 0, max = 250)]
    pub min_corner_distance_permille: u16,
    #[port(default = 3, min = 0, max = 1000)]
    pub min_distance_to_border: u16,
    #[port(default = 60, min = 0, max = 100)]
    pub error_correction_percent: u8,
    #[port(default = 35, min = 0, max = 100)]
    pub max_border_error_percent: u8,
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
    #[port(default = 1.5, min = 0.0, max = 15.75)]
    pub polish_radius: f32,
    #[port(default = 4.0, min = 0.0, max = 255.0)]
    pub min_step: f32,
    #[port(default = 32, min = 4, max = 64)]
    pub max_side_samples: usize,
}

/// Foreground runs of a pyramid level (an opaque type).
#[type_key("daedalus.frame_bench:runs")]
#[derive(Clone, Debug, Default)]
pub struct Runs {
    pub rows: Vec<(u32, u32, u32)>,
    pub level: u8,
}

/// A value type: `DaedalusTypeExpr` + `ToValue` under its own key, like the detector's
/// structured outputs.
macro_rules! value_type {
    ($name:ident, $key:literal) => {
        #[derive(Clone, Debug, Default, PartialEq)]
        pub struct $name {
            pub items: Vec<[f32; 8]>,
            pub frame_width: u32,
            pub frame_height: u32,
        }

        impl daedalus::data::daedalus_type::DaedalusTypeExpr for $name {
            const TYPE_KEY: &'static str = $key;

            fn type_expr() -> TypeExpr {
                TypeExpr::Struct(vec![
                    field_type("frame_width", TypeExpr::Scalar(ValueType::Int)),
                    field_type("frame_height", TypeExpr::Scalar(ValueType::Int)),
                    field_type(
                        "items",
                        TypeExpr::List(Box::new(TypeExpr::Scalar(ValueType::Float))),
                    ),
                ])
            }
        }

        impl ToValue for $name {
            fn to_value(&self) -> Value {
                Value::List(
                    self.items
                        .iter()
                        .flatten()
                        .map(|v| Value::Float(f64::from(*v)))
                        .collect(),
                )
            }
        }
    };
}

fn field_type(name: &str, ty: TypeExpr) -> daedalus::data::model::StructField {
    daedalus::data::model::StructField {
        name: name.to_owned(),
        ty,
    }
}

value_type!(Quads, "daedalus.frame_bench:quads");
value_type!(Detections, "daedalus.frame_bench:detections");
value_type!(RejectedMarkers, "daedalus.frame_bench:rejected_markers");
value_type!(RefinedCorners, "daedalus.frame_bench:refined_corners");

/// Outputs a node hands out as `Arc`s and refills in place once no payload holds them.
pub struct OutputPool<T> {
    slots: Vec<Arc<T>>,
}

impl<T> Default for OutputPool<T> {
    fn default() -> Self {
        Self { slots: Vec::new() }
    }
}

impl<T: Default> OutputPool<T> {
    fn fill(&mut self, fill: impl FnOnce(&mut T)) -> Arc<T> {
        let index = match self.slots.iter().position(|s| Arc::strong_count(s) == 1) {
            Some(index) => index,
            None => {
                self.slots.push(Arc::default());
                self.slots.len() - 1
            }
        };
        let slot = &mut self.slots[index];
        if let Some(value) = Arc::get_mut(slot) {
            fill(value);
        }
        Arc::clone(slot)
    }
}

/// A stage's state: its config as last built and its pooled outputs.
pub struct StageState<C, T> {
    built: Option<C>,
    outputs: OutputPool<T>,
}

impl<C, T> Default for StageState<C, T> {
    fn default() -> Self {
        Self {
            built: None,
            outputs: OutputPool::default(),
        }
    }
}

impl<C: Copy + PartialEq, T: Default> StageState<C, T> {
    /// Rebuilds the "stage" only when the config changed, then refills an output.
    fn run(&mut self, config: C, fill: impl FnOnce(&mut T)) -> Arc<T> {
        if self.built != Some(config) {
            self.built = Some(config);
        }
        self.outputs.fill(fill)
    }
}

pub type MaskPrepState = StageState<MaskPrepConfig, Runs>;
pub type QuadsState = StageState<QuadConfig, Quads>;
pub type DecodeState = StageState<DecodeConfig, Detections>;
pub type RefineState = StageState<RefineConfig, RefinedCorners>;

#[derive(Default)]
pub struct ValidateState {
    built: Option<ValidateConfig>,
    kept: OutputPool<Detections>,
    rejected: OutputPool<RejectedMarkers>,
}

fn size(frame: &SyntheticFrame) -> (u32, u32) {
    (frame.width(), frame.height())
}

#[node(
    id = "mask_prep_runs",
    inputs("frame", config = MaskPrepConfig),
    outputs("runs"),
    state(MaskPrepState),
    shareable
)]
pub fn mask_prep_runs(
    frame: FrameView<'_>,
    config: MaskPrepConfig,
    state: &mut MaskPrepState,
    ctx: &ExecutionContext,
) -> Result<Arc<Runs>, NodeError> {
    let _ = ctx;
    let (width, height) = (frame.width(), frame.height());
    // The stage that reads pixels: a CPU access to the plane, ended when the guard drops.
    let pixels = frame
        .plane_bytes(0)
        .ok_or_else(|| NodeError::InvalidInput("frame plane is not CPU-readable".into()))?;
    let row = pixels.get(..64.min(width as usize)).unwrap_or_default();
    let dark = row.iter().filter(|&&pixel| pixel < 128).count() as u32;
    Ok(state.run(config, |runs| {
        runs.level = config.pyramid_level;
        runs.rows.clear();
        runs.rows
            .extend((0..4).map(|row| (row, dark.min(width), width >> 1)));
        std::hint::black_box((height, config.radius, config.invert));
    }))
}

#[node(
    id = "quads_from_runs",
    inputs("runs", config = QuadConfig),
    outputs("quads"),
    state(QuadsState),
    shareable
)]
pub fn quads_from_runs(
    runs: &Runs,
    config: QuadConfig,
    state: &mut QuadsState,
    ctx: &ExecutionContext,
) -> Result<Arc<Quads>, NodeError> {
    let _ = ctx;
    Ok(state.run(config, |quads| {
        quads.items.clear();
        quads
            .items
            .extend(runs.rows.iter().map(|row| [row.0 as f32; 8]));
        quads.frame_width = config.min_width << runs.level;
    }))
}

#[node(
    id = "decode",
    inputs("frame", "quads", config = DecodeConfig),
    outputs("detections"),
    state(DecodeState)
)]
pub fn decode(
    frame: &SyntheticFrame,
    quads: &Quads,
    config: DecodeConfig,
    state: &mut DecodeState,
    ctx: &ExecutionContext,
) -> Result<Arc<Detections>, NodeError> {
    let _ = ctx;
    let (width, height) = size(frame);
    Ok(state.run(config, |detections| {
        detections.items.clear();
        detections.items.extend_from_slice(&quads.items);
        (detections.frame_width, detections.frame_height) = (width, height);
        std::hint::black_box((config.dictionary, config.sampler, config.threshold));
    }))
}

#[node(
    id = "validate",
    inputs("frame", "detections", config = ValidateConfig),
    outputs("detections", "rejected"),
    state(ValidateState)
)]
pub fn validate(
    frame: &SyntheticFrame,
    detections: &Detections,
    config: ValidateConfig,
    state: &mut ValidateState,
    ctx: &ExecutionContext,
) -> Result<(Arc<Detections>, Arc<RejectedMarkers>), NodeError> {
    let _ = ctx;
    state.built = Some(config);
    let (width, height) = size(frame);
    let border = f32::from(config.min_distance_to_border);
    let kept = state.kept.fill(|kept| {
        kept.items.clear();
        kept.items
            .extend(detections.items.iter().filter(|d| d[0] >= border));
        (kept.frame_width, kept.frame_height) = (width, height);
    });
    let rejected = state.rejected.fill(|rejected| {
        rejected.items.clear();
        rejected
            .items
            .extend(detections.items.iter().filter(|d| d[0] < border));
    });
    Ok((kept, rejected))
}

#[node(
    id = "refine",
    inputs("frame", "detections", config = RefineConfig),
    outputs("refined_corners"),
    state(RefineState)
)]
pub fn refine(
    frame: &SyntheticFrame,
    detections: &Detections,
    config: RefineConfig,
    state: &mut RefineState,
    ctx: &ExecutionContext,
) -> Result<Arc<RefinedCorners>, NodeError> {
    let _ = ctx;
    let (width, _) = size(frame);
    Ok(state.run(config, |refined| {
        refined.items.clear();
        refined.items.extend(
            detections
                .items
                .iter()
                .map(|d| d.map(|v| v + config.search_rate)),
        );
        refined.frame_width = width;
    }))
}

/// The detector stages, their types and the config enums.
#[plugin(
    id = "daedalus.frame_bench.detector",
    types(Runs, Dictionary, Sampler),
    values(Quads, Detections, RejectedMarkers, RefinedCorners),
    nodes(mask_prep_runs, quads_from_runs, decode, validate, refine)
)]
pub struct DetectorPlugin;

/// One stage inside the group: alias, node id and config ports.
struct Stage {
    alias: &'static str,
    id: &'static str,
    ports: Vec<PortDecl>,
    metadata: BTreeMap<String, Value>,
}

impl Stage {
    fn of<C: daedalus::runtime::config::NodeConfig>(
        alias: &'static str,
        id: &'static str,
        types: &TypeRegistry,
    ) -> Self {
        Self {
            alias,
            id,
            ports: C::ports(types),
            metadata: C::metadata(),
        }
    }
}

pub(crate) const MASK_PREP: &str = "daedalus.frame_bench.detector:mask_prep_runs";
pub(crate) const QUADS: &str = "daedalus.frame_bench.detector:quads_from_runs";
pub(crate) const DECODE: &str = "daedalus.frame_bench.detector:decode";
pub(crate) const VALIDATE: &str = "daedalus.frame_bench.detector:validate";
pub(crate) const REFINE: &str = "daedalus.frame_bench.detector:refine";

fn stages() -> [Stage; 5] {
    let types = TypeRegistry::new();
    [
        Stage::of::<MaskPrepConfig>("mask_prep", MASK_PREP, &types),
        Stage::of::<QuadConfig>("quads", QUADS, &types),
        Stage::of::<DecodeConfig>("decode", DECODE, &types),
        Stage::of::<ValidateConfig>("validate", VALIDATE, &types),
        Stage::of::<RefineConfig>("refine", REFINE, &types),
    ]
}

/// Decode config ports the group fixes instead of exposing.
const FIXED_DECODE_PORTS: &[&str] = &["validate"];

fn install_error(error: impl ToString) -> PluginError {
    PluginError::Install {
        message: error.to_string(),
    }
}

/// Registers [`DETECTOR_GROUP_ID`]: the stages in an embedded graph behind one node whose config
/// ports are the stages' parameters (a port two stages share, `dictionary`, feeds both).
pub fn register_detector_group(registry: &mut PluginRegistry) -> Result<(), PluginError> {
    let capabilities = registry.combined_transport_capabilities()?;
    let stages = stages();
    let mut config: Vec<(String, Vec<&'static str>)> = Vec::new();
    for stage in &stages {
        for port in &stage.ports {
            if stage.id == DECODE && FIXED_DECODE_PORTS.contains(&port.name.as_str()) {
                continue;
            }
            match config.iter_mut().find(|(name, _)| *name == port.name) {
                Some((_, feeds)) => feeds.push(stage.alias),
                None => config.push((port.name.clone(), vec![stage.alias])),
            }
        }
    }
    let mut inputs = vec!["frame"];
    inputs.extend(config.iter().map(|(name, _)| name.as_str()));
    let mut ctx = GraphCtx::new(capabilities.clone(), &inputs, &DETECTOR_OUTPUTS);
    let handles = stages
        .iter()
        .map(|stage| ctx.try_node_as(stage.id, stage.alias))
        .collect::<Result<Vec<_>, _>>()
        .map_err(install_error)?;
    let [mask_prep, quads, decode, validate, refine] = &handles[..] else {
        return Err(install_error("detector group stages"));
    };
    let edges = [
        (ctx.input("frame"), mask_prep.input("frame")),
        (ctx.input("frame"), decode.input("frame")),
        (ctx.input("frame"), validate.input("frame")),
        (ctx.input("frame"), refine.input("frame")),
        (mask_prep.output("runs"), quads.input("runs")),
        (quads.output("quads"), decode.input("quads")),
        (decode.output("detections"), validate.input("detections")),
        (validate.output("detections"), refine.input("detections")),
    ];
    for (from, to) in &edges {
        ctx.try_connect(from, to).map_err(install_error)?;
    }
    for (name, feeds) in &config {
        for (handle, _) in handles
            .iter()
            .zip(&stages)
            .filter(|(_, stage)| feeds.contains(&stage.alias))
        {
            ctx.try_connect(&ctx.input(name), &handle.input(name.as_str()))
                .map_err(install_error)?;
        }
    }
    for port in FIXED_DECODE_PORTS {
        ctx.try_const_input(&decode.input(*port), Value::Bool(false))
            .map_err(install_error)?;
    }
    let outputs = [
        ("detections", validate.output("detections")),
        ("rejected", validate.output("rejected")),
        ("refined_corners", refine.output("refined_corners")),
    ];
    for (name, from) in &outputs {
        ctx.try_bind_output(name, from).map_err(install_error)?;
    }
    let graph = ctx.try_build().map_err(install_error)?;
    let graph_json = graph_to_json(&graph).map_err(install_error)?;

    let mut decl = NodeDecl::new(DETECTOR_GROUP_ID).label("Detector").input(
        PortDecl::new("frame", SYNTHETIC_FRAME_KEY)
            .schema(TypeExpr::Opaque(SYNTHETIC_FRAME_KEY.into())),
    );
    let mut metadata = BTreeMap::new();
    for (name, feeds) in &config {
        let Some(stage) = stages.iter().find(|stage| stage.alias == feeds[0]) else {
            continue;
        };
        let Some(port) = stage.ports.iter().find(|port| port.name == *name) else {
            continue;
        };
        let port = if name == "dictionary" {
            port.clone()
                .const_value(Dictionary::AprilTag36h11.to_value())
        } else {
            port.clone()
        };
        decl = decl.input(port);
        let prefix = format!("inputs.{name}.");
        metadata.extend(
            stage
                .metadata
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .map(|(key, value)| (key.clone(), value.clone())),
        );
    }
    for (name, from) in &outputs {
        let stage_id = stages
            .iter()
            .find(|stage| stage.alias == from.node_alias())
            .map_or("", |stage| stage.id);
        let mut port = capabilities
            .nodes()
            .get(&NodeId::new(stage_id))
            .and_then(|stage| stage.outputs.iter().find(|port| port.name == from.port()))
            .cloned()
            .ok_or_else(|| install_error(format!("detector group output {name}")))?;
        port.name = (*name).to_owned();
        decl = decl.output(port);
    }
    for (key, value) in metadata {
        decl = decl.metadata(key, value);
    }
    decl = decl
        .metadata(EMBEDDED_GRAPH_KEY, Value::String(graph_json.into()))
        .metadata(EMBEDDED_HOST_KEY, Value::String("host".into()));
    registry.register_node_decl(decl)
}

/// [`frame_bench_registry`] with the detector stages and the group installed.
pub fn detector_registry() -> Result<PluginRegistry, BenchError> {
    let mut registry = frame_bench_registry()?;
    registry.install(&DetectorPlugin::new())?;
    register_detector_group(&mut registry)?;
    Ok(registry)
}

fn port(alias: &str, port: &str) -> PortHandle {
    PortHandle::new(alias, port)
}

/// The detector graph for `shape`: host `frame` (the [`SyntheticFrame`] owner type, fed with
/// [`crate::FrameFeed::Owner`]) in, [`DETECTOR_OUTPUTS`] out, dictionary `apriltag_36h11`.
pub fn detector_graph(
    registry: &PluginRegistry,
    shape: DetectorShape,
) -> Result<Graph, BenchError> {
    detector_graph_for(registry, shape, Dictionary::AprilTag36h11)
}

/// [`detector_graph`] decoding `dictionary`.
pub fn detector_graph_for(
    registry: &PluginRegistry,
    shape: DetectorShape,
    dictionary: Dictionary,
) -> Result<Graph, BenchError> {
    let dictionary = Some(dictionary.to_value());
    let builder = registry
        .graph_builder()?
        .input_typed::<SyntheticFrame>("frame")?;
    let builder = match shape {
        DetectorShape::Group => builder
            .try_node_id(DETECTOR_GROUP_ID, "detector")?
            .try_connect("frame", &port("detector", "frame"))?
            .try_connect(&port("detector", "detections"), "detections")?
            .try_connect(&port("detector", "rejected"), "rejected")?
            .try_connect(&port("detector", "refined_corners"), "refined_corners")?
            .const_input(&port("detector", "dictionary"), dictionary),
        DetectorShape::Flat => builder
            .try_node_id(MASK_PREP, "mask_prep")?
            .try_node_id(QUADS, "quads")?
            .try_node_id(DECODE, "decode")?
            .try_connect("frame", &port("mask_prep", "frame"))?
            .try_connect(&port("mask_prep", "runs"), &port("quads", "runs"))?
            .try_connect("frame", &port("decode", "frame"))?
            .try_connect(&port("quads", "quads"), &port("decode", "quads"))?
            .const_input(&port("mask_prep", "pyramid_level"), Some(Value::Int(1)))
            .const_input(&port("decode", "dictionary"), dictionary.clone())
            .const_input(&port("decode", "validate"), Some(Value::Bool(false)))
            .try_node_id(VALIDATE, "validate")?
            .try_connect("frame", &port("validate", "frame"))?
            .try_connect(
                &port("decode", "detections"),
                &port("validate", "detections"),
            )?
            .try_connect(&port("validate", "rejected"), "rejected")?
            .const_input(&port("validate", "dictionary"), dictionary)
            .try_connect(&port("validate", "detections"), "detections")?
            .try_node_id(REFINE, "refine")?
            .try_connect("frame", &port("refine", "frame"))?
            .try_connect(
                &port("validate", "detections"),
                &port("refine", "detections"),
            )?
            .try_connect(&port("refine", "refined_corners"), "refined_corners")?,
    };
    Ok(builder.build())
}

/// Compile and prepare the detector graph for `shape`.
pub fn compile_detector(
    shape: DetectorShape,
    config: EngineConfig,
) -> Result<HostGraph<HandlerRegistry>, BenchError> {
    let registry = detector_registry()?;
    let graph = detector_graph(&registry, shape)?;
    prepare(&registry, graph, config)
}
