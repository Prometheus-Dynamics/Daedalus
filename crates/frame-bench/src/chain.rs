//! A chain of no-op stages that read a `daedalus:frame` view and pass the frame on.

use daedalus::{
    data::model::TypeExpr,
    engine::{Engine, EngineConfig, HostGraph},
    macros::{node, plugin},
    runtime::{
        NodeError, RuntimeNode, handler_registry::HandlerRegistry, io::NodeIo,
        plugins::PluginRegistry, state::ExecutionContext,
    },
    transport::{FRAME_INTERFACE_KEY, FrameInterface, FrameView},
};

use crate::source::{FrameFeed, SyntheticFrame};

/// Registry id of the no-op stage.
pub const STAGE_NODE_ID: &str = "daedalus.frame_bench:stage";
/// Host input of a compiled chain.
pub const CHAIN_INPUT: &str = "frame";
/// Host output of a compiled chain (the frame, after the last stage).
pub const CHAIN_OUTPUT: &str = "out";

type BenchError = Box<dyn std::error::Error + Send + Sync>;

/// Reads the frame's metadata through `daedalus:frame` (like a detector checking its input),
/// then forwards the same payload: no copy, no allocation.
#[node(
    id = "stage",
    inputs(port(name = "frame", type_key = "daedalus:frame")),
    outputs(port(name = "frame", type_key = "daedalus:frame"))
)]
fn stage(_node: &RuntimeNode, _ctx: &ExecutionContext, io: &mut NodeIo) -> Result<(), NodeError> {
    let frame: FrameView<'_> = io.get_foreign("frame")?;
    std::hint::black_box((frame.width(), frame.height(), frame.sequence()));
    let payload = io
        .take_input_payload("frame")
        .ok_or_else(|| NodeError::InvalidInput("missing frame".into()))?;
    io.push_correlated_payload(daedalus::runtime::PortId::from_static("frame"), payload);
    Ok(())
}

/// The synthetic frame type with its `daedalus:frame` provider, and the no-op stage.
#[plugin(
    id = "daedalus.frame_bench",
    types(SyntheticFrame),
    foreign_providers(SyntheticFrame => FrameInterface),
    nodes(stage)
)]
pub struct FrameBenchPlugin;

/// A registry with [`FrameBenchPlugin`] installed; install the nodes under test next to it.
pub fn frame_bench_registry() -> Result<PluginRegistry, BenchError> {
    let mut registry = PluginRegistry::new();
    registry.install(&FrameBenchPlugin::new())?;
    Ok(registry)
}

/// Compile `host.frame -> stage_0 -> ... -> stage_{stages-1} -> host.out` (`stages >= 1`),
/// fed as `feed`, and prepare it.
pub fn compile_frame_chain(
    stages: usize,
    feed: FrameFeed,
    config: EngineConfig,
) -> Result<HostGraph<HandlerRegistry>, BenchError> {
    let registry = frame_bench_registry()?;
    let stages: Vec<_> = (0..stages.max(1))
        .map(|idx| daedalus::NodeHandle::new(STAGE_NODE_ID).alias(format!("stage_{idx}")))
        .collect();
    let mut builder = registry.graph_builder()?;
    builder = match feed {
        FrameFeed::Interface => {
            builder.input_as(CHAIN_INPUT, TypeExpr::opaque(FRAME_INTERFACE_KEY))
        }
        FrameFeed::Owner => builder.input_typed::<SyntheticFrame>(CHAIN_INPUT)?,
    };
    for stage in &stages {
        builder = builder.try_node(stage)?;
    }
    builder = builder.try_connect(CHAIN_INPUT, &stages[0].input("frame"))?;
    for pair in stages.windows(2) {
        builder = builder.try_connect(&pair[0].output("frame"), &pair[1].input("frame"))?;
    }
    let last = stages.last().expect("at least one stage");
    let graph = builder
        .try_connect(&last.output("frame"), CHAIN_OUTPUT)?
        .build();
    let mut host =
        Engine::new(config.with_host_event_recording(false))?.compile_registry(&registry, graph)?;
    host.prepare()?;
    Ok(host)
}
