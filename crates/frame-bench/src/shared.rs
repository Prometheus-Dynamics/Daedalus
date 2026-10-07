//! Shared preprocessing: one camera's mask prep and quads computed once per frame for several
//! detector graphs (one per dictionary) in an [`ExecutionDomain`], against one full detector
//! graph per dictionary.
//!
//! [`compile_shared_detectors`] lays the domain out by hand: a `preprocess` graph
//! (`frame -> mask_prep -> quads -> host.quads`) linked to one tail graph per dictionary
//! (`frame, quads -> decode -> validate -> refine`), the frame routed to all of them.
//! [`compile_structural_detectors`] loads the full per-dictionary graphs with
//! [`ExecutionDomain::load_shared`], which finds the same split from the `shareable` stages.

use std::time::Instant;

use daedalus::{
    PortHandle,
    data::{model::Value, to_value::ToValue},
    engine::{DomainOverhead, Engine, EngineConfig, EngineError, ExecutionDomain, LinkMode},
    planner::Graph,
    runtime::{handler_registry::HandlerRegistry, plugins::PluginRegistry},
};

use crate::chain::BenchError;
use crate::detector::{
    DECODE, DetectorShape, Dictionary, MASK_PREP, QUADS, Quads, REFINE, VALIDATE,
    detector_graph_for, detector_registry,
};
use crate::source::{SyntheticFrame, SyntheticFrameSource};
use crate::{FrameBenchConfig, InstructionCounter, percentile};

/// Name of the preprocessing graph [`compile_shared_detectors`] builds.
pub const PREPROCESS_GRAPH: &str = "preprocess";

/// The graph name of a dictionary's detector in a shared-detector domain.
pub fn dictionary_name(dictionary: Dictionary) -> &'static str {
    match dictionary {
        Dictionary::Aruco4x4_50 => "aruco_4x4_50",
        Dictionary::Aruco6x6_250 => "aruco_6x6_250",
        Dictionary::AprilTag16h5 => "apriltag_16h5",
        Dictionary::AprilTag36h11 => "apriltag_36h11",
    }
}

fn port(alias: &str, port: &str) -> PortHandle {
    PortHandle::new(alias, port)
}

/// `frame -> mask_prep -> quads -> host.quads`, the stages and constants of the flat detector.
pub fn preprocess_graph(registry: &PluginRegistry) -> Result<Graph, BenchError> {
    Ok(registry
        .graph_builder()?
        .input_typed::<SyntheticFrame>("frame")?
        .try_node_id(MASK_PREP, "mask_prep")?
        .try_node_id(QUADS, "quads")?
        .try_connect("frame", &port("mask_prep", "frame"))?
        .try_connect(&port("mask_prep", "runs"), &port("quads", "runs"))?
        .try_connect(&port("quads", "quads"), "quads")?
        .const_input(&port("mask_prep", "pyramid_level"), Some(Value::Int(1)))
        .build())
}

/// `frame, quads -> decode -> validate -> refine` decoding `dictionary`, with the flat
/// detector's constants and host outputs.
pub fn detector_tail_graph(
    registry: &PluginRegistry,
    dictionary: Dictionary,
) -> Result<Graph, BenchError> {
    let dictionary = Some(dictionary.to_value());
    Ok(registry
        .graph_builder()?
        .input_typed::<SyntheticFrame>("frame")?
        .input_typed::<Quads>("quads")?
        .try_node_id(DECODE, "decode")?
        .try_node_id(VALIDATE, "validate")?
        .try_node_id(REFINE, "refine")?
        .try_connect("frame", &port("decode", "frame"))?
        .try_connect("quads", &port("decode", "quads"))?
        .const_input(&port("decode", "dictionary"), dictionary.clone())
        .const_input(&port("decode", "validate"), Some(Value::Bool(false)))
        .try_connect("frame", &port("validate", "frame"))?
        .try_connect(
            &port("decode", "detections"),
            &port("validate", "detections"),
        )?
        .const_input(&port("validate", "dictionary"), dictionary)
        .try_connect(&port("validate", "detections"), "detections")?
        .try_connect(&port("validate", "rejected"), "rejected")?
        .try_connect("frame", &port("refine", "frame"))?
        .try_connect(
            &port("validate", "detections"),
            &port("refine", "detections"),
        )?
        .try_connect(&port("refine", "refined_corners"), "refined_corners")?
        .build())
}

fn compile(
    engine: &Engine,
    registry: &PluginRegistry,
    graph: Graph,
) -> Result<daedalus::engine::HostGraph<HandlerRegistry>, BenchError> {
    let mut host = engine.compile_registry(registry, graph)?;
    host.prepare()?;
    Ok(host)
}

/// The hand-laid domain: [`PREPROCESS_GRAPH`] linked (latest-only) to one
/// [`detector_tail_graph`] per dictionary, named by [`dictionary_name`]; domain input `frame`
/// routed to all of them.
pub fn compile_shared_detectors(
    dictionaries: &[Dictionary],
    config: EngineConfig,
) -> Result<ExecutionDomain<HandlerRegistry>, BenchError> {
    let registry = detector_registry()?;
    let engine = Engine::new(config.with_host_event_recording(false))?;
    let mut domain = ExecutionDomain::new();
    domain.add_graph(
        PREPROCESS_GRAPH,
        compile(&engine, &registry, preprocess_graph(&registry)?)?,
    )?;
    domain.route_input("frame", PREPROCESS_GRAPH, "frame")?;
    for &dictionary in dictionaries {
        let name = dictionary_name(dictionary);
        let graph = compile(
            &engine,
            &registry,
            detector_tail_graph(&registry, dictionary)?,
        )?;
        domain.add_graph(name, graph)?;
        domain.route_input("frame", name, "frame")?;
        domain.link(PREPROCESS_GRAPH, "quads", name, "quads", LinkMode::Latest)?;
    }
    Ok(domain)
}

/// One full flat detector per dictionary, compiled separately (the unshared baseline).
pub fn compile_separate_detectors(
    dictionaries: &[Dictionary],
    config: EngineConfig,
) -> Result<ExecutionDomain<HandlerRegistry>, BenchError> {
    let registry = detector_registry()?;
    let engine = Engine::new(config.with_host_event_recording(false))?;
    let mut domain = ExecutionDomain::new();
    for &dictionary in dictionaries {
        let name = dictionary_name(dictionary);
        let graph = detector_graph_for(&registry, DetectorShape::Flat, dictionary)?;
        domain.add_graph(name, compile(&engine, &registry, graph)?)?;
        domain.route_input("frame", name, "frame")?;
    }
    Ok(domain)
}

/// The full per-dictionary detectors loaded with [`ExecutionDomain::load_shared`]: the
/// `shareable` mask prep and quads stages run once in the shared upstream.
pub fn compile_structural_detectors(
    dictionaries: &[Dictionary],
    config: EngineConfig,
) -> Result<ExecutionDomain<HandlerRegistry>, BenchError> {
    let registry = detector_registry()?;
    let engine = Engine::new(config.with_host_event_recording(false))?;
    let graphs = dictionaries
        .iter()
        .map(|&dictionary| {
            let graph = detector_graph_for(&registry, DetectorShape::Flat, dictionary)?;
            Ok((dictionary_name(dictionary), graph))
        })
        .collect::<Result<Vec<_>, BenchError>>()?;
    Ok(ExecutionDomain::load_shared(&engine, &registry, graphs)?)
}

/// One run of [`run_domain_bench`].
#[derive(Clone, Debug)]
pub struct DomainBenchRun {
    pub ticks: usize,
    pub frame_p50_ns: u64,
    pub frame_p99_ns: u64,
    pub frame_mean_ns: f64,
    /// Runtime, node, host and other allocations per frame; `None` without the counting
    /// allocator.
    pub allocs_per_frame: Option<[f64; 4]>,
    pub instructions_per_frame: Option<f64>,
    /// Per-graph frame overhead and domain totals, when recording.
    pub overhead: Option<DomainOverhead>,
}

impl core::fmt::Display for DomainBenchRun {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        writeln!(
            f,
            "frame (push + domain tick + take): p50 {} ns, p99 {} ns, mean {:.0} ns over {} frames",
            self.frame_p50_ns, self.frame_p99_ns, self.frame_mean_ns, self.ticks
        )?;
        if let Some(instructions) = self.instructions_per_frame {
            writeln!(f, "instructions per frame: {instructions:.0}")?;
        }
        if let Some([runtime, node, host, other]) = self.allocs_per_frame {
            writeln!(
                f,
                "allocations per frame: runtime {runtime:.2}, node {node:.2}, host {host:.2}, other {other:.2}"
            )?;
        }
        match &self.overhead {
            Some(overhead) => write!(f, "{overhead}"),
            None => Ok(()),
        }
    }
}

/// Drive `domain` like [`crate::run_frame_bench`] drives one graph: per frame push a frame to
/// domain input `frame`, tick the domain and take every `(graph, port)` of `outputs`.
pub fn run_domain_bench(
    domain: &mut ExecutionDomain<HandlerRegistry>,
    source: &mut SyntheticFrameSource,
    config: &FrameBenchConfig,
    outputs: &[(&str, &str)],
) -> Result<DomainBenchRun, EngineError> {
    let mut frame = |domain: &mut ExecutionDomain<HandlerRegistry>| {
        domain.push_payload(config.input.as_str(), source.next_payload())?;
        let tick = domain.tick();
        if !tick.is_ok() {
            return Err(EngineError::Config(format!("domain tick failed: {tick:?}")));
        }
        for (graph, port) in outputs {
            while domain.take_payload(graph, port).is_some() {}
        }
        Ok(())
    };
    for _ in 0..config.warmup {
        frame(domain)?;
    }
    domain.reset_frame_overhead();
    let mut wall = vec![0u64; config.ticks.max(1)];
    let before = daedalus::alloc_probe::counts();
    let counter = InstructionCounter::start();
    for slot in wall.iter_mut() {
        let start = Instant::now();
        frame(domain)?;
        *slot = start.elapsed().as_nanos() as u64;
    }
    let instructions = counter.as_ref().map(InstructionCounter::read);
    let during = daedalus::alloc_probe::counts().since(&before);
    let ticks = wall.len();
    let mean = wall.iter().sum::<u64>() as f64 / ticks as f64;
    wall.sort_unstable();
    let per = |count: u64| count as f64 / ticks as f64;
    Ok(DomainBenchRun {
        ticks,
        frame_p50_ns: percentile(&wall, 50),
        frame_p99_ns: percentile(&wall, 99),
        frame_mean_ns: mean,
        allocs_per_frame: daedalus::alloc_probe::is_installed().then(|| {
            [
                per(during.runtime),
                per(during.node),
                per(during.host),
                per(during.other),
            ]
        }),
        instructions_per_frame: instructions.map(|count| count as f64 / ticks as f64),
        overhead: domain.frame_overhead(),
    })
}
