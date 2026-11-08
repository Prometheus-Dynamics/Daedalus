//! The four-point process probe for the lazy device, run on a board (ignored by default): the
//! GPU driver maps, the thread count and the RSS after compile, before a GPU touch (a calibrating
//! tick), after the first dispatch, and after the second. Run with `--ignored --nocapture`.
#![cfg(all(feature = "gpu-wgpu", feature = "plugins", target_os = "linux"))]
// The probe's output is the measurement: its points go to stderr.
#![allow(clippy::print_stderr)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use daedalus_engine::{Engine, EngineConfig, GpuBackend};
use daedalus_planner::{ComputeAffinity, Graph, NodeInstance};
use daedalus_runtime::executor::{NodeError, NodeHandler};
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;
use daedalus_runtime::{RuntimeNode, SchedulerConfig, build_runtime};

/// The GPU driver files this process maps (the Vulkan loader, an ICD or a Mesa driver).
fn gpu_libraries() -> Vec<String> {
    let maps = std::fs::read_to_string("/proc/self/maps").unwrap_or_default();
    let mut libs: Vec<String> = maps
        .lines()
        .filter_map(|line| line.split_whitespace().nth(5))
        .filter(|path| {
            let name = path.rsplit('/').next().unwrap_or(path);
            [
                "libvulkan",
                "vulkan_",
                "libgallium",
                "libGLX",
                "libEGL",
                "libnvidia",
                "mesa",
            ]
            .iter()
            .any(|needle| name.contains(needle) || path.contains(needle))
        })
        .map(str::to_string)
        .collect();
    libs.sort();
    libs.dedup();
    libs
}

fn threads() -> usize {
    std::fs::read_dir("/proc/self/task").map_or(0, |dir| dir.count())
}

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

fn report(point: &str) {
    eprintln!(
        "PROBE {point}: threads={} rss_kib={} gpu_libs={:?}",
        threads(),
        rss_kib(),
        gpu_libraries()
    );
}

/// A calibrating node: it asks for its GPU context only once `touch` is set.
struct Calibrating {
    touch: Arc<AtomicBool>,
}

impl NodeHandler for Calibrating {
    fn run(
        &self,
        _node: &RuntimeNode,
        ctx: &ExecutionContext,
        _io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        if self.touch.load(Ordering::SeqCst) {
            ctx.gpu()?;
        }
        Ok(())
    }
}

#[test]
#[ignore = "needs a GPU device; run on the board with --ignored"]
fn the_lazy_device_maps_no_driver_until_a_node_asks() {
    report("start");
    let engine = Engine::new(
        EngineConfig::default()
            .with_gpu(GpuBackend::Device)
            .with_planner_gpu(false),
    )
    .expect("engine");
    report("engine");

    let mut graph = Graph::default();
    let mut node = NodeInstance::new("n0");
    node.compute = ComputeAffinity::GpuPreferred;
    graph.nodes.push(node);
    let plan = build_runtime(
        &daedalus_planner::ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig::default(),
    );
    let touch = Arc::new(AtomicBool::new(false));
    let mut compiled = engine
        .compile_runtime_plan(
            plan,
            Calibrating {
                touch: touch.clone(),
            },
        )
        .expect("compile");
    report("compile");

    compiled.run().expect("calibrating tick");
    report("calibrating_tick");

    touch.store(true, Ordering::SeqCst);
    compiled.run().expect("first dispatch");
    report("first_dispatch");

    compiled.run().expect("second dispatch");
    report("second_dispatch");
}
