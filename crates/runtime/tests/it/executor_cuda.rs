//! A graph whose GPU node runs on CUDA: the source emits `f32` values, the `scale` node uploads
//! them to the device, runs the `scale_f32` kernel, downloads, and emits the result; the sink
//! checks the values. Skips (passes, printing why) without a CUDA device.
#![cfg(feature = "gpu-cuda")]
#![allow(clippy::print_stderr)] // the skip reason

use std::sync::Arc;

use daedalus_gpu::{GpuBackend, GpuBackendKind, GpuOptions, select_backend};
use daedalus_planner::{ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::executor::NodeError;
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;
use daedalus_runtime::sync::Mutex;
use daedalus_runtime::{
    BackpressureStrategy, Executor, NodeHandler, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig,
    build_runtime,
};
use daedalus_transport::Payload;

const INPUT: [f32; 5] = [1.0, -2.0, 0.5, 4.0, 10.25];
const FACTOR: f32 = 2.5;

fn gpu_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("src"));
    graph
        .nodes
        .push(NodeInstance::new("scale").with_compute(ComputeAffinity::GpuRequired));
    graph
        .nodes
        .push(NodeInstance::new("sink").with_inputs(["in"]));
    graph.edges.push(Edge::new(0, "out", 1, "in"));
    graph.edges.push(Edge::new(1, "out", 2, "in"));
    ExecutionPlan::new(graph, vec![])
}

#[derive(Default)]
struct Seen {
    /// The values the sink received, and the device the `scale` node ran on.
    values: Vec<f32>,
    device: Option<String>,
}

struct CudaHandler {
    seen: Arc<Mutex<Seen>>,
}

impl NodeHandler for CudaHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "src" => {
                io.push_payload("out", Payload::owned("test:f32s", INPUT.to_vec()));
            }
            "scale" => {
                let input = io
                    .inputs_for("in")
                    .find_map(|p| p.inner.get_ref::<Vec<f32>>().cloned())
                    .ok_or_else(|| NodeError::InvalidInput("no f32 input".into()))?;
                let gpu = ctx
                    .gpu
                    .as_ref()
                    .ok_or_else(|| NodeError::Handler("no GPU context".into()))?;
                let cuda = gpu.cuda().ok_or_else(|| {
                    NodeError::Handler(format!("GPU is {}, not CUDA", gpu.backend_kind().as_str()))
                })?;
                let bytes: Vec<u8> = input.iter().flat_map(|v| v.to_ne_bytes()).collect();
                let buffer = cuda
                    .upload(&bytes)
                    .map_err(|e| NodeError::Handler(e.to_string()))?;
                cuda.scale_f32(&buffer, FACTOR)
                    .map_err(|e| NodeError::Handler(e.to_string()))?;
                let out = cuda
                    .download(&buffer)
                    .map_err(|e| NodeError::Handler(e.to_string()))?;
                let values: Vec<f32> = out
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| f32::from_ne_bytes(*c))
                    .collect();
                let mut seen = self.seen.lock();
                seen.device = Some(cuda.adapter_info().name);
                drop(seen);
                io.push_payload("out", Payload::owned("test:f32s", values));
            }
            "sink" => {
                let values = io
                    .inputs_for("in")
                    .find_map(|p| p.inner.get_ref::<Vec<f32>>().cloned())
                    .ok_or_else(|| NodeError::InvalidInput("sink got no values".into()))?;
                self.seen.lock().values = values;
            }
            other => return Err(NodeError::Handler(format!("unexpected node {other}"))),
        }
        Ok(())
    }
}

#[test]
fn a_gpu_node_runs_upload_kernel_download_on_cuda() {
    let gpu = match select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Cuda),
        ..Default::default()
    }) {
        Ok(gpu) if gpu.backend_kind() == GpuBackendKind::Cuda => gpu,
        Ok(gpu) => {
            eprintln!(
                "skipped: CUDA unavailable, selected {}",
                gpu.backend_kind().as_str()
            );
            return;
        }
        Err(e) => {
            eprintln!("skipped: {e}");
            return;
        }
    };
    assert!(
        gpu.cuda().is_some(),
        "the executor's handle is the CUDA backend"
    );

    let rt = build_runtime(
        &gpu_plan(),
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    );
    let seen = Arc::new(Mutex::new(Seen::default()));
    let telemetry = Executor::new(
        &rt,
        CudaHandler {
            seen: Arc::clone(&seen),
        },
    )
    .with_gpu(gpu)
    .run()
    .expect("executor run");

    assert_eq!(telemetry.nodes_executed, 3);
    assert_eq!(telemetry.gpu_fallbacks, 0);
    let seen = seen.lock();
    assert!(
        seen.device.as_deref().is_some_and(|name| !name.is_empty()),
        "the scale node ran on a CUDA device"
    );
    let expected: Vec<f32> = INPUT.iter().map(|v| v * FACTOR).collect();
    assert_eq!(seen.values, expected);
}
