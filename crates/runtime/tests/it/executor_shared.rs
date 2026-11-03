//! A wgpu node, a CUDA node and a wgpu node share one buffer, planned by the planner and run by
//! the executor over several ticks. The edges between devices are cost-0 `SharedView` adapters
//! (`share.gpu_to_cuda`, `share.cuda_to_gpu`), so the plan has no transfer step; both backends'
//! transfer counters stay at 0; the values are checked after each tick. Skips (passes, printing
//! why) without an NVIDIA wgpu device and the CUDA driver.
#![cfg(all(feature = "gpu-cuda", feature = "gpu-vulkan-raw"))]
#![allow(clippy::print_stderr)] // the skip reason

use std::sync::Arc;

use daedalus_gpu::interop::SharedValue;
use daedalus_gpu::{CudaBackend, GpuBackend, GpuContextHandle, select_wgpu_on_vendor};
use daedalus_planner::{
    ComputeAffinity, Edge, EdgeResolutionKind, Graph, NodeInstance, PlannerConfig, PlannerInput,
    build_plan, edge_explanations,
};
use daedalus_registry::capability::{AdapterDecl, CapabilityRegistry, NodeDecl, PortDecl};
use daedalus_runtime::executor::NodeError;
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;
use daedalus_runtime::sync::Mutex;
use daedalus_runtime::transport::RuntimeTransport;
use daedalus_runtime::{
    BackpressureStrategy, Executor, NodeHandler, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig,
    build_runtime,
};
use daedalus_transport::{AdaptCost, AdaptKind, Payload, Residency, TransportError};

const NVIDIA: u32 = 0x10DE;
const COUNT: usize = 1000;
const TICKS: u32 = 3;
const FACTOR: f32 = 2.5;
const WGPU: &str = "test:f32@gpu";
const CUDA: &str = "test:f32@cuda";
const TO_CUDA: &str = "share.gpu_to_cuda";
const TO_WGPU: &str = "share.cuda_to_gpu";
const WG: u32 = daedalus_gpu::interop::WORKGROUP_SIZE;

/// The registry: two cost-0 shared adapters between the device keys, and the four nodes' ports.
fn registry() -> CapabilityRegistry {
    let mut reg = CapabilityRegistry::new();
    for (id, from, to) in [(TO_CUDA, WGPU, CUDA), (TO_WGPU, CUDA, WGPU)] {
        reg.register_adapter(
            AdapterDecl::new(id, from, to)
                .cost(AdaptCost::new(AdaptKind::SharedView))
                .requires_gpu(true)
                .residency(Residency::Gpu),
        )
        .expect("shared adapter");
    }
    for decl in [
        NodeDecl::new("gen").output(PortDecl::new("out", WGPU)),
        NodeDecl::new("scale")
            .input(PortDecl::new("in", CUDA))
            .output(PortDecl::new("out", CUDA)),
        NodeDecl::new("addone")
            .input(PortDecl::new("in", WGPU))
            .output(PortDecl::new("out", WGPU)),
        NodeDecl::new("sink").input(PortDecl::new("in", WGPU)),
    ] {
        reg.register_node(decl).expect("node");
    }
    reg
}

/// gen (wgpu) -> scale (CUDA) -> addone (wgpu) -> sink (wgpu, checks the values).
fn graph() -> Graph {
    let mut graph = Graph::default();
    graph.nodes.push(
        NodeInstance::new("gen")
            .with_outputs(["out"])
            .with_compute(ComputeAffinity::GpuRequired),
    );
    graph.nodes.push(
        NodeInstance::new("scale")
            .with_inputs(["in"])
            .with_outputs(["out"])
            .with_compute(ComputeAffinity::GpuRequired),
    );
    graph.nodes.push(
        NodeInstance::new("addone")
            .with_inputs(["in"])
            .with_outputs(["out"])
            .with_compute(ComputeAffinity::GpuRequired),
    );
    graph
        .nodes
        .push(NodeInstance::new("sink").with_inputs(["in"]));
    graph.edges.push(Edge::new(0, "out", 1, "in"));
    graph.edges.push(Edge::new(1, "out", 2, "in"));
    graph.edges.push(Edge::new(2, "out", 3, "in"));
    graph
}

/// The wgpu compute entry points: `gen` writes `i * 0.5 + tick`, `addone` adds 1.
fn gen_source(tick: u32) -> String {
    format!(
        "@group(0) @binding(0) var<storage, read_write> data: array<f32>;\n\
         @compute @workgroup_size({WG})\n\
         fn gen(@builtin(global_invocation_id) id: vec3<u32>) {{\n\
           if (id.x < arrayLength(&data)) {{ data[id.x] = f32(id.x) * 0.5 + {tick}.0; }}\n\
         }}\n"
    )
}

const ADDONE_SOURCE: &str = "@group(0) @binding(0) var<storage, read_write> data: array<f32>;
@compute @workgroup_size(64)
fn addone(@builtin(global_invocation_id) id: vec3<u32>) {
  if (id.x < arrayLength(&data)) { data[id.x] = data[id.x] + 1.0; }
}
";

#[derive(Default)]
struct Seen {
    values: Vec<f32>,
}

struct Handler {
    gpu: GpuContextHandle,
    cuda: Arc<CudaBackend>,
    tick: u32,
    seen: Arc<Mutex<Seen>>,
}

fn shared_input(io: &NodeIo) -> Result<Arc<SharedValue>, NodeError> {
    io.inputs_for("in")
        .find_map(|p| p.inner.get_arc::<SharedValue>())
        .ok_or_else(|| NodeError::InvalidInput("no shared value on the input".into()))
}

fn handler_err(e: impl std::fmt::Display) -> NodeError {
    NodeError::Handler(e.to_string())
}

impl NodeHandler for Handler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "gen" => {
                let bytes = (COUNT * 4) as u64;
                let value = SharedValue::create(&self.gpu, bytes).map_err(handler_err)?;
                value
                    .dispatch_wgsl(&gen_source(self.tick), "gen")
                    .map_err(handler_err)?;
                io.push_payload("out", Payload::shared(WGPU, Arc::new(value)));
            }
            "scale" => {
                let value = shared_input(io)?;
                value.cuda_scale(&self.cuda, FACTOR).map_err(handler_err)?;
                io.push_payload("out", Payload::shared(CUDA, value));
            }
            "addone" => {
                let value = shared_input(io)?;
                value
                    .dispatch_wgsl(ADDONE_SOURCE, "addone")
                    .map_err(handler_err)?;
                io.push_payload("out", Payload::shared(WGPU, value));
            }
            "sink" => {
                let value = shared_input(io)?;
                let bytes = value.read().map_err(handler_err)?;
                self.seen.lock().values = bytes
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| f32::from_ne_bytes(*c))
                    .collect();
            }
            other => return Err(NodeError::Handler(format!("unexpected node {other}"))),
        }
        Ok(())
    }
}

/// The executable adapters: each takes the value across the boundary (no copy) and retags it.
fn transport(cuda: &Arc<CudaBackend>) -> RuntimeTransport {
    let mut t = RuntimeTransport::new();
    let c = Arc::clone(cuda);
    t.register_adapter_fn(TO_CUDA, move |payload, _| {
        let value = payload
            .get_arc::<SharedValue>()
            .ok_or_else(|| TransportError::Unsupported("not a shared value".into()))?;
        value
            .enter_cuda(&c)
            .map_err(|e| TransportError::Unsupported(e.to_string()))?;
        Ok(Payload::shared(CUDA, value))
    })
    .expect("adapter");
    let c = Arc::clone(cuda);
    t.register_adapter_fn(TO_WGPU, move |payload, _| {
        let value = payload
            .get_arc::<SharedValue>()
            .ok_or_else(|| TransportError::Unsupported("not a shared value".into()))?;
        value
            .leave_cuda(&c)
            .map_err(|e| TransportError::Unsupported(e.to_string()))?;
        Ok(Payload::shared(WGPU, value))
    })
    .expect("adapter");
    t
}

#[test]
fn a_wgpu_cuda_wgpu_graph_shares_one_buffer_with_no_transfer() {
    let test = "a_wgpu_cuda_wgpu_graph_shares_one_buffer_with_no_transfer";
    let gpu = match select_wgpu_on_vendor(NVIDIA) {
        Ok(gpu) => gpu,
        Err(e) => {
            eprintln!("skipped {test}: no NVIDIA wgpu device: {e}");
            return;
        }
    };
    let cuda = match CudaBackend::new() {
        Ok(cuda) => Arc::new(cuda),
        Err(e) => {
            eprintln!("skipped {test}: {e}");
            return;
        }
    };

    // The planner: every edge between the devices is one shared adapter; no transfer step at all.
    let out = build_plan(
        PlannerInput { graph: graph() },
        PlannerConfig {
            transport_capabilities: Some(registry()),
            enable_gpu: true,
            ..PlannerConfig::default()
        },
    );
    let explanations = edge_explanations(&out.plan.graph.metadata);
    let steps_into = |node: &str| -> Vec<String> {
        explanations
            .iter()
            .find(|e| e.to_node == node)
            .map(|e| e.converter_steps.clone())
            .unwrap_or_default()
    };
    assert_eq!(steps_into("scale"), [TO_CUDA], "gen -> scale");
    assert_eq!(steps_into("addone"), [TO_WGPU], "scale -> addone");
    assert!(
        steps_into("sink").is_empty(),
        "addone -> sink is device-to-device"
    );
    for e in &explanations {
        assert_ne!(e.resolution_kind, EdgeResolutionKind::Missing, "{e:?}");
        for step in &e.converter_steps {
            assert!(
                step == TO_CUDA || step == TO_WGPU,
                "a non-shared step in the plan: {step}"
            );
        }
    }

    let rt = build_runtime(
        &out.plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    );
    let seen = Arc::new(Mutex::new(Seen::default()));
    let cuda_before = cuda.stats();
    let wgpu_before = gpu.stats();
    for tick in 0..TICKS {
        let telemetry = Executor::new(
            &rt,
            Handler {
                gpu: gpu.clone(),
                cuda: Arc::clone(&cuda),
                tick,
                seen: Arc::clone(&seen),
            },
        )
        .with_gpu(gpu.clone())
        .with_runtime_transport(transport(&cuda))
        .run()
        .expect("executor run");
        assert_eq!(telemetry.nodes_executed, 4, "tick {tick}");
        assert_eq!(telemetry.gpu_fallbacks, 0, "tick {tick}");
        let values = seen.lock().values.clone();
        assert_eq!(values.len(), COUNT, "tick {tick}");
        for (i, got) in values.iter().enumerate() {
            let want = (i as f32 * 0.5 + tick as f32) * FACTOR + 1.0;
            assert_eq!(*got, want, "tick {tick}, element {i}");
        }
    }
    assert_eq!(cuda.stats(), cuda_before, "CUDA transfer counters moved");
    assert_eq!(gpu.stats(), wgpu_before, "wgpu transfer counters moved");
    eprintln!(
        "{test}: {TICKS} ticks of {COUNT} f32 through wgpu -> CUDA -> wgpu on {}, plan steps {:?}, 0 transfers",
        cuda.adapter_info().name,
        [steps_into("scale"), steps_into("addone")]
    );
}
