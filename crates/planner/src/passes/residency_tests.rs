//! A bare GPU request (`Residency::Gpu` on a GPU-affine node's input) lands on the device key its
//! neighbours are already on, with no transfer, when both `…@cuda` and `…@gpu` are registered.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use daedalus_registry::capability::{
    AdapterDecl, CapabilityRegistry, DeviceDecl, NodeDecl, PortDecl,
};
use daedalus_transport::{AdaptCost, Residency};

use crate::graph::{ComputeAffinity, Edge, Graph, NodeInstance};
use crate::passes::{EdgeResolutionExplanation, PlannerConfig, PlannerInput, build_plan};

const CPU: &str = "test:f32";
const CUDA: &str = "test:f32@cuda";
const WGPU: &str = "test:f32@gpu";

/// Two devices for `test:f32`: CUDA and wgpu, each with an upload and a download adapter.
fn two_devices() -> CapabilityRegistry {
    let mut reg = CapabilityRegistry::new();
    for (name, device) in [("cuda", CUDA), ("gpu", WGPU)] {
        reg.register_adapter(
            AdapterDecl::new(format!("test.{name}.upload"), CPU, device)
                .cost(AdaptCost::device_transfer())
                .requires_gpu(true)
                .residency(Residency::Gpu),
        )
        .expect("upload");
        reg.register_adapter(
            AdapterDecl::new(format!("test.{name}.download"), device, CPU)
                .cost(AdaptCost::device_transfer())
                .requires_gpu(true)
                .residency(Residency::Cpu),
        )
        .expect("download");
        reg.register_device(DeviceDecl::new(
            format!("test.{name}.device"),
            CPU,
            device,
            format!("test.{name}.upload"),
            format!("test.{name}.download"),
        ))
        .expect("device");
    }
    reg
}

/// `source` emits `out_ty`; `scale` is GPU-affine with a bare `f32` input (`CPU`), and `sink`
/// consumes `sink_ty`. Its input is the GPU request under test.
fn registry(out_ty: &'static str, sink_ty: &'static str) -> CapabilityRegistry {
    let mut reg = two_devices();
    for decl in [
        NodeDecl::new("test:source").output(PortDecl::new("out", out_ty)),
        NodeDecl::new("test:scale")
            .input(PortDecl::new("in", CPU))
            .output(PortDecl::new("out", CPU)),
        NodeDecl::new("test:sink").input(PortDecl::new("in", sink_ty)),
    ] {
        reg.register_node(decl).expect("node");
    }
    reg
}

fn graph(with_sink: bool) -> Graph {
    let mut graph = Graph::default();
    graph
        .nodes
        .push(NodeInstance::new("test:source").with_outputs(["out"]));
    graph.nodes.push(
        NodeInstance::new("test:scale")
            .with_inputs(["in"])
            .with_outputs(["out"])
            .with_compute(ComputeAffinity::GpuRequired),
    );
    graph.edges.push(Edge::new(0, "out", 1, "in"));
    if with_sink {
        graph
            .nodes
            .push(NodeInstance::new("test:sink").with_inputs(["in"]));
        graph.edges.push(Edge::new(1, "out", 2, "in"));
    }
    graph
}

/// The explanation of the edge into `scale` (the source's output).
fn into_scale(reg: &CapabilityRegistry, with_sink: bool) -> EdgeResolutionExplanation {
    let config = PlannerConfig {
        transport_capabilities: Some(reg.clone()),
        enable_gpu: true,
        ..PlannerConfig::default()
    };
    let out = build_plan(
        PlannerInput {
            graph: graph(with_sink),
        },
        config,
    );
    let explanations = crate::passes::explain::edge_explanations(&out.plan.graph.metadata);
    explanations
        .into_iter()
        .find(|e| e.to_node == "test:scale")
        .expect("an explanation for the edge into test:scale")
}

fn steps(e: &EdgeResolutionExplanation) -> Vec<String> {
    e.converter_steps.clone()
}

#[test]
fn a_cuda_resident_producer_stays_on_cuda_with_no_transfer() {
    let reg = registry(CUDA, CPU);
    let e = into_scale(&reg, false);
    assert_eq!(
        e.transport_target,
        Some(daedalus_transport::TypeKey::new(CUDA))
    );
    assert!(steps(&e).is_empty(), "no transfer adapter: {:?}", steps(&e));
    assert_eq!(e.total_cost, 0);
}

#[test]
fn a_wgpu_resident_producer_stays_on_wgpu_with_no_transfer() {
    let reg = registry(WGPU, CPU);
    let e = into_scale(&reg, false);
    assert_eq!(
        e.transport_target,
        Some(daedalus_transport::TypeKey::new(WGPU))
    );
    assert!(steps(&e).is_empty(), "no transfer adapter: {:?}", steps(&e));
    assert_eq!(e.total_cost, 0);
}

#[test]
fn a_host_producer_follows_its_cuda_consumer() {
    // The producer is on the host, so an upload is needed either way; the consumer's device key
    // decides where it goes, so the value needs no device-to-device transfer after it.
    let reg = registry(CPU, CUDA);
    let e = into_scale(&reg, true);
    // The request was retargeted to the consumer's key, so the resolved key is the requested one.
    assert_eq!(e.transport_target, None);
    assert_eq!(steps(&e), vec!["test.cuda.upload".to_string()]);
}

#[test]
fn a_host_producer_follows_its_wgpu_consumer() {
    let reg = registry(CPU, WGPU);
    let e = into_scale(&reg, true);
    assert_eq!(e.transport_target, None);
    assert_eq!(steps(&e), vec!["test.gpu.upload".to_string()]);
}

#[test]
fn a_device_producer_wins_over_a_consumer_on_the_other_device() {
    // A CUDA producer feeding a wgpu consumer keeps the value on CUDA: its own neighbour is where
    // it is, so no transfer; the consumer side is the next edge's concern.
    let reg = registry(CUDA, WGPU);
    let e = into_scale(&reg, true);
    assert_eq!(
        e.transport_target,
        Some(daedalus_transport::TypeKey::new(CUDA))
    );
    assert!(steps(&e).is_empty());
}
