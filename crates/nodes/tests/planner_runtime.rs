#![cfg(all(
    feature = "planner-adapter",
    feature = "registry-adapter",
    feature = "bundle-starter"
))]

use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_nodes::{node, planner_adapter, registry_adapter};
use daedalus_planner::{ComputeAffinity, Edge, NodeRef, PlannerConfig, PlannerInput, PortRef};
use daedalus_registry::store::Registry;
use daedalus_runtime::NodeError;

#[node(id = "test.cpu", bundle = "test", outputs("out"))]
fn cpu_node() -> Result<i32, NodeError> {
    Ok(1)
}

#[node(
    id = "test.gpu",
    bundle = "test",
    compute(ComputeAffinity::GpuPreferred),
    inputs("in")
)]
fn gpu_node(input: i32) -> Result<(), NodeError> {
    let _ = input;
    Ok(())
}

#[test]
fn planner_runtime_smoke_gpu_optional() {
    let mut registry = Registry::new();
    // Register CPU node.
    let cpu_desc = registry_adapter::registry_builder(&cpu_node::descriptor())
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(cpu_desc).unwrap();
    // Register GPU-optional node.
    let gpu_desc = registry_adapter::registry_builder(&gpu_node::descriptor())
        .input("in", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(gpu_desc).unwrap();

    // Build planner graph.
    let mut graph = daedalus_planner::Graph::default();
    graph.nodes.push(planner_adapter::node_instance(
        &cpu_node::descriptor(),
        std::iter::empty::<&'static str>(),
        ["out"],
    ));
    graph.nodes.push(planner_adapter::node_instance(
        &gpu_node::descriptor(),
        ["in"],
        std::iter::empty::<&'static str>(),
    ));
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: Default::default(),
    });

    let plan_out = daedalus_planner::build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig {
            enable_gpu: true,
            ..Default::default()
        },
    );
    assert!(
        plan_out.diagnostics.is_empty(),
        "expected no planner diagnostics, got: {:?}",
        plan_out.diagnostics
    );

    // Build runtime plan; segments should group GPU node with affinity marked.
    let runtime_plan = daedalus_runtime::build_runtime(&plan_out.plan, &Default::default());
    assert_eq!(runtime_plan.nodes.len(), 2);
    assert!(
        runtime_plan
            .nodes
            .iter()
            .any(|n| n.id == "test.gpu" && n.compute == ComputeAffinity::GpuPreferred)
    );
    assert_eq!(runtime_plan.segments.len(), 2);
    assert!(
        runtime_plan
            .segments
            .iter()
            .any(|s| s.compute == ComputeAffinity::GpuPreferred)
    );
}
