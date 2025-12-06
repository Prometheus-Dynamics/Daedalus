use daedalus_data::model::{TypeExpr, ValueType};
#[cfg(feature = "gpu")]
use daedalus_gpu::{GpuCapabilities, GpuFormat, GpuFormatFeatures};
use daedalus_planner::{
    ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PlannerConfig, PlannerInput, PortRef,
    build_plan, debug,
};
use daedalus_registry::store::{NodeDescriptorBuilder, Registry};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::PathBuf;

fn assert_plan_matches_golden(plan: &daedalus_planner::ExecutionPlan, name: &str) {
    let actual_json = debug::to_pretty_json(plan);
    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "goldens", name]
        .iter()
        .collect();
    if env::var("UPDATE_GOLDENS").as_deref() == Ok("1") {
        fs::write(&path, &actual_json).expect("write golden");
    }
    let expected_json =
        fs::read_to_string(&path).unwrap_or_else(|_| panic!("missing golden {}", path.display()));
    let actual: Value = serde_json::from_str(&actual_json).expect("actual json");
    let expected: Value = serde_json::from_str(&expected_json).expect("expected json");
    assert_eq!(actual, expected);
}

#[test]
fn execution_plan_matches_cpu_golden() {
    // Build registry with two CPU nodes: a(out:int) -> b(in:int)
    let mut registry = Registry::new();
    let a = NodeDescriptorBuilder::new("a")
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(a).unwrap();
    let b = NodeDescriptorBuilder::new("b")
        .input("in", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(b).unwrap();

    // Planner graph.
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("a"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("b"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
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

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_cpu.json");
}

#[test]
fn missing_node_golden() {
    let mut registry = Registry::new();
    let a = NodeDescriptorBuilder::new("a")
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(a).unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("a"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("c"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_missing_node.json");
}

#[test]
fn missing_port_golden() {
    let mut registry = Registry::new();
    let a = NodeDescriptorBuilder::new("a")
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(a).unwrap();
    let b = NodeDescriptorBuilder::new("b")
        .input("in", TypeExpr::Scalar(ValueType::Bool))
        .build()
        .unwrap();
    registry.register_node(b).unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("a"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("b"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "missing".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: Default::default(),
    });

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_missing_port.json");
}

#[test]
fn converter_gap_golden() {
    let mut registry = Registry::new();
    let a = NodeDescriptorBuilder::new("a")
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(a).unwrap();
    let b = NodeDescriptorBuilder::new("b")
        .input("in", TypeExpr::Scalar(ValueType::Bool))
        .build()
        .unwrap();
    registry.register_node(b).unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("a"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("b"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
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

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_converter_gap.json");
}

#[test]
fn converter_success_golden() {
    struct IntToBool;
    impl daedalus_data::convert::Converter for IntToBool {
        fn id(&self) -> daedalus_data::convert::ConverterId {
            daedalus_data::convert::ConverterId("int_to_bool".into())
        }
        fn input(&self) -> &TypeExpr {
            static TY: once_cell::sync::Lazy<TypeExpr> =
                once_cell::sync::Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
            &TY
        }
        fn output(&self) -> &TypeExpr {
            static TY: once_cell::sync::Lazy<TypeExpr> =
                once_cell::sync::Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
            &TY
        }
        fn cost(&self) -> u64 {
            1
        }
        fn convert(
            &self,
            _value: daedalus_data::model::Value,
        ) -> Result<daedalus_data::model::Value, daedalus_data::errors::DataError> {
            Ok(daedalus_data::model::Value::Bool(true))
        }
    }

    let mut registry = Registry::new();
    let a = NodeDescriptorBuilder::new("a")
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(a).unwrap();
    let b = NodeDescriptorBuilder::new("b")
        .input("in", TypeExpr::Scalar(ValueType::Bool))
        .build()
        .unwrap();
    registry.register_node(b).unwrap();
    registry
        .register_converter(Box::new(IntToBool))
        .expect("converter");

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("a"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("b"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
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

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_converter_success.json");
}

#[test]
fn cycle_golden() {
    let mut registry = Registry::new();
    let desc = NodeDescriptorBuilder::new("n")
        .input("in", TypeExpr::Scalar(ValueType::Int))
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(desc).unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
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
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(1),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(0),
            port: "in".into(),
        },
        metadata: Default::default(),
    });

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_cycle.json");
}

#[test]
fn gpu_required_without_flag_golden() {
    let mut registry = Registry::new();
    let desc = NodeDescriptorBuilder::new("n")
        .input("in", TypeExpr::Scalar(ValueType::Int))
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(desc).unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::GpuRequired,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig {
            enable_gpu: false,
            ..Default::default()
        },
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_gpu_required_disabled.json");
}

#[test]
fn lint_warnings_golden() {
    let mut registry = Registry::new();
    // Node a: source with unused output (since no edges)
    let a = NodeDescriptorBuilder::new("a")
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(a).unwrap();
    // Node b: sink with unconnected input
    let b = NodeDescriptorBuilder::new("b")
        .input("in", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(b).unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("a"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec!["out".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("b"),
        bundle: None,
        label: None,
        inputs: vec!["in".into()],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig {
            enable_lints: true,
            ..Default::default()
        },
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_lint_warnings.json");
}

#[cfg(feature = "gpu")]
#[test]
fn gpu_caps_insufficient_golden() {
    let mut registry = Registry::new();
    let desc = NodeDescriptorBuilder::new("n")
        .input("in", TypeExpr::Scalar(ValueType::Int))
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(desc).unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::GpuPreferred,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });

    let caps = GpuCapabilities {
        supported_formats: vec![],
        format_features: vec![],
        format_blocks: vec![],
        max_buffer_size: 0,
        max_texture_dimension: 0,
        max_texture_samples: 0,
        staging_alignment: 0,
        max_inflight_copies: 0,
        queue_count: 0,
        min_buffer_copy_offset_alignment: 0,
        bytes_per_row_alignment: 0,
        rows_per_image_alignment: 0,
        has_transfer_queue: false,
    };

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig {
            enable_gpu: true,
            gpu_caps: Some(caps),
            ..Default::default()
        },
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_gpu_caps_insufficient.json");
}

#[cfg(feature = "gpu")]
#[test]
fn gpu_caps_sufficient_golden() {
    let mut registry = Registry::new();
    let desc = NodeDescriptorBuilder::new("n")
        .input("in", TypeExpr::Scalar(ValueType::Int))
        .output("out", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    registry.register_node(desc).unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::GpuPreferred,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });

    let caps = GpuCapabilities {
        supported_formats: vec![GpuFormat::Rgba8Unorm],
        format_features: vec![GpuFormatFeatures {
            format: GpuFormat::Rgba8Unorm,
            sampleable: true,
            renderable: true,
            storage: false,
            max_samples: 1,
        }],
        format_blocks: vec![],
        max_buffer_size: 1024,
        max_texture_dimension: 2048,
        max_texture_samples: 4,
        staging_alignment: 256,
        max_inflight_copies: 1,
        queue_count: 1,
        min_buffer_copy_offset_alignment: 1,
        bytes_per_row_alignment: 1,
        rows_per_image_alignment: 1,
        has_transfer_queue: true,
    };

    let plan = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig {
            enable_gpu: true,
            gpu_caps: Some(caps),
            ..Default::default()
        },
    )
    .plan;

    assert_plan_matches_golden(&plan, "plan_gpu_caps_sufficient.json");
}
