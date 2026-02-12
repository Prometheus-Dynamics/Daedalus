//! Planner passes and execution plan model scaffolding. See `PLAN.md` for staged tasks.
//! Exposes a deterministic pass pipeline from registry-sourced graphs to an `ExecutionPlan`.
//!
//! Pass order (stubs today, contract documented):
//! hydrate_registry -> typecheck -> convert -> align -> gpu -> schedule -> lint.

pub mod debug;
mod diagnostics;
mod graph;
pub mod helpers;
mod passes;
mod patch;

pub use diagnostics::{
    Diagnostic, DiagnosticCode, DiagnosticSpan, DiagnosticsBundle, MissingGroup, MissingNode,
    MissingPort, TypeMismatch, bundle,
};
pub use graph::{
    ComputeAffinity, DEFAULT_PLAN_VERSION, Edge, EdgeBufferInfo, ExecutionPlan, GpuSegment, Graph,
    NodeInstance, NodeRef, PortRef, StableHash,
};
pub use passes::{PlannerConfig, PlannerInput, PlannerOutput, build_plan};
pub use patch::{GraphMetadataSelector, GraphNodeSelector, GraphPatch, GraphPatchOp, PatchReport};

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, ValueType};
    use daedalus_registry::store::NodeDescriptorBuilder;
    use daedalus_registry::store::Registry;

    #[test]
    fn stable_hash_changes_with_edges() {
        let mut graph = Graph::default();
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new("n1"),
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
            id: daedalus_registry::ids::NodeId::new("n2"),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
        let g1 = graph.clone();
        let p1 = build_plan(
            PlannerInput {
                graph: g1,
                registry: &Registry::new(),
            },
            PlannerConfig::default(),
        )
        .plan;

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
        let p2 = build_plan(
            PlannerInput {
                graph,
                registry: &Registry::new(),
            },
            PlannerConfig::default(),
        )
        .plan;

        assert_ne!(p1.hash, p2.hash);
    }

    #[test]
    fn reports_missing_node_and_ports_and_converter_gap() {
        // Registry with node a (out:int) and b (in:bool)
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
        // Edge uses wrong output port name to trigger port missing + type mismatch
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

        let out = build_plan(
            PlannerInput {
                graph,
                registry: &registry,
            },
            PlannerConfig::default(),
        );

        // Expect port-missing on source, no type mismatch because missing source type.
        assert!(
            out.diagnostics
                .iter()
                .any(|d| matches!(d.code, DiagnosticCode::PortMissing)
                    && d.span.node.as_deref() == Some("a"))
        );

        // Now add a correct port but wrong type to trigger converter resolution.
        let mut registry2 = Registry::new();
        let a2 = NodeDescriptorBuilder::new("a")
            .output("out", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        registry2.register_node(a2).unwrap();
        let b2 = NodeDescriptorBuilder::new("b")
            .input("in", TypeExpr::Scalar(ValueType::Bool))
            .build()
            .unwrap();
        registry2.register_node(b2).unwrap();
        let mut graph2 = Graph::default();
        graph2.nodes.push(NodeInstance {
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
        graph2.nodes.push(NodeInstance {
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
        graph2.edges.push(Edge {
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

        let out2 = build_plan(
            PlannerInput {
                graph: graph2,
                registry: &registry2,
            },
            PlannerConfig::default(),
        );

        assert!(
            out2.diagnostics
                .iter()
                .any(|d| matches!(d.code, DiagnosticCode::ConverterMissing))
        );

        // Register a converter to remove the gap.
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
            fn convert(
                &self,
                _value: daedalus_data::model::Value,
            ) -> Result<daedalus_data::model::Value, daedalus_data::errors::DataError> {
                Ok(daedalus_data::model::Value::Bool(true))
            }
            fn cost(&self) -> u64 {
                1
            }
        }

        let mut registry3 = Registry::new();
        let a3 = NodeDescriptorBuilder::new("a")
            .output("out", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        registry3.register_node(a3).unwrap();
        let b3 = NodeDescriptorBuilder::new("b")
            .input("in", TypeExpr::Scalar(ValueType::Bool))
            .build()
            .unwrap();
        registry3.register_node(b3).unwrap();
        registry3
            .register_converter(Box::new(IntToBool))
            .expect("converter registers");

        let mut graph3 = Graph::default();
        graph3.nodes.push(NodeInstance {
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
        graph3.nodes.push(NodeInstance {
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
        graph3.edges.push(Edge {
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

        let out3 = build_plan(
            PlannerInput {
                graph: graph3,
                registry: &registry3,
            },
            PlannerConfig::default(),
        );

        assert!(
            !out3
                .diagnostics
                .iter()
                .any(|d| matches!(d.code, DiagnosticCode::ConverterMissing))
        );
    }

    #[test]
    fn detects_cycle_in_align() {
        let mut registry = Registry::new();
        let node_desc = NodeDescriptorBuilder::new("n")
            .input("in", TypeExpr::Scalar(ValueType::Int))
            .output("out", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        registry.register_node(node_desc).unwrap();

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

        // Cycle: 0 -> 1 -> 0
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

        let out = build_plan(
            PlannerInput {
                graph,
                registry: &registry,
            },
            PlannerConfig {
                enable_lints: true,
                ..Default::default()
            },
        );

        assert!(
            out.diagnostics
                .iter()
                .any(|d| matches!(d.code, DiagnosticCode::ScheduleConflict))
        );
    }

    #[test]
    fn gpu_required_without_flag_reports() {
        let mut registry = Registry::new();
        let node_desc = NodeDescriptorBuilder::new("n")
            .input("in", TypeExpr::Scalar(ValueType::Int))
            .output("out", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        registry.register_node(node_desc).unwrap();

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

        let out = build_plan(
            PlannerInput {
                graph,
                registry: &registry,
            },
            PlannerConfig {
                enable_gpu: false,
                ..Default::default()
            },
        );

        assert!(
            out.diagnostics
                .iter()
                .any(|d| matches!(d.code, DiagnosticCode::GpuUnsupported))
        );
    }
}
