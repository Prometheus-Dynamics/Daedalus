use std::sync::Arc;

use daedalus_data::descriptor::DescriptorBuilder;
use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_engine::{Engine, EngineConfig};
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_registry::store::{NodeDescriptor, Port, Registry};
use daedalus_runtime::RuntimeNode;
use daedalus_runtime::executor::EdgePayload;
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;

fn sample_registry() -> Registry {
    let mut reg = Registry::new();
    let ty = TypeExpr::Scalar(ValueType::Int);
    let producer = NodeDescriptor {
        id: daedalus_registry::ids::NodeId::new("producer"),
        feature_flags: vec![],
        label: None,
        inputs: vec![],
        fanin_inputs: vec![],
        outputs: vec![Port {
            name: "out".into(),
            ty: ty.clone(),
            source: None,
            const_value: None,
        }],
        default_compute: ComputeAffinity::CpuOnly,
        sync_groups: Vec::new(),
        metadata: Default::default(),
    };
    let consumer = NodeDescriptor {
        id: daedalus_registry::ids::NodeId::new("consumer"),
        feature_flags: vec![],
        label: None,
        inputs: vec![Port {
            name: "in".into(),
            ty,
            source: None,
            const_value: None,
        }],
        fanin_inputs: vec![],
        outputs: vec![],
        default_compute: ComputeAffinity::CpuOnly,
        sync_groups: Vec::new(),
        metadata: Default::default(),
    };
    reg.register_node(producer).unwrap();
    reg.register_node(consumer).unwrap();
    reg.register_value(
        DescriptorBuilder::new("int", "1.0.0")
            .type_expr(TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap(),
    )
    .unwrap();
    reg
}

fn sample_graph() -> Graph {
    let nodes = vec![
        NodeInstance {
            id: daedalus_registry::ids::NodeId::new("producer"),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec!["out".into()],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        },
        NodeInstance {
            id: daedalus_registry::ids::NodeId::new("consumer"),
            bundle: None,
            label: None,
            inputs: vec!["in".into()],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        },
    ];
    let edges = vec![Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: Default::default(),
    }];
    Graph {
        nodes,
        edges,
        metadata: Default::default(),
    }
}

#[test]
fn end_to_end_runs_serial() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let registry = sample_registry();
    let graph = sample_graph();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = {
        let seen = Arc::clone(&seen);
        move |node: &RuntimeNode, _ctx: &ExecutionContext, io: &mut NodeIo| {
            if node.id == "producer" {
                io.push_output(Some("out"), EdgePayload::Unit);
            } else if node.id == "consumer" {
                for _ in io.inputs_for("in") {
                    seen.lock().unwrap().push("hit");
                }
            }
            Ok(())
        }
    };
    let res = engine.run(&registry, graph, handler).unwrap();
    assert_eq!(res.telemetry.warnings.len(), 0);
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[test]
fn graph_metadata_is_visible_to_nodes() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let registry = sample_registry();
    let mut graph = sample_graph();
    graph.metadata.insert("multiplier".into(), Value::Int(3));

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = {
        let seen = Arc::clone(&seen);
        move |node: &RuntimeNode, ctx: &ExecutionContext, io: &mut NodeIo| {
            if node.id == "producer" {
                let m = match ctx.graph_metadata.get("multiplier") {
                    Some(Value::Int(v)) => *v,
                    _ => 1,
                };
                for _ in 0..m {
                    io.push_output(Some("out"), EdgePayload::Unit);
                }
            } else if node.id == "consumer" {
                for _ in io.inputs_for("in") {
                    seen.lock().unwrap().push("hit");
                }
            }
            Ok(())
        }
    };

    let res = engine.run(&registry, graph, handler).unwrap();
    assert_eq!(res.telemetry.warnings.len(), 0);
    assert_eq!(seen.lock().unwrap().len(), 3);
}
