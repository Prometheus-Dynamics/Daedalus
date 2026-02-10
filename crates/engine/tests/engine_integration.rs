#[cfg(feature = "gpu-mock")]
use std::sync::Arc;

#[cfg(feature = "gpu-mock")]
use daedalus_engine::GpuBackend;
use daedalus_engine::{Engine, EngineConfig};
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_registry::store::{NodeDescriptor, Port, Registry};
#[cfg(feature = "gpu-mock")]
use daedalus_runtime::RuntimeNode;
#[cfg(feature = "gpu-mock")]
use daedalus_runtime::executor::EdgePayload;
#[cfg(feature = "gpu-mock")]
use daedalus_runtime::io::NodeIo;
#[cfg(feature = "gpu-mock")]
use daedalus_runtime::state::ExecutionContext;

fn make_registry() -> Registry {
    let mut reg = Registry::new();
    let ty = daedalus_data::model::TypeExpr::Scalar(daedalus_data::model::ValueType::Int);
    let producer = NodeDescriptor {
        id: daedalus_registry::ids::NodeId::new("producer"),
        feature_flags: vec![],
        label: None,
        group: None,
        inputs: vec![],
        fanin_inputs: vec![],
        outputs: vec![Port {
            name: "out".into(),
            ty: ty.clone(),
            access: Default::default(),
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
        group: None,
        inputs: vec![Port {
            name: "in".into(),
            ty,
            access: Default::default(),
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
    reg
}

fn make_graph(compute: ComputeAffinity) -> Graph {
    let nodes = vec![
        NodeInstance {
            id: daedalus_registry::ids::NodeId::new("producer"),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec!["out".into()],
            compute,
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
            compute,
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
fn planner_is_deterministic() {
    let registry = make_registry();
    let graph = make_graph(ComputeAffinity::CpuOnly);
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let p1 = engine.plan(&registry, graph.clone()).unwrap().plan;
    let p2 = engine.plan(&registry, graph).unwrap().plan;
    assert_eq!(p1.hash, p2.hash);
    assert_eq!(p1.graph, p2.graph);
}

#[cfg(feature = "gpu-mock")]
#[test]
#[allow(clippy::field_reassign_with_default)]
fn runs_with_gpu_mock() {
    let registry = make_registry();
    let mut cfg = EngineConfig::default();
    cfg.gpu = GpuBackend::Mock;
    cfg.planner.enable_gpu = true;
    let engine = Engine::new(cfg).unwrap();
    let graph = make_graph(ComputeAffinity::GpuPreferred);
    let hits = Arc::new(std::sync::Mutex::new(0usize));
    let handler = {
        let hits = Arc::clone(&hits);
        move |node: &RuntimeNode, _ctx: &ExecutionContext, io: &mut NodeIo| {
            if node.id == "producer" {
                io.push_output(Some("out"), EdgePayload::Unit);
            } else if node.id == "consumer" {
                for _ in io.inputs_for("in") {
                    *hits.lock().unwrap() += 1;
                }
            }
            Ok(())
        }
    };
    let res = engine.run(&registry, graph, handler).unwrap();
    assert_eq!(*hits.lock().unwrap(), 1);
    assert!(
        res.telemetry.warnings.is_empty()
            || res.telemetry.warnings.iter().any(|w| w.contains("gpu")),
        "gpu telemetry should be present or empty depending on mock implementation"
    );
}
