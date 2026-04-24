use std::sync::{Arc, Mutex};

use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_engine::{CacheStatus, Engine, EngineConfig};
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_registry::store::{NodeDescriptor, Port, Registry};
use daedalus_runtime::RuntimeNode;
use daedalus_runtime::executor::RuntimeValue;
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;

fn sample_registry() -> Registry {
    let mut reg = Registry::new();
    let ty = TypeExpr::Scalar(ValueType::Int);
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

fn sample_graph() -> Graph {
    Graph {
        nodes: vec![
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
        ],
        edges: vec![Edge {
            from: PortRef {
                node: NodeRef(0),
                port: "out".into(),
            },
            to: PortRef {
                node: NodeRef(1),
                port: "in".into(),
            },
            metadata: Default::default(),
        }],
        metadata: Default::default(),
    }
}

fn branch_registry() -> Registry {
    let mut reg = Registry::new();
    let ty = TypeExpr::Scalar(ValueType::Int);
    for (id, inputs, outputs) in [
        ("source", vec![], vec!["left", "right"]),
        ("left_mid", vec!["in"], vec!["out"]),
        ("left_sink", vec!["in"], vec![]),
        ("right_mid", vec!["in"], vec!["out"]),
        ("right_sink", vec!["in"], vec![]),
    ] {
        reg.register_node(NodeDescriptor {
            id: daedalus_registry::ids::NodeId::new(id),
            feature_flags: vec![],
            label: None,
            group: None,
            inputs: inputs
                .into_iter()
                .map(|name| Port {
                    name: name.into(),
                    ty: ty.clone(),
                    access: Default::default(),
                    source: None,
                    const_value: None,
                })
                .collect(),
            fanin_inputs: vec![],
            outputs: outputs
                .into_iter()
                .map(|name| Port {
                    name: name.into(),
                    ty: ty.clone(),
                    access: Default::default(),
                    source: None,
                    const_value: None,
                })
                .collect(),
            default_compute: ComputeAffinity::CpuOnly,
            sync_groups: Vec::new(),
            metadata: Default::default(),
        })
        .unwrap();
    }
    reg
}

fn branch_graph() -> Graph {
    Graph {
        nodes: vec![
            NodeInstance {
                id: daedalus_registry::ids::NodeId::new("source"),
                bundle: None,
                label: None,
                inputs: vec![],
                outputs: vec!["left".into(), "right".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
            NodeInstance {
                id: daedalus_registry::ids::NodeId::new("left_mid"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec!["out".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
            NodeInstance {
                id: daedalus_registry::ids::NodeId::new("left_sink"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec![],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
            NodeInstance {
                id: daedalus_registry::ids::NodeId::new("right_mid"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec!["out".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
            NodeInstance {
                id: daedalus_registry::ids::NodeId::new("right_sink"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec![],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
        ],
        edges: vec![
            Edge {
                from: PortRef {
                    node: NodeRef(0),
                    port: "left".into(),
                },
                to: PortRef {
                    node: NodeRef(1),
                    port: "in".into(),
                },
                metadata: Default::default(),
            },
            Edge {
                from: PortRef {
                    node: NodeRef(1),
                    port: "out".into(),
                },
                to: PortRef {
                    node: NodeRef(2),
                    port: "in".into(),
                },
                metadata: Default::default(),
            },
            Edge {
                from: PortRef {
                    node: NodeRef(0),
                    port: "right".into(),
                },
                to: PortRef {
                    node: NodeRef(3),
                    port: "in".into(),
                },
                metadata: Default::default(),
            },
            Edge {
                from: PortRef {
                    node: NodeRef(3),
                    port: "out".into(),
                },
                to: PortRef {
                    node: NodeRef(4),
                    port: "in".into(),
                },
                metadata: Default::default(),
            },
        ],
        metadata: Default::default(),
    }
}

#[test]
fn prepared_plan_reports_cache_hits() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let registry = sample_registry();
    let graph = sample_graph();

    let first = engine.prepare_plan(&registry, graph.clone()).unwrap();
    assert_eq!(first.cache_status(), CacheStatus::Miss);

    let second = engine.prepare_plan(&registry, graph).unwrap();
    assert_eq!(second.cache_status(), CacheStatus::Hit);
}

#[test]
fn runtime_build_reports_cache_hits() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let registry = sample_registry();
    let graph = sample_graph();

    let first_plan = engine.prepare_plan(&registry, graph.clone()).unwrap();
    let first_runtime = first_plan.build().unwrap();
    assert_eq!(first_runtime.cache_status(), CacheStatus::Miss);

    let second_plan = engine.prepare_plan(&registry, graph).unwrap();
    let second_runtime = second_plan.build().unwrap();
    assert_eq!(second_runtime.cache_status(), CacheStatus::Hit);
}

#[test]
fn planner_cache_invalidates_on_registry_change() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let registry = sample_registry();
    let graph = sample_graph();
    let _ = engine.prepare_plan(&registry, graph.clone()).unwrap();

    let mut changed_registry = sample_registry();
    changed_registry
        .register_node(NodeDescriptor {
            id: daedalus_registry::ids::NodeId::new("extra"),
            feature_flags: vec![],
            label: None,
            group: None,
            inputs: vec![],
            fanin_inputs: vec![],
            outputs: vec![],
            default_compute: ComputeAffinity::CpuOnly,
            sync_groups: Vec::new(),
            metadata: Default::default(),
        })
        .unwrap();

    let prepared = engine.prepare_plan(&changed_registry, graph).unwrap();
    assert_eq!(prepared.cache_status(), CacheStatus::Miss);
}

#[test]
fn runtime_cache_invalidates_on_runtime_shape_change() {
    let engine_a = Engine::new(EngineConfig::default()).unwrap();
    let mut config_b = EngineConfig::default();
    config_b.runtime.lockfree_queues = true;
    let engine_b = match Engine::new(config_b) {
        Ok(engine) => engine,
        Err(_) => return,
    };

    let registry = sample_registry();
    let graph = sample_graph();

    let prepared_a = engine_a.prepare_plan(&registry, graph.clone()).unwrap();
    let runtime_a = prepared_a.build().unwrap();
    assert_eq!(runtime_a.cache_status(), CacheStatus::Miss);

    let prepared_b = engine_b.prepare_plan(&registry, graph).unwrap();
    let runtime_b = prepared_b.build().unwrap();
    assert_eq!(runtime_b.cache_status(), CacheStatus::Miss);
}

#[test]
fn cache_metrics_track_hits_misses_and_invalidation() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let registry = sample_registry();
    let graph = sample_graph();

    let first = engine.prepare_plan(&registry, graph.clone()).unwrap();
    assert_eq!(first.cache_status(), CacheStatus::Miss);
    let first_runtime = first.build().unwrap();
    assert_eq!(first_runtime.cache_status(), CacheStatus::Miss);

    let second = engine.prepare_plan(&registry, graph).unwrap();
    assert_eq!(second.cache_status(), CacheStatus::Hit);
    let second_runtime = second.build().unwrap();
    assert_eq!(second_runtime.cache_status(), CacheStatus::Hit);

    let metrics = engine.cache_metrics();
    assert_eq!(metrics.planner.hits, 1);
    assert_eq!(metrics.planner.misses, 1);
    assert_eq!(metrics.runtime_plan.hits, 1);
    assert_eq!(metrics.runtime_plan.misses, 1);
    assert_eq!(metrics.planner.invalidations, 0);
    assert_eq!(metrics.runtime_plan.invalidations, 0);

    let cleared = engine.clear_caches();
    assert_eq!(cleared.planner.invalidations, 1);
    assert_eq!(cleared.runtime_plan.invalidations, 1);
}

#[test]
fn patch_run_scopes_execution_to_affected_branch() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let registry = branch_registry();
    let graph = branch_graph();
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let patch = daedalus_planner::GraphPatch {
        version: 1,
        ops: vec![daedalus_planner::GraphPatchOp::SetNodeConst {
            node: daedalus_planner::GraphNodeSelector {
                id: Some("left_mid".into()),
                ..Default::default()
            },
            port: "threshold".into(),
            value: Some(daedalus_data::model::Value::Int(5)),
        }],
    };

    let handler = {
        let seen = Arc::clone(&seen);
        move |node: &RuntimeNode, _ctx: &ExecutionContext, io: &mut NodeIo| {
            seen.lock().unwrap().push(node.id.clone());
            match node.id.as_str() {
                "source" => {
                    io.push_output(Some("left"), RuntimeValue::Unit);
                    io.push_output(Some("right"), RuntimeValue::Unit);
                }
                "left_mid" | "right_mid" => {
                    let count = io.inputs_for("in").count();
                    for _ in 0..count {
                        io.push_output(Some("out"), RuntimeValue::Unit);
                    }
                }
                "left_sink" | "right_sink" => for _ in io.inputs_for("in") {},
                _ => {}
            }
            Ok(())
        }
    };

    let _ = engine
        .run_with_patch(&registry, &graph, &patch, handler)
        .unwrap();
    let seen = seen.lock().unwrap().clone();
    assert!(seen.iter().any(|id| id == "source"));
    assert!(seen.iter().any(|id| id == "left_mid"));
    assert!(seen.iter().any(|id| id == "left_sink"));
    assert!(!seen.iter().any(|id| id == "right_mid"));
    assert!(!seen.iter().any(|id| id == "right_sink"));
}
