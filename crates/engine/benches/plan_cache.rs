use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_engine::{Engine, EngineConfig};
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_registry::store::{NodeDescriptor, Port, Registry};

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

fn bench_plan_cache(c: &mut Criterion) {
    let registry = sample_registry();
    let graph = sample_graph();

    c.bench_function("engine_prepare_plan_cold", |b| {
        b.iter(|| {
            let engine = Engine::new(EngineConfig::default()).unwrap();
            let prepared = engine
                .prepare_plan(&registry, black_box(graph.clone()))
                .unwrap();
            black_box(prepared.cache_status());
        })
    });

    c.bench_function("engine_prepare_plan_warm", |b| {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        let _ = engine.prepare_plan(&registry, graph.clone()).unwrap();
        b.iter(|| {
            let prepared = engine
                .prepare_plan(&registry, black_box(graph.clone()))
                .unwrap();
            black_box(prepared.cache_status());
        })
    });

    c.bench_function("engine_build_runtime_plan_warm", |b| {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        let prepared = engine.prepare_plan(&registry, graph.clone()).unwrap();
        let _ = prepared.build().unwrap();
        b.iter(|| {
            let prepared = engine
                .prepare_plan(&registry, black_box(graph.clone()))
                .unwrap();
            let runtime = prepared.build().unwrap();
            black_box(runtime.cache_status());
        })
    });
}

criterion_group!(benches, bench_plan_cache);
criterion_main!(benches);
