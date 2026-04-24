use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
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

fn bench_warm_engine_lifecycle(c: &mut Criterion) {
    let registry = sample_registry();
    let graph = sample_graph();

    let mut group = c.benchmark_group("engine_prepare_build");

    group.bench_function(BenchmarkId::new("cold", "new_engine_per_iteration"), |b| {
        b.iter(|| {
            let engine = Engine::new(EngineConfig::default()).unwrap();
            let prepared = engine
                .prepare_plan(&registry, black_box(graph.clone()))
                .unwrap();
            let built = prepared.build().unwrap();
            black_box((prepared.cache_status(), built.cache_status()));
        })
    });

    group.bench_function(
        BenchmarkId::new("warm", "reused_engine_cached_graph"),
        |b| {
            let engine = Engine::new(EngineConfig::default()).unwrap();
            let prepared = engine.prepare_plan(&registry, graph.clone()).unwrap();
            let built = prepared.build().unwrap();
            black_box((prepared.cache_status(), built.cache_status()));

            b.iter(|| {
                let prepared = engine
                    .prepare_plan(&registry, black_box(graph.clone()))
                    .unwrap();
                let built = prepared.build().unwrap();
                black_box((prepared.cache_status(), built.cache_status()));
            })
        },
    );

    group.bench_function(
        BenchmarkId::new("warm", "reused_engine_cleared_each_iteration"),
        |b| {
            let engine = Engine::new(EngineConfig::default()).unwrap();
            b.iter(|| {
                black_box(engine.clear_caches());
                let prepared = engine
                    .prepare_plan(&registry, black_box(graph.clone()))
                    .unwrap();
                let built = prepared.build().unwrap();
                black_box((prepared.cache_status(), built.cache_status()));
            })
        },
    );

    group.finish();
}

fn bench_repeated_warm_path(c: &mut Criterion) {
    let registry = sample_registry();
    let graph = sample_graph();
    let engine = Engine::new(EngineConfig::default()).unwrap();

    c.bench_function("engine_repeated_prepare_build_warm_path", |b| {
        let prepared = engine.prepare_plan(&registry, graph.clone()).unwrap();
        let built = prepared.build().unwrap();
        black_box((prepared.cache_status(), built.cache_status()));

        b.iter(|| {
            let prepared = engine
                .prepare_plan(&registry, black_box(graph.clone()))
                .unwrap();
            let built = prepared.build().unwrap();
            let metrics = engine.cache_metrics();
            black_box((prepared.cache_status(), built.cache_status(), metrics));
        })
    });
}

criterion_group!(
    benches,
    bench_warm_engine_lifecycle,
    bench_repeated_warm_path
);
criterion_main!(benches);
