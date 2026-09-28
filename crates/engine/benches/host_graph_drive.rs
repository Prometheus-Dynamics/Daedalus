//! Host bridge and `HostGraph` drive-path benchmarks.
//!
//! Run with `cargo bench -p daedalus-engine --features plugins --bench host_graph_drive`.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_engine::{Engine, EngineConfig, HostGraph};
use daedalus_planner::{Edge, Graph, NodeInstance};
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_runtime::RuntimeNode;
use daedalus_runtime::executor::{MetricsLevel, NodeError, NodeHandler};
use daedalus_runtime::handles::PortId;
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_transport::Payload;

const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";
const FRAME_KEY: &str = "bench:frame";
const FRAME_BYTES: usize = 1 << 20;
const BURST: i64 = 100;

struct IncrementHandler;

impl NodeHandler for IncrementHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "inc" {
            let Some(Value::Int(value)) = io.get_typed_ref::<Value>("in") else {
                return Err(NodeError::InvalidInput("expected int".to_string()));
            };
            let next = Value::Int(value + 1);
            io.push_payload("out", Payload::owned(INT_KEY, next));
        }
        Ok(())
    }
}

fn node(id: &str, label: &str, inputs: &[&str], outputs: &[&str]) -> NodeInstance {
    NodeInstance::new(id)
        .with_label(label)
        .with_inputs(inputs.iter().copied())
        .with_outputs(outputs.iter().copied())
}

/// `host.in -> inc -> host.out`, one compute node.
fn compile_graph(config: EngineConfig) -> (PluginRegistry, HostGraph<IncrementHandler>) {
    let int_ty = TypeExpr::Scalar(ValueType::Int);
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new(HOST_BRIDGE_ID)
                .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .input(PortDecl::new("out", INT_KEY).schema(int_ty.clone()))
                .output(PortDecl::new("in", INT_KEY).schema(int_ty.clone())),
        )
        .expect("register host decl");
    plugins
        .register_node_decl(
            NodeDecl::new("inc")
                .input(PortDecl::new("in", INT_KEY).schema(int_ty.clone()))
                .output(PortDecl::new("out", INT_KEY).schema(int_ty)),
        )
        .expect("register inc decl");

    let mut host = node(HOST_BRIDGE_ID, "host", &["out"], &["in"]);
    host.metadata
        .insert(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true));
    let graph = Graph {
        nodes: vec![host, node("inc", "adder", &["in"], &["out"])],
        edges: vec![Edge::new(0, "in", 1, "in"), Edge::new(1, "out", 0, "out")],
        metadata: Default::default(),
    };

    let mut host_graph = Engine::new(config)
        .expect("engine")
        .compile_host_graph_plugin_registry(
            &plugins,
            graph,
            IncrementHandler,
            HostBridgeManager::new(),
            "host",
        )
        .expect("compile host graph");
    host_graph.prepare().expect("prepare");
    (plugins, host_graph)
}

fn bench_bridge_push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("host_bridge_push_pop");
    group.throughput(Throughput::Elements(1));
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    let mut inbound = Vec::new();
    handle.set_event_recording(false);
    let port = PortId::from("in");
    let out_port = PortId::from("out");
    let frame: Arc<Vec<u8>> = Arc::new(vec![7u8; FRAME_BYTES]);

    // Host -> graph: feed an inbound payload, then drain it as the bridge node does.
    group.bench_function(BenchmarkId::new("inbound", "small"), |b| {
        b.iter(|| {
            black_box(handle.feed_payload(port.clone(), Payload::owned(INT_KEY, 1i64)));
            manager.take_inbound_into("host", &mut inbound);
            black_box(&inbound);
            inbound.clear();
        })
    });
    group.bench_function(BenchmarkId::new("inbound", "arc_1mib"), |b| {
        b.iter(|| {
            black_box(handle.feed_payload(port.clone(), Payload::shared(FRAME_KEY, frame.clone())));
            manager.take_inbound_into("host", &mut inbound);
            black_box(&inbound);
            inbound.clear();
        })
    });

    // Graph -> host: enqueue an outbound payload, then pop it from the host side.
    group.bench_function(BenchmarkId::new("outbound", "small"), |b| {
        b.iter(|| {
            manager.push_outbound("host", "out", Payload::owned(INT_KEY, 1i64));
            black_box(handle.try_pop_payload(&out_port));
        })
    });
    group.bench_function(BenchmarkId::new("outbound", "arc_1mib"), |b| {
        b.iter(|| {
            manager.push_outbound("host", "out", Payload::shared(FRAME_KEY, frame.clone()));
            black_box(handle.try_pop_payload(&out_port));
        })
    });
    group.finish();
}

fn bench_graph_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("host_graph_round_trip");
    group.throughput(Throughput::Elements(1));
    let port = PortId::from("in");
    let out_port = PortId::from("out");
    for (name, level) in [
        ("push_tick_take", MetricsLevel::default()),
        ("push_tick_take_metrics_off", MetricsLevel::Off),
    ] {
        let (_plugins, mut graph) =
            compile_graph(EngineConfig::default().with_metrics_level(level));
        graph.host().set_event_recording(false);
        group.bench_function(name, |b| {
            b.iter(|| {
                black_box(graph.push_payload(port.clone(), Payload::owned(INT_KEY, Value::Int(1))));
                black_box(graph.tick().expect("tick"));
                black_box(graph.take_payload(&out_port));
            })
        });
    }
    group.finish();
}

fn bench_latest_only_burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("host_graph_latest_only");
    group.throughput(Throughput::Elements(BURST as u64));
    let (_plugins, mut graph) = compile_graph(EngineConfig::default());
    graph.host().set_event_recording(false);
    graph.set_latest_input("in").expect("latest input policy");
    let port = PortId::from("in");
    let out_port = PortId::from("out");
    group.bench_function("burst_100_then_tick", |b| {
        b.iter(|| {
            for value in 0..BURST {
                black_box(
                    graph.push_payload(port.clone(), Payload::owned(INT_KEY, Value::Int(value))),
                );
            }
            black_box(graph.tick().expect("tick"));
            black_box(graph.take_payload(&out_port));
        })
    });
    group.finish();
}

fn bench_event_recording(c: &mut Criterion) {
    let mut group = c.benchmark_group("host_bridge_events");
    group.throughput(Throughput::Elements(1));
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    let mut inbound = Vec::new();
    let port = PortId::from("in");
    for enabled in [false, true] {
        handle.set_event_recording(enabled);
        let label = if enabled { "on" } else { "off" };
        group.bench_function(BenchmarkId::new("push", label), |b| {
            b.iter(|| {
                black_box(handle.feed_payload(port.clone(), Payload::owned(INT_KEY, 1i64)));
                manager.take_inbound_into("host", &mut inbound);
                black_box(&inbound);
                inbound.clear();
            })
        });
    }
    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
}

criterion_group! {
    name = benches;
    config = config();
    targets = bench_bridge_push_pop, bench_graph_round_trip, bench_latest_only_burst,
        bench_event_recording
}
criterion_main!(benches);
