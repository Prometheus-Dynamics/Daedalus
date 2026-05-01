use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef,
};
use daedalus_registry::ids::NodeId;
use daedalus_runtime::executor::OwnedExecutor;
use daedalus_runtime::executor::{DataLifecycleRecord, DataLifecycleStage};
use daedalus_runtime::host_bridge::HOST_BRIDGE_ID;
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;
use daedalus_runtime::{
    BackpressureStrategy, DEFAULT_STREAM_IDLE_SLEEP, ExecutionTelemetry, GraphOutput,
    HOST_BRIDGE_META_KEY, ManagedByteBuffer, MetricsLevel, NodeError, NodeHandler,
    RuntimeEdgePolicy, RuntimeNode, SchedulerConfig, StreamGraph, build_runtime,
};
use daedalus_transport::{FreshnessPolicy, Payload, PressurePolicy};

const TYPE_KEY: &str = "bench:i64";
const BURST_COUNT: i64 = 128;

#[derive(Clone)]
struct BenchHandler;

impl NodeHandler for BenchHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        if node.id.starts_with("bench.add") {
            let value = io
                .get_typed::<i64>("in")
                .ok_or_else(|| NodeError::InvalidInput(node.id.to_string()))?;
            io.push_payload("out", Payload::owned(TYPE_KEY, value + 1));
        } else if node.id == "bench.burst" {
            for value in 0..BURST_COUNT {
                io.push_payload("out", Payload::owned(TYPE_KEY, value));
            }
        } else if node.id == "bench.sink" {
            for payload in io.inputs_for("in") {
                black_box(payload.inner.type_key());
            }
        } else if node.id.starts_with("bench.state") {
            let retained = ctx
                .with_persistent_state("counter", ManagedByteBuffer::persistent_state, |buffer| {
                    buffer.reserve_exact(64);
                    buffer.capacity()
                })
                .map_err(|err| NodeError::Handler(err.to_string()))?;
            black_box(retained);
        }
        Ok(())
    }
}

fn cpu_node(id: &str, inputs: &[&str], outputs: &[&str]) -> NodeInstance {
    NodeInstance {
        id: NodeId::new(id),
        bundle: None,
        label: None,
        inputs: inputs.iter().map(|port| (*port).to_string()).collect(),
        outputs: outputs.iter().map(|port| (*port).to_string()).collect(),
        compute: ComputeAffinity::CpuOnly,
        const_inputs: Vec::new(),
        sync_groups: Vec::new(),
        metadata: BTreeMap::new(),
    }
}

fn host_bridge() -> NodeInstance {
    NodeInstance {
        id: NodeId::new(HOST_BRIDGE_ID),
        bundle: None,
        label: Some("host".to_string()),
        inputs: vec!["out".to_string()],
        outputs: vec!["in".to_string()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: Vec::new(),
        sync_groups: Vec::new(),
        metadata: BTreeMap::from([(
            HOST_BRIDGE_META_KEY.to_string(),
            daedalus_data::model::Value::Bool(true),
        )]),
    }
}

fn edge(from: usize, from_port: &str, to: usize, to_port: &str) -> Edge {
    Edge {
        from: PortRef {
            node: NodeRef(from),
            port: from_port.to_string(),
        },
        to: PortRef {
            node: NodeRef(to),
            port: to_port.to_string(),
        },
        metadata: BTreeMap::new(),
    }
}

fn runtime_from_graph(graph: Graph) -> Arc<daedalus_runtime::RuntimePlan> {
    let plan = ExecutionPlan::new(graph, Vec::new());
    Arc::new(build_runtime(&plan, &SchedulerConfig::default()))
}

fn serial_executor() -> OwnedExecutor<BenchHandler> {
    let graph = Graph {
        nodes: vec![
            cpu_node("bench.noop.0", &[], &[]),
            cpu_node("bench.noop.1", &[], &[]),
            cpu_node("bench.noop.2", &[], &[]),
        ],
        edges: Vec::new(),
        metadata: BTreeMap::new(),
    };
    OwnedExecutor::new(runtime_from_graph(graph), BenchHandler)
}

fn parallel_executor() -> OwnedExecutor<BenchHandler> {
    let graph = Graph {
        nodes: (0..8)
            .map(|idx| cpu_node(&format!("bench.noop.{idx}"), &[], &[]))
            .collect(),
        edges: Vec::new(),
        metadata: BTreeMap::new(),
    };
    OwnedExecutor::new(runtime_from_graph(graph), BenchHandler).with_pool_size(Some(4))
}

fn scoped_parallel_executor() -> OwnedExecutor<BenchHandler> {
    let graph = Graph {
        nodes: (0..8)
            .map(|idx| cpu_node(&format!("bench.noop.{idx}"), &[], &[]))
            .collect(),
        edges: Vec::new(),
        metadata: BTreeMap::new(),
    };
    OwnedExecutor::new(runtime_from_graph(graph), BenchHandler)
}

fn state_resource_executor() -> OwnedExecutor<BenchHandler> {
    let graph = Graph {
        nodes: (0..8)
            .map(|idx| cpu_node(&format!("bench.state.{idx}"), &[], &[]))
            .collect(),
        edges: Vec::new(),
        metadata: BTreeMap::new(),
    };
    OwnedExecutor::new(runtime_from_graph(graph), BenchHandler).with_pool_size(Some(4))
}

fn direct_host_executor() -> OwnedExecutor<BenchHandler> {
    let graph = Graph {
        nodes: vec![
            host_bridge(),
            cpu_node("bench.add.a", &["in"], &["out"]),
            cpu_node("bench.add.b", &["in"], &["out"]),
        ],
        edges: vec![
            edge(0, "in", 1, "in"),
            edge(1, "out", 2, "in"),
            edge(2, "out", 0, "out"),
        ],
        metadata: BTreeMap::new(),
    };
    OwnedExecutor::new(runtime_from_graph(graph), BenchHandler)
}

fn pressure_executor(
    edge_policy: RuntimeEdgePolicy,
    backpressure: BackpressureStrategy,
) -> OwnedExecutor<BenchHandler> {
    let graph = Graph {
        nodes: vec![
            cpu_node("bench.burst", &[], &["out"]),
            cpu_node("bench.sink", &["in"], &[]),
        ],
        edges: vec![edge(0, "out", 1, "in")],
        metadata: BTreeMap::new(),
    };
    let plan = ExecutionPlan::new(graph, Vec::new());
    let mut runtime = build_runtime(&plan, &SchedulerConfig::default());
    *runtime.edges[0].policy_mut() = edge_policy;
    runtime.backpressure = backpressure;
    OwnedExecutor::new(Arc::new(runtime), BenchHandler)
}

fn stream_graph() -> StreamGraph<BenchHandler> {
    let graph = Graph {
        nodes: vec![
            host_bridge(),
            cpu_node("bench.add.stream", &["in"], &["out"]),
        ],
        edges: vec![edge(0, "in", 1, "in"), edge(1, "out", 0, "out")],
        metadata: BTreeMap::new(),
    };
    StreamGraph::new(runtime_from_graph(graph), BenchHandler)
}

fn recv_stream_i64(output: &GraphOutput, timeout: Duration) -> i64 {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(payload) = output
            .recv_timeout(Duration::from_millis(1))
            .expect("stream receive")
        {
            return payload
                .try_into_owned::<i64>()
                .map_err(|_| "stream output type mismatch")
                .expect("typed stream output");
        }
        assert!(
            Instant::now() < deadline,
            "stream output payload within benchmark timeout"
        );
    }
}

fn executor_snapshot_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("executor_snapshot_overhead");

    group.bench_function(
        BenchmarkId::new("retained_serial_tick", "3_noop_nodes"),
        |b| {
            let mut exec = serial_executor();
            b.iter(|| {
                let telemetry = exec.run_in_place().expect("serial run");
                black_box(telemetry.nodes_executed);
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("retained_scoped_parallel_tick", "8_noop_segments"),
        |b| {
            let mut exec = scoped_parallel_executor();
            b.iter(|| {
                let telemetry = exec.run_parallel_in_place().expect("parallel run");
                black_box(telemetry.nodes_executed);
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("retained_worker_pool_tick", "8_noop_segments_pool_4"),
        |b| {
            let mut exec = parallel_executor();
            b.iter(|| {
                let telemetry = exec.run_parallel_in_place().expect("parallel run");
                black_box(telemetry.nodes_executed);
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("cached_direct_host_route", "2_nodes"),
        |b| {
            let mut exec = direct_host_executor();
            let route = exec
                .direct_host_route("in", "out")
                .expect("direct host route");
            let mut value = 0_i64;
            b.iter(|| {
                let (_, output) = exec
                    .run_direct_host_route(&route, Payload::owned(TYPE_KEY, black_box(value)))
                    .expect("direct host run");
                let output = output
                    .expect("direct host output")
                    .try_into_owned::<i64>()
                    .map_err(|_| "direct host output type mismatch")
                    .expect("typed output");
                value = value.wrapping_add(1);
                black_box(output);
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("parallel_state_resources", "8_nodes_pool_4"),
        |b| {
            let mut exec = state_resource_executor();
            b.iter(|| {
                let telemetry = exec.run_parallel_in_place().expect("state resource run");
                black_box(telemetry.nodes_executed);
            });
        },
    );

    for (name, policy, backpressure) in [
        (
            "fifo_buffer_all",
            RuntimeEdgePolicy::default(),
            BackpressureStrategy::None,
        ),
        (
            "latest_only",
            RuntimeEdgePolicy::latest_only(),
            BackpressureStrategy::None,
        ),
        (
            "bounded_drop_oldest_cap_8",
            RuntimeEdgePolicy::bounded(8),
            BackpressureStrategy::None,
        ),
        (
            "bounded_error_on_overflow_cap_8",
            RuntimeEdgePolicy::bounded(8),
            BackpressureStrategy::ErrorOnOverflow,
        ),
    ] {
        group.bench_function(BenchmarkId::new("edge_pressure_matrix", name), |b| {
            let mut exec = pressure_executor(policy.clone(), backpressure.clone());
            let expect_overflow_error =
                matches!(backpressure, BackpressureStrategy::ErrorOnOverflow);
            b.iter(|| match exec.run_in_place() {
                Ok(telemetry) => {
                    black_box(telemetry.backpressure_events);
                }
                Err(daedalus_runtime::ExecuteError::HandlerFailed { error, .. })
                    if expect_overflow_error && matches!(error, NodeError::BackpressureDrop(_)) =>
                {
                    black_box(1usize);
                }
                Err(error) => panic!("pressure matrix run: {error:?}"),
            });
        });
    }

    group.bench_function(BenchmarkId::new("stream_round_trip", "sync_drain"), |b| {
        let mut graph = stream_graph();
        graph.start().expect("stream start");
        let input = graph.input("in").expect("stream input");
        let output = graph.output("out").expect("stream output");
        let mut value = 0_i64;
        b.iter(|| {
            input
                .feed(Payload::owned(TYPE_KEY, black_box(value)))
                .expect("stream feed");
            graph.drain().expect("stream drain");
            let output = recv_stream_i64(&output, Duration::from_millis(10));
            value = value.wrapping_add(1);
            black_box(output);
        });
    });

    group.bench_function(
        BenchmarkId::new("stream_round_trip", "continuous_worker"),
        |b| {
            let graph = Arc::new(Mutex::new(stream_graph()));
            let (input, output) = {
                let mut guard = graph.lock().expect("stream graph lock");
                guard.start().expect("stream start");
                (
                    guard.input("in").expect("stream input"),
                    guard.output("out").expect("stream output"),
                )
            };
            let worker =
                StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_micros(50));
            let mut value = 0_i64;
            b.iter(|| {
                input
                    .feed(Payload::owned(TYPE_KEY, black_box(value)))
                    .expect("stream feed");
                let output = recv_stream_i64(&output, Duration::from_millis(100));
                value = value.wrapping_add(1);
                black_box(output);
            });
            graph
                .lock()
                .expect("stream graph lock")
                .close()
                .expect("stream close");
            worker.stop();
        },
    );

    group.bench_function(
        BenchmarkId::new("stream_worker_idle", "default_idle_sleep_start_stop"),
        |b| {
            b.iter(|| {
                let graph = Arc::new(Mutex::new(stream_graph()));
                {
                    let mut guard = graph.lock().expect("stream graph lock");
                    guard.start().expect("stream start");
                }
                let worker =
                    StreamGraph::spawn_continuous(Arc::clone(&graph), DEFAULT_STREAM_IDLE_SLEEP);
                std::thread::sleep(DEFAULT_STREAM_IDLE_SLEEP);
                graph
                    .lock()
                    .expect("stream graph lock")
                    .close()
                    .expect("stream close");
                black_box(worker.stop());
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("stream_worker_backpressure", "latest_only_burst_32"),
        |b| {
            let graph = Arc::new(Mutex::new(stream_graph()));
            let (input, output) = {
                let mut guard = graph.lock().expect("stream graph lock");
                guard.start().expect("stream start");
                (
                    guard.input("in").expect("stream input"),
                    guard.output("out").expect("stream output"),
                )
            };
            input
                .set_policy(
                    PressurePolicy::LatestOnly,
                    FreshnessPolicy::LatestBySequence,
                )
                .expect("input policy");
            output
                .set_policy(
                    PressurePolicy::LatestOnly,
                    FreshnessPolicy::LatestBySequence,
                )
                .expect("output policy");
            let worker =
                StreamGraph::spawn_continuous(Arc::clone(&graph), DEFAULT_STREAM_IDLE_SLEEP);
            let mut value = 0_i64;
            b.iter(|| {
                while output.try_recv().expect("drain stale output").is_some() {}
                for _ in 0..32 {
                    input
                        .feed(Payload::owned(TYPE_KEY, black_box(value)))
                        .expect("stream feed");
                    value = value.wrapping_add(1);
                }
                let output = recv_stream_i64(&output, Duration::from_millis(100));
                black_box(output);
            });
            graph
                .lock()
                .expect("stream graph lock")
                .close()
                .expect("stream close");
            worker.stop();
        },
    );

    group.finish();
}

fn telemetry_sample(level: MetricsLevel) -> ExecutionTelemetry {
    let mut telemetry = ExecutionTelemetry::with_level(level);
    telemetry.nodes_executed = 64;
    telemetry.cpu_segments = 16;
    telemetry.backpressure_events = 4;
    for idx in 0..64 {
        telemetry.record_node_duration(idx, Duration::from_micros(10 + idx as u64));
        telemetry.record_node_handler_duration(idx, Duration::from_micros(5 + idx as u64));
        telemetry.record_node_transport_in(idx, Some(256 + idx as u64));
        telemetry.record_node_transport_out(idx, Some(512 + idx as u64));
    }
    for idx in 0..96 {
        telemetry.record_edge_wait(idx, Duration::from_micros(2 + idx as u64));
        telemetry.record_edge_depth(idx, idx % 16);
        telemetry.record_edge_capacity(idx, Some(16));
        telemetry.record_edge_queue_bytes(idx, (idx as u64 + 1) * 128);
        telemetry.record_edge_transport(idx, Some(128 + idx as u64));
        telemetry.record_edge_drop(idx, (idx % 3) as u64);
    }
    if level.is_trace() {
        for idx in 0..64 {
            telemetry.record_trace_event(
                idx,
                Duration::from_micros(idx as u64),
                Duration::from_micros(3),
            );
        }
    }
    if level.is_profile() {
        for idx in 0..64 {
            let mut record = DataLifecycleRecord::new(idx as u64, DataLifecycleStage::EdgeEnqueued);
            record.edge_idx = Some(idx);
            record.payload = Some("Payload(bench:i64)".to_string());
            telemetry.record_data_lifecycle(record);
        }
    }
    telemetry
}

fn telemetry_clone_costs(c: &mut Criterion) {
    let mut group = c.benchmark_group("telemetry_clone_costs");
    black_box(std::mem::size_of::<ExecutionTelemetry>());
    for level in [
        MetricsLevel::Basic,
        MetricsLevel::Detailed,
        MetricsLevel::Profile,
        MetricsLevel::Trace,
    ] {
        group.bench_function(BenchmarkId::new("clone", format!("{level:?}")), |b| {
            let telemetry = telemetry_sample(level);
            b.iter(|| black_box(telemetry.clone()));
        });
        group.bench_function(BenchmarkId::new("report", format!("{level:?}")), |b| {
            let telemetry = telemetry_sample(level);
            b.iter(|| black_box(telemetry.report()));
        });
    }
    group.finish();
}

criterion_group!(benches, executor_snapshot_overhead, telemetry_clone_costs);
criterion_main!(benches);
