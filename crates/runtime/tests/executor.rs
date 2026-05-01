use daedalus_data::model::Value;
use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, GraphNodeSelector, GraphPatch, GraphPatchOp,
    NodeInstance, NodeRef, PortRef,
};
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::{
    BackpressureStrategy, ExecuteError, Executor, NodeHandler, ResourceLifecycleEvent,
    RuntimeEdgePolicy, RuntimeNode, SchedulerConfig, StateStore, build_runtime,
    executor::{NodeError, OwnedExecutor},
};
use daedalus_transport::Payload;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

struct LogHandler {
    log: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl NodeHandler for LogHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        self.log.lock().unwrap().push(node.id.clone());
        Ok(())
    }
}

struct ConstLogHandler {
    log: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for ConstLogHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        let mode = io
            .inputs_for("mode")
            .find_map(|payload| payload.inner.get_ref::<Value>().cloned())
            .unwrap_or(Value::String("missing".into()));
        self.log
            .lock()
            .unwrap()
            .push(format!("{}:{mode:?}", node.id));
        Ok(())
    }
}

struct FailOnHandler {
    fail_node: &'static str,
}

impl NodeHandler for FailOnHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == self.fail_node {
            return Err(NodeError::InvalidInput("parity failure".into()));
        }
        Ok(())
    }
}

struct EchoHandler;

impl NodeHandler for EchoHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "echo" {
            let inputs: Vec<_> = io
                .inputs_for("in")
                .map(|payload| payload.inner.clone())
                .collect();
            for payload in inputs {
                io.push_payload("out", payload);
            }
        }
        Ok(())
    }
}

#[cfg(feature = "metrics")]
struct CustomMetricsHandler;

#[cfg(feature = "metrics")]
impl NodeHandler for CustomMetricsHandler {
    fn run(
        &self,
        _node: &RuntimeNode,
        ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        ctx.increment_metric("detections", 2)
            .map_err(|err| NodeError::Handler(err.to_string()))?;
        ctx.increment_metric("detections", 3)
            .map_err(|err| NodeError::Handler(err.to_string()))?;
        ctx.gauge_metric("confidence", 0.875)
            .map_err(|err| NodeError::Handler(err.to_string()))?;
        ctx.duration_metric("model_time", std::time::Duration::from_millis(7))
            .map_err(|err| NodeError::Handler(err.to_string()))?;
        ctx.bytes_metric("scratch_bytes", 4096)
            .map_err(|err| NodeError::Handler(err.to_string()))?;
        ctx.text_metric("model", "yolo-lite")
            .map_err(|err| NodeError::Handler(err.to_string()))?;
        ctx.bool_metric("saturated", false)
            .map_err(|err| NodeError::Handler(err.to_string()))?;
        ctx.json_metric(
            "classes",
            serde_json::json!({
                "person": 3,
                "car": 2,
            }),
        )
        .map_err(|err| NodeError::Handler(err.to_string()))?;
        Ok(())
    }
}

fn tiny_exec_plan(compute: &[ComputeAffinity]) -> ExecutionPlan {
    let mut graph = Graph::default();
    for (idx, c) in compute.iter().enumerate() {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(format!("n{idx}")),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: *c,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    for i in 0..compute.len().saturating_sub(1) {
        graph.edges.push(Edge {
            from: PortRef {
                node: NodeRef(i),
                port: "out".into(),
            },
            to: PortRef {
                node: NodeRef(i + 1),
                port: "in".into(),
            },
            metadata: Default::default(),
        });
    }
    ExecutionPlan::new(graph, vec![])
}

fn independent_exec_plan(count: usize) -> ExecutionPlan {
    let mut graph = Graph::default();
    for idx in 0..count {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(format!("n{idx}")),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    ExecutionPlan::new(graph, vec![])
}

fn const_exec_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    for (idx, mode) in ["cold", "warm"].into_iter().enumerate() {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(format!("n{idx}")),
            bundle: None,
            label: None,
            inputs: vec!["mode".into()],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![("mode".into(), Value::String(mode.into()))],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    ExecutionPlan::new(graph, vec![])
}

fn host_echo_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new(HOST_BRIDGE_ID),
        bundle: None,
        label: Some("host".into()),
        inputs: vec!["out".into()],
        outputs: vec!["in".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: BTreeMap::from([
            (HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true)),
            (
                "dynamic_inputs".to_string(),
                Value::String("generic".into()),
            ),
            (
                "dynamic_outputs".to_string(),
                Value::String("generic".into()),
            ),
        ]),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("echo"),
        bundle: None,
        label: None,
        inputs: vec!["in".into()],
        outputs: vec!["out".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: BTreeMap::new(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "in".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: BTreeMap::new(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(1),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(0),
            port: "out".into(),
        },
        metadata: BTreeMap::new(),
    });
    ExecutionPlan::new(graph, vec![])
}

fn assert_handler_failed_invalid_input(err: ExecuteError, node: &str) {
    match err {
        ExecuteError::HandlerFailed {
            node: actual,
            error,
        } => {
            assert_eq!(actual, node);
            assert_eq!(error, NodeError::InvalidInput("parity failure".into()));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn cpu_only_executes_in_order() {
    let exec = tiny_exec_plan(&[ComputeAffinity::CpuOnly, ComputeAffinity::CpuOnly]);
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    );
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = LogHandler { log: log.clone() };
    let telemetry = Executor::new(&rt, handler).run().expect("exec ok");
    assert_eq!(
        log.lock().unwrap().clone(),
        vec!["n0".to_string(), "n1".to_string()]
    );
    assert_eq!(telemetry.nodes_executed, 2);
    assert_eq!(telemetry.cpu_segments, 2);
}

#[test]
fn gpu_preferred_falls_back_without_handle() {
    let exec = tiny_exec_plan(&[ComputeAffinity::GpuPreferred]);
    let rt = build_runtime(&exec, &SchedulerConfig::default());
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = LogHandler { log };
    let telemetry = Executor::new(&rt, handler).run().expect("exec ok");
    assert_eq!(telemetry.gpu_fallbacks, 1);
    assert!(
        telemetry
            .warnings
            .iter()
            .any(|w| w.contains("gpu_preferred"))
    );
}

#[test]
fn gpu_required_errors_without_handle() {
    let exec = tiny_exec_plan(&[ComputeAffinity::GpuRequired]);
    let rt = build_runtime(&exec, &SchedulerConfig::default());
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = LogHandler { log };
    let err = Executor::new(&rt, handler).run().unwrap_err();
    match err {
        daedalus_runtime::ExecuteError::GpuUnavailable { segment } => {
            assert_eq!(segment, vec![NodeRef(0)]);
        }
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn borrowed_and_owned_executors_match_basic_in_place_runs() {
    let exec = tiny_exec_plan(&[
        ComputeAffinity::CpuOnly,
        ComputeAffinity::CpuOnly,
        ComputeAffinity::CpuOnly,
    ]);
    let rt = build_runtime(&exec, &SchedulerConfig::default());

    let borrowed_log = Arc::new(Mutex::new(Vec::new()));
    let mut borrowed = Executor::new(
        &rt,
        LogHandler {
            log: borrowed_log.clone(),
        },
    );
    let borrowed_telemetry = borrowed.run_in_place().expect("borrowed run");

    let owned_log = Arc::new(Mutex::new(Vec::new()));
    let mut owned = OwnedExecutor::new(
        Arc::new(rt.clone()),
        LogHandler {
            log: owned_log.clone(),
        },
    );
    let owned_telemetry = owned.run_in_place().expect("owned run");

    assert_eq!(
        borrowed_log.lock().unwrap().clone(),
        owned_log.lock().unwrap().clone()
    );
    assert_eq!(borrowed_telemetry.nodes_executed, 3);
    assert_eq!(owned_telemetry.nodes_executed, 3);
    assert_eq!(
        borrowed_telemetry.cpu_segments,
        owned_telemetry.cpu_segments
    );
}

#[test]
fn borrowed_and_owned_executors_match_parallel_in_place_runs() {
    let exec = independent_exec_plan(8);
    let rt = build_runtime(&exec, &SchedulerConfig::default());

    let borrowed_log = Arc::new(Mutex::new(Vec::new()));
    let mut borrowed = Executor::new(
        &rt,
        LogHandler {
            log: borrowed_log.clone(),
        },
    )
    .with_pool_size(Some(2));
    let borrowed_telemetry = borrowed.run_parallel_in_place().expect("borrowed run");

    let owned_log = Arc::new(Mutex::new(Vec::new()));
    let mut owned = OwnedExecutor::new(
        Arc::new(rt.clone()),
        LogHandler {
            log: owned_log.clone(),
        },
    )
    .with_pool_size(Some(2));
    let owned_telemetry = owned.run_parallel_in_place().expect("owned run");

    let mut borrowed_nodes = borrowed_log.lock().unwrap().clone();
    let mut owned_nodes = owned_log.lock().unwrap().clone();
    borrowed_nodes.sort();
    owned_nodes.sort();
    assert_eq!(borrowed_nodes, owned_nodes);
    assert_eq!(
        borrowed_telemetry.nodes_executed,
        owned_telemetry.nodes_executed
    );
}

#[test]
fn borrowed_and_owned_executors_match_patch_application() {
    let exec = const_exec_plan();
    let rt = build_runtime(&exec, &SchedulerConfig::default());
    let patch = GraphPatch {
        version: 1,
        ops: vec![GraphPatchOp::SetNodeConst {
            node: GraphNodeSelector {
                index: Some(1),
                ..GraphNodeSelector::default()
            },
            port: "mode".into(),
            value: Some(Value::String("patched".into())),
        }],
    };

    let borrowed_log = Arc::new(Mutex::new(Vec::new()));
    let mut borrowed = Executor::new(
        &rt,
        ConstLogHandler {
            log: borrowed_log.clone(),
        },
    );
    let borrowed_report = borrowed.apply_patch(&patch);
    borrowed.run_in_place().expect("borrowed run");

    let owned_log = Arc::new(Mutex::new(Vec::new()));
    let mut owned = OwnedExecutor::new(
        Arc::new(rt.clone()),
        ConstLogHandler {
            log: owned_log.clone(),
        },
    );
    let owned_report = owned.apply_patch(&patch);
    owned.run_in_place().expect("owned run");

    assert_eq!(borrowed_report.applied_ops, owned_report.applied_ops);
    assert_eq!(borrowed_report.skipped_ops, owned_report.skipped_ops);
    assert_eq!(borrowed_report.matched_nodes, owned_report.matched_nodes);
    assert_eq!(
        borrowed_log.lock().unwrap().clone(),
        owned_log.lock().unwrap().clone()
    );
}

#[test]
fn borrowed_and_owned_executors_match_handler_errors() {
    let exec = tiny_exec_plan(&[
        ComputeAffinity::CpuOnly,
        ComputeAffinity::CpuOnly,
        ComputeAffinity::CpuOnly,
    ]);
    let rt = build_runtime(&exec, &SchedulerConfig::default());

    let mut borrowed = Executor::new(&rt, FailOnHandler { fail_node: "n1" });
    let borrowed_err = borrowed.run_in_place().expect_err("borrowed should fail");

    let mut owned = OwnedExecutor::new(Arc::new(rt.clone()), FailOnHandler { fail_node: "n1" });
    let owned_err = owned.run_in_place().expect_err("owned should fail");

    assert_handler_failed_invalid_input(borrowed_err, "n1");
    assert_handler_failed_invalid_input(owned_err, "n1");
}

#[test]
fn borrowed_host_bridge_run_matches_owned_direct_host_route() {
    let exec = host_echo_plan();
    let rt = build_runtime(&exec, &SchedulerConfig::default());

    let borrowed_bridges = HostBridgeManager::new();
    borrowed_bridges.populate_from_plan(&rt);
    let borrowed_host = borrowed_bridges.ensure_handle("host");
    borrowed_host.push_payload("in", Payload::owned("demo:u32", 7_u32));
    let mut borrowed = Executor::new(&rt, EchoHandler).with_host_bridges(borrowed_bridges.clone());
    let borrowed_telemetry = borrowed.run_in_place().expect("borrowed run");
    let borrowed_output = borrowed_host
        .try_pop_payload("out")
        .expect("borrowed output payload");

    let mut owned = OwnedExecutor::new(Arc::new(rt.clone()), EchoHandler);
    let route = owned
        .direct_host_route("in", "out")
        .expect("owned direct host route");
    let (owned_telemetry, owned_output) = owned
        .run_direct_host_route(&route, Payload::owned("demo:u32", 7_u32))
        .expect("owned route run");
    let owned_output = owned_output.expect("owned output payload");

    assert_eq!(
        borrowed_output.get_ref::<u32>(),
        owned_output.get_ref::<u32>()
    );
    assert_eq!(
        borrowed_telemetry.nodes_executed,
        owned_telemetry.nodes_executed
    );
}

#[test]
fn execution_context_contains_node_metadata() {
    let mut graph = Graph::default();
    let mut metadata = BTreeMap::new();
    metadata.insert("pos".into(), Value::Int(7));
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n0"),
        bundle: Some("bundle".into()),
        label: Some("alias".into()),
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata,
    });
    let plan = ExecutionPlan::new(graph, vec![]);
    let rt = build_runtime(&plan, &SchedulerConfig::default());
    let seen = Arc::new(Mutex::new(None));
    let handler = {
        let seen = seen.clone();
        move |_node: &RuntimeNode,
              ctx: &daedalus_runtime::state::ExecutionContext,
              _io: &mut daedalus_runtime::io::NodeIo| {
            seen.lock().unwrap().replace(ctx.metadata.clone());
            Ok(())
        }
    };
    let telemetry = Executor::new(&rt, handler).run().expect("exec ok");
    assert_eq!(telemetry.nodes_executed, 1);
    let captured = seen.lock().unwrap().clone().expect("metadata captured");
    assert_eq!(captured.get("pos"), Some(&Value::Int(7)));
    assert_eq!(captured.get("label"), Some(&Value::String("alias".into())));
    assert_eq!(
        captured.get("bundle"),
        Some(&Value::String("bundle".into()))
    );
}

#[test]
fn executor_resource_lifecycle_controls_shared_state() {
    let exec = tiny_exec_plan(&[ComputeAffinity::CpuOnly]);
    let rt = build_runtime(&exec, &SchedulerConfig::default());
    let state = StateStore::default();
    state
        .record_node_resource_usage(
            "n0",
            "cache",
            daedalus_runtime::ResourceClass::WarmCache,
            8,
            32,
        )
        .unwrap();
    let handler = LogHandler {
        log: Arc::new(Mutex::new(Vec::new())),
    };
    let executor = Executor::new(&rt, handler).with_state(state.clone());

    executor.on_memory_pressure().unwrap();
    let compacted = state.snapshot_node_resources("n0").unwrap();
    assert_eq!(compacted.warm_cache.live_bytes, 8);
    assert_eq!(compacted.warm_cache.retained_bytes, 8);

    executor
        .apply_resource_lifecycle(ResourceLifecycleEvent::Idle)
        .unwrap();
    let idled = state.snapshot_node_resources("n0").unwrap();
    assert_eq!(idled.warm_cache.live_bytes, 0);
    assert_eq!(idled.warm_cache.retained_bytes, 0);

    state
        .record_node_resource_usage(
            "n0",
            "persistent",
            daedalus_runtime::ResourceClass::PersistentState,
            3,
            6,
        )
        .unwrap();
    executor.shutdown_resources().unwrap();
    assert_eq!(
        state.snapshot_node_resources("n0").unwrap(),
        daedalus_runtime::NodeResourceSnapshot::default()
    );
}

#[test]
#[cfg(feature = "metrics")]
fn node_can_publish_custom_metrics_into_telemetry() {
    let exec = tiny_exec_plan(&[ComputeAffinity::CpuOnly]);
    let rt = build_runtime(&exec, &SchedulerConfig::default());
    let telemetry = Executor::new(&rt, CustomMetricsHandler)
        .with_metrics_level(daedalus_runtime::MetricsLevel::Detailed)
        .run()
        .expect("exec ok");
    let node = telemetry.node_metrics.get(&0).expect("node metrics");

    assert_eq!(
        node.custom.get("detections"),
        Some(&daedalus_runtime::CustomMetricValue::Counter(5))
    );
    assert_eq!(
        node.custom.get("confidence"),
        Some(&daedalus_runtime::CustomMetricValue::Gauge(0.875))
    );
    assert_eq!(
        node.custom.get("model_time"),
        Some(&daedalus_runtime::CustomMetricValue::Duration(
            std::time::Duration::from_millis(7)
        ))
    );
    assert_eq!(
        node.custom.get("scratch_bytes"),
        Some(&daedalus_runtime::CustomMetricValue::Bytes(4096))
    );
    assert_eq!(
        node.custom.get("model"),
        Some(&daedalus_runtime::CustomMetricValue::Text(
            "yolo-lite".to_string()
        ))
    );
    assert_eq!(
        node.custom.get("saturated"),
        Some(&daedalus_runtime::CustomMetricValue::Bool(false))
    );
    assert_eq!(
        node.custom.get("classes"),
        Some(&daedalus_runtime::CustomMetricValue::Json(
            serde_json::json!({
                "person": 3,
                "car": 2,
            })
        ))
    );

    let table = telemetry.report().to_table();
    assert!(table.contains("node\t0\tcustom.detections"));
    assert!(table.contains("counter"));
}
