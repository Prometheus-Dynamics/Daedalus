use daedalus_data::model::Value;
use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef,
};
use daedalus_runtime::{
    BackpressureStrategy, EdgePolicyKind, Executor, NodeHandler, ResourceLifecycleEvent,
    RuntimeNode, SchedulerConfig, StateStore, build_runtime, executor::NodeError,
};
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

#[test]
fn cpu_only_executes_in_order() {
    let exec = tiny_exec_plan(&[ComputeAffinity::CpuOnly, ComputeAffinity::CpuOnly]);
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: EdgePolicyKind::Fifo,
            backpressure: BackpressureStrategy::None,
            lockfree_queues: false,
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
