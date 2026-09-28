//! Allocation budget for the host-graph hot path (push -> tick -> take) with metrics off.
//!
//! Counts heap allocations made on the calling thread, so it is robust against other tests
//! running in parallel.
#![cfg(feature = "plugins")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_engine::{Engine, EngineConfig, HostGraph};
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_registry::ids::NodeId;
use daedalus_runtime::executor::{MetricsLevel, NodeError, NodeHandler};
use daedalus_runtime::handles::PortId;
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_runtime::{RuntimeNode, state::ExecutionContext};
use daedalus_transport::Payload;

struct CountingAlloc;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn allocations_during(f: impl FnOnce()) -> usize {
    let before = ALLOCATIONS.with(Cell::get);
    f();
    ALLOCATIONS.with(Cell::get) - before
}

const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";

struct IncrementHandler;

impl NodeHandler for IncrementHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &ExecutionContext,
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

fn node(id: &str, inputs: &[&str], outputs: &[&str]) -> NodeInstance {
    NodeInstance {
        id: NodeId::new(id),
        bundle: None,
        label: Some(
            if id == HOST_BRIDGE_ID {
                "host"
            } else {
                "adder"
            }
            .to_string(),
        ),
        inputs: inputs.iter().map(|port| port.to_string()).collect(),
        outputs: outputs.iter().map(|port| port.to_string()).collect(),
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    }
}

fn port(node: usize, port: &str) -> PortRef {
    PortRef {
        node: NodeRef(node),
        port: port.into(),
    }
}

fn compile(level: MetricsLevel) -> (PluginRegistry, HostGraph<IncrementHandler>) {
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
    let mut host = node(HOST_BRIDGE_ID, &["out"], &["in"]);
    host.metadata
        .insert(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true));
    let edges = [(0, "in", 1, "in"), (1, "out", 0, "out")]
        .into_iter()
        .map(|(from, from_port, to, to_port)| Edge {
            from: port(from, from_port),
            to: port(to, to_port),
            metadata: Default::default(),
        })
        .collect();
    let graph = Graph {
        nodes: vec![host, node("inc", &["in"], &["out"])],
        edges,
        metadata: Default::default(),
    };
    let mut host_graph = Engine::new(EngineConfig::default().with_metrics_level(level))
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

fn round_trip(graph: &mut HostGraph<IncrementHandler>, value: i64) -> Option<i64> {
    graph.push_payload("in", Payload::owned(INT_KEY, Value::Int(value)));
    graph.tick().expect("tick");
    match graph.take_payload("out")?.get_ref::<Value>() {
        Some(Value::Int(out)) => Some(*out),
        _ => None,
    }
}

#[test]
fn static_port_ids_do_not_allocate() {
    let id = PortId::from("frame");
    let cloned = allocations_during(|| {
        let again = PortId::from("frame");
        assert_eq!(again, id);
    });
    assert_eq!(cloned, 0, "PortId::from(&'static str) must not allocate");
}

#[test]
fn metrics_off_round_trip_allocation_budget() {
    let (_plugins, mut graph) = compile(MetricsLevel::Off);
    graph.host().set_event_recording(false);
    // Warm up: first use of each port and queue creates state once.
    for value in 0..4 {
        assert_eq!(round_trip(&mut graph, value), Some(value + 1));
    }
    let allocations = allocations_during(|| {
        assert_eq!(round_trip(&mut graph, 41), Some(42));
    });
    // Only the two payloads (host input + node output) allocate, two allocations each (value
    // Arc + storage Arc). Anything above this budget is per-tick bookkeeping.
    assert!(
        allocations <= 4,
        "metrics-off round trip allocated {allocations} times"
    );
}
