#![cfg(feature = "plugins")]

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use daedalus_data::model::{TypeExpr, Value, ValueType};
#[cfg(feature = "threads")]
use daedalus_engine::InboundWait;
use daedalus_engine::{Engine, EngineConfig, HostGraph, HostGraphDriveExit, HostPortDirection};
use daedalus_planner::{Edge, Graph, NodeInstance};
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_runtime::RuntimeNode;
use daedalus_runtime::executor::{NodeError, NodeHandler};
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_transport::Payload;

const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";

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

fn node(id: &str, label: Option<&str>, inputs: &[&str], outputs: &[&str]) -> NodeInstance {
    NodeInstance {
        label: label.map(str::to_string),
        ..NodeInstance::new(id)
            .with_inputs(inputs.iter().copied())
            .with_outputs(outputs.iter().copied())
    }
}

fn compile_increment_graph() -> (PluginRegistry, HostGraph<IncrementHandler>) {
    let int_ty = TypeExpr::Scalar(ValueType::Int);
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new(HOST_BRIDGE_ID)
                .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .input(PortDecl::new("out", INT_KEY).schema(int_ty.clone()))
                .output(PortDecl::new("in", INT_KEY).schema(int_ty.clone())),
        )
        .unwrap();
    plugins
        .register_node_decl(
            NodeDecl::new("inc")
                .input(PortDecl::new("in", INT_KEY).schema(int_ty.clone()))
                .output(PortDecl::new("out", INT_KEY).schema(int_ty)),
        )
        .unwrap();

    let mut host = node(HOST_BRIDGE_ID, Some("host"), &["out"], &["in"]);
    host.metadata
        .insert(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true));
    let graph = Graph {
        nodes: vec![host, node("inc", Some("adder"), &["in"], &["out"])],
        edges: vec![Edge::new(0, "in", 1, "in"), Edge::new(1, "out", 0, "out")],
        metadata: Default::default(),
    };

    let host_graph = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_host_graph_plugin_registry(
            &plugins,
            graph,
            IncrementHandler,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap();
    (plugins, host_graph)
}

#[test]
fn host_ports_are_typed_from_plan() {
    let (_plugins, graph) = compile_increment_graph();
    let int_ty = TypeExpr::Scalar(ValueType::Int);

    let inputs = graph.host_inputs();
    assert_eq!(inputs.len(), 1);
    let input = &inputs[0];
    assert_eq!(input.name(), "in");
    assert_eq!(input.alias, "host");
    assert_eq!(input.direction, HostPortDirection::Input);
    assert_eq!(input.type_expr.as_ref(), Some(&int_ty));
    assert_eq!(
        input.type_key.as_ref().map(|key| key.as_str()),
        Some(INT_KEY)
    );
    assert_eq!(input.connections.len(), 1);
    assert_eq!(input.connections[0].node_id, "inc");
    assert_eq!(input.connections[0].node_label.as_deref(), Some("adder"));
    assert_eq!(input.connections[0].port.as_str(), "in");
    assert_eq!(input.connections[0].type_expr.as_ref(), Some(&int_ty));

    let outputs = graph.host_outputs();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].name(), "out");
    assert_eq!(outputs[0].direction, HostPortDirection::Output);
    assert_eq!(outputs[0].type_expr.as_ref(), Some(&int_ty));
    assert_eq!(outputs[0].connections[0].port.as_str(), "out");

    assert_eq!(graph.host_ports().len(), 2);
    assert!(graph.host_ports().iter().all(|port| port.alias == "host"));
    assert!(graph.runtime_plan().host_ports_for("other").is_empty());
}

#[test]
fn inspect_payload_uses_registry_serializers() {
    #[derive(Clone)]
    struct Pose(i64);
    struct Unregistered;

    let (mut plugins, graph) = compile_increment_graph();
    // Registrations after compilation are visible: the graph shares the registry's map.
    plugins.register_value_serializer::<Pose, _>(|pose| Value::Int(pose.0));

    let pose = graph.inspect_payload(&Payload::owned("pose", Pose(4)));
    assert_eq!(pose.value(), Some(&Value::Int(4)));

    let text = graph.inspect_payload(&Payload::owned("string", String::from("hi")));
    assert_eq!(text.to_json(), serde_json::json!("hi"));

    let opaque = graph.inspect_payload(&Payload::owned("frame", Unregistered));
    assert!(opaque.is_opaque());
    assert_eq!(opaque.to_json()["type_key"], "frame");
}

#[test]
#[cfg(feature = "threads")]
fn tick_on_input_waits_for_feed() {
    let (_plugins, mut graph) = compile_increment_graph();
    let turn = graph.tick_on_input(Some(Duration::from_millis(5))).unwrap();
    assert_eq!(turn.wait, InboundWait::TimedOut);
    assert!(!turn.ticked());

    let input = graph.bind_payload_input("in");
    let feeder = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        input.push(Payload::owned(INT_KEY, Value::Int(1)));
    });
    let turn = graph.tick_on_input(None).unwrap();
    feeder.join().unwrap();
    assert_eq!(turn.wait, InboundWait::Ready);
    assert!(turn.ticked());
    let out = graph.drain_payloads("out");
    assert_eq!(out[0].get_ref::<Value>(), Some(&Value::Int(2)));
}

#[test]
#[cfg(feature = "threads")]
fn drive_blocking_processes_inputs_until_stopped() {
    let (_plugins, mut graph) = compile_increment_graph();
    graph.set_latest_input("in").unwrap();
    let stop = graph.stop_handle();
    let input = graph.bind_payload_input("in");
    let feeder_stop = stop.clone();
    let feeder = thread::spawn(move || {
        for value in 0..3 {
            input.push(Payload::owned(INT_KEY, Value::Int(value)));
            thread::sleep(Duration::from_millis(10));
        }
        thread::sleep(Duration::from_millis(20));
        feeder_stop.stop();
    });

    let mut seen = Vec::new();
    let exit = graph
        .drive_blocking(&stop, |graph, turn| {
            assert!(turn.ticked());
            for payload in graph.drain_payloads("out") {
                seen.push(graph.inspect_payload(&payload).into_value());
            }
            Ok(())
        })
        .unwrap();
    feeder.join().unwrap();

    assert_eq!(exit, HostGraphDriveExit::Stopped);
    assert!(!seen.is_empty());
    assert_eq!(seen.last(), Some(&Some(Value::Int(3))));
}

#[test]
#[cfg(feature = "threads")]
fn drive_blocking_exits_when_bridge_closes() {
    let (_plugins, mut graph) = compile_increment_graph();
    let stop = graph.stop_handle();
    graph.push_payload("in", Payload::owned(INT_KEY, Value::Int(10)));
    let host = graph.host().clone();
    let mut outputs = Vec::new();
    let exit = graph
        .drive_blocking(&stop, |graph, _turn| {
            outputs.extend(graph.drain_payloads("out"));
            host.close();
            Ok(())
        })
        .unwrap();
    assert_eq!(exit, HostGraphDriveExit::Closed);
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].get_ref::<Value>(), Some(&Value::Int(11)));
}

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

#[test]
fn async_drive_wakes_on_input_and_stop() {
    let (_plugins, mut graph) = compile_increment_graph();
    let stop = graph.stop_handle();
    let input = graph.bind_payload_input("in");
    let feeder_stop = stop.clone();
    let feeder = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        input.push(Payload::owned(INT_KEY, Value::Int(41)));
        thread::sleep(Duration::from_millis(20));
        feeder_stop.stop();
    });

    let mut seen = Vec::new();
    let exit = block_on(graph.drive(&stop, |graph, _turn| {
        seen.extend(graph.drain_payloads("out"));
        Ok(())
    }))
    .unwrap();
    feeder.join().unwrap();

    assert_eq!(exit, HostGraphDriveExit::Stopped);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].get_ref::<Value>(), Some(&Value::Int(42)));
}
