//! A planner-inserted adapter on an edge into a host output runs when the host bridge drains the
//! edge: a node's `Int` reaches a host port typed `String` as a string, through the serial and
//! parallel executors, a tick, and the direct host route (whose single-node path steps aside).
#![cfg(feature = "plugins")]

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_engine::{Engine, EngineConfig, RuntimeMode, RuntimeSection};
use daedalus_planner::{Edge, Graph, NodeInstance};
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_runtime::RuntimeNode;
use daedalus_runtime::executor::{NodeError, NodeHandler};
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_transport::{Payload, TransportError};

const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";
const STRING_KEY: &str = "typeexpr:{\"Scalar\":\"String\"}";

struct Increment;

impl NodeHandler for Increment {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "inc" {
            let Some(Value::Int(value)) = io.get_typed_ref::<Value>("in") else {
                return Err(NodeError::InvalidInput("expected an int".into()));
            };
            let out = Value::Int(value + 1);
            io.push_payload("out", Payload::owned(INT_KEY, out));
        }
        Ok(())
    }
}

/// Host `in` (Int) -> `inc` -> host `out` (String): the planner puts `int_to_string` on the
/// edge into the host output.
fn plugins() -> PluginRegistry {
    let (int, string) = (
        TypeExpr::Scalar(ValueType::Int),
        TypeExpr::Scalar(ValueType::String),
    );
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new(HOST_BRIDGE_ID)
                .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .input(PortDecl::new("out", STRING_KEY).schema(string.clone()))
                .output(PortDecl::new("in", INT_KEY).schema(int.clone())),
        )
        .unwrap();
    plugins
        .register_node_decl(
            NodeDecl::new("inc")
                .input(PortDecl::new("in", INT_KEY).schema(int.clone()))
                .output(PortDecl::new("out", INT_KEY).schema(int.clone())),
        )
        .unwrap();
    plugins
        .register_transport_adapter_fn("int_to_string", int, string, |payload, _request| {
            match payload.get_ref::<Value>() {
                Some(Value::Int(value)) => Ok(Payload::owned(
                    STRING_KEY,
                    Value::String(value.to_string().into()),
                )),
                _ => Err(TransportError::Unsupported("expected an int".into())),
            }
        })
        .unwrap();
    plugins
}

fn graph() -> Graph {
    Graph {
        nodes: vec![
            NodeInstance {
                metadata: [(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true))]
                    .into_iter()
                    .collect(),
                ..NodeInstance::new(HOST_BRIDGE_ID)
                    .with_label("host")
                    .with_inputs(["out"])
                    .with_outputs(["in"])
            },
            NodeInstance::new("inc")
                .with_inputs(["in"])
                .with_outputs(["out"]),
        ],
        edges: vec![Edge::new(0, "in", 1, "in"), Edge::new(1, "out", 0, "out")],
        metadata: Default::default(),
    }
}

fn string_of(payload: &Payload) -> Option<&str> {
    match payload.get_ref::<Value>() {
        Some(Value::String(text)) => Some(text),
        _ => None,
    }
}

fn run(mode: RuntimeMode) {
    let config = EngineConfig {
        runtime: RuntimeSection {
            mode: mode.clone(),
            pool_size: Some(2),
            ..RuntimeSection::default()
        },
        ..EngineConfig::default()
    };
    let plugins = plugins();
    let mut host = Engine::new(config)
        .unwrap()
        .compile_host_graph_plugin_registry(
            &plugins,
            graph(),
            Increment,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap();
    let steps = &host.runtime_plan().edge_transports[1]
        .as_ref()
        .expect("the edge into the host output adapts")
        .adapter_steps;
    assert_eq!(steps.len(), 1, "{steps:?}");

    // A tick: the drain runs the adapter.
    host.push_payload("in", Payload::owned(INT_KEY, Value::Int(41)));
    host.tick().unwrap();
    let out = host.drain_payloads("out");
    assert_eq!(out.len(), 1, "{mode:?}");
    assert_eq!(string_of(&out[0]), Some("42"), "{mode:?}: {:?}", out[0]);

    // The direct host route: a single node, but the output edge adapts.
    let (_, output) = host
        .tick_direct_payload("in", Payload::owned(INT_KEY, Value::Int(99)), "out")
        .unwrap()
        .expect("a direct route");
    let output = output.expect("an output");
    assert_eq!(string_of(&output), Some("100"), "{mode:?}: {output:?}");
}

#[test]
fn a_host_output_edge_runs_its_adapter_serially() {
    run(RuntimeMode::Serial);
}

#[test]
fn a_host_output_edge_runs_its_adapter_in_parallel() {
    run(RuntimeMode::Parallel);
}
