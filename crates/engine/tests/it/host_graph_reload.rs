//! Hot reload of a `HostGraph`: kept node state carries over, removed nodes drop theirs, host
//! ports keep their values, and a graph that fails to compile leaves the running one untouched.
#![cfg(feature = "plugins")]

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_engine::{Engine, EngineConfig, HostGraph};
use daedalus_planner::{Edge, Graph, NodeInstance};
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_runtime::RuntimeNode;
use daedalus_runtime::executor::{NodeError, NodeHandler};
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_runtime::state::ExecutionContext;

pub(crate) const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";

/// `counter` counts its runs in node state; `relay` forwards its input unchanged.
pub(crate) struct Nodes;

impl NodeHandler for Nodes {
    fn run(
        &self,
        node: &RuntimeNode,
        ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "counter" => {
                let count = ctx.take_node_state::<u64>().unwrap_or(0) + 1;
                ctx.set_node_state(count);
                io.push_as_to("out", INT_KEY.into(), Value::Int(count as i64));
            }
            "relay" => {
                let value = io
                    .get_typed_ref::<Value>("in")
                    .cloned()
                    .ok_or_else(|| NodeError::InvalidInput("expected a value".to_string()))?;
                io.push_as_to("out", INT_KEY.into(), value);
            }
            _ => {}
        }
        Ok(())
    }
}

pub(crate) fn setup() -> (Engine, PluginRegistry) {
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
    for id in ["counter", "relay"] {
        plugins
            .register_node_decl(
                NodeDecl::new(id)
                    .input(PortDecl::new("in", INT_KEY).schema(int_ty.clone()))
                    .output(PortDecl::new("out", INT_KEY).schema(int_ty.clone())),
            )
            .unwrap();
    }
    (Engine::new(EngineConfig::default()).unwrap(), plugins)
}

fn host() -> NodeInstance {
    let mut host = NodeInstance::new(HOST_BRIDGE_ID)
        .with_label("host")
        .with_inputs(["out"])
        .with_outputs(["in"]);
    host.metadata
        .insert(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true));
    host
}

/// Host → each node in `ids` in turn → host.
pub(crate) fn chain(ids: &[&str]) -> Graph {
    let mut nodes = vec![host()];
    nodes.extend(ids.iter().map(|id| {
        NodeInstance::new(*id)
            .with_inputs(["in"])
            .with_outputs(["out"])
    }));
    let last = nodes.len() - 1;
    let mut edges = vec![Edge::new(0, "in", 1, "in")];
    edges.extend((1..last).map(|i| Edge::new(i, "out", i + 1, "in")));
    edges.push(Edge::new(last, "out", 0, "out"));
    Graph {
        nodes,
        edges,
        metadata: Default::default(),
    }
}

/// Compile `graph` onto `current`'s host bridges and alias, ready for `reload_with`.
pub(crate) fn next_for(
    engine: &Engine,
    plugins: &PluginRegistry,
    graph: Graph,
    current: &HostGraph<Nodes>,
) -> HostGraph<Nodes> {
    engine
        .compile_host_graph_plugin_registry(
            plugins,
            graph,
            Nodes,
            current.bridge_manager().clone(),
            current.host().alias().to_string(),
        )
        .unwrap()
}

/// Push `input` through the graph and return what it emitted.
fn run(graph: &mut HostGraph<Nodes>, input: i64) -> Vec<i64> {
    graph.push_as("in", INT_KEY, Value::Int(input));
    graph.tick_until_idle().unwrap();
    graph
        .drain_owned::<Value>("out")
        .unwrap()
        .into_iter()
        .map(|value| match value {
            Value::Int(n) => n,
            other => panic!("unexpected output {other:?}"),
        })
        .collect()
}

fn has(names: &[String], name: &str) -> bool {
    names.iter().any(|n| n == name)
}

#[test]
fn reload_carries_state_of_kept_nodes() {
    let (engine, plugins) = setup();
    let mut graph = engine
        .compile_host_graph_plugin_registry(
            &plugins,
            chain(&["counter"]),
            Nodes,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap();
    assert_eq!(run(&mut graph, 0), vec![1]);
    assert_eq!(run(&mut graph, 0), vec![2]);

    // Rewire: a relay is inserted in front of the counter; the counter keeps its state.
    let next = next_for(&engine, &plugins, chain(&["relay", "counter"]), &graph);
    let report = graph.reload_with(next).unwrap();
    assert!(has(&report.nodes_kept, "counter"), "{report:?}");
    assert!(has(&report.nodes_added, "relay"), "{report:?}");
    assert!(report.nodes_removed.is_empty(), "{report:?}");
    assert_eq!(run(&mut graph, 0), vec![3]);
}

#[test]
fn removed_node_state_is_dropped_and_reported() {
    let (engine, plugins) = setup();
    let mut graph = engine
        .compile_host_graph_plugin_registry(
            &plugins,
            chain(&["counter"]),
            Nodes,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap();
    assert_eq!(run(&mut graph, 0), vec![1]);

    let next = next_for(&engine, &plugins, chain(&["relay"]), &graph);
    let report = graph.reload_with(next).unwrap();
    assert!(has(&report.nodes_removed, "counter"), "{report:?}");
    assert_eq!(run(&mut graph, 7), vec![7]);

    // Re-adding the counter starts it from fresh state.
    let next = next_for(&engine, &plugins, chain(&["counter"]), &graph);
    graph.reload_with(next).unwrap();
    assert_eq!(run(&mut graph, 0), vec![1]);
}

#[test]
fn failed_compile_keeps_the_running_graph() {
    let (engine, plugins) = setup();
    let mut graph = engine
        .compile_host_graph_plugin_registry(
            &plugins,
            chain(&["counter"]),
            Nodes,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap();
    assert_eq!(run(&mut graph, 0), vec![1]);

    // An unregistered node type fails to compile.
    let broken = chain(&["no-such-node"]);
    let compiled = engine.compile_host_graph_plugin_registry(
        &plugins,
        broken,
        Nodes,
        graph.bridge_manager().clone(),
        graph.host().alias().to_string(),
    );
    assert!(compiled.is_err());

    // Nothing was swapped: the counter's state carried on.
    assert_eq!(run(&mut graph, 0), vec![2]);
}

#[test]
fn reload_rejects_a_graph_on_other_bridges() {
    let (engine, plugins) = setup();
    let mut graph = engine
        .compile_host_graph_plugin_registry(
            &plugins,
            chain(&["counter"]),
            Nodes,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap();
    assert_eq!(run(&mut graph, 0), vec![1]);

    let foreign = engine
        .compile_host_graph_plugin_registry(
            &plugins,
            chain(&["counter"]),
            Nodes,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap();
    assert!(graph.reload_with(foreign).is_err());
    assert_eq!(run(&mut graph, 0), vec![2]);
}
