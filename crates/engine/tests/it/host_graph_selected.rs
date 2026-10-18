//! `tick_selected` runs only the branch its sinks reach, and a repeated sink set reuses the
//! demand computed for it.
#![cfg(feature = "plugins")]

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_engine::{Engine, EngineConfig, HostGraph, MetricsLevel};
use daedalus_planner::{Edge, Graph, NodeInstance};
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_runtime::RuntimeSink;
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::plugins::PluginRegistry;

use super::host_graph_reload::{INT_KEY, Nodes};

/// The host node has two inputs (`a`, `b`) fed back from the branches, one output (`in`).
fn setup() -> (Engine, PluginRegistry) {
    let int_ty = TypeExpr::Scalar(ValueType::Int);
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new(HOST_BRIDGE_ID)
                .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .input(PortDecl::new("a", INT_KEY).schema(int_ty.clone()))
                .input(PortDecl::new("b", INT_KEY).schema(int_ty.clone()))
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

/// Host input `in` fans out to `counter` (answers on host port `a`) and `relay` (host port `b`).
fn fork() -> Graph {
    let mut host = NodeInstance::new(HOST_BRIDGE_ID)
        .with_label("host")
        .with_inputs(["a", "b"])
        .with_outputs(["in"]);
    host.metadata
        .insert(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true));
    Graph {
        nodes: vec![
            host,
            NodeInstance::new("counter")
                .with_inputs(["in"])
                .with_outputs(["out"]),
            NodeInstance::new("relay")
                .with_inputs(["in"])
                .with_outputs(["out"]),
        ],
        edges: vec![
            Edge::new(0, "in", 1, "in"),
            Edge::new(0, "in", 2, "in"),
            Edge::new(1, "out", 0, "a"),
            Edge::new(2, "out", 0, "b"),
        ],
        metadata: Default::default(),
    }
}

fn compile(engine: &Engine, plugins: &PluginRegistry) -> HostGraph<Nodes> {
    engine
        .compile_host_graph_plugin_registry(
            plugins,
            fork(),
            Nodes,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap()
}

fn sink(port: &str) -> RuntimeSink {
    RuntimeSink::node_id(HOST_BRIDGE_ID).port(port)
}

fn drain(graph: &mut HostGraph<Nodes>, port: &str) -> usize {
    graph.drain_owned::<Value>(port).unwrap().len()
}

#[test]
fn selected_sinks_run_only_their_branch() {
    let (engine, plugins) = setup();
    let mut graph = compile(&engine, &plugins);

    graph.push_as("in", INT_KEY, Value::Int(1));
    let telemetry = graph.tick_selected([sink("a")]).unwrap();
    assert!(drain(&mut graph, "a") > 0);
    assert_eq!(drain(&mut graph, "b"), 0);
    assert!(!telemetry.demand.selected_sinks.is_empty());

    // Switching to the other sink, then back to the first: each set gets its own masks.
    graph.push_as("in", INT_KEY, Value::Int(2));
    graph.tick_selected([sink("b")]).unwrap();
    assert!(drain(&mut graph, "b") > 0);
    assert_eq!(drain(&mut graph, "a"), 0);

    graph.push_as("in", INT_KEY, Value::Int(3));
    graph.tick_selected([sink("a")]).unwrap();
    assert!(drain(&mut graph, "a") > 0);
}

#[test]
fn demand_summary_is_only_built_with_metrics_on() {
    let (_, plugins) = setup();
    for (level, expect_summary) in [(MetricsLevel::Off, false), (MetricsLevel::Basic, true)] {
        let engine = Engine::new(EngineConfig::default().with_metrics_level(level)).unwrap();
        let mut graph = compile(&engine, &plugins);
        graph.push_as("in", INT_KEY, Value::Int(1));
        let telemetry = graph.tick_selected([sink("a")]).unwrap();
        assert_eq!(
            !telemetry.demand.selected_sinks.is_empty(),
            expect_summary,
            "metrics {level:?}"
        );
    }
}
