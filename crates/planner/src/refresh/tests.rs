use alloc::string::String;
use alloc::vec::Vec;

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_registry::capability::{CapabilityRegistry, FanInDecl, NodeDecl, PortDecl};

use crate::diagnostics::DiagnosticCode;
use crate::graph::{Edge, Graph, NodeInstance};
use crate::passes::{PlannerConfig, PlannerInput, build_plan};
use crate::refresh::RefreshPolicy;

const MAPPER: &str = "test:mapping.live_map";

fn port(name: &str) -> PortDecl {
    PortDecl::new(name, "test:blob").schema(TypeExpr::Scalar(ValueType::Int))
}

/// The mapper as its plugin now declares it: `map`/`save` inputs and a `saved_map` output were
/// added after the stored graph below was saved, with their per-port metadata.
fn registry() -> CapabilityRegistry {
    let mut registry = CapabilityRegistry::new();
    for decl in [
        NodeDecl::new("test:source").output(port("frame")),
        NodeDecl::new(MAPPER)
            .input(port("frame"))
            .input(port("map"))
            .input(port("save"))
            .output(port("pose"))
            .output(port("saved_map"))
            .metadata("inputs.map.access", Value::String("owned".into()))
            .metadata("inputs.save.access", Value::String("owned".into()))
            .metadata("outputs.saved_map.conditional", Value::Bool(true))
            .metadata("inputs.frame.description", Value::String("camera".into())),
        NodeDecl::new("test:merge")
            .fanin_input(FanInDecl::new("in", 0, "test:blob"))
            .output(port("out")),
    ] {
        registry.register_node(decl).expect("register node");
    }
    registry
}

/// A graph stored before the new ports: the mapper also still lists a removed `legacy` input
/// (unwired) and a removed `debug` output that an edge uses.
fn stored_graph() -> Graph {
    Graph {
        nodes: vec![
            NodeInstance::new("test:source").with_outputs(["frame"]),
            NodeInstance::new(MAPPER)
                .with_label("mapper")
                .with_inputs(["frame", "legacy"])
                .with_outputs(["pose", "debug"])
                .with_metadata("inputs.legacy.access", Value::String("owned".into()))
                .with_metadata(
                    "inputs.frame.description",
                    Value::String("user label".into()),
                ),
            NodeInstance::new("test:merge")
                .with_inputs(["in0", "in1"])
                .with_outputs(["out"]),
            NodeInstance::new("test:unknown").with_inputs(["x"]),
        ],
        edges: vec![
            Edge::new(0, "frame", 1, "frame"),
            Edge::new(1, "debug", 2, "in0"),
        ],
        ..Graph::default()
    }
}

/// The planner's diagnostics for `graph`, as (code, message).
fn planner_diagnostics(
    graph: Graph,
    registry: &CapabilityRegistry,
) -> Vec<(DiagnosticCode, String)> {
    let config = PlannerConfig {
        transport_capabilities: Some(registry.clone()),
        ..PlannerConfig::default()
    };
    build_plan(PlannerInput { graph }, config)
        .diagnostics
        .into_iter()
        .map(|diag| (diag.code, diag.message))
        .collect()
}

#[test]
fn refresh_adds_new_ports_with_metadata_and_drops_unused_old_ones() {
    let registry = registry();
    let mut graph = stored_graph();
    let stale = |diags: &[(DiagnosticCode, String)]| {
        diags
            .iter()
            .filter(|(code, message)| {
                *code == DiagnosticCode::PortMissing && message.contains("graph is stale")
            })
            .count()
    };
    assert_eq!(stale(&planner_diagnostics(graph.clone(), &registry)), 3);

    let report = graph.refresh_ports(&registry);
    assert!(report.changed());
    assert_eq!(report.nodes.len(), 1, "{report:?}");
    let mapper = &report.nodes[0];
    assert_eq!(mapper.node, 1);
    assert_eq!(mapper.added_inputs, ["map", "save"]);
    assert_eq!(mapper.added_outputs, ["saved_map"]);
    assert_eq!(mapper.removed_inputs, ["legacy"]);
    assert!(mapper.removed_outputs.is_empty());
    // `debug` still feeds the merge node: kept for the planner to report.
    assert_eq!(mapper.kept_outputs, ["debug"]);

    let node = &graph.nodes[1];
    assert_eq!(node.inputs, ["frame", "map", "save"]);
    assert_eq!(node.outputs, ["pose", "debug", "saved_map"]);
    assert_eq!(node.label.as_deref(), Some("mapper"));
    assert_eq!(
        node.metadata.get("inputs.map.access"),
        Some(&Value::String("owned".into()))
    );
    assert_eq!(
        node.metadata.get("outputs.saved_map.conditional"),
        Some(&Value::Bool(true))
    );
    assert!(!node.metadata.contains_key("inputs.legacy.access"));
    // Metadata the node already sets is the graph's: not overwritten.
    assert_eq!(
        node.metadata.get("inputs.frame.description"),
        Some(&Value::String("user label".into()))
    );
    // Fan-in instances and unknown nodes are untouched; edges are kept.
    assert_eq!(graph.nodes[2].inputs, ["in0", "in1"]);
    assert_eq!(graph.nodes[3].inputs, ["x"]);
    assert_eq!(graph.edges.len(), 2);

    let diags = planner_diagnostics(graph.clone(), &registry);
    assert_eq!(stale(&diags), 0, "{diags:?}");
    // What remains is the kept `debug` output's edge, reported by the planner as before.
    assert!(
        diags
            .iter()
            .any(|(code, message)| *code == DiagnosticCode::PortMissing
                && message.contains("`debug`")),
        "{diags:?}"
    );
}

#[test]
fn refresh_is_idempotent() {
    let registry = registry();
    let mut graph = stored_graph();
    graph.refresh_ports(&registry);
    let refreshed = graph.clone();
    let again = graph.refresh_ports(&registry);
    assert!(!again.changed(), "{again:?}");
    // Only the still-used undeclared port is reported.
    assert_eq!(again.nodes.len(), 1);
    assert_eq!(again.nodes[0].kept_outputs, ["debug"]);
    assert_eq!(graph, refreshed);
}

#[test]
fn nodes_without_port_lists_and_current_graphs_are_left_alone() {
    let registry = registry();
    let mut graph = Graph {
        nodes: vec![
            NodeInstance::new(MAPPER),
            NodeInstance::new("test:source").with_outputs(["frame"]),
        ],
        ..Graph::default()
    };
    let before = graph.clone();
    let report = graph.refresh_ports(&registry);
    assert!(report.is_empty(), "{report:?}");
    assert_eq!(graph, before);
}

#[test]
fn document_refresh_updates_its_graph() {
    let registry = registry();
    let mut document = crate::document::GraphDocument::new(stored_graph());
    assert!(document.refresh_ports(&registry).changed());
    assert_eq!(document.graph.nodes[1].inputs, ["frame", "map", "save"]);
}

/// A pose node whose `huber_delta_px` knob was folded into a constant: the stored graph still
/// lists the port and sets it; `legacy_wired` is also gone but an edge feeds it.
fn stale_knob_graph() -> (CapabilityRegistry, Graph) {
    let mut registry = registry();
    registry
        .register_node(
            NodeDecl::new("test:pose")
                .input(port("frame"))
                .output(port("pose")),
        )
        .expect("register pose");
    let graph = Graph {
        nodes: vec![
            NodeInstance::new("test:source").with_outputs(["frame"]),
            NodeInstance::new("test:pose")
                .with_inputs(["frame", "huber_delta_px", "legacy_wired"])
                .with_outputs(["pose"])
                .with_const_input("huber_delta_px", Value::Float(1.5))
                .with_const_input("legacy_wired", Value::Int(3))
                .with_metadata("inputs.huber_delta_px.min", Value::Float(0.0)),
            // No port list: only the const is stale.
            NodeInstance::new("test:pose").with_const_input("huber_delta_px", Value::Float(2.0)),
        ],
        edges: vec![
            Edge::new(0, "frame", 1, "frame"),
            Edge::new(0, "frame", 1, "legacy_wired"),
        ],
        ..Graph::default()
    };
    (registry, graph)
}

#[test]
fn stale_consts_are_kept_by_default() {
    let (registry, mut graph) = stale_knob_graph();
    let report = graph.refresh_ports(&registry);
    assert_eq!(report.nodes.len(), 1, "{report:?}");
    assert_eq!(
        report.nodes[0].kept_inputs,
        ["huber_delta_px", "legacy_wired"]
    );
    assert!(report.nodes[0].dropped_consts.is_empty());
    let diags = planner_diagnostics(graph, &registry);
    assert!(
        diags
            .iter()
            .any(|(code, message)| *code == DiagnosticCode::PortExtra
                && message.contains("`huber_delta_px`")),
        "{diags:?}"
    );
}

#[test]
fn drop_stale_consts_drops_const_only_ports_and_reports_values() {
    let (registry, mut graph) = stale_knob_graph();
    let policy = RefreshPolicy {
        drop_stale_consts: true,
    };
    let report = graph.refresh_ports_with(&registry, policy);
    assert!(report.changed());
    assert_eq!(report.nodes.len(), 2, "{report:?}");

    let pose = &report.nodes[0];
    assert_eq!(pose.node, 1);
    assert_eq!(
        pose.dropped_consts,
        [(String::from("huber_delta_px"), Value::Float(1.5))]
    );
    assert_eq!(pose.removed_inputs, ["huber_delta_px"]);
    // An edge still uses `legacy_wired`: kept with its const, for the planner to report.
    assert_eq!(pose.kept_inputs, ["legacy_wired"]);
    let node = &graph.nodes[1];
    assert_eq!(node.inputs, ["frame", "legacy_wired"]);
    assert_eq!(
        node.const_inputs,
        [(String::from("legacy_wired"), Value::Int(3))]
    );
    assert!(!node.metadata.contains_key("inputs.huber_delta_px.min"));

    let portless = &report.nodes[1];
    assert_eq!(portless.node, 2);
    assert_eq!(
        portless.dropped_consts,
        [(String::from("huber_delta_px"), Value::Float(2.0))]
    );
    assert!(graph.nodes[2].const_inputs.is_empty());

    let diags = planner_diagnostics(graph.clone(), &registry);
    assert!(
        !diags
            .iter()
            .any(|(_, message)| message.contains("`huber_delta_px`")),
        "{diags:?}"
    );
    // Idempotent: nothing more to drop.
    let again = graph.refresh_ports_with(&registry, policy);
    assert!(!again.changed(), "{again:?}");
}

#[test]
fn drop_stale_consts_keeps_declared_and_fanin_consts() {
    let registry = registry();
    let mut graph = Graph {
        nodes: vec![
            NodeInstance::new(MAPPER)
                .with_inputs(["frame", "map", "save"])
                .with_outputs(["pose", "saved_map"])
                .with_const_input("save", Value::Bool(true)),
            NodeInstance::new("test:merge")
                .with_inputs(["in0"])
                .with_outputs(["out"])
                .with_const_input("in0", Value::Int(1)),
        ],
        ..Graph::default()
    };
    let before = graph.clone();
    let report = graph.refresh_ports_with(
        &registry,
        RefreshPolicy {
            drop_stale_consts: true,
        },
    );
    assert!(report.is_empty(), "{report:?}");
    assert_eq!(graph, before);
}
