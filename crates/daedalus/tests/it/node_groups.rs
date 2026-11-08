//! Node groups exported by a plugin: a `NodeGroup` registers as one node that lists like any
//! node, expands into its stages at plan time, keeps per-stage telemetry rolled up under the
//! group id, and requires the union of its stages' features and plugins.

use daedalus::{
    data::model::Value,
    engine::{Engine, EngineConfig, HostGraph},
    macros::{node, plugin},
    planner::{GraphNodeSelector, GraphPatch, GraphPatchOp},
    registry::{
        capability::{NodeDecl, PortDecl},
        ids::NodeId,
    },
    runtime::{
        NodeError,
        handler_registry::HandlerRegistry,
        plugins::{NodeGroup, PluginRegistry, StagePort},
    },
};

const GROUP: &str = "ng:pipe";
const OUTER: &str = "ng:outer";

#[node(id = "ng.add", inputs("x"), outputs("out"))]
fn add_one(x: i32) -> Result<i32, NodeError> {
    Ok(x + 1)
}

#[node(id = "ng.double", inputs("x"), outputs("out"))]
fn double(x: i32) -> Result<i32, NodeError> {
    Ok(x * 2)
}

#[plugin(id = "ng", nodes(add_one, double))]
struct NgPlugin;

/// `ng:pipe` = `double(add_one(x))`; the macro plugin registers its nodes as `ng:<id>`., exposed as input `x` and output `out`.
fn pipe_group() -> NodeGroup {
    NodeGroup::new(GROUP)
        .label("Add then double")
        .stage("add", "ng:ng.add")
        .stage("double", "ng:ng.double")
        .connect(StagePort::new("add", "out"), StagePort::new("double", "x"))
        .input("x", [StagePort::new("add", "x")])
        .output("out", StagePort::new("double", "out"))
}

fn installed() -> (PluginRegistry, NgPlugin) {
    let mut registry = PluginRegistry::new();
    let plugin = NgPlugin::new();
    registry.install(&plugin).expect("install");
    (registry, plugin)
}

fn compile(
    registry: &PluginRegistry,
    graph: daedalus::planner::Graph,
) -> HostGraph<HandlerRegistry> {
    Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(registry, graph)
        .expect("compile")
}

/// A graph with one group node `g` wired from host `x` to host `out`.
fn group_graph(registry: &PluginRegistry, id: &str) -> daedalus::planner::Graph {
    registry
        .graph_builder()
        .expect("builder")
        .try_node_from_id(id, "g")
        .and_then(|b| b.try_connect("x", ("g", "x")))
        .and_then(|b| b.try_connect(("g", "out"), "out"))
        .expect("wire group graph")
        .build()
}

#[test]
fn group_is_registered_and_listed_like_a_node() {
    let (mut registry, _plugin) = installed();
    registry
        .register_node_group(pipe_group())
        .expect("register group");

    let caps = registry.combined_transport_capabilities().expect("caps");
    let decl = caps
        .nodes()
        .get(&NodeId::new(GROUP))
        .expect("group is a listed node");
    assert_eq!(decl.label.as_deref(), Some("Add then double"));
    let inputs: Vec<&str> = decl.inputs.iter().map(|p| p.name.as_str()).collect();
    let outputs: Vec<&str> = decl.outputs.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(inputs, ["x"]);
    assert_eq!(outputs, ["out"]);
}

#[test]
fn group_config_default_reaches_its_stage() {
    // A const on a stage input the group does not expose is the group's built-in config.
    let (mut registry, _plugin) = installed();
    registry
        .register_node_group(pipe_group().const_input(StagePort::new("add", "x"), Value::Int(5)))
        .expect("register group");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .try_node_from_id(GROUP, "g")
        .and_then(|b| b.try_connect(("g", "out"), "out"))
        .expect("wire")
        .build();
    let mut host = compile(&registry, graph);
    host.tick().expect("tick");
    // Exposed `x` is unconnected here, so the stage constant feeds the add: (5 + 1) * 2.
    assert_eq!(host.take::<i32>("out"), Some(12));
}

#[test]
fn group_runs_through_its_stages_with_per_stage_telemetry() {
    let (mut registry, _plugin) = installed();
    registry
        .register_node_group(pipe_group())
        .expect("register group");
    let mut host = compile(&registry, group_graph(&registry, GROUP));
    host.push("x", 3_i32);
    let telemetry = host.tick().expect("tick");
    assert_eq!(host.take::<i32>("out"), Some(8));

    // The group is not one runtime node: each stage has its own telemetry, labelled under the
    // group node, and both roll up under the canonical group id.
    let labels = host.node_labels();
    for stage in ["::add", "::double"] {
        let idx = labels
            .iter()
            .position(|label| label.ends_with(stage))
            .unwrap_or_else(|| panic!("no stage labelled `{stage}` in {labels:?}"));
        assert!(
            telemetry.node_metrics.get(idx).is_some(),
            "stage `{stage}` has no telemetry"
        );
    }
    assert!(
        !labels.iter().any(|label| label == "g"),
        "the group node itself must not be a runtime node"
    );
    assert!(
        telemetry.group_metrics.contains_key(GROUP),
        "group rollup keyed by the canonical id"
    );
}

#[test]
fn const_patch_on_group_node_reaches_its_stage() {
    let (mut registry, _plugin) = installed();
    registry
        .register_node_group(pipe_group())
        .expect("register group");
    // No host edge into the group input: the patched const is the only source of `x`.
    let mut graph = registry
        .graph_builder()
        .expect("builder")
        .try_node_from_id(GROUP, "g")
        .and_then(|b| b.try_connect(("g", "out"), "out"))
        .expect("wire")
        .build();
    let patch = GraphPatch {
        version: 1,
        ops: vec![GraphPatchOp::SetNodeConst {
            node: GraphNodeSelector {
                id: Some(GROUP.into()),
                ..Default::default()
            },
            port: "x".into(),
            value: Some(Value::Int(4)),
        }],
    };
    patch.apply_to_graph(&mut graph);
    let mut host = compile(&registry, graph);
    host.tick().expect("tick");
    // The patched group input is the stage's input: (4 + 1) * 2.
    assert_eq!(host.take::<i32>("out"), Some(10));
}

#[test]
fn nested_group_expands_fully_and_rolls_up_under_the_outer_id() {
    let (mut registry, _plugin) = installed();
    registry
        .register_node_group(pipe_group())
        .expect("register inner");
    registry
        .register_node_group(
            NodeGroup::new(OUTER)
                .stage("p", GROUP)
                .input("x", [StagePort::new("p", "x")])
                .output("out", StagePort::new("p", "out")),
        )
        .expect("register outer");
    let mut host = compile(&registry, group_graph(&registry, OUTER));
    host.push("x", 3_i32);
    let telemetry = host.tick().expect("tick");
    assert_eq!(host.take::<i32>("out"), Some(8));
    assert!(telemetry.group_metrics.contains_key(OUTER));
    assert!(!telemetry.group_metrics.contains_key(GROUP));
    let labels = host.node_labels();
    assert!(labels.iter().any(|label| label.ends_with("::add")));
}

#[test]
fn requires_unions_stage_features_and_plugins() {
    let (mut registry, _plugin) = installed();
    registry
        .register_node_group(pipe_group())
        .expect("register pipe");
    // Two stages outside any plugin, each with a feature flag: the group must require both.
    let feature_stage = |id: &str, flag: &str| {
        NodeDecl::new(id)
            .input(i32_port("x"))
            .output(i32_port("out"))
            .feature_flag(flag)
    };
    registry
        .register_node_decl(feature_stage("ng.feat_a", "feat-a"))
        .expect("feat_a");
    registry
        .register_node_decl(feature_stage("ng.feat_b", "feat-b"))
        .expect("feat_b");
    registry
        .register_node_group(
            NodeGroup::new("ng:feat")
                .stage("a", "ng.feat_a")
                .stage("b", "ng.feat_b")
                .connect(StagePort::new("a", "out"), StagePort::new("b", "x"))
                .input("x", [StagePort::new("a", "x")])
                .output("out", StagePort::new("b", "out")),
        )
        .expect("register feature group");

    let caps = registry.combined_transport_capabilities().expect("caps");
    let decl = caps.nodes().get(&NodeId::new("ng:feat")).expect("listed");
    assert_eq!(decl.feature_flags, ["feat-a", "feat-b"]);

    // The group names no plugin, but its stages `ng.add` and `ng.double` come from plugin `ng`:
    // a graph that places only the group requires that plugin.
    let requires = registry.graph_requirements(&group_graph(&registry, GROUP));
    assert!(
        requires.iter().any(|req| req.id == "ng"),
        "stage plugin `ng` must be required, got {requires:?}"
    );
}

/// The port type of the `i32` stage node, so the test ports match the macro-built stages.
fn i32_port(name: &str) -> PortDecl {
    let base = AddOneNode::node_decl().expect("decl");
    let mut port = base.inputs[0].clone();
    port.name = name.into();
    port
}

#[test]
fn reload_with_a_group_keeps_expansion_and_telemetry_consistent() {
    let (mut registry, _plugin) = installed();
    registry
        .register_node_group(pipe_group())
        .expect("register group");
    let engine = Engine::new(EngineConfig::default()).expect("engine");

    let mut host = compile(&registry, group_graph(&registry, GROUP));
    host.push("x", 3_i32);
    host.tick().expect("tick before reload");
    assert_eq!(host.take::<i32>("out"), Some(8));

    // A reload with the same group swaps in and expands and rolls up again.
    let next = registry.graph_document(group_graph(&registry, GROUP));
    host.reload_document(&engine, &registry, next, registry.handlers())
        .expect("reload with group");
    host.push("x", 10_i32);
    let telemetry = host.tick().expect("tick after reload");
    assert_eq!(host.take::<i32>("out"), Some(22));
    let labels = host.node_labels();
    assert!(labels.iter().any(|label| label.ends_with("::add")));
    assert!(labels.iter().any(|label| label.ends_with("::double")));
    assert!(telemetry.group_metrics.contains_key(GROUP));

    // A reload naming a missing group fails and leaves the running graph unchanged.
    let mut missing = group_graph(&registry, GROUP);
    missing.nodes[0].id = daedalus::registry::ids::NodeId::new("groups_example:missing");
    let missing = registry.graph_document(missing);
    assert!(
        host.reload_document(&engine, &registry, missing, registry.handlers())
            .is_err(),
        "a reload to an unknown node must be refused"
    );
    host.push("x", 4_i32);
    host.tick().expect("tick after refused reload");
    assert_eq!(
        host.take::<i32>("out"),
        Some(10),
        "the old graph keeps running"
    );
}
