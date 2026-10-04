//! Optional inputs (`Option<T>` / `Option<&T>` parameters) carry `T`'s key, so producers of `T`
//! connect directly, and never block their node: it runs with `None` when the port is
//! unconnected or its producer pushed nothing this tick (an `Option` return is a conditional
//! output).

use daedalus::{
    GraphDocument,
    engine::{Engine, EngineConfig, HostGraph},
    macros::{node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
    transport::{AccessMode, TypeKey},
    type_key,
};

const CORNERS_KEY: &str = "test:optional:corners";

#[type_key(CORNERS_KEY)]
#[derive(Clone, Debug, PartialEq)]
struct Corners(i64);

/// `frame * 100 + refined`, or `- 1` without refined corners.
#[node(id = "test.optional.pose", inputs("frame", "refined"), outputs("pose"))]
fn pose(frame: i64, refined: Option<Corners>) -> Result<i64, NodeError> {
    Ok(frame * 100 + refined.map_or(-1, |c| c.0))
}

/// Borrowing variant: `Option<&T>` reads the payload in place.
#[node(
    id = "test.optional.pose_ref",
    inputs("frame", "refined"),
    outputs("pose")
)]
fn pose_ref(frame: i64, refined: Option<&Corners>) -> Result<i64, NodeError> {
    Ok(frame * 100 + refined.map_or(-1, |c| c.0))
}

/// A conditional output: corners only for positive seeds.
#[node(id = "test.optional.refine", inputs("seed"), outputs("corners"))]
fn refine(seed: i64) -> Result<Option<Corners>, NodeError> {
    Ok((seed > 0).then_some(Corners(seed)))
}

#[plugin(id = "test.optional", types(Corners), nodes(pose, pose_ref, refine))]
struct OptionalPlugin;

fn registry() -> (PluginRegistry, OptionalPlugin) {
    let mut registry = PluginRegistry::new();
    let plugin = OptionalPlugin::new();
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

/// `frame` → pose, then `seed` → refine → pose.refined, or a typed host input `corners` →
/// pose.refined.
macro_rules! connected_graph {
    ($node:ident, $refined_from_host:expr) => {{
        let (registry, plugin) = registry();
        let pose = plugin.$node.alias("pose");
        let refine = plugin.refine.alias("refine");
        let builder = registry
            .graph_builder()
            .expect("builder")
            .input_typed::<i64>("frame")
            .and_then(|b| b.try_node(&pose))
            .and_then(|b| b.try_connect("frame", &pose.inputs.frame))
            .and_then(|b| b.try_connect(&pose.outputs.pose, "pose"));
        let builder = if $refined_from_host {
            builder
                .and_then(|b| b.input_typed::<Corners>("corners"))
                .and_then(|b| b.try_connect("corners", &pose.inputs.refined))
        } else {
            builder
                .and_then(|b| b.input_typed::<i64>("seed"))
                .and_then(|b| b.try_node(&refine))
                .and_then(|b| b.try_connect("seed", &refine.inputs.seed))
                .and_then(|b| b.try_connect(&refine.outputs.corners, &pose.inputs.refined))
        };
        let graph = builder.expect("wire").build();
        (registry, graph)
    }};
}

#[test]
fn optional_port_carries_the_inner_key() {
    let (registry, _) = registry();
    let decl = registry
        .transport_capabilities
        .nodes()
        .values()
        .find(|decl| decl.id.0 == "test.optional:test.optional.pose")
        .expect("pose declared");
    let refined = decl.inputs.iter().find(|p| p.name == "refined").unwrap();
    assert!(refined.optional);
    assert_eq!(refined.type_key, TypeKey::new(CORNERS_KEY));
    assert_eq!(refined.access, AccessMode::Read);
    let frame = decl.inputs.iter().find(|p| p.name == "frame").unwrap();
    assert!(!frame.optional);
    let refine = registry
        .transport_capabilities
        .nodes()
        .values()
        .find(|decl| decl.id.0 == "test.optional:test.optional.refine")
        .expect("refine declared");
    assert_eq!(refine.outputs[0].type_key, TypeKey::new(CORNERS_KEY));
}

#[test]
fn unconnected_optional_input_runs_with_none() {
    let (registry, plugin) = registry();
    let pose = plugin.pose.alias("pose");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("frame")
        .and_then(|b| b.try_node(&pose))
        .and_then(|b| b.try_connect("frame", &pose.inputs.frame))
        .and_then(|b| b.try_connect(&pose.outputs.pose, "pose"))
        .expect("wire")
        .build();
    let mut host = compile(&registry, graph);
    host.push("frame", 2i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("pose"), Some(199));
}

#[test]
fn connected_optional_input_gets_the_value_or_none_when_nothing_was_produced() {
    for (pose_id, (registry, graph)) in [
        ("pose", connected_graph!(pose, false)),
        ("pose_ref", connected_graph!(pose_ref, false)),
    ] {
        let mut host = compile(&registry, graph);
        host.push("frame", 2i64);
        host.push("seed", 7i64);
        host.tick().expect("tick");
        assert_eq!(host.take::<i64>("pose"), Some(207), "{pose_id}: Some");

        // The producer runs but its conditional output pushes nothing: no wait, `None`.
        host.push("frame", 3i64);
        host.push("seed", -1i64);
        host.tick().expect("tick");
        assert_eq!(
            host.take::<i64>("pose"),
            Some(299),
            "{pose_id}: produced nothing"
        );
    }
}

#[test]
fn typed_host_inputs_of_the_inner_type_feed_optional_ports() {
    let (registry, graph) = connected_graph!(pose, true);
    let mut host = compile(&registry, graph);
    host.push("frame", 4i64);
    host.push("corners", Corners(5));
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("pose"), Some(405));
    host.push("frame", 4i64);
    host.tick().expect("tick");
    assert_eq!(
        host.take::<i64>("pose"),
        Some(399),
        "no host value this tick"
    );
}

#[test]
fn graph_documents_with_optional_ports_round_trip() {
    let (registry, graph) = connected_graph!(pose, false);
    let json = registry.graph_document(graph).to_json_pretty().unwrap();
    let document = GraphDocument::from_json(&json).expect("parse");
    assert_eq!(document.to_json_pretty().unwrap(), json);
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_document(&registry, document)
        .expect("compile document");
    host.push("frame", 1i64);
    host.push("seed", 9i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("pose"), Some(109));
    host.push("frame", 1i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("pose"), Some(99));
}

#[node(id = "test.optional.corner_value", inputs("corners"), outputs("value"))]
fn corner_value(corners: &Corners) -> Result<i64, NodeError> {
    Ok(corners.0)
}

#[plugin(
    id = "test.optional.required",
    deps("test.optional"),
    nodes(corner_value)
)]
struct RequiredPlugin;

#[test]
fn nodes_missing_a_connected_required_input_are_skipped() {
    let (mut registry, plugin) = registry();
    let required = RequiredPlugin::new();
    registry.install(&required).expect("install");
    let (refine, value) = (
        plugin.refine.alias("refine"),
        required.corner_value.alias("value"),
    );
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("seed")
        .and_then(|b| b.try_node(&refine))
        .and_then(|b| b.try_node(&value))
        .and_then(|b| b.try_connect("seed", &refine.inputs.seed))
        .and_then(|b| b.try_connect(&refine.outputs.corners, &value.inputs.corners))
        .and_then(|b| b.try_connect(&value.outputs.value, "value"))
        .expect("wire")
        .build();
    let mut host = compile(&registry, graph);
    host.push("seed", 6i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("value"), Some(6));

    // Nothing produced, nothing pushed: the required consumer is skipped, not failed.
    host.push("seed", -6i64);
    let telemetry = host.tick().expect("tick with nothing produced");
    assert_eq!(telemetry.nodes_executed, 1, "only refine ran");
    assert_eq!(host.take::<i64>("value"), None);
    host.tick().expect("tick without any input");
    assert_eq!(host.take::<i64>("value"), None);
}
