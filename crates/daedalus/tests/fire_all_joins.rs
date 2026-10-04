//! Cross-tick joins: a `fire = "all"` node waits until every connected required input has a
//! value, holding what arrived in its edges across ticks, where a default (`any`) node is skipped
//! and drops it. Optional inputs never block.

use daedalus::{
    GraphDocument,
    engine::{Engine, EngineConfig, HostGraph},
    macros::{node, plugin},
    planner::{DiagnosticCode, PlannerConfig, PlannerInput, build_plan},
    runtime::{
        NODE_FIRE_META_KEY, NodeError, NodeFire, handler_registry::HandlerRegistry,
        plugins::PluginRegistry,
    },
};

/// Emits `tick` on even ticks.
#[node(id = "join.fast", inputs("tick"), outputs("out"))]
fn fast(tick: i64) -> Result<Option<i64>, NodeError> {
    Ok((tick % 2 == 0).then_some(tick))
}

/// Emits `tick` on ticks divisible by three.
#[node(id = "join.slow", inputs("tick"), outputs("out"))]
fn slow(tick: i64) -> Result<Option<i64>, NodeError> {
    Ok((tick % 3 == 0).then_some(tick))
}

#[node(id = "join.pair", inputs("a", "b"), outputs("out"), fire = "all")]
fn pair(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a * 100 + b)
}

/// `pair` without a declared fire mode (`any`).
#[node(id = "join.pair_any", inputs("a", "b"), outputs("out"))]
fn pair_any(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a * 100 + b)
}

#[node(
    id = "join.with_extra",
    inputs("a", "extra"),
    outputs("out"),
    fire = "all"
)]
fn with_extra(a: i64, extra: Option<i64>) -> Result<i64, NodeError> {
    Ok(a * 100 + extra.unwrap_or(-1))
}

#[plugin(id = "join", nodes(fast, slow, pair, pair_any, with_extra))]
struct JoinPlugin;

fn registry() -> (PluginRegistry, JoinPlugin) {
    let mut registry = PluginRegistry::new();
    let plugin = JoinPlugin::new();
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

/// `tick` → fast → join.a and `tick` → slow → join.b, where `join` is `pair` (`all` by
/// declaration), `pair_any`, or `pair_any` made `all` with `fire_all`; `latest` makes the join's
/// input edges latest-only (default edges are FIFO).
fn rates_graph(join: &str, latest: bool) -> (PluginRegistry, daedalus::planner::Graph) {
    let (registry, plugin) = registry();
    let (fast, slow) = (plugin.fast.alias("fast"), plugin.slow.alias("slow"));
    let (pair, pair_any) = (plugin.pair.alias("join"), plugin.pair_any.alias("join"));
    let (in_a, in_b, out) = match join {
        "pair" => (&pair.inputs.a, &pair.inputs.b, &pair.outputs.out),
        _ => (
            &pair_any.inputs.a,
            &pair_any.inputs.b,
            &pair_any.outputs.out,
        ),
    };
    let mut builder = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("tick")
        .and_then(|b| b.try_node(&fast))
        .and_then(|b| b.try_node(&slow))
        .and_then(|b| match join {
            "pair" => b.try_node(&pair),
            _ => b.try_node(&pair_any),
        })
        .and_then(|b| b.try_connect("tick", &fast.inputs.tick))
        .and_then(|b| b.try_connect("tick", &slow.inputs.tick))
        .and_then(|b| b.try_connect(&fast.outputs.out, in_a))
        .and_then(|b| b.try_connect(&slow.outputs.out, in_b))
        .and_then(|b| b.try_connect(out, "out"))
        .expect("wire");
    if join == "fire_all" {
        builder = builder.fire_all(&pair_any);
    }
    if latest {
        builder = builder
            .edge_latest_only(&fast.outputs.out, in_a)
            .edge_latest_only(&slow.outputs.out, in_b);
    }
    (registry, builder.build())
}

/// Outputs of ticks 1..=`ticks`.
fn drive(host: &mut HostGraph<HandlerRegistry>, ticks: i64) -> Vec<Option<i64>> {
    (1..=ticks)
        .map(|tick| {
            host.push("tick", tick);
            host.tick().expect("tick");
            host.take::<i64>("out")
        })
        .collect()
}

#[test]
fn all_joins_producers_at_different_rates_across_ticks() {
    for join in ["pair", "fire_all"] {
        let (registry, graph) = rates_graph(join, true);
        let mut host = compile(&registry, graph);
        // t2: a=2 held; t3: b=3 joins it. t4: a=4 held, then replaced by a=6, which joins b=6.
        assert_eq!(
            drive(&mut host, 6),
            [None, None, Some(203), None, None, Some(606)],
            "{join}"
        );
    }
}

#[test]
fn any_drops_what_arrived_when_skipped() {
    let (registry, graph) = rates_graph("pair_any", true);
    let mut host = compile(&registry, graph);
    // t2's a is dropped when t2 skips; only t6, where both arrive in one tick, fires.
    assert_eq!(
        drive(&mut host, 6),
        [None, None, None, None, None, Some(606)]
    );
}

#[test]
fn fifo_edges_pair_values_in_arrival_order() {
    let (registry, graph) = rates_graph("pair", false);
    let mut host = compile(&registry, graph);
    // Each firing takes the oldest value of every edge: a=4 waits for the next b (6), and a=6
    // stays queued for the b after that (9).
    assert_eq!(
        drive(&mut host, 9),
        [
            None,
            None,
            Some(203),
            None,
            None,
            Some(406),
            None,
            None,
            Some(609)
        ]
    );
}

#[test]
fn host_inputs_are_held_until_the_join_fires() {
    let (registry, plugin) = registry();
    let pair = plugin.pair.alias("pair");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("a")
        .and_then(|b| b.input_typed::<i64>("b"))
        .and_then(|b| b.try_node(&pair))
        .and_then(|b| b.try_connect("a", &pair.inputs.a))
        .and_then(|b| b.try_connect("b", &pair.inputs.b))
        .and_then(|b| b.try_connect(&pair.outputs.out, "out"))
        .expect("wire")
        .build();
    let mut host = compile(&registry, graph);
    host.push("b", 5_i64);
    let telemetry = host.tick().expect("tick");
    assert_eq!(telemetry.nodes_executed, 0, "pair waits");
    host.tick().expect("idle tick");
    host.push("a", 4_i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("out"), Some(405));
}

#[test]
fn optional_inputs_never_block_an_all_node() {
    let (registry, plugin) = registry();
    let node = plugin.with_extra.alias("node");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("a")
        .and_then(|b| b.input_typed::<i64>("extra"))
        .and_then(|b| b.try_node(&node))
        .and_then(|b| b.try_connect("a", &node.inputs.a))
        .and_then(|b| b.try_connect("extra", &node.inputs.extra))
        .and_then(|b| b.try_connect(&node.outputs.out, "out"))
        .expect("wire")
        .build();
    let mut host = compile(&registry, graph);
    host.push("a", 2_i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("out"), Some(199), "fires without extra");
    host.push("extra", 3_i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("out"), None, "waits for a, holding extra");
    host.push("a", 4_i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("out"), Some(403));
}

#[test]
fn fire_mode_round_trips_through_graph_documents() {
    let (registry, graph) = rates_graph("fire_all", true);
    let join = graph
        .nodes
        .iter()
        .find(|node| node.label.as_deref() == Some("join"))
        .expect("join node");
    assert_eq!(NodeFire::from_metadata(&join.metadata), NodeFire::All);
    let json = registry.graph_document(graph).to_json_pretty().unwrap();
    assert!(json.contains(NODE_FIRE_META_KEY), "{json}");
    let document = GraphDocument::from_json(&json).expect("parse");
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_document(&registry, document)
        .expect("compile document");
    assert_eq!(drive(&mut host, 3), [None, None, Some(203)]);
}

#[test]
fn planner_warns_when_an_all_node_joins_a_producer_that_may_not_produce() {
    let warnings = |join: &str| {
        let (registry, graph) = rates_graph(join, false);
        let config = registry
            .planner_config_with_transport(PlannerConfig {
                enable_lints: true,
                ..PlannerConfig::default()
            })
            .expect("planner config");
        build_plan(PlannerInput { graph }, config)
            .diagnostics
            .into_iter()
            .filter(|diag| {
                diag.code == DiagnosticCode::LintWarning && diag.message.contains("fires on all")
            })
            .map(|diag| diag.message)
            .collect::<Vec<_>>()
    };
    let all = warnings("pair");
    assert_eq!(all.len(), 2, "{all:?}");
    assert!(all[0].contains("comes from fast:out"), "{all:?}");
    assert!(warnings("pair_any").is_empty());
}
