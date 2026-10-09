//! Several instances of one node id run in dependency order, whatever their order in the graph:
//! the planner's order metadata carries node indices, so instances stay distinct end to end.

use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

/// `x * 10 + 1`: chaining two instances turns 2 into 211, and a run on a missing input fails.
#[node(id = "test.same_id.step", inputs("x"), outputs("y"))]
fn step(x: i64) -> Result<i64, NodeError> {
    Ok(x * 10 + 1)
}

#[plugin(id = "test.same_id", nodes(step))]
struct SameIdPlugin;

#[test]
fn later_instance_feeding_an_earlier_one_runs_first() {
    let mut registry = PluginRegistry::new();
    let plugin = SameIdPlugin::new();
    registry.install(&plugin).expect("install");
    // `second` is added before `first`, so the producer has the higher node index.
    let second = plugin.step.clone().alias("second");
    let first = plugin.step.alias("first");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("x")
        .and_then(|b| b.try_node(&second))
        .and_then(|b| b.try_node(&first))
        .and_then(|b| b.try_connect("x", &first.inputs.x))
        .and_then(|b| b.try_connect(&first.outputs.y, &second.inputs.x))
        .and_then(|b| b.try_connect(&second.outputs.y, "y"))
        .expect("wire")
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");

    for (x, expected) in [(2i64, 211), (5, 511)] {
        host.push("x", x);
        host.tick().expect("tick");
        assert_eq!(host.take::<i64>("y"), Some(expected));
    }
}
