use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::{FreshnessPolicy, OverflowPolicy, PressurePolicy},
};

#[node(id = "quickstart.label", inputs("value"), outputs("label"))]
fn label(value: i64) -> Result<String, NodeError> {
    Ok(format!("value={value}"))
}

#[plugin(id = "quickstart.bounded_streaming_io", nodes(label))]
struct QuickstartPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = QuickstartPlugin::new();
    registry.install(&plugin)?;

    let label = plugin.label.alias("label");
    let builder = registry.graph_builder()?;
    let input = builder.input("in");
    let output = builder.output("out");
    let graph = builder
        .try_node_handle_like(&label)?
        .try_connect(&input, &label.inputs.value)?
        .try_connect(&label.outputs.label, &output)?
        .build();

    let engine = Engine::new(EngineConfig::default())?;
    let mut graph = engine.compile_registry(&registry, graph)?;
    graph.set_input_policy(
        "in",
        PressurePolicy::Bounded {
            capacity: 2,
            overflow: OverflowPolicy::DropOldest,
        },
        FreshnessPolicy::PreserveAll,
    )?;
    graph.set_output_policy(
        "out",
        PressurePolicy::Bounded {
            capacity: 2,
            overflow: OverflowPolicy::DropOldest,
        },
        FreshnessPolicy::PreserveAll,
    )?;

    for value in 1_i64..=4 {
        graph.push("in", value);
    }
    graph.tick_until_idle()?;

    println!("out={:?}", graph.drain::<String>("out"));
    println!("stats={:?}", graph.host().stats());
    Ok(())
}
