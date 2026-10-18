//! Graph documents: a planner graph saved as versioned JSON, loaded back and run.
//!
//! A `GraphDocument` wraps a graph with a format marker, a schema version, the plugins it
//! requires and application metadata. Documents are strict: unknown fields are rejected, so an
//! edited file fails at load rather than at run. `Engine::compile_document` checks the
//! requirements against the installed plugins, then compiles the graph like `compile_registry`.

use daedalus::{
    engine::{Engine, EngineConfig, MetricsLevel},
    macros::{node, plugin},
    prelude::{GraphDocument, PluginRequirement},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "document.scale", inputs("value"), outputs("value"))]
fn scale(value: i64) -> Result<i64, NodeError> {
    Ok(value * 3)
}

#[plugin(id = "example.graph_document", nodes(scale))]
struct DocumentPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = DocumentPlugin::new();
    registry.install(&plugin)?;
    let engine = Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Off))?;

    // Save: the planner graph, the plugin it needs and a title, as JSON.
    let scale = plugin.scale.alias("scale");
    let graph = registry
        .graph_builder()?
        .try_single_node_io(&scale, "in", "value", "value", "out")?
        .build();
    let document = GraphDocument::new(graph)
        .require(PluginRequirement::new("example.graph_document"))
        .with_metadata("title", "triple a value");
    let json = document.to_json_pretty()?;
    let head: Vec<&str> = json.lines().take(5).collect();
    println!(
        "saved document ({} bytes), starts:\n{}",
        json.len(),
        head.join("\n")
    );

    // Load and run.
    let loaded = GraphDocument::from_json(&json)?;
    let mut host = engine.compile_document(&registry, loaded)?;
    host.push("in", 14_i64);
    host.tick_until_idle()?;
    println!("out={:?}", host.take::<i64>("out"));

    // A document that needs a plugin this registry lacks is refused before it compiles.
    let missing = GraphDocument::new(daedalus::planner::Graph::default())
        .require(PluginRequirement::new("example.not_installed"));
    match engine.compile_document(&registry, missing) {
        Ok(_) => println!("unexpected: compiled with a missing plugin"),
        Err(err) => println!("refused: {err}"),
    }
    Ok(())
}
