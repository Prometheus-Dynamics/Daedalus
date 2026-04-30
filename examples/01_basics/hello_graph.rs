use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "hello.prefix", inputs("subject"), outputs("text"))]
fn prefix(subject: String) -> Result<String, NodeError> {
    Ok(format!("hello {subject}"))
}

#[node(id = "hello.capitalize", inputs("text"), outputs("text"))]
fn capitalize(text: String) -> Result<String, NodeError> {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return Ok(text);
    };
    Ok(format!("{}{}", first.to_uppercase(), chars.as_str()))
}

#[node(id = "hello.punctuate", inputs("text"), outputs("text"))]
fn punctuate(text: String) -> Result<String, NodeError> {
    Ok(format!("{text}!"))
}

#[plugin(id = "example.hello", nodes(prefix, capitalize, punctuate))]
struct HelloPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = HelloPlugin::new();
    registry.install(&plugin)?;

    let prefix = plugin.prefix.alias("prefix");
    let capitalize = plugin.capitalize.alias("capitalize");
    let punctuate = plugin.punctuate.alias("punctuate");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("subject");
        })
        .outputs(|g| {
            g.output("message");
        })
        .nodes(|g| {
            g.add_handle(&prefix);
            g.add_handle(&capitalize);
            g.add_handle(&punctuate);
        })
        .try_edges(|g| {
            let prefix = g.node("prefix");
            let capitalize = g.node("capitalize");
            let punctuate = g.node("punctuate");
            g.try_connect("subject", &prefix.input("subject"))?;
            g.try_connect(&prefix.output("text"), &capitalize.input("text"))?;
            g.try_connect(&capitalize.output("text"), &punctuate.input("text"))?;
            g.try_connect(&punctuate.output("text"), "message")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("subject", String::from("graph"));
    let telemetry = runtime.tick_until_idle()?.unwrap_or_default();
    println!("message: {:?}", runtime.take::<String>("message"));
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
