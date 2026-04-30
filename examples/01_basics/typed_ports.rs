use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    type_key,
};

#[type_key("example:temperature:celsius")]
#[derive(Clone, Debug)]
struct Celsius(f64);

#[type_key("example:temperature:fahrenheit")]
#[derive(Clone, Debug)]
struct Fahrenheit(f64);

#[node(
    id = "typed.clamp_celsius",
    inputs("temperature"),
    outputs("temperature")
)]
fn clamp_celsius(temperature: &Celsius) -> Result<Celsius, NodeError> {
    Ok(Celsius(temperature.0.max(-273.15)))
}

#[node(
    id = "typed.celsius_to_fahrenheit",
    inputs("temperature"),
    outputs("temperature")
)]
fn celsius_to_fahrenheit(temperature: &Celsius) -> Result<Fahrenheit, NodeError> {
    Ok(Fahrenheit(temperature.0 * 9.0 / 5.0 + 32.0))
}

#[node(
    id = "typed.format_temperature",
    inputs("temperature"),
    outputs("label")
)]
fn format_temperature(temperature: &Fahrenheit) -> Result<String, NodeError> {
    Ok(format!("{:.1} F", temperature.0))
}

#[plugin(
    id = "example.typed_ports",
    types(Celsius, Fahrenheit),
    nodes(clamp_celsius, celsius_to_fahrenheit, format_temperature)
)]
struct TypedPortsPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = TypedPortsPlugin::new();
    registry.install(&plugin)?;

    let clamp = plugin.clamp_celsius.alias("clamp");
    let convert = plugin.celsius_to_fahrenheit.alias("convert");
    let format = plugin.format_temperature.alias("format");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("celsius");
        })
        .outputs(|g| {
            g.output("label");
        })
        .nodes(|g| {
            g.add_handle(&clamp);
            g.add_handle(&convert);
            g.add_handle(&format);
        })
        .try_edges(|g| {
            let clamp = g.node("clamp");
            let convert = g.node("convert");
            let format = g.node("format");
            g.try_connect("celsius", &clamp.input("temperature"))?;
            g.try_connect(&clamp.output("temperature"), &convert.input("temperature"))?;
            g.try_connect(&convert.output("temperature"), &format.input("temperature"))?;
            g.try_connect(&format.output("label"), "label")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("celsius", Celsius(21.0));
    let telemetry = runtime.tick_until_idle()?.unwrap_or_default();
    println!("label: {:?}", runtime.take::<String>("label"));
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
