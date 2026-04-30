use daedalus::{
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::{Cpu, Gpu},
};

#[node(id = "quickstart.gpu_fallback.cpu", inputs("frame"), outputs("frame"))]
fn cpu_filter(frame: Cpu<Vec<u8>>) -> Result<Cpu<Vec<u8>>, NodeError> {
    Ok(frame)
}

#[node(
    id = "quickstart.gpu_fallback.gpu",
    fallback = "quickstart.gpu_fallback.cpu",
    inputs("frame"),
    outputs("frame")
)]
fn gpu_filter(frame: Gpu<Vec<u8>>) -> Result<Gpu<Vec<u8>>, NodeError> {
    Ok(frame)
}

#[plugin(id = "quickstart.gpu_fallback", nodes(cpu_filter, gpu_filter))]
struct QuickstartPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    registry.install(&QuickstartPlugin::new())?;
    println!("registered GPU node with CPU fallback");
    Ok(())
}
