use daedalus::{
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::{Cpu, Gpu},
};

#[node(id = "fallback.cpu", inputs("frame"), outputs("frame"))]
fn cpu(frame: Cpu<Vec<u8>>) -> Result<Cpu<Vec<u8>>, NodeError> {
    Ok(frame)
}

#[node(
    id = "fallback.gpu",
    fallback = "fallback.cpu",
    inputs("frame"),
    outputs("frame")
)]
fn gpu(frame: Gpu<Vec<u8>>) -> Result<Gpu<Vec<u8>>, NodeError> {
    Ok(frame)
}

#[plugin(id = "example.gpu_fallback", nodes(cpu, gpu))]
struct FallbackPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    registry.install(&FallbackPlugin::new())?;
    println!("fallback plugin installed");
    Ok(())
}
