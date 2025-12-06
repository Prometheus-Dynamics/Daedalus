//! CPU image pipeline: load a PNG from the repo, invert it on CPU, and write it out.
//! Run with: `cargo run -p daedalus-rs --features "engine,plugins" --example cpu_image`
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};
use image::{DynamicImage, ImageBuffer, Rgba};

#[node(id = "image_load", outputs("img"))]
fn load_image() -> Result<DynamicImage, NodeError> {
    let path = format!("{}/examples/assets/input.png", env!("CARGO_MANIFEST_DIR"));
    image::open(&path).map_err(|e| NodeError::Handler(e.to_string()))
}

#[node(id = "image_invert", inputs("img"), outputs("img"))]
fn invert(img: DynamicImage) -> Result<DynamicImage, NodeError> {
    let mut img = img;
    img.invert();
    Ok(img)
}

#[node(id = "image_save", inputs("img"))]
fn save_image(img: DynamicImage) -> Result<(), NodeError> {
    let path = format!(
        "{}/examples/assets/output_cpu.png",
        env!("CARGO_MANIFEST_DIR")
    );
    // Ensure RGBA8 output to keep it simple.
    let rgba: ImageBuffer<Rgba<u8>, _> = img.to_rgba8();
    rgba.save(&path)
        .map_err(|e| NodeError::Handler(e.to_string()))
}

declare_plugin!(
    CpuImagePlugin,
    "example.image",
    [load_image, invert, save_image]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = CpuImagePlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();
    println!(
        "registry nodes installed: {}",
        reg.registry.view().nodes.len()
    );

    let load = plugin.load_image.alias("load");
    let inv = plugin.invert.alias("invert");
    let save = plugin.save_image.alias("save");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&load)
        .node(&inv)
        .node(&save)
        .connect(&load.outputs.img, &inv.inputs.img)
        .connect(&inv.outputs.img, &save.inputs.img)
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.pool_size = None;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.gpu = GpuBackend::Cpu;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&reg.registry, graph, handlers)?;
    println!(
        "wrote examples/assets/output_cpu.png, telemetry: {:?}",
        result.telemetry
    );
    Ok(())
}
