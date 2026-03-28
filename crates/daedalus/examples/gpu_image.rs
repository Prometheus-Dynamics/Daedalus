//! GPU image pipeline using a real wgpu compute shader to brighten an image.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins,gpu-wgpu" --example gpu_image

#[cfg(all(feature = "engine", feature = "plugins", feature = "gpu"))]
use daedalus::{
    Compute, ComputeAffinity, DeviceBridge,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    gpu::shader::ShaderContext,
    graph_builder::GraphBuilder,
    macros::node,
    registry::store::Registry,
    runtime::{BackpressureStrategy, EdgePolicyKind, NodeError, handler_registry::HandlerRegistry},
};
#[cfg(all(feature = "engine", feature = "plugins", feature = "gpu"))]
use image::{DynamicImage, ImageBuffer};

#[cfg(all(feature = "engine", feature = "plugins", feature = "gpu"))]
#[node(id = "example.image_load", outputs("img"))]
fn load_image() -> Result<Compute<DynamicImage>, NodeError> {
    let path = format!("{}/examples/assets/input.png", env!("CARGO_MANIFEST_DIR"));
    image::open(&path)
        .map(Compute::Cpu)
        .map_err(|e| NodeError::Handler(e.to_string()))
}

/// Runs a real wgpu compute shader to brighten the image by +30 on RGB.
#[cfg(all(feature = "engine", feature = "plugins", feature = "gpu"))]
#[node(
    id = "example.image_shader",
    inputs("img"),
    outputs("img"),
    shaders("assets/brighten.wgsl")
)]
fn shader_wgpu(
    img: Compute<DynamicImage>,
    _ctx: ShaderContext,
) -> Result<Compute<DynamicImage>, NodeError> {
    match img {
        Compute::Gpu(handle) => Ok(Compute::Gpu(handle)),
        Compute::Cpu(cpu) => {
            // CPU fallback if GPU not available.
            let mut cpu = cpu;
            cpu.invert();
            Ok(Compute::Cpu(cpu))
        }
    }
}

#[cfg(all(feature = "engine", feature = "plugins", feature = "gpu"))]
#[node(id = "example.image_sink_gpu", inputs("img"))]
fn sink_noop(
    img: Compute<DynamicImage>,
    ctx: &daedalus_runtime::state::ExecutionContext,
) -> Result<(), NodeError> {
    let cpu_img = match img {
        Compute::Cpu(img) => img,
        Compute::Gpu(handle) => {
            let Some(gpu) = ctx.gpu.as_ref() else {
                return Err(NodeError::Handler(
                    "missing GPU context for readback".into(),
                ));
            };
            DynamicImage::download(&handle, gpu).map_err(|e| NodeError::Handler(e.to_string()))?
        }
    };
    let path = format!(
        "{}/examples/assets/output_gpu.png",
        env!("CARGO_MANIFEST_DIR")
    );
    let rgba: ImageBuffer<image::Rgba<u8>, _> = cpu_img.to_rgba8();
    rgba.save(&path)
        .map_err(|e| NodeError::Handler(e.to_string()))
}

#[cfg(all(feature = "engine", feature = "plugins", feature = "gpu"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = Registry::new();
    reg.register_node(load_image::descriptor())?;
    reg.register_node(shader_wgpu::descriptor())?;
    reg.register_node(sink_noop::descriptor())?;
    let mut handlers = HandlerRegistry::new();
    handlers.merge(load_image::handler_registry());
    handlers.merge(shader_wgpu::handler_registry());
    handlers.merge(sink_noop::handler_registry());

    let graph = GraphBuilder::new(&reg)
        .node_with_compute("example.image_load", "load", ComputeAffinity::CpuOnly)
        .node_with_compute(
            "example.image_shader",
            "shader",
            ComputeAffinity::GpuPreferred,
        )
        .node_with_compute("example.image_sink_gpu", "sink", ComputeAffinity::CpuOnly)
        .connect("load:img", "shader:img")
        .connect("shader:img", "sink:img")
        .build();

    let mut cfg = EngineConfig::default();
    cfg.planner.enable_gpu = true;
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.pool_size = None;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.gpu = GpuBackend::Device;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&reg, graph, handlers)?;
    println!(
        "wrote examples/assets/output_gpu.png via wgpu compute, telemetry: {:?}",
        result.telemetry
    );
    Ok(())
}

// Fallback main so `cargo check --examples` succeeds when GPU features are off.
#[cfg(not(all(feature = "engine", feature = "plugins", feature = "gpu")))]
fn main() {
    eprintln!("enable `engine`, `plugins`, and `gpu-wgpu` features to run this example");
}
