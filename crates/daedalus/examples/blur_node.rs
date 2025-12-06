//! GPU blur using the flexible ShaderBinding API via the node system.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins,gpu-wgpu" --example blur_node

#![cfg(all(feature = "engine", feature = "plugins", feature = "gpu"))]

use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    gpu::shader::{Access, BindingData, BindingKind, BufferInit, ShaderBinding, ShaderContext},
    graph_builder::GraphBuilder,
    macros::node,
    registry::store::Registry,
    runtime::{BackpressureStrategy, EdgePolicyKind, NodeError, handler_registry::HandlerRegistry},
};
use image::{DynamicImage, ImageBuffer, Rgba};

#[node(id = "example.blur_load", outputs("img"))]
fn load_image() -> Result<DynamicImage, NodeError> {
    let path = format!("{}/examples/assets/input.png", env!("CARGO_MANIFEST_DIR"));
    image::open(&path).map_err(|e| NodeError::Handler(e.to_string()))
}

/// Blur node: builds bindings explicitly and dispatches the WGSL shader.
#[node(
    id = "example.blur_shader",
    inputs("img"),
    outputs("img"),
    shaders("assets/blur_main.wgsl")
)]
fn blur_shader(img: DynamicImage, ctx: ShaderContext) -> Result<DynamicImage, NodeError> {
    let rgba = img.to_rgba8();
    let (width, height) = rgba.dimensions();
    let pixels = rgba.into_raw();
    let pixel_count = width * height;

    // Simple symmetric weights with radius 2.
    let weights: [f32; 5] = [0.0625, 0.25, 0.375, 0.25, 0.0625];
    let weight_bytes: Vec<u8> = weights.iter().flat_map(|w| w.to_ne_bytes()).collect();
    let params = [
        width,
        height,
        2u32,                 // radius
        weights.len() as u32, // kernel_len
    ];

    let bindings = [
        // texture_2d<f32>
        ShaderBinding {
            binding: 0,
            kind: BindingKind::Texture2D,
            access: Access::ReadOnly,
            data: BindingData::TextureRgba8 {
                width,
                height,
                bytes: std::borrow::Cow::Borrowed(&pixels),
            },
            readback: false,
        },
        // sampler
        ShaderBinding {
            binding: 1,
            kind: BindingKind::Sampler,
            access: Access::ReadOnly,
            data: BindingData::Sampler(Default::default()),
            readback: false,
        },
        // texture_storage_2d<rgba8unorm, write>
        ShaderBinding {
            binding: 2,
            kind: BindingKind::StorageTexture2D,
            access: Access::WriteOnly,
            data: BindingData::TextureAlloc { width, height },
            readback: true,
        },
        // var<storage, read> blurWeights
        ShaderBinding {
            binding: 3,
            kind: BindingKind::Storage,
            access: Access::ReadOnly,
            data: BindingData::Buffer(BufferInit::Bytes(&weight_bytes)),
            readback: false,
        },
        // var<uniform> blurParams
        ShaderBinding {
            binding: 4,
            kind: BindingKind::Uniform,
            access: Access::ReadOnly,
            data: BindingData::Buffer(BufferInit::Bytes(bytemuck::cast_slice(&params))),
            readback: false,
        },
    ];

    let out = ctx
        .dispatch_first(&bindings, None, None, Some([pixel_count, 1, 1]))
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    let out_img = out
        .texture_rgba8_image(2, width, height)
        .ok_or_else(|| NodeError::Handler("missing output texture readback".into()))?;
    let img_buf: ImageBuffer<Rgba<u8>, _> = out_img.to_rgba8();
    Ok(DynamicImage::ImageRgba8(img_buf))
}

#[node(id = "example.blur_sink", inputs("img"))]
fn sink_image(img: DynamicImage) -> Result<(), NodeError> {
    let path = format!(
        "{}/examples/assets/output_gpu_blur.png",
        env!("CARGO_MANIFEST_DIR")
    );
    let rgba: ImageBuffer<Rgba<u8>, _> = img.to_rgba8();
    rgba.save(&path)
        .map_err(|e| NodeError::Handler(e.to_string()))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = Registry::new();
    reg.register_node(load_image::descriptor())?;
    reg.register_node(blur_shader::descriptor())?;
    reg.register_node(sink_image::descriptor())?;
    let mut handlers = HandlerRegistry::new();
    handlers.merge(load_image::handler_registry());
    handlers.merge(blur_shader::handler_registry());
    handlers.merge(sink_image::handler_registry());

    let graph = GraphBuilder::new(&reg)
        .node_pair(("example.blur_load", "1.0.0"), "load")
        .node_pair(("example.blur_shader", "1.0.0"), "shader")
        .node_pair(("example.blur_sink", "1.0.0"), "sink")
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
        "wrote examples/assets/output_gpu_blur.png via flexible GPU shader, telemetry: {:?}",
        result.telemetry
    );
    Ok(())
}

// Fallback main so `cargo check --examples` succeeds when GPU features are off.
#[cfg(not(all(feature = "engine", feature = "plugins", feature = "gpu")))]
fn main() {
    eprintln!("enable `engine`, `plugins`, and `gpu-wgpu` features to run this example");
}
