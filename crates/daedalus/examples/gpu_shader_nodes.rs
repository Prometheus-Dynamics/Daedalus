//! GPU shader node pipeline that actually runs a few WGSL kernels (blur -> edges -> binary).
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins,gpu-wgpu" --example gpu_shader_nodes

#![cfg(all(feature = "engine", feature = "plugins", feature = "gpu"))]

use bytemuck::{Pod, Zeroable};
use daedalus::macros::{GpuStateful, node};
use daedalus::{
    Payload,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    gpu::shader::{GpuState, ShaderContext, TextureOut, Uniform},
    graph_builder::GraphBuilder,
    registry::store::Registry,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError, handler_registry::HandlerRegistry,
        state::ExecutionContext,
    },
};
use image::{DynamicImage, GenericImageView};

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct BlurParams {
    width: u32,
    height: u32,
    radius: u32,
    kernel_len: u32,
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct EdgeParams {
    width: u32,
    height: u32,
    _pad: [u32; 2],
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct BinaryParams {
    width: u32,
    height: u32,
    threshold: f32,
    _pad: f32,
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable, Default, GpuStateful)]
#[gpu_state(readback)]
struct BinaryStats {
    mean: f32,
    count: u32,
    pad: [u32; 2],
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct MorphParams {
    width: u32,
    height: u32,
    radius: u32,
    norm: i32,
    op: u32,
    _pad0: [u32; 3],
    pad: [u32; 4],
}

#[derive(::daedalus::macros::GpuBindings)]
#[gpu(spec(
    src = "assets/blur.wgsl",
    entry = "blur_horizontal_main",
    workgroup_size = 64
))]
struct BlurBindings<'a> {
    #[gpu(binding = 0, texture2d(format = "rgba8unorm"))]
    input: &'a Payload<DynamicImage>,
    #[gpu(binding = 2, texture2d(format = "rgba8unorm", write))]
    output: TextureOut,
    #[gpu(binding = 3, storage(read))]
    weights: &'a [f32],
    #[gpu(binding = 4, uniform)]
    params: Uniform<BlurParams>,
}

#[derive(::daedalus::macros::GpuBindings)]
#[gpu(spec(src = "assets/edges.wgsl", entry = "sobel_main", workgroup_size = 256))]
struct EdgeBindings<'a> {
    #[gpu(binding = 0, texture2d(format = "rgba8unorm"))]
    input: &'a Payload<DynamicImage>,
    #[gpu(binding = 2, texture2d(format = "rgba8unorm", write))]
    output: TextureOut,
    #[gpu(binding = 3, uniform)]
    params: Uniform<EdgeParams>,
}

#[derive(::daedalus::macros::GpuBindings)]
#[allow(dead_code)]
#[gpu(spec(
    src = "assets/binary.wgsl",
    entry = "binary_main",
    workgroup_size = 256
))]
struct BinaryBindings<'a> {
    #[gpu(binding = 0, texture2d(format = "rgba8unorm"))]
    input: &'a Payload<DynamicImage>,
    #[gpu(binding = 2, texture2d(format = "rgba8unorm", write))]
    output: TextureOut,
    #[gpu(binding = 3, uniform)]
    params: Uniform<BinaryParams>,
}

#[derive(::daedalus::macros::GpuBindings)]
#[gpu(spec(src = "assets/morph.wgsl", entry = "morph_main", workgroup_size = 256))]
struct MorphBindings<'a> {
    #[gpu(binding = 0, texture2d(format = "rgba8unorm"))]
    input: &'a Payload<DynamicImage>,
    #[gpu(binding = 2, texture2d(format = "rgba8unorm", write))]
    output: TextureOut,
    #[gpu(binding = 3, uniform)]
    params: Uniform<MorphParams>,
}

#[derive(::daedalus::macros::GpuBindings)]
#[gpu(spec(
    src = "assets/texture_sample.wgsl",
    entry = "texture_sample_main",
    workgroup_size = 64
))]
struct TextureSampleBindings<'a> {
    #[gpu(binding = 0, texture2d(format = "rgba8unorm"))]
    input: &'a Payload<DynamicImage>,
    #[gpu(binding = 2, texture2d(format = "rgba8unorm", write))]
    output: TextureOut,
}

#[derive(::daedalus::macros::GpuBindings)]
#[gpu(spec(
    src = "assets/binary_state.wgsl",
    entry = "binary_state_main",
    workgroup_size = 256
))]
struct BinaryStateBindings<'a> {
    #[gpu(binding = 0, texture2d(format = "rgba8unorm"))]
    input: &'a Payload<DynamicImage>,
    #[gpu(binding = 2, texture2d(format = "rgba8unorm", write))]
    output: TextureOut,
    #[gpu(binding = 3, uniform)]
    params: Uniform<BinaryParams>,
    #[gpu(binding = 4, storage(read_write), state)]
    stats: &'a GpuState<BinaryStats>,
}

#[node(id = "example.nodes.load", outputs("img"))]
fn load_image() -> Result<Payload<DynamicImage>, NodeError> {
    let path = format!("{}/examples/assets/input.png", env!("CARGO_MANIFEST_DIR"));
    let img = image::open(&path).map_err(|e| NodeError::Handler(e.to_string()))?;
    Ok(Payload::Cpu(img))
}

/// Simple texture path that samples with an overridden sampler and writes to a storage texture.
#[node(
    id = "example.nodes.texture_sample",
    inputs("img"),
    outputs("img"),
    compute(::daedalus::ComputeAffinity::GpuPreferred),
    shaders(TextureSampleBindings)
)]
fn texture_sample_node(
    img: Payload<DynamicImage>,
    ctx: ShaderContext,
) -> Result<Payload<DynamicImage>, NodeError> {
    let (w, h) = img.dimensions();

    let bindings = TextureSampleBindings {
        input: &img,
        output: TextureOut::from_input_ctx(&img, &ctx),
    };

    ctx.dispatch_bindings(&bindings, None, None, None)
        .and_then(|out| out.into_payload_with_ctx(2, &ctx, w, h))
        .map_err(|e| NodeError::Handler(e.to_string()))
}

/// Blur pass (horizontal) using a real WGSL shader.
#[node(
    id = "example.nodes.blur",
    inputs("img"),
    outputs("img"),
    compute(::daedalus::ComputeAffinity::GpuPreferred),
    shaders(BlurBindings)
)]
fn blur_node(
    img: Payload<DynamicImage>,
    ctx: ShaderContext,
) -> Result<Payload<DynamicImage>, NodeError> {
    let (w, h) = img.dimensions();
    let weights: [f32; 5] = [0.0625, 0.25, 0.375, 0.25, 0.0625];
    let params = BlurParams {
        width: w,
        height: h,
        radius: 2,
        kernel_len: weights.len() as u32,
    };

    let bindings = BlurBindings {
        input: &img,
        output: TextureOut::from_input_ctx(&img, &ctx),
        weights: &weights,
        params: Uniform::new(params),
    };

    ctx.single(&bindings)
        .dispatch_auto()
        .and_then(|out| out.into_payload_with_ctx(2, &ctx, w, h))
        .map_err(|e| NodeError::Handler(e.to_string()))
}

/// Sobel edges after an implicit grayscale conversion inside the shader.
#[node(
    id = "example.nodes.edges",
    inputs("img"),
    outputs("img"),
    compute(::daedalus::ComputeAffinity::GpuPreferred),
    shaders(EdgeBindings)
)]
fn edges_node(
    img: Payload<DynamicImage>,
    ctx: ShaderContext,
) -> Result<Payload<DynamicImage>, NodeError> {
    let (w, h) = img.dimensions();
    let params = EdgeParams {
        width: w,
        height: h,
        _pad: [0, 0],
    };

    let bindings = EdgeBindings {
        input: &img,
        output: TextureOut::from_input_ctx(&img, &ctx),
        params: Uniform::new(params),
    };

    ctx.single(&bindings)
        .dispatch_auto()
        .and_then(|out| out.into_payload_with_ctx(2, &ctx, w, h))
        .map_err(|e| NodeError::Handler(e.to_string()))
}

/// Morphological pass to clean up the binary mask (dilation here).
#[node(
    id = "example.nodes.morph",
    inputs("img"),
    outputs("img"),
    compute(::daedalus::ComputeAffinity::GpuPreferred),
    shaders(MorphBindings)
)]
fn morph_node(
    img: Payload<DynamicImage>,
    ctx: ShaderContext,
) -> Result<Payload<DynamicImage>, NodeError> {
    let (w, h) = img.dimensions();
    let radius = 1u32;
    let norm = 1i32; // Manhattan kernel
    let op = 1u32; // dilation
    let params = MorphParams {
        width: w,
        height: h,
        radius,
        norm,
        op,
        _pad0: [0; 3],
        pad: [0; 4],
    };

    let bindings = MorphBindings {
        input: &img,
        output: TextureOut::from_input_ctx(&img, &ctx),
        params: Uniform::new(params),
    };

    ctx.single(&bindings)
        .dispatch_auto()
        .and_then(|out| out.into_payload_with_ctx(2, &ctx, w, h))
        .map_err(|e| NodeError::Handler(e.to_string()))
}

#[derive(Default)]
struct BinaryGpuState {
    stats: Option<GpuState<BinaryStats>>,
}

#[node(
    id = "example.nodes.binary_state",
    inputs("img"),
    outputs("img"),
    compute(::daedalus::ComputeAffinity::GpuPreferred),
    shaders(BinaryStateBindings),
    state(BinaryGpuState)
)]
fn adaptive_binary_node(
    img: Payload<DynamicImage>,
    state: &mut BinaryGpuState,
    ctx: ShaderContext,
) -> Result<Payload<DynamicImage>, NodeError> {
    let (w, h) = img.dimensions();
    let gpu = ctx
        .gpu
        .as_ref()
        .ok_or_else(|| NodeError::Handler("GPU required for binary_state node".into()))?;

    if state.stats.is_none() {
        state.stats = Some(
            GpuState::<BinaryStats>::new_stateful_with_gpu(gpu)
                .map_err(|e| NodeError::Handler(e.to_string()))?,
        );
    }
    let stats = state
        .stats
        .as_ref()
        .ok_or_else(|| NodeError::Handler("gpu state missing".into()))?;

    let params = BinaryParams {
        width: w,
        height: h,
        threshold: 0.0,
        _pad: 0.0,
    };
    let bindings = BinaryStateBindings {
        input: &img,
        output: TextureOut::from_input(&img, Some(gpu)),
        params: Uniform::new(params),
        stats,
    };

    let out = ctx
        .single(&bindings)
        .dispatch_auto()
        .map_err(|e| NodeError::Handler(e.to_string()))?;

    if let Some(stat_bytes) = out.buffers.get(&4)
        && stat_bytes.len() >= std::mem::size_of::<BinaryStats>()
    {
        let s =
            bytemuck::from_bytes::<BinaryStats>(&stat_bytes[..std::mem::size_of::<BinaryStats>()]);
        let _ = s.mean;
    }

    out.into_payload_with_ctx(2, &ctx, w, h)
        .map_err(|e| NodeError::Handler(e.to_string()))
}

#[node(id = "example.nodes.save", inputs("img"))]
fn save_image(exec: &ExecutionContext, img: Payload<DynamicImage>) -> Result<(), NodeError> {
    let (bytes, w, h) = img
        .to_rgba_bytes(exec.gpu.as_ref())
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    let path = format!(
        "{}/examples/assets/output_nodes.png",
        env!("CARGO_MANIFEST_DIR")
    );
    image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(w, h, bytes)
        .ok_or_else(|| NodeError::Handler("output dimensions mismatch".into()))?
        .save(&path)
        .map_err(|e| NodeError::Handler(e.to_string()))
}

fn register(reg: &mut Registry, handlers: &mut HandlerRegistry) {
    macro_rules! add {
        ($modname:ident) => {{
            reg.register_node($modname::descriptor())
                .expect("register node");
            handlers.merge($modname::handler_registry());
        }};
    }
    add!(load_image);
    add!(texture_sample_node);
    add!(blur_node);
    add!(edges_node);
    add!(adaptive_binary_node);
    add!(morph_node);
    add!(save_image);
}

fn build_graph(registry: &Registry) -> daedalus::planner::Graph {
    let load = load_image::handle().alias("load");
    let texture = texture_sample_node::handle().alias("texture");
    let blur = blur_node::handle().alias("blur");
    let edges = edges_node::handle().alias("edges");
    let binary = adaptive_binary_node::handle().alias("binary");
    let morph = morph_node::handle().alias("morph");
    let save = save_image::handle().alias("save");

    GraphBuilder::new(registry)
        .node(&load)
        .node(&texture)
        .node(&blur)
        .node(&edges)
        .node(&binary)
        .node(&morph)
        .node(&save)
        .connect(&load.outputs.img, &texture.inputs.img)
        .connect(&texture.outputs.img, &blur.inputs.img)
        .connect(&blur.outputs.img, &edges.inputs.img)
        .connect(&edges.outputs.img, &binary.inputs.img)
        .connect(&binary.outputs.img, &morph.inputs.img)
        .connect(&morph.outputs.img, &save.inputs.img)
        .build()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = Registry::new();
    let mut handlers = HandlerRegistry::new();
    register(&mut registry, &mut handlers);

    let graph = build_graph(&registry);

    let mut cfg = EngineConfig::default();
    cfg.planner.enable_gpu = true;
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.gpu = GpuBackend::Device;

    let engine = Engine::new(cfg)?;
    let result = engine.run(&registry, graph, handlers)?;
    println!(
        "gpu shader nodes pipeline wrote examples/assets/output_nodes.png, telemetry: {:?}",
        result.telemetry
    );
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins", feature = "gpu")))]
fn main() {
    eprintln!("enable `engine`, `plugins`, and `gpu-wgpu` features to run this example");
}
