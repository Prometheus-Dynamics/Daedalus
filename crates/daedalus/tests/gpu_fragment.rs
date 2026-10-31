//! A fragment-pass node with two render targets (multiple render targets) keeps its pass and
//! textures in `state(T)` and matches its CPU pair. Skipped without a GPU adapter.
#![allow(clippy::print_stderr)] // the skip reason
use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend},
    gpu::{FragmentPass, GpuStorage, GpuTexture, wgpu},
    macros::{node, plugin},
    runtime::{ExecutionContext, NodeError, plugins::PluginRegistry},
};

const SIZE: (u32, u32) = (61, 37);

/// Target 0: the 3x3 box mean of the input (clamped at the edges). Target 1: the input times
/// `params.scale`, and the input plus `params.offset`.
const FRAGMENT: &str = "
struct Params { scale: f32, offset: f32 }
@group(0) @binding(0) var src: texture_2d<f32>;
@group(0) @binding(1) var<storage, read> params: Params;
struct Out { @location(0) mean: vec4<f32>, @location(1) pair: vec4<f32> }
@fragment fn fs(@builtin(position) pos: vec4<f32>) -> Out {
    let c = vec2<i32>(pos.xy);
    let size = vec2<i32>(textureDimensions(src));
    var acc = 0.0;
    for (var dy = -1; dy <= 1; dy++) {
        for (var dx = -1; dx <= 1; dx++) {
            acc += textureLoad(src, clamp(c + vec2<i32>(dx, dy), vec2<i32>(0), size - 1), 0).x;
        }
    }
    let v = textureLoad(src, c, 0).x;
    var out: Out;
    out.mean = vec4<f32>(acc / 9.0);
    out.pair = vec4<f32>(v * params.scale, v + params.offset, 0.0, 0.0);
    return out;
}";

fn input() -> Vec<f32> {
    (0..SIZE.0 * SIZE.1)
        .map(|i| ((i * 37) % 101) as f32 / 7.0)
        .collect()
}

/// The CPU pair: the same two outputs.
fn cpu(src: &[f32], scale: f32, offset: f32) -> (Vec<f32>, Vec<f32>) {
    let (w, h) = (SIZE.0 as i32, SIZE.1 as i32);
    let at = |x: i32, y: i32| src[(y.clamp(0, h - 1) * w + x.clamp(0, w - 1)) as usize];
    let mut mean = Vec::new();
    let mut pair = Vec::new();
    for y in 0..h {
        for x in 0..w {
            let acc: f32 = (-1..=1)
                .flat_map(|dy| (-1..=1).map(move |dx| (dx, dy)))
                .map(|(dx, dy)| at(x + dx, y + dy))
                .sum();
            mean.push(acc / 9.0);
            pair.extend([at(x, y) * scale, at(x, y) + offset]);
        }
    }
    (mean, pair)
}

struct Pass {
    pass: FragmentPass,
    src: GpuTexture,
    params: GpuStorage,
    mean: GpuTexture,
    pair: GpuTexture,
}

#[derive(Default)]
struct State {
    pass: Option<Pass>,
}

fn to_bytes(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn from_bytes(bytes: &[u8]) -> Vec<f32> {
    let (texels, _) = bytes.as_chunks::<4>();
    texels.iter().map(|b| f32::from_le_bytes(*b)).collect()
}

fn gpu_err(err: daedalus::gpu::GpuError) -> NodeError {
    NodeError::Handler(err.to_string())
}

#[node(
    id = "test.fragment_pair",
    inputs("scale"),
    outputs("mean", "pair"),
    state(State)
)]
fn fragment_pair(
    scale: f64,
    ctx: &ExecutionContext,
    state: &mut State,
) -> Result<(Vec<f32>, Vec<f32>), NodeError> {
    let gpu = ctx
        .gpu
        .as_ref()
        .ok_or_else(|| NodeError::Handler("no GPU".into()))?;
    if state.pass.is_none() {
        let pass = FragmentPass::new(
            gpu,
            FRAGMENT,
            &[
                wgpu::TextureFormat::R32Float,
                wgpu::TextureFormat::Rg32Float,
            ],
        )
        .map_err(gpu_err)?;
        let src = GpuTexture::new(gpu, SIZE, wgpu::TextureFormat::R32Float, Some("src"))
            .map_err(gpu_err)?;
        src.write(&to_bytes(&input())).map_err(gpu_err)?;
        state.pass = Some(Pass {
            pass,
            src,
            params: GpuStorage::new(gpu, 8, Some("params")).map_err(gpu_err)?,
            mean: GpuTexture::new(gpu, SIZE, wgpu::TextureFormat::R32Float, None)
                .map_err(gpu_err)?,
            pair: GpuTexture::new(gpu, SIZE, wgpu::TextureFormat::Rg32Float, None)
                .map_err(gpu_err)?,
        });
    }
    let p = state.pass.as_ref().expect("built above");
    p.params
        .write(0, &to_bytes(&[scale as f32, 0.5]))
        .map_err(gpu_err)?;
    p.pass
        .run(&[&p.src], &[&p.params], &[&p.mean, &p.pair])
        .map_err(gpu_err)?;
    let mut mean = vec![0u8; p.mean.len_bytes()];
    let mut pair = vec![0u8; p.pair.len_bytes()];
    p.mean.read_into(&mut mean).map_err(gpu_err)?;
    p.pair.read_into(&mut pair).map_err(gpu_err)?;
    let (mean, pair) = (from_bytes(&mean), from_bytes(&pair));
    Ok((mean, pair))
}

#[plugin(id = "test.gpu_fragment", nodes(fragment_pair))]
struct FragmentPlugin;

#[test]
fn fragment_pass_node_matches_its_cpu_pair() {
    let engine = match Engine::new(EngineConfig::gpu(GpuBackend::Device)) {
        Ok(engine) => engine,
        Err(err) => return eprintln!("skipping: no GPU engine ({err})"),
    };
    let mut registry = PluginRegistry::new();
    let plugin = FragmentPlugin::new();
    registry.install(&plugin).expect("install plugin");
    let node = plugin.fragment_pair.alias("frag");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&node)
        .expect("node")
        .try_connect("scale", &node.inputs.scale)
        .expect("input edge")
        .try_connect(&node.outputs.mean, "mean")
        .expect("mean edge")
        .try_connect(&node.outputs.pair, "pair")
        .expect("pair edge")
        .build();
    let mut runtime = match engine.compile_registry(&registry, graph) {
        Ok(runtime) => runtime,
        Err(err) => return eprintln!("skipping: no GPU adapter ({err})"),
    };
    let src = input();
    // Two ticks: the second reuses the pass and textures with new parameters.
    for scale in [2.0f64, -3.0] {
        runtime.push("scale", scale);
        runtime.tick().expect("tick");
        let mean = runtime.drain_owned::<Vec<f32>>("mean").expect("mean");
        let pair = runtime.drain_owned::<Vec<f32>>("pair").expect("pair");
        let (want_mean, want_pair) = cpu(&src, scale as f32, 0.5);
        let close = |a: &[f32], b: &[f32]| {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(x, y)| (x - y).abs() <= 1e-4 * y.abs().max(1.0))
        };
        assert_eq!(mean.len(), 1);
        assert!(close(&mean[0], &want_mean), "mean differs at scale {scale}");
        assert!(close(&pair[0], &want_pair), "pair differs at scale {scale}");
    }
}
