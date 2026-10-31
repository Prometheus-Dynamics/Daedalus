//! A GPU node in the shape `docs/gpu-nodes.md` describes: a 3x3 box filter whose device is a
//! per-node config value the consumer chooses (`device = "gpu"` or `"cpu"`). The GPU path keeps
//! its fragment pass, textures and binding in the node's state, submits without waiting and
//! emits each result on a later tick. Without a GPU (no `gpu-wgpu` build, or no adapter) the node
//! runs on the CPU and says so. Either way the outputs match the CPU reference.
//!
//! ```text
//! cargo run -p daedalus-examples --bin gpu_fragment_node                        # CPU
//! cargo run -p daedalus-examples --features gpu-wgpu --bin gpu_fragment_node -- gpu
//! ```
#![allow(clippy::print_stdout)] // the example reports what ran

use daedalus::{
    data::model::Value,
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{ExecutionContext, NodeError, plugins::PluginRegistry},
};

const SIZE: (u32, u32) = (64, 48);
const FRAMES: usize = 8;

/// The CPU reference and fallback: 3x3 box mean, edges clamped.
fn box3_cpu(frame: &[u8]) -> Vec<u8> {
    let (w, h) = (SIZE.0 as i32, SIZE.1 as i32);
    let at =
        |x: i32, y: i32| u32::from(frame[(y.clamp(0, h - 1) * w + x.clamp(0, w - 1)) as usize]);
    (0..h)
        .flat_map(|y| (0..w).map(move |x| (x, y)))
        .map(|(x, y)| {
            let sum: u32 = (-1..=1)
                .flat_map(|dy| (-1..=1).map(move |dx| (dx, dy)))
                .map(|(dx, dy)| at(x + dx, y + dy))
                .sum();
            ((sum + 4) / 9) as u8
        })
        .collect()
}

#[cfg(feature = "gpu-wgpu")]
mod gpu {
    use daedalus::gpu::{
        FragmentBinding, FragmentPass, GpuContextHandle, GpuError, GpuInFlight, GpuReadback,
        GpuTexture, wgpu,
    };

    use super::SIZE;

    /// Same filter as a fragment pass: on tile-based GPUs (V3D, Mali) the fast form. Integer
    /// math on an r8uint texture keeps it bit-exact with the CPU path.
    const BOX3: &str = "
        @group(0) @binding(0) var src: texture_2d<u32>;
        @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<u32> {
            let c = vec2<i32>(pos.xy);
            let size = vec2<i32>(textureDimensions(src));
            var sum = 0u;
            for (var dy = -1; dy <= 1; dy++) {
                for (var dx = -1; dx <= 1; dx++) {
                    sum += textureLoad(src, clamp(c + vec2<i32>(dx, dy), vec2<i32>(0), size - 1), 0).x;
                }
            }
            return vec4<u32>((sum + 4u) / 9u);
        }";

    /// The node's GPU state: built once, reused every tick.
    pub struct Box3Gpu {
        pass: FragmentPass,
        src: GpuTexture,
        binding: FragmentBinding,
        targets: Vec<GpuTexture>,
        /// Frames in flight: their readbacks, each into its own target.
        in_flight: GpuInFlight<(usize, GpuReadback)>,
        next_target: usize,
    }

    impl Box3Gpu {
        pub fn new(gpu: &GpuContextHandle) -> Result<Self, GpuError> {
            let format = wgpu::TextureFormat::R8Uint;
            let pass = FragmentPass::new(gpu, BOX3, &[format])?;
            let src = GpuTexture::new(gpu, SIZE, format, Some("box3-src"))?;
            let binding = pass.bind(&[&src], &[])?;
            let targets = (0..2)
                .map(|_| GpuTexture::new(gpu, SIZE, format, Some("box3-dst")))
                .collect::<Result<_, _>>()?;
            Ok(Self {
                pass,
                src,
                binding,
                targets,
                in_flight: GpuInFlight::new(2),
                next_target: 0,
            })
        }

        /// Submit `frame` (never waits). `false` when two frames are already in flight.
        pub fn submit(&mut self, frame: &[u8]) -> Result<bool, GpuError> {
            if self.in_flight.is_full() {
                return Ok(false);
            }
            let index = self.next_target;
            self.next_target = (index + 1) % self.targets.len();
            self.src.write(frame)?;
            let ticket = self
                .pass
                .run_bound(&self.binding, &[&self.targets[index]])?;
            // The readback is queued after the pass, so it carries the pass's result.
            let readback = self.targets[index].start_read()?;
            let _ = self.in_flight.push(ticket, (index, readback));
            Ok(true)
        }

        /// The oldest finished frame, if any (never waits).
        pub fn poll(&mut self) -> Option<Result<Vec<u8>, GpuError>> {
            let (_, mut readback) = self.in_flight.pop_completed()?;
            let mut out = vec![0u8; readback.len()];
            loop {
                if let Some(result) = readback.try_read_into(&mut out) {
                    return Some(result.map(|()| out));
                }
                // The pass is done; the copy behind it lands within a poll or two.
                std::hint::spin_loop();
            }
        }
    }
}

#[derive(Default)]
struct Box3State {
    #[cfg(feature = "gpu-wgpu")]
    gpu: Option<gpu::Box3Gpu>,
    reported: bool,
}

/// `frame` is a new image, or empty to only collect results. `device` is the consumer's choice.
#[node(
    id = "example.box3",
    inputs("frame", "device"),
    outputs("out"),
    state(Box3State)
)]
fn box3(
    frame: Vec<u8>,
    device: String,
    ctx: &ExecutionContext,
    state: &mut Box3State,
) -> Result<Option<Vec<u8>>, NodeError> {
    if device == "gpu" {
        #[cfg(feature = "gpu-wgpu")]
        if let Some(handle) = ctx.gpu.as_ref().filter(|g| g.wgpu().is_some()) {
            let gpu_err = |e: daedalus::gpu::GpuError| NodeError::Handler(e.to_string());
            if state.gpu.is_none() {
                state.gpu = Some(gpu::Box3Gpu::new(handle).map_err(gpu_err)?);
            }
            let gpu = state.gpu.as_mut().expect("built above");
            if !frame.is_empty() && !gpu.submit(&frame).map_err(gpu_err)? {
                return Err(NodeError::Handler("two frames already in flight".into()));
            }
            return gpu.poll().transpose().map_err(gpu_err);
        }
        if !state.reported {
            println!("box3: GPU requested but unavailable (no adapter or no gpu-wgpu build): CPU");
            state.reported = true;
        }
    }
    let _ = ctx;
    Ok((!frame.is_empty()).then(|| box3_cpu(&frame)))
}

#[plugin(id = "example.gpu_fragment_node", nodes(box3))]
struct ExamplePlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let device = std::env::args().nth(1).unwrap_or_else(|| "cpu".into());
    let mut registry = PluginRegistry::new();
    let plugin = ExamplePlugin::new();
    registry.install(&plugin)?;
    let node = plugin.box3.alias("box3");
    let graph = registry
        .graph_builder()?
        .try_node(&node)?
        .const_input(
            &node.inputs.device,
            Some(Value::String(device.clone().into())),
        )
        .try_connect("frame", &node.inputs.frame)?
        .try_connect(&node.outputs.out, "out")?
        .build();
    // The engine's GPU backend when one is built and present; else the CPU engine.
    #[cfg(feature = "gpu-wgpu")]
    let engine = Engine::new(EngineConfig::gpu(daedalus::engine::GpuBackend::Device))
        .or_else(|_| Engine::new(EngineConfig::default()))?;
    #[cfg(not(feature = "gpu-wgpu"))]
    let engine = Engine::new(EngineConfig::default())?;
    let mut runtime = engine.compile_registry(&registry, graph)?;

    let frames: Vec<Vec<u8>> = (0..FRAMES)
        .map(|f| {
            (0..SIZE.0 * SIZE.1)
                .map(|i| ((i as usize * 31 + f * 17) % 256) as u8)
                .collect()
        })
        .collect();
    let mut results = Vec::new();
    let mut ticks = 0;
    for frame in &frames {
        runtime.push("frame", frame.clone());
        runtime.tick()?;
        ticks += 1;
        results.extend(runtime.drain_owned::<Vec<u8>>("out")?);
    }
    // Collect what is still in flight: ticks with no new frame.
    while results.len() < FRAMES && ticks < FRAMES + 10_000 {
        runtime.push("frame", Vec::<u8>::new());
        runtime.tick()?;
        ticks += 1;
        results.extend(runtime.drain_owned::<Vec<u8>>("out")?);
    }
    assert_eq!(results.len(), FRAMES, "every frame produced a result");
    for (frame, result) in frames.iter().zip(&results) {
        assert_eq!(result, &box3_cpu(frame), "matches the CPU reference");
    }
    println!("box3 on {device}: {FRAMES} frames in {ticks} ticks, all equal to the CPU reference");
    Ok(())
}
