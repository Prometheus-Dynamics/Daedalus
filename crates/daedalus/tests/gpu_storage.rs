//! A node keeps a `GpuStorage` in its `state(T)` on the engine's device across ticks: allocated
//! once, accumulated into by its own pipeline every tick, read back at the end. Skipped without a
//! GPU adapter.
#![allow(clippy::print_stderr)] // the skip reason
use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend},
    gpu::{GpuStorage, wgpu},
    macros::{node, plugin},
    runtime::{ExecutionContext, NodeError, plugins::PluginRegistry},
};

const TICKS: i64 = 100;

#[derive(Default)]
struct Volume {
    storage: Option<GpuStorage>,
    pipeline: Option<(wgpu::ComputePipeline, wgpu::BindGroup)>,
    first_buffer: Option<wgpu::Buffer>,
}

/// Adds 1 to the stored counter on the GPU; on the last tick reads it back (0 before).
#[node(
    id = "test.gpu_volume",
    inputs("tick"),
    outputs("value"),
    state(Volume)
)]
fn gpu_volume(tick: i64, ctx: &ExecutionContext, state: &mut Volume) -> Result<i64, NodeError> {
    let gpu = ctx
        .gpu
        .as_ref()
        .ok_or_else(|| NodeError::Handler("no GPU context".into()))?;
    let (device, queue) = gpu
        .wgpu()
        .ok_or_else(|| NodeError::Handler("not a wgpu backend".into()))?;
    if state.storage.is_none() {
        let storage = GpuStorage::new(gpu, 16, Some("volume"))
            .map_err(|err| NodeError::Handler(err.to_string()))?;
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: None,
            source: wgpu::ShaderSource::Wgsl(
                "@group(0) @binding(0) var<storage, read_write> v: array<u32>;
                 @compute @workgroup_size(1) fn main() { v[0] = v[0] + 1u; }"
                    .into(),
            ),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: None,
            layout: None,
            module: &module,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: None,
            layout: &pipeline.get_bind_group_layout(0),
            entries: &[wgpu::BindGroupEntry {
                binding: 0,
                resource: storage.buffer().as_entire_binding(),
            }],
        });
        state.first_buffer = Some(storage.buffer().clone());
        state.pipeline = Some((pipeline, bind_group));
        state.storage = Some(storage);
    }
    let storage = state.storage.as_ref().expect("allocated above");
    assert!(
        state.first_buffer.as_ref() == Some(storage.buffer()),
        "the buffer is reused across ticks"
    );
    let (pipeline, bind_group) = state.pipeline.as_ref().expect("built above");
    let mut encoder = device.create_command_encoder(&Default::default());
    {
        let mut pass = encoder.begin_compute_pass(&Default::default());
        pass.set_pipeline(pipeline);
        pass.set_bind_group(0, bind_group, &[]);
        pass.dispatch_workgroups(1, 1, 1);
    }
    if tick < TICKS {
        queue.submit([encoder.finish()]);
        return Ok(0);
    }
    let readback = device.create_buffer(&wgpu::BufferDescriptor {
        label: None,
        size: 4,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });
    encoder.copy_buffer_to_buffer(storage.buffer(), 0, &readback, 0, 4);
    queue.submit([encoder.finish()]);
    let slice = readback.slice(..);
    slice.map_async(wgpu::MapMode::Read, |_| {});
    let _ = device.poll(wgpu::PollType::Wait {
        submission_index: None,
        timeout: None,
    });
    let bytes = slice
        .get_mapped_range()
        .map_err(|err| NodeError::Handler(format!("{err:?}")))?;
    Ok(i64::from(u32::from_le_bytes(
        bytes[..4].try_into().unwrap(),
    )))
}

#[plugin(id = "test.gpu_storage", nodes(gpu_volume))]
struct GpuStoragePlugin;

#[test]
fn gpu_storage_in_node_state_persists_across_ticks() {
    let engine = match Engine::new(EngineConfig::gpu(GpuBackend::Device)) {
        Ok(engine) => engine,
        Err(err) => return eprintln!("skipping: no GPU engine ({err})"),
    };
    let mut registry = PluginRegistry::new();
    let plugin = GpuStoragePlugin::new();
    registry.install(&plugin).expect("install plugin");
    let volume = plugin.gpu_volume.alias("volume");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&volume)
        .expect("node")
        .try_connect("in", &volume.inputs.tick)
        .expect("input edge")
        .try_connect(&volume.outputs.value, "out")
        .expect("output edge")
        .build();
    let mut runtime = match engine.compile_registry(&registry, graph) {
        Ok(runtime) => runtime,
        Err(err) => return eprintln!("skipping: no GPU adapter ({err})"),
    };
    let mut last = None;
    for tick in 1..=TICKS {
        last = runtime
            .run_direct_once::<_, i64>("in", "out", tick)
            .expect("tick");
    }
    assert_eq!(
        last,
        Some(TICKS),
        "one increment per tick on the same buffer"
    );
}
