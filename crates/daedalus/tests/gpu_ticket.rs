//! A node submits long GPU work on its first tick and emits the result on a later tick, polling
//! its `GpuTicket` each tick: no tick waits on the fence. Skipped without a hardware GPU adapter.
#![allow(clippy::print_stderr)] // the skip reason
use std::time::{Duration, Instant};

use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend},
    gpu::{GpuStorage, GpuTicket, wgpu},
    macros::{node, plugin},
    runtime::{ExecutionContext, NodeError, plugins::PluginRegistry},
};

#[derive(Default)]
struct Pending {
    work: Option<(GpuTicket, GpuStorage)>,
    done: bool,
}

/// About 3e10 FLOP: tens of milliseconds on a desktop GPU, far longer than a tick's own work.
const HEAVY: &str = "
    @group(0) @binding(0) var<storage, read_write> out: array<f32>;
    @compute @workgroup_size(64) fn main(@builtin(global_invocation_id) id: vec3<u32>,
                                         @builtin(num_workgroups) n: vec3<u32>) {
        let i = id.x + id.y * n.x * 64u;
        var a = f32(i);
        for (var k = 0u; k < 16384u; k++) { a = a * 0.9999 + 0.0001; }
        if i < arrayLength(&out) { out[i] = a; }
    }";

/// Outputs 0 while the GPU works, 1 on the tick it sees the work finished.
#[node(
    id = "test.gpu_ticket",
    inputs("tick"),
    outputs("done"),
    state(Pending)
)]
fn gpu_ticket(_tick: i64, ctx: &ExecutionContext, state: &mut Pending) -> Result<i64, NodeError> {
    let err = |e: daedalus::gpu::GpuError| NodeError::Handler(e.to_string());
    if state.done {
        return Ok(0);
    }
    if state.work.is_none() {
        let gpu = ctx
            .gpu
            .as_ref()
            .ok_or_else(|| NodeError::Handler("no GPU".into()))?;
        let (device, _) = gpu
            .wgpu()
            .ok_or_else(|| NodeError::Handler("not wgpu".into()))?;
        let elements = 1u32 << 20;
        let out = GpuStorage::new(gpu, u64::from(elements) * 4, Some("heavy")).map_err(err)?;
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: None,
            source: wgpu::ShaderSource::Wgsl(HEAVY.into()),
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
                resource: out.buffer().as_entire_binding(),
            }],
        });
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(elements / 64, 1, 1);
        }
        let ticket = GpuTicket::submit(gpu, [encoder.finish()]).map_err(err)?;
        state.work = Some((ticket, out));
    }
    let (ticket, _) = state.work.as_ref().expect("submitted above");
    if ticket.try_complete().is_some() {
        state.done = true;
        return Ok(1);
    }
    Ok(0)
}

#[plugin(id = "test.gpu_ticket", nodes(gpu_ticket))]
struct GpuTicketPlugin;

#[test]
fn a_tick_never_waits_for_the_gpu() {
    let engine = match Engine::new(EngineConfig::gpu(GpuBackend::Device)) {
        Ok(engine) => engine,
        Err(err) => return eprintln!("skipping: no GPU engine ({err})"),
    };
    let mut registry = PluginRegistry::new();
    let plugin = GpuTicketPlugin::new();
    registry.install(&plugin).expect("install plugin");
    let node = plugin.gpu_ticket.alias("ticket");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&node)
        .expect("node")
        .try_connect("in", &node.inputs.tick)
        .expect("input edge")
        .try_connect(&node.outputs.done, "out")
        .expect("output edge")
        .build();
    let mut runtime = match engine.compile_registry(&registry, graph) {
        Ok(runtime) => runtime,
        Err(err) => return eprintln!("skipping: no GPU adapter ({err})"),
    };
    let start = Instant::now();
    let mut slowest_tick = Duration::ZERO;
    let mut done_on = None;
    for tick in 1..=200_000_i64 {
        let tick_start = Instant::now();
        let done = runtime
            .run_direct_once::<_, i64>("in", "out", tick)
            .expect("tick");
        // The first tick also builds the pipeline; time only the polling ticks.
        if tick > 1 {
            slowest_tick = slowest_tick.max(tick_start.elapsed());
        }
        if done == Some(1) {
            done_on = Some(tick);
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "GPU work never finished"
        );
    }
    let total = start.elapsed();
    let done_on = done_on.expect("finished");
    assert!(
        done_on > 1,
        "the result arrives on a later tick than the submit"
    );
    assert!(
        slowest_tick * 4 < total,
        "a tick took {slowest_tick:?} of the GPU work's {total:?}: it waited"
    );
}
