//! `FragmentPass` and `GpuTexture` against a real wgpu device (skipped without one).

use crate::{
    FragmentPass, GpuBackendKind, GpuContextHandle, GpuError, GpuOptions, GpuTexture,
    select_backend,
};

fn wgpu_ctx() -> Option<GpuContextHandle> {
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .unwrap();
    ctx.wgpu().is_some().then_some(ctx)
}

/// Every supported target format renders a constant that reads back exactly.
#[test]
fn every_target_format_renders_and_reads_back() {
    let Some(ctx) = wgpu_ctx() else { return };
    let size = (13, 7);
    let texels = 13 * 7;
    for (format, value, want) in [
        (wgpu::TextureFormat::R8Unorm, "vec4<f32>(1.0)", vec![255u8]),
        (
            wgpu::TextureFormat::Rgba8Unorm,
            "vec4<f32>(1.0, 0.0, 1.0, 1.0)",
            vec![255, 0, 255, 255],
        ),
        (
            wgpu::TextureFormat::R32Float,
            "vec4<f32>(2.5)",
            2.5f32.to_le_bytes().to_vec(),
        ),
        (
            wgpu::TextureFormat::Rg32Float,
            "vec4<f32>(1.5, -4.0, 0.0, 0.0)",
            [1.5f32.to_le_bytes(), (-4.0f32).to_le_bytes()].concat(),
        ),
    ] {
        let shader = format!(
            "@fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{ return {value}; }}"
        );
        let pass = FragmentPass::new(&ctx, &shader, &[format]).unwrap();
        let target = GpuTexture::new(&ctx, size, format, None).unwrap();
        let ticket = pass.run(&[], &[], &[&target]).unwrap();
        ticket.wait();
        assert!(ticket.try_complete().is_some());
        let mut out = vec![0u8; target.len_bytes()];
        target.read_into(&mut out).unwrap();
        assert_eq!(out.len(), texels * want.len(), "{format:?}");
        for texel in out.chunks(want.len()) {
            assert_eq!(texel, &want[..], "{format:?}");
        }
    }
}

/// A texture written from the CPU is read by the pass with `textureLoad` at the pixel it writes,
/// and a polled readback reuses its staging buffer.
#[test]
fn pass_reads_inputs_at_its_own_pixel() {
    let Some(ctx) = wgpu_ctx() else { return };
    let size = (300, 5); // rows wider than the 256-byte copy alignment
    let src = GpuTexture::new(&ctx, size, wgpu::TextureFormat::R8Unorm, None).unwrap();
    let data: Vec<u8> = (0..300 * 5).map(|i| (i % 251) as u8).collect();
    src.write(&data).unwrap();
    assert!(matches!(
        src.write(&data[1..]),
        Err(GpuError::InvalidInput(_))
    ));
    let pass = FragmentPass::new(
        &ctx,
        "@group(0) @binding(0) var src: texture_2d<f32>;
         @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {
             return textureLoad(src, vec2<i32>(pos.xy), 0);
         }",
        &[wgpu::TextureFormat::R8Unorm],
    )
    .unwrap();
    let dst = GpuTexture::new(&ctx, size, wgpu::TextureFormat::R8Unorm, None).unwrap();
    for _ in 0..3 {
        pass.run(&[&src], &[], &[&dst]).unwrap();
        let mut readback = dst.start_read().unwrap();
        let mut out = vec![0u8; dst.len_bytes()];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let result = loop {
            if let Some(result) = readback.try_read_into(&mut out) {
                break result;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "readback never arrived"
            );
            std::thread::yield_now();
        };
        result.unwrap();
        assert_eq!(out, data);
    }
    assert_eq!(dst.staging_buffers_created(), 1);
}

/// Bad shaders, empty or mismatched targets and unused bindings are errors, not panics.
#[test]
fn invalid_passes_are_reported() {
    let Some(ctx) = wgpu_ctx() else { return };
    let r32 = wgpu::TextureFormat::R32Float;
    let ok = "@fragment fn fs() -> @location(0) vec4<f32> { return vec4<f32>(1.0); }";
    assert!(matches!(
        FragmentPass::new(&ctx, "not wgsl", &[r32]),
        Err(GpuError::InvalidInput(_))
    ));
    assert!(matches!(
        FragmentPass::new(&ctx, ok, &[]),
        Err(GpuError::InvalidInput(_))
    ));
    let pass = FragmentPass::new(&ctx, ok, &[r32]).unwrap();
    let r8 = GpuTexture::new(&ctx, (4, 4), wgpu::TextureFormat::R8Unorm, None).unwrap();
    assert!(matches!(
        pass.run(&[], &[], &[&r8]),
        Err(GpuError::InvalidInput(_))
    ));
    let a = GpuTexture::new(&ctx, (4, 4), r32, None).unwrap();
    // The shader declares no bindings: passing an input is rejected.
    assert!(matches!(
        pass.run(&[&r8], &[], &[&a]),
        Err(GpuError::InvalidInput(_))
    ));
    assert!(matches!(
        GpuTexture::new(&ctx, (0, 4), r32, None),
        Err(GpuError::InvalidInput(_))
    ));
}

/// A timed fragment pass and a timed compute pass report a GPU time; heavier work takes longer.
#[test]
fn timers_report_gpu_time_per_pass() {
    use crate::{GpuTimeSource, GpuTimer};
    let Some(ctx) = wgpu_ctx() else { return };
    let timer = GpuTimer::new(&ctx).unwrap();
    let expected = if ctx.capabilities().compute.features.timestamps {
        GpuTimeSource::Timestamps
    } else {
        GpuTimeSource::SubmitToCompletion
    };
    assert_eq!(timer.source(), expected);

    let shader = |iterations: u32| {
        format!(
            "@fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{
                 var a = pos.x;
                 for (var k = 0u; k < {iterations}u; k++) {{ a = a * 0.9999 + 0.0001; }}
                 return vec4<f32>(a);
             }}"
        )
    };
    let target = GpuTexture::new(&ctx, (512, 512), wgpu::TextureFormat::R32Float, None).unwrap();
    let time = |iterations| {
        let pass =
            FragmentPass::new(&ctx, &shader(iterations), &[wgpu::TextureFormat::R32Float]).unwrap();
        let mut best = std::time::Duration::MAX;
        for _ in 0..3 {
            let mut timed = pass.run_timed(&timer, &[], &[], &[&target]).unwrap();
            assert_eq!(timed.source(), expected);
            best = best.min(timed.wait().unwrap());
        }
        best
    };
    let light = time(1);
    let heavy = time(4096);
    assert!(heavy > light, "heavy {heavy:?} vs light {light:?}");
    // Sequential measurements reuse one staging buffer.
    if expected == GpuTimeSource::Timestamps {
        assert_eq!(timer.staging_buffers_created(), 1);
    }

    // A compute pass, timed by the same timer.
    let (device, _) = ctx.wgpu().unwrap();
    let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: None,
        source: wgpu::ShaderSource::Wgsl(
            "@compute @workgroup_size(64) fn main() { var a = 1.0; for (var k = 0u; k < 1024u; k++) { a = a * 0.5 + 0.5; } }"
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
    let mut encoder = device.create_command_encoder(&Default::default());
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: None,
            timestamp_writes: timer.compute_writes(),
        });
        pass.set_pipeline(&pipeline);
        pass.dispatch_workgroups(256, 1, 1);
    }
    let mut timed = timer.submit(encoder);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let elapsed = loop {
        if let Some(result) = timed.try_elapsed() {
            break result.unwrap();
        }
        assert!(std::time::Instant::now() < deadline, "timing never arrived");
        std::thread::yield_now();
    };
    assert!(elapsed > std::time::Duration::ZERO);
}

/// The pipeline cache saves what Daedalus pipelines compiled, reloads it, and survives a corrupt
/// file.
#[test]
fn pipeline_cache_round_trips_through_its_file() {
    use crate::GpuPipelineCache;
    let Some(ctx) = wgpu_ctx() else { return };
    let dir = std::env::temp_dir().join(format!("daedalus-pipeline-cache-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cache = match GpuPipelineCache::open(&ctx, &dir) {
        Ok(cache) => cache,
        Err(GpuError::Unsupported) => return, // no Vulkan pipeline cache on this device
        Err(err) => panic!("open: {err}"),
    };
    assert_eq!(cache.loaded_bytes(), 0);
    cache.install();
    let (device, _) = ctx.wgpu().unwrap();
    assert!(crate::pipeline_cache_for(device).is_some());
    FragmentPass::new(
        &ctx,
        "@fragment fn fs() -> @location(0) vec4<f32> { return vec4<f32>(0.25); }",
        &[wgpu::TextureFormat::Rgba8Unorm],
    )
    .unwrap();
    let saved = cache.save().unwrap();
    assert!(saved > 0, "the driver saved pipeline data");
    assert!(cache.path().exists());

    let reopened = GpuPipelineCache::open(&ctx, &dir).unwrap();
    assert_eq!(reopened.loaded_bytes(), saved);

    std::fs::write(cache.path(), b"not a pipeline cache").unwrap();
    let fresh = GpuPipelineCache::open(&ctx, &dir).unwrap();
    fresh.install();
    FragmentPass::new(
        &ctx,
        "@fragment fn fs() -> @location(0) vec4<f32> { return vec4<f32>(0.5); }",
        &[wgpu::TextureFormat::Rgba8Unorm],
    )
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
