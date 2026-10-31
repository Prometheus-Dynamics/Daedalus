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
