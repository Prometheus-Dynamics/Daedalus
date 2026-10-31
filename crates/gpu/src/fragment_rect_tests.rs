//! `FragmentPass` bindings reused across encodes, rectangle draws, and integer formats (skipped
//! without a wgpu device).

use crate::{
    FragmentPass, FragmentRects, GpuBackendKind, GpuContextHandle, GpuError, GpuOptions,
    GpuTexture, select_backend,
};

fn wgpu_ctx() -> Option<GpuContextHandle> {
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .unwrap();
    ctx.wgpu().is_some().then_some(ctx)
}

fn floats(texture: &GpuTexture) -> Vec<f32> {
    let mut bytes = vec![0u8; texture.len_bytes()];
    texture.read_into(&mut bytes).unwrap();
    let (texels, _) = bytes.as_chunks::<4>();
    texels.iter().map(|b| f32::from_le_bytes(*b)).collect()
}

const ADD_ONE: &str = "
@group(0) @binding(0) var src: texture_2d<f32>;
@fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {
    return textureLoad(src, vec2<i32>(pos.xy), 0) + vec4<f32>(1.0);
}";

/// One binding, 150 encodes into one encoder, ping-ponging A and B: the result counts them.
#[test]
fn a_binding_is_reused_across_encodes() {
    let Some(ctx) = wgpu_ctx() else { return };
    let r32 = wgpu::TextureFormat::R32Float;
    let pass = FragmentPass::new(&ctx, ADD_ONE, &[r32]).unwrap();
    let a = GpuTexture::new(&ctx, (16, 8), r32, None).unwrap();
    let b = GpuTexture::new(&ctx, (16, 8), r32, None).unwrap();
    let from_a = pass.bind(&[&a], &[]).unwrap();
    let from_b = pass.bind(&[&b], &[]).unwrap();
    let (device, queue) = ctx.wgpu().unwrap();
    let mut encoder = device.create_command_encoder(&Default::default());
    for i in 0..150 {
        let (binding, target) = if i % 2 == 0 {
            (&from_a, &b)
        } else {
            (&from_b, &a)
        };
        pass.encode_bound(&mut encoder, binding, &[target]).unwrap();
    }
    queue.submit([encoder.finish()]);
    // 150 passes, the last into `a` (odd index 149 reads b, writes a).
    assert!(
        floats(&a).iter().all(|&v| v == 150.0),
        "{:?}",
        &floats(&a)[..4]
    );
    // A binding with the wrong resources is rejected once, at bind time.
    assert!(matches!(
        pass.bind(&[], &[])
            .map(|_| ())
            .and_then(|()| pass.bind(&[&a, &b], &[]).map(|_| ())),
        Err(GpuError::InvalidInput(_))
    ));
}

/// Rectangles redraw only their pixels; `load` keeps the rest, without it the rest is cleared.
#[test]
fn rects_draw_only_their_pixels() {
    let Some(ctx) = wgpu_ctx() else { return };
    let r32 = wgpu::TextureFormat::R32Float;
    let size = (64, 8);
    let constant = |v: f32| {
        format!(
            "@fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{ return vec4<f32>({v:.1}); }}"
        )
    };
    let fill = FragmentPass::new(&ctx, &constant(1.0), &[r32]).unwrap();
    let mark = FragmentPass::new(&ctx, &constant(2.0), &[r32]).unwrap();
    let target = GpuTexture::new(&ctx, size, r32, None).unwrap();
    let mut rects = FragmentRects::new(&ctx, 4).unwrap();
    // Two 8x8 tiles of the 64x8 atlas.
    rects.write(size, &[[8, 0, 8, 8], [40, 0, 8, 8]]).unwrap();
    let unbound = fill.bind(&[], &[]).unwrap();
    let (device, queue) = ctx.wgpu().unwrap();
    let inside = |x: usize| (8..16).contains(&x) || (40..48).contains(&x);
    for load in [true, false] {
        let mut encoder = device.create_command_encoder(&Default::default());
        fill.encode_bound(&mut encoder, &unbound, &[&target])
            .unwrap();
        mark.encode_rects(&mut encoder, &unbound, &rects, &[&target], load)
            .unwrap();
        queue.submit([encoder.finish()]);
        let values = floats(&target);
        for (i, &v) in values.iter().enumerate() {
            let want = if inside(i % 64) {
                2.0
            } else if load {
                1.0
            } else {
                0.0
            };
            assert_eq!(v, want, "texel {i}, load {load}");
        }
    }
    // Rectangles must fit the target, and match the targets they draw into.
    assert!(rects.write(size, &[[60, 0, 8, 8]]).is_err());
    assert!(rects.write(size, &[[0, 0, 1, 1]; 5]).is_err());
    rects.write((32, 8), &[[0, 0, 8, 8]]).unwrap();
    let mut encoder = device.create_command_encoder(&Default::default());
    assert!(matches!(
        mark.encode_rects(&mut encoder, &unbound, &rects, &[&target], true),
        Err(GpuError::InvalidInput(_))
    ));
}

/// Integer textures: `texture_2d<u32>` inputs and rgba16uint / rg32uint targets keep exact
/// values (the CPU's voxel encoding).
#[test]
fn integer_formats_round_trip_exactly() {
    let Some(ctx) = wgpu_ctx() else { return };
    let size = (8, 4);
    let rgba16 = wgpu::TextureFormat::Rgba16Uint;
    let rg32 = wgpu::TextureFormat::Rg32Uint;
    let src = GpuTexture::new(&ctx, size, rgba16, None).unwrap();
    let voxels: Vec<u16> = (0..8 * 4 * 4).map(|i| (i * 977 % 65536) as u16).collect();
    let bytes: Vec<u8> = voxels.iter().flat_map(|v| v.to_le_bytes()).collect();
    src.write(&bytes).unwrap();
    let pass = FragmentPass::new(
        &ctx,
        "@group(0) @binding(0) var src: texture_2d<u32>;
         struct Out { @location(0) same: vec4<u32>, @location(1) packed: vec4<u32> }
         @fragment fn fs(@builtin(position) pos: vec4<f32>) -> Out {
             let v = textureLoad(src, vec2<i32>(pos.xy), 0);
             var out: Out;
             out.same = v;
             out.packed = vec4<u32>(v.x | (v.y << 16u), v.z | (v.w << 16u), 0u, 0u);
             return out;
         }",
        &[rgba16, rg32],
    )
    .unwrap();
    let same = GpuTexture::new(&ctx, size, rgba16, None).unwrap();
    let packed = GpuTexture::new(&ctx, size, rg32, None).unwrap();
    pass.run(&[&src], &[], &[&same, &packed]).unwrap().wait();
    let mut out = vec![0u8; same.len_bytes()];
    same.read_into(&mut out).unwrap();
    assert_eq!(out, bytes);
    let mut out = vec![0u8; packed.len_bytes()];
    packed.read_into(&mut out).unwrap();
    let (words, _) = out.as_chunks::<4>();
    let words: Vec<u32> = words.iter().map(|b| u32::from_le_bytes(*b)).collect();
    for (texel, pair) in voxels.chunks(4).zip(words.chunks(2)) {
        assert_eq!(pair[0], u32::from(texel[0]) | (u32::from(texel[1]) << 16));
        assert_eq!(pair[1], u32::from(texel[2]) | (u32::from(texel[3]) << 16));
    }
}

/// A frame in a storage buffer (rows `stride` apart, as a camera writes them) copies into a
/// texture a fragment pass then samples; misaligned strides and short buffers are refused.
#[test]
fn storage_frames_copy_into_textures() {
    use crate::GpuStorage;
    let Some(ctx) = wgpu_ctx() else { return };
    let (width, height, stride) = (100u32, 6u32, 256u32);
    let frame: Vec<u8> = (0..stride * height)
        .map(|i| {
            if i % stride < width {
                (i % 251) as u8
            } else {
                0xEE
            }
        })
        .collect();
    let storage = GpuStorage::with_data(&ctx, &frame, None).unwrap();
    let r8 = wgpu::TextureFormat::R8Unorm;
    let texture = GpuTexture::new(&ctx, (width, height), r8, None).unwrap();
    texture
        .upload_from_storage(&storage, 0, stride)
        .unwrap()
        .wait();
    let mut out = vec![0u8; texture.len_bytes()];
    texture.read_into(&mut out).unwrap();
    let want: Vec<u8> = frame
        .chunks(stride as usize)
        .flat_map(|row| row[..width as usize].to_vec())
        .collect();
    assert_eq!(out, want, "rows de-strided, padding dropped");

    // The copied texture feeds a pass like any other input.
    let pass = FragmentPass::new(
        &ctx,
        "@group(0) @binding(0) var src: texture_2d<f32>;
         @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {
             return textureLoad(src, vec2<i32>(pos.xy), 0);
         }",
        &[r8],
    )
    .unwrap();
    let dst = GpuTexture::new(&ctx, (width, height), r8, None).unwrap();
    pass.run(&[&texture], &[], &[&dst]).unwrap().wait();
    dst.read_into(&mut out).unwrap();
    assert_eq!(out, want);

    // A stride that is not a multiple of 256 (a 640-wide GREY level's 640) is re-pitched.
    for (frame_w, frame_stride) in [(640u32, 640u32), (100, 132)] {
        let rows: Vec<u8> = (0..frame_stride * height)
            .map(|i| {
                if i % frame_stride < frame_w {
                    (i % 241) as u8
                } else {
                    0xEE
                }
            })
            .collect();
        let src = GpuStorage::with_data(&ctx, &rows, None).unwrap();
        let tex = GpuTexture::new(&ctx, (frame_w, height), r8, None).unwrap();
        for _ in 0..2 {
            tex.upload_from_storage(&src, 0, frame_stride)
                .unwrap()
                .wait();
        }
        let mut got = vec![0u8; tex.len_bytes()];
        tex.read_into(&mut got).unwrap();
        let want: Vec<u8> = rows
            .chunks(frame_stride as usize)
            .flat_map(|row| row[..frame_w as usize].to_vec())
            .collect();
        assert_eq!(got, want, "stride {frame_stride}");
    }
    let odd = GpuTexture::new(&ctx, (101, height), r8, None).unwrap();
    assert!(
        matches!(
            odd.upload_from_storage(&storage, 0, 130),
            Err(GpuError::InvalidInput(_))
        ),
        "re-pitching needs 4-byte rows"
    );

    for (offset, stride) in [(0, 99), (1 << 20, 256)] {
        assert!(
            matches!(
                texture.upload_from_storage(&storage, offset, stride),
                Err(GpuError::InvalidInput(_))
            ),
            "offset {offset}, stride {stride}"
        );
    }
}
