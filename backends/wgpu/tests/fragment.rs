//! Fragment passes on a real device: every target format, inputs read at the pass's own pixel,
//! rectangles, and what is refused.

mod common;

use daedalus_wgpu::fragment::{FragmentError, FragmentPass, FragmentRects};
use daedalus_wgpu::{Format, Options, Storage, Texture, TextureDesc, WgpuBackend};

fn private() -> Option<WgpuBackend> {
    let record = common::adapter()?;
    Some(WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }))
}

fn texture(backend: &WgpuBackend, format: Format, width: u32, height: u32) -> Texture {
    let desc = TextureDesc {
        format,
        width,
        height,
    };
    Texture::new(backend, desc).expect("a texture")
}

fn read(texture: &Texture) -> Vec<u8> {
    let desc = texture.desc();
    let mut out = vec![0; (desc.width * desc.height * desc.format.bytes_per_pixel()) as usize];
    texture.read_into(&mut out).expect("a readback");
    out
}

/// The WGSL type a fragment writes for a format, and whether it is an integer one.
fn output_of(format: Format) -> (&'static str, bool) {
    match format {
        Format::R8 | Format::Rg8 | Format::Rgba8 | Format::Bgra8 | Format::R32F | Format::Rg32F => {
            ("vec4<f32>", false)
        }
        Format::R8Uint | Format::Rgba16Uint | Format::Rg32Uint => ("vec4<u32>", true),
    }
}

#[test]
fn every_target_format_renders_and_reads_back() {
    let Some(backend) = private() else { return };
    for format in Format::ALL {
        let (ty, integer) = output_of(format);
        // Each pixel writes its own x and y, so a wrong pixel is a wrong value.
        let value = if integer {
            "vec4<u32>(u32(pos.x), u32(pos.y), 7u, 9u)"
        } else {
            "vec4<f32>(pos.x / 16.0, pos.y / 16.0, 0.25, 1.0)"
        };
        let wgsl = format!(
            "@fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) {ty} {{ return {value}; }}"
        );
        let pass = FragmentPass::new(&backend, &wgsl, &[format]).expect("a pass");
        assert_eq!(pass.targets(), [format]);
        let target = texture(&backend, format, 5, 3);
        let binding = pass.bind(&[], &[]).expect("nothing to bind");
        pass.run_bound(&binding, &[&target])
            .expect("draws")
            .wait()
            .expect("finishes");
        let bytes = read(&target);
        let pixel = format.bytes_per_pixel() as usize;
        for (at, got) in bytes.chunks(pixel).enumerate() {
            let (x, y) = ((at % 5) as u32, (at / 5) as u32);
            // The position of a pixel is its centre.
            let (fx, fy) = ((x as f32 + 0.5) / 16.0, (y as f32 + 0.5) / 16.0);
            let want: Vec<u8> = match format {
                Format::R8 => vec![(fx * 255.0).round() as u8],
                Format::Rg8 => vec![(fx * 255.0).round() as u8, (fy * 255.0).round() as u8],
                Format::Bgra8 => {
                    vec![
                        64,
                        (fy * 255.0).round() as u8,
                        (fx * 255.0).round() as u8,
                        255,
                    ]
                }
                Format::Rgba8 => {
                    vec![
                        (fx * 255.0).round() as u8,
                        (fy * 255.0).round() as u8,
                        64,
                        255,
                    ]
                }
                Format::R32F => fx.to_le_bytes().to_vec(),
                Format::Rg32F => [fx.to_le_bytes(), fy.to_le_bytes()].concat(),
                Format::R8Uint => vec![x as u8],
                Format::Rgba16Uint => [x as u16, y as u16, 7, 9]
                    .iter()
                    .flat_map(|word| word.to_le_bytes())
                    .collect(),
                Format::Rg32Uint => [x.to_le_bytes(), y.to_le_bytes()].concat(),
            };
            assert_eq!(got, want, "{format:?} at ({x}, {y})");
        }
    }
}

const DOUBLE: &str = "
    @group(0) @binding(0) var src: texture_2d<u32>;
    @group(0) @binding(1) var<storage, read> add: array<u32>;
    @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<u32> {
        let here = textureLoad(src, vec2<i32>(pos.xy), 0);
        return vec4<u32>(here.x * 2u + add[0], here.y, 0u, 0u);
    }
";

fn pairs(bytes: &[u8]) -> Vec<(u32, u32)> {
    bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|pair| {
            let (x, y) = pair.split_at(4);
            (
                u32::from_le_bytes(x.try_into().expect("four bytes")),
                u32::from_le_bytes(y.try_into().expect("four bytes")),
            )
        })
        .collect()
}

#[test]
fn pass_reads_inputs_at_its_own_pixel() {
    let Some(backend) = private() else { return };
    let (width, height) = (7, 4);
    let source = texture(&backend, Format::Rg32Uint, width, height);
    let values: Vec<(u32, u32)> = (0..width * height)
        .map(|at| (at * 3 + 1, 1000 + at))
        .collect();
    let bytes: Vec<u8> = values
        .iter()
        .flat_map(|(x, y)| [x.to_le_bytes(), y.to_le_bytes()].concat())
        .collect();
    source.write(&bytes, width * 8).expect("valid bytes");
    let add = Storage::new(&backend, 4).expect("a buffer");
    add.write(0, &5u32.to_le_bytes()).expect("in range");

    let pass = FragmentPass::new(&backend, DOUBLE, &[Format::Rg32Uint]).expect("a pass");
    let target = texture(&backend, Format::Rg32Uint, width, height);
    let binding = pass.bind(&[&source], &[&add]).expect("binds");
    pass.run_bound(&binding, &[&target])
        .expect("draws")
        .wait()
        .expect("finishes");
    let want: Vec<(u32, u32)> = values.iter().map(|&(x, y)| (x * 2 + 5, y)).collect();
    assert_eq!(pairs(&read(&target)), want);
}

#[test]
fn a_binding_is_reused_across_encodes() {
    let Some(backend) = private() else { return };
    let source = texture(&backend, Format::Rg32Uint, 4, 4);
    let add = Storage::new(&backend, 4).expect("a buffer");
    let pass = FragmentPass::new(&backend, DOUBLE, &[Format::Rg32Uint]).expect("a pass");
    let target = texture(&backend, Format::Rg32Uint, 4, 4);
    // Bound once. What the inputs hold when each draw runs is what that draw reads.
    let binding = pass.bind(&[&source], &[&add]).expect("binds");
    for round in 1..=3u32 {
        let bytes: Vec<u8> = (0..16)
            .flat_map(|_| [round.to_le_bytes(), [0; 4]].concat())
            .collect();
        source.write(&bytes, 32).expect("valid bytes");
        add.write(0, &(round * 100).to_le_bytes())
            .expect("in range");
        pass.run_bound(&binding, &[&target])
            .expect("draws")
            .wait()
            .expect("finishes");
        assert!(
            pairs(&read(&target))
                .iter()
                .all(|&pair| pair == (round * 2 + round * 100, 0))
        );
    }
    // Two draws recorded into one encoder are one submit.
    let before = backend.shader_stats().expect("open");
    let other = texture(&backend, Format::Rg32Uint, 4, 4);
    let mut encoder = pass.encoder();
    pass.encode_bound(&mut encoder, &binding, &[&target])
        .expect("records");
    pass.encode_bound(&mut encoder, &binding, &[&other])
        .expect("records");
    pass.submit(encoder).wait().expect("finishes");
    let after = backend.shader_stats().expect("open");
    assert_eq!(
        (after.submits - before.submits, after.passes - before.passes),
        (1, 2)
    );
    assert_eq!(read(&other), read(&target));
}

const FILL: &str = "
    @fragment fn fs() -> @location(0) vec4<u32> { return vec4<u32>(9u, 0u, 0u, 0u); }
";

#[test]
fn rects_draw_only_their_pixels() {
    let Some(backend) = private() else { return };
    let (width, height) = (8u32, 6u32);
    let pass = FragmentPass::new(&backend, FILL, &[Format::R8Uint]).expect("a pass");
    let target = texture(&backend, Format::R8Uint, width, height);
    let binding = pass.bind(&[], &[]).expect("nothing to bind");
    let mut rects = FragmentRects::new(&backend, 4).expect("rectangles");
    assert_eq!(
        (rects.capacity(), rects.len(), rects.is_empty()),
        (4, 0, true)
    );
    let drawn = [[1, 1, 2, 2], [5, 0, 3, 1], [0, 5, 8, 1]];
    rects
        .write((width, height), &drawn)
        .expect("inside the target");
    assert_eq!(rects.len(), 3);

    let inside = |x: u32, y: u32, list: &[[u32; 4]]| {
        list.iter()
            .any(|&[rx, ry, w, h]| x >= rx && x < rx + w && y >= ry && y < ry + h)
    };
    let draw = |rects: &FragmentRects, load: bool| {
        let mut encoder = pass.encoder();
        pass.encode_rects(&mut encoder, &binding, rects, &[&target], load)
            .expect("records");
        pass.submit(encoder).wait().expect("finishes");
        read(&target)
    };
    // Cleared first: the rectangles and nothing else.
    for (at, &got) in draw(&rects, false).iter().enumerate() {
        let (x, y) = (at as u32 % width, at as u32 / width);
        assert_eq!(got, if inside(x, y, &drawn) { 9 } else { 0 }, "({x}, {y})");
    }
    // Loaded: what was there stays, and the new rectangle is added.
    rects
        .write((width, height), &[[7, 3, 1, 1]])
        .expect("inside the target");
    for (at, &got) in draw(&rects, true).iter().enumerate() {
        let (x, y) = (at as u32 % width, at as u32 / width);
        let want = inside(x, y, &drawn) || (x, y) == (7, 3);
        assert_eq!(got, if want { 9 } else { 0 }, "({x}, {y})");
    }
    // No rectangles and a clear is an empty target.
    rects.write((width, height), &[]).expect("none");
    assert!(draw(&rects, false).iter().all(|&byte| byte == 0));

    let refused = |result: Result<(), FragmentError>, text: &str| {
        assert!(
            matches!(&result, Err(FragmentError::Invalid(why)) if why.contains(text)),
            "{result:?}"
        );
    };
    refused(
        rects.write((width, height), &[[7, 0, 2, 1]]),
        "leaves the 8x6 target",
    );
    refused(rects.write((width, height), &[[0; 4]; 5]), "room for 4");
    rects
        .write((4, 4), &[[0, 0, 1, 1]])
        .expect("inside a smaller target");
    let mut encoder = pass.encoder();
    refused(
        pass.encode_rects(&mut encoder, &binding, &rects, &[&target], false),
        "written for a (4, 4) target",
    );
}

#[test]
fn invalid_passes_are_reported() {
    let Some(backend) = private() else { return };
    let invalid = |result: Result<FragmentPass, FragmentError>| {
        assert!(
            matches!(result, Err(FragmentError::Invalid(_))),
            "it was accepted"
        );
    };
    invalid(FragmentPass::new(&backend, FILL, &[]));
    invalid(FragmentPass::new(
        &backend,
        "this isn't WGSL",
        &[Format::R8Uint],
    ));
    // The function has to be `fs`, and write what the target takes.
    invalid(FragmentPass::new(
        &backend,
        "@fragment fn other() -> @location(0) vec4<u32> { return vec4<u32>(); }",
        &[Format::R8Uint],
    ));
    invalid(FragmentPass::new(&backend, FILL, &[Format::Rgba8]));

    let pass = FragmentPass::new(&backend, DOUBLE, &[Format::Rg32Uint]).expect("a pass");
    let source = texture(&backend, Format::Rg32Uint, 4, 4);
    let add = Storage::new(&backend, 4).expect("a buffer");
    // A binding the shader declares and nothing is bound to, and a texture of the wrong kind.
    assert!(matches!(
        pass.bind(&[&source], &[]),
        Err(FragmentError::Invalid(_))
    ));
    let floats = texture(&backend, Format::Rgba8, 4, 4);
    assert!(matches!(
        pass.bind(&[&floats], &[&add]),
        Err(FragmentError::Invalid(_))
    ));
    // Targets of the wrong format, or of two sizes.
    let binding = pass.bind(&[&source], &[&add]).expect("binds");
    let wrong = pass.run_bound(&binding, &[&floats]).map(drop);
    assert!(matches!(wrong, Err(FragmentError::Invalid(why)) if why.contains("the pass writes")));
    let two = FragmentPass::new(
        &backend,
        "struct Out { @location(0) a: vec4<u32>, @location(1) b: vec4<u32> }
         @fragment fn fs() -> Out { return Out(vec4<u32>(1u), vec4<u32>(2u)); }",
        &[Format::R8Uint, Format::R8Uint],
    )
    .expect("a pass with two targets");
    let (small, large) = (
        texture(&backend, Format::R8Uint, 2, 2),
        texture(&backend, Format::R8Uint, 4, 4),
    );
    let nothing = two.bind(&[], &[]).expect("nothing to bind");
    let sizes = two.run_bound(&nothing, &[&small, &large]).map(drop);
    assert!(matches!(sizes, Err(FragmentError::Invalid(why)) if why.contains("one size")));
    let twin = texture(&backend, Format::R8Uint, 2, 2);
    two.run_bound(&nothing, &[&small, &twin])
        .expect("draws")
        .wait()
        .expect("finishes");
    assert_eq!((read(&small), read(&twin)), (vec![1; 4], vec![2; 4]));
}
