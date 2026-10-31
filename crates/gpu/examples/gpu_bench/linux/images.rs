//! The kernels of the `throughput` section and a box filter through image paths: storage images,
//! sampled textures read by a compute shader, and a full-screen fragment pass, against packed
//! storage buffers and the CPU. Tile-based GPUs (V3D, Mali) can be much faster through the
//! texture and render paths than through general buffer loads and stores.

use std::time::{Duration, Instant};

use daedalus_gpu::wgpu::{self, util::DeviceExt};

use super::{iters, kv, median, wait};

/// Box filter frame size (r8, like a grey camera frame).
const FRAME: (u32, u32) = (1280, 800);
/// saxpy / FMA image size: 4M texels, as the buffer kernels.
const FIELD: (u32, u32) = (2048, 2048);

const SAXPY_EXPR: &str = "2.5 * x + y";
/// 256 dependent FMAs over 4 chains, as the buffer `fma256` kernel.
const FMA_BODY: &str = "
    var f0 = x; var f1 = x + 1.0; var f2 = x + 2.0; var f3 = x + 3.0;
    for (var k = 0u; k < 64u; k++) {
        f0 = f0 * 0.999 + 0.001; f1 = f1 * 0.999 + 0.001;
        f2 = f2 * 0.999 + 0.001; f3 = f3 * 0.999 + 0.001;
    }
    let r = f0 + f1 + f2 + f3 + y * 0.0;";

/// A timed pass: bytes it moves (unique reads + writes) and flops, for the rates, and how to
/// build it (pipeline, bind group) into a runnable submit-and-wait.
struct Pass<'a> {
    name: String,
    bytes: f64,
    flops: f64,
    build: Box<dyn FnOnce() -> Box<dyn Fn()> + 'a>,
}

/// Build and run the pass once under a validation error scope (a format or access the device
/// lacks is reported, not fatal), then time it.
fn report(device: &wgpu::Device, pass: Pass<'_>) {
    let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
    let run = (pass.build)();
    run();
    if let Some(err) = pollster::block_on(scope.pop()) {
        let first = err.to_string();
        let first = first.split_whitespace().collect::<Vec<_>>().join(" ");
        return kv(&pass.name, format!("unsupported: {first}"));
    }
    let mut samples: Vec<Duration> = (0..iters().min(30))
        .map(|_| {
            let start = Instant::now();
            run();
            start.elapsed()
        })
        .collect();
    let t = median(&mut samples).as_secs_f64();
    let mut line = format!("{:.2} ms, {:.2} GB/s", t * 1e3, pass.bytes / t / 1e9);
    if pass.flops > 0.0 {
        line.push_str(&format!(", {:.2} GFLOP/s", pass.flops / t / 1e9));
    }
    kv(&pass.name, line);
}

fn texture(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    (width, height): (u32, u32),
    format: wgpu::TextureFormat,
    usage: wgpu::TextureUsages,
    contents: Option<&[u8]>,
) -> wgpu::Texture {
    let desc = wgpu::TextureDescriptor {
        label: None,
        size: wgpu::Extent3d {
            width,
            height,
            depth_or_array_layers: 1,
        },
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format,
        usage,
        view_formats: &[],
    };
    match contents {
        Some(data) => device.create_texture_with_data(
            queue,
            &desc,
            wgpu::util::TextureDataOrder::LayerMajor,
            data,
        ),
        None => device.create_texture(&desc),
    }
}

fn view(texture: &wgpu::Texture) -> wgpu::TextureView {
    texture.create_view(&Default::default())
}

/// A compute pass over `size` with 8x8 workgroups, its bindings in order.
fn compute(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    wgsl: &str,
    resources: Vec<wgpu::BindingResource<'_>>,
    (width, height): (u32, u32),
) -> Box<dyn Fn()> {
    let pipeline = super::pipeline(device, wgsl);
    let entries: Vec<wgpu::BindGroupEntry<'_>> = resources
        .into_iter()
        .enumerate()
        .map(|(i, resource)| wgpu::BindGroupEntry {
            binding: i as u32,
            resource,
        })
        .collect();
    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: None,
        layout: &pipeline.get_bind_group_layout(0),
        entries: &entries,
    });
    let (device, queue) = (device.clone(), queue.clone());
    let groups = (width.div_ceil(8), height.div_ceil(8));
    Box::new(move || {
        super::run_once(&device, &queue, &pipeline, &bind_group, groups);
    })
}

/// A full-screen fragment pass into `target`, the fragment shader reading `resources`.
fn fragment(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    fragment_wgsl: &str,
    resources: Vec<wgpu::BindingResource<'_>>,
    target: &wgpu::Texture,
) -> Box<dyn Fn()> {
    let wgsl = format!(
        "@vertex fn vs(@builtin(vertex_index) i: u32) -> @builtin(position) vec4<f32> {{
             let p = vec2<f32>(f32((i << 1u) & 2u), f32(i & 2u));
             return vec4<f32>(p * 2.0 - 1.0, 0.0, 1.0);
         }}
         {fragment_wgsl}"
    );
    let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("gpu_bench fragment"),
        source: wgpu::ShaderSource::Wgsl(wgsl.into()),
    });
    let pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
        label: Some("gpu_bench fragment"),
        layout: None,
        vertex: wgpu::VertexState {
            module: &module,
            entry_point: Some("vs"),
            compilation_options: Default::default(),
            buffers: &[],
        },
        fragment: Some(wgpu::FragmentState {
            module: &module,
            entry_point: Some("fs"),
            compilation_options: Default::default(),
            targets: &[Some(target.format().into())],
        }),
        primitive: Default::default(),
        depth_stencil: None,
        multisample: Default::default(),
        multiview_mask: None,
        cache: None,
    });
    let entries: Vec<wgpu::BindGroupEntry<'_>> = resources
        .into_iter()
        .enumerate()
        .map(|(i, resource)| wgpu::BindGroupEntry {
            binding: i as u32,
            resource,
        })
        .collect();
    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: None,
        layout: &pipeline.get_bind_group_layout(0),
        entries: &entries,
    });
    let target = view(target);
    let (device, queue) = (device.clone(), queue.clone());
    Box::new(move || {
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: None,
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &target,
                    depth_slice: None,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: None,
                timestamp_writes: None,
                occlusion_query_set: None,
                multiview_mask: None,
            });
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.draw(0..3, 0..1);
        }
        queue.submit([encoder.finish()]);
        wait(&device);
    })
}

/// WGSL summing a `(2r+1)^2` neighbourhood of `load(p)` (clamped to `size`) into `acc`.
fn box_sum(radius: i32, load: &str) -> String {
    format!(
        "var acc = vec4<f32>(0.0);
         for (var dy = -{radius}; dy <= {radius}; dy++) {{
             for (var dx = -{radius}; dx <= {radius}; dx++) {{
                 let p = clamp(c + vec2<i32>(dx, dy), vec2<i32>(0), size - 1);
                 acc += {load};
             }}
         }}
         let mean = acc / {taps}.0;",
        taps = (2 * radius + 1) * (2 * radius + 1)
    )
}

/// The box filter, saxpy and FMA kernels through every path; skipped paths say why.
pub(super) fn image_paths(device: &wgpu::Device, queue: &wgpu::Queue) {
    // r8unorm is not a storage format in WebGPU: sampled and rendered only.
    let usage = wgpu::TextureUsages::TEXTURE_BINDING
        | wgpu::TextureUsages::RENDER_ATTACHMENT
        | wgpu::TextureUsages::COPY_DST;
    let (w, h) = FRAME;
    let pixels = f64::from(w * h);
    let grey: Vec<u8> = (0..w * h).map(|i| (i % 251) as u8).collect();
    let rgba: Vec<u8> = grey.iter().flat_map(|&g| [g, g, g, 255]).collect();
    let grey_f32: Vec<f32> = grey.iter().map(|&g| f32::from(g) / 255.0).collect();
    let r8 = texture(
        device,
        queue,
        FRAME,
        wgpu::TextureFormat::R8Unorm,
        usage,
        Some(&grey),
    );
    let r8_out = texture(
        device,
        queue,
        FRAME,
        wgpu::TextureFormat::R8Unorm,
        usage,
        None,
    );
    let rgba8_storage = wgpu::TextureUsages::TEXTURE_BINDING
        | wgpu::TextureUsages::STORAGE_BINDING
        | wgpu::TextureUsages::RENDER_ATTACHMENT;
    let rgba8 = texture(
        device,
        queue,
        FRAME,
        wgpu::TextureFormat::Rgba8Unorm,
        rgba8_storage | wgpu::TextureUsages::COPY_DST,
        Some(&rgba),
    );
    let rgba8_out = texture(
        device,
        queue,
        FRAME,
        wgpu::TextureFormat::Rgba8Unorm,
        rgba8_storage,
        None,
    );
    let r32f = texture(
        device,
        queue,
        FRAME,
        wgpu::TextureFormat::R32Float,
        wgpu::TextureUsages::TEXTURE_BINDING
            | wgpu::TextureUsages::STORAGE_BINDING
            | wgpu::TextureUsages::COPY_DST,
        Some(bytemuck::cast_slice(&grey_f32)),
    );
    let r32f_out = texture(
        device,
        queue,
        FRAME,
        wgpu::TextureFormat::R32Float,
        wgpu::TextureUsages::STORAGE_BINDING | wgpu::TextureUsages::RENDER_ATTACHMENT,
        None,
    );
    // Packed r8 in a storage buffer (4 pixels per u32), the layout Eidos planned.
    let packed = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("packed r8"),
        contents: &grey,
        usage: wgpu::BufferUsages::STORAGE,
    });
    let packed_out = super::storage(device, u64::from(w * h));
    let frame_params = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("frame"),
        contents: bytemuck::cast_slice(&[w, h, 0u32, 0u32]),
        usage: wgpu::BufferUsages::STORAGE,
    });

    for radius in [1, 2] {
        let k = 2 * radius + 1;
        let tag = format!("box{k}x{k}_{w}x{h}");
        let id = "let c = vec2<i32>(id.xy); let size = vec2<i32>(textureDimensions(src));
                  if c.x >= size.x || c.y >= size.y { return; }";
        let passes = [
            Pass {
                name: format!("{tag}_buffer_packed_r8"),
                bytes: pixels * 2.0,
                flops: 0.0,
                build: Box::new(|| {
                    compute(
                    device,
                    queue,
                    &format!(
                        "struct Frame {{ width: u32, height: u32, _a: u32, _b: u32 }}
                         @group(0) @binding(0) var<storage, read> src: array<u32>;
                         @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
                         @group(0) @binding(2) var<storage, read> frame: Frame;
                         fn px(p: vec2<i32>) -> vec4<f32> {{
                             let i = u32(p.y) * frame.width + u32(p.x);
                             return vec4<f32>(f32((src[i >> 2u] >> ((i & 3u) * 8u)) & 0xffu));
                         }}
                         // One invocation per 4 pixels: one packed output word.
                         @compute @workgroup_size(8, 8) fn main(@builtin(global_invocation_id) id: vec3<u32>) {{
                             let size = vec2<i32>(i32(frame.width), i32(frame.height));
                             if i32(id.x) * 4 >= size.x || i32(id.y) >= size.y {{ return; }}
                             var word = 0u;
                             for (var j = 0; j < 4; j++) {{
                                 let c = vec2<i32>(i32(id.x) * 4 + j, i32(id.y));
                                 {sum}
                                 word |= (u32(mean.x) & 0xffu) << (u32(j) * 8u);
                             }}
                             dst[id.y * (frame.width / 4u) + id.x] = word;
                         }}",
                        sum = box_sum(radius, "px(p)")
                    ),
                    vec![
                        packed.as_entire_binding(),
                        packed_out.as_entire_binding(),
                        frame_params.as_entire_binding(),
                    ],
                    (w / 4, h),
                )
                }),
            },
            Pass {
                name: format!("{tag}_storage_image_r32f"),
                bytes: pixels * 8.0,
                flops: 0.0,
                build: Box::new(|| {
                    compute(
                    device,
                    queue,
                    &format!(
                        "@group(0) @binding(0) var src: texture_storage_2d<r32float, read>;
                         @group(0) @binding(1) var dst: texture_storage_2d<r32float, write>;
                         @compute @workgroup_size(8, 8) fn main(@builtin(global_invocation_id) id: vec3<u32>) {{
                             {id} {sum} textureStore(dst, c, mean);
                         }}",
                        sum = box_sum(radius, "textureLoad(src, p)")
                    ),
                    vec![
                        wgpu::BindingResource::TextureView(&view(&r32f)),
                        wgpu::BindingResource::TextureView(&view(&r32f_out)),
                    ],
                    FRAME,
                )
                }),
            },
            Pass {
                name: format!("{tag}_storage_image_rgba8"),
                bytes: pixels * 8.0,
                flops: 0.0,
                build: Box::new(|| {
                    compute(
                    device,
                    queue,
                    &format!(
                        "@group(0) @binding(0) var src: texture_storage_2d<rgba8unorm, read>;
                         @group(0) @binding(1) var dst: texture_storage_2d<rgba8unorm, write>;
                         @compute @workgroup_size(8, 8) fn main(@builtin(global_invocation_id) id: vec3<u32>) {{
                             {id} {sum} textureStore(dst, c, mean);
                         }}",
                        sum = box_sum(radius, "textureLoad(src, p)")
                    ),
                    vec![
                        wgpu::BindingResource::TextureView(&view(&rgba8)),
                        wgpu::BindingResource::TextureView(&view(&rgba8_out)),
                    ],
                    FRAME,
                )
                }),
            },
            Pass {
                name: format!("{tag}_sampled_r8_to_storage_r32f"),
                bytes: pixels * 5.0,
                flops: 0.0,
                build: Box::new(|| {
                    compute(
                    device,
                    queue,
                    &format!(
                        "@group(0) @binding(0) var src: texture_2d<f32>;
                         @group(0) @binding(1) var dst: texture_storage_2d<r32float, write>;
                         @compute @workgroup_size(8, 8) fn main(@builtin(global_invocation_id) id: vec3<u32>) {{
                             {id} {sum} textureStore(dst, c, mean);
                         }}",
                        sum = box_sum(radius, "textureLoad(src, p, 0)")
                    ),
                    vec![
                        wgpu::BindingResource::TextureView(&view(&r8)),
                        wgpu::BindingResource::TextureView(&view(&r32f_out)),
                    ],
                    FRAME,
                )
                }),
            },
            Pass {
                name: format!("{tag}_fragment_r8"),
                bytes: pixels * 2.0,
                flops: 0.0,
                build: Box::new(|| {
                    fragment(
                    device,
                    queue,
                    &format!(
                        "@group(0) @binding(0) var src: texture_2d<f32>;
                         @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{
                             let c = vec2<i32>(pos.xy); let size = vec2<i32>(textureDimensions(src));
                             {sum} return mean;
                         }}",
                        sum = box_sum(radius, "textureLoad(src, p, 0)")
                    ),
                    vec![wgpu::BindingResource::TextureView(&view(&r8))],
                    &r8_out,
                )
                }),
            },
            Pass {
                name: format!("{tag}_fragment_rgba8"),
                bytes: pixels * 8.0,
                flops: 0.0,
                build: Box::new(|| {
                    fragment(
                    device,
                    queue,
                    &format!(
                        "@group(0) @binding(0) var src: texture_2d<f32>;
                         @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{
                             let c = vec2<i32>(pos.xy); let size = vec2<i32>(textureDimensions(src));
                             {sum} return mean;
                         }}",
                        sum = box_sum(radius, "textureLoad(src, p, 0)")
                    ),
                    vec![wgpu::BindingResource::TextureView(&view(&rgba8))],
                    &rgba8_out,
                )
                }),
            },
        ];
        for pass in passes {
            report(device, pass);
        }
        helper_box(&grey, radius, &tag);
        cpu_box(&grey, FRAME, radius as usize, &tag);
        cpu_box_separable(&grey, FRAME, radius as usize, &tag);
    }
    field_kernels(device, queue);
}

/// saxpy and the FMA loop over a 2048x2048 r32float field through the image paths.
fn field_kernels(device: &wgpu::Device, queue: &wgpu::Queue) {
    let n = f64::from(FIELD.0 * FIELD.1);
    let values: Vec<f32> = (0..FIELD.0 * FIELD.1).map(|i| (i % 1024) as f32).collect();
    let usage = wgpu::TextureUsages::TEXTURE_BINDING
        | wgpu::TextureUsages::STORAGE_BINDING
        | wgpu::TextureUsages::COPY_DST;
    let make = |usage| {
        texture(
            device,
            queue,
            FIELD,
            wgpu::TextureFormat::R32Float,
            usage,
            Some(bytemuck::cast_slice(&values)),
        )
    };
    let (x, y) = (make(usage), make(usage));
    let out = texture(
        device,
        queue,
        FIELD,
        wgpu::TextureFormat::R32Float,
        wgpu::TextureUsages::STORAGE_BINDING | wgpu::TextureUsages::RENDER_ATTACHMENT,
        None,
    );
    for (name, body, flops) in [
        ("saxpy", format!("let r = {SAXPY_EXPR};"), 2.0),
        ("fma256", FMA_BODY.to_string(), 512.0),
    ] {
        let tag = format!("{name}_{}x{}_r32f", FIELD.0, FIELD.1);
        let passes = [
            Pass {
                name: format!("{tag}_storage_image"),
                bytes: n * 12.0,
                flops: n * flops,
                build: Box::new(|| {
                    compute(
                    device,
                    queue,
                    &format!(
                        "@group(0) @binding(0) var xs: texture_storage_2d<r32float, read>;
                         @group(0) @binding(1) var ys: texture_storage_2d<r32float, read>;
                         @group(0) @binding(2) var dst: texture_storage_2d<r32float, write>;
                         @compute @workgroup_size(8, 8) fn main(@builtin(global_invocation_id) id: vec3<u32>) {{
                             let c = vec2<i32>(id.xy);
                             let x = textureLoad(xs, c).x; let y = textureLoad(ys, c).x;
                             {body}
                             textureStore(dst, c, vec4<f32>(r));
                         }}"
                    ),
                    vec![
                        wgpu::BindingResource::TextureView(&view(&x)),
                        wgpu::BindingResource::TextureView(&view(&y)),
                        wgpu::BindingResource::TextureView(&view(&out)),
                    ],
                    FIELD,
                )
                }),
            },
            Pass {
                name: format!("{tag}_sampled"),
                bytes: n * 12.0,
                flops: n * flops,
                build: Box::new(|| {
                    compute(
                    device,
                    queue,
                    &format!(
                        "@group(0) @binding(0) var xs: texture_2d<f32>;
                         @group(0) @binding(1) var ys: texture_2d<f32>;
                         @group(0) @binding(2) var dst: texture_storage_2d<r32float, write>;
                         @compute @workgroup_size(8, 8) fn main(@builtin(global_invocation_id) id: vec3<u32>) {{
                             let c = vec2<i32>(id.xy);
                             let x = textureLoad(xs, c, 0).x; let y = textureLoad(ys, c, 0).x;
                             {body}
                             textureStore(dst, c, vec4<f32>(r));
                         }}"
                    ),
                    vec![
                        wgpu::BindingResource::TextureView(&view(&x)),
                        wgpu::BindingResource::TextureView(&view(&y)),
                        wgpu::BindingResource::TextureView(&view(&out)),
                    ],
                    FIELD,
                )
                }),
            },
            Pass {
                name: format!("{tag}_fragment"),
                bytes: n * 12.0,
                flops: n * flops,
                build: Box::new(|| {
                    fragment(
                    device,
                    queue,
                    &format!(
                        "@group(0) @binding(0) var xs: texture_2d<f32>;
                         @group(0) @binding(1) var ys: texture_2d<f32>;
                         @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{
                             let c = vec2<i32>(pos.xy);
                             let x = textureLoad(xs, c, 0).x; let y = textureLoad(ys, c, 0).x;
                             {body}
                             return vec4<f32>(r);
                         }}"
                    ),
                    vec![
                        wgpu::BindingResource::TextureView(&view(&x)),
                        wgpu::BindingResource::TextureView(&view(&y)),
                    ],
                    &out,
                )
                }),
            },
        ];
        for pass in passes {
            report(device, pass);
        }
    }
}

/// Naive (non-separable, scalar) box filter on u8 rows, on 1 thread and on every core.
fn cpu_box(src: &[u8], (w, h): (u32, u32), radius: usize, tag: &str) {
    let (w, h) = (w as usize, h as usize);
    let mut dst = vec![0u8; w * h];
    let taps = ((2 * radius + 1) * (2 * radius + 1)) as u32;
    let rows = |first: usize, out: &mut [u8]| {
        for (r, row) in out.chunks_mut(w).enumerate() {
            let y = first + r;
            for (x, px) in row.iter_mut().enumerate() {
                let mut acc = 0u32;
                for dy in 0..=2 * radius {
                    let sy = (y + dy).saturating_sub(radius).min(h - 1);
                    for dx in 0..=2 * radius {
                        let sx = (x + dx).saturating_sub(radius).min(w - 1);
                        acc += u32::from(src[sy * w + sx]);
                    }
                }
                *px = (acc / taps) as u8;
            }
        }
    };
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    for t in [1, threads] {
        let mut samples = Vec::new();
        for _ in 0..5 {
            let start = Instant::now();
            let chunk_rows = h.div_ceil(t);
            std::thread::scope(|s| {
                for (i, out) in dst.chunks_mut(chunk_rows * w).enumerate() {
                    let rows = &rows;
                    s.spawn(move || rows(i * chunk_rows, out));
                }
            });
            samples.push(start.elapsed());
        }
        let secs = median(&mut samples).as_secs_f64();
        kv(
            &format!("{tag}_cpu_naive_{t}t"),
            format!(
                "{:.2} ms, {:.2} GB/s",
                secs * 1e3,
                (w * h * 2) as f64 / secs / 1e9
            ),
        );
    }
    std::hint::black_box(&dst);
}

/// Separable box filter with running sums (a horizontal pass into u16 row sums, then a vertical
/// pass over whole rows the compiler vectorizes): what a reasonable CPU implementation costs.
fn cpu_box_separable(src: &[u8], (w, h): (u32, u32), radius: usize, tag: &str) {
    let (w, h) = (w as usize, h as usize);
    let taps = ((2 * radius + 1) * (2 * radius + 1)) as u32;
    let mut dst = vec![0u8; w * h];
    let horizontal = |y: usize, out: &mut [u16]| {
        let row = &src[y * w..(y + 1) * w];
        let at = |x: isize| u16::from(row[x.clamp(0, w as isize - 1) as usize]);
        let r = radius as isize;
        let mut acc: u16 = (-r..=r).map(at).sum();
        for (x, o) in out.iter_mut().enumerate() {
            *o = acc;
            let x = x as isize;
            acc = acc + at(x + r + 1) - at(x - r);
        }
    };
    let band = |first: usize, out: &mut [u8]| {
        let lo = first.saturating_sub(radius);
        let hi = (first + out.len() / w + radius).min(h);
        let mut rows: Vec<Vec<u16>> = Vec::with_capacity(hi - lo);
        for y in lo..hi {
            let mut row = vec![0u16; w];
            horizontal(y, &mut row);
            rows.push(row);
        }
        let mut col = vec![0u32; w];
        for (r, out_row) in out.chunks_mut(w).enumerate() {
            let y = first + r;
            col.iter_mut().for_each(|c| *c = 0);
            for dy in 0..=2 * radius {
                let sy = (y + dy).saturating_sub(radius).min(h - 1);
                let row = &rows[sy - lo];
                for (c, v) in col.iter_mut().zip(row) {
                    *c += u32::from(*v);
                }
            }
            for (o, c) in out_row.iter_mut().zip(&col) {
                *o = (*c / taps) as u8;
            }
        }
    };
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    for t in [1, threads] {
        let mut samples = Vec::new();
        for _ in 0..5 {
            let start = Instant::now();
            let chunk_rows = h.div_ceil(t);
            std::thread::scope(|s| {
                for (i, out) in dst.chunks_mut(chunk_rows * w).enumerate() {
                    let band = &band;
                    s.spawn(move || band(i * chunk_rows, out));
                }
            });
            samples.push(start.elapsed());
        }
        let secs = median(&mut samples).as_secs_f64();
        kv(
            &format!("{tag}_cpu_separable_{t}t"),
            format!(
                "{:.2} ms, {:.2} GB/s",
                secs * 1e3,
                (w * h * 2) as f64 / secs / 1e9
            ),
        );
    }
    std::hint::black_box(&dst);
}

/// The r8 box filter through `FragmentPass` + `GpuTexture` (the node helper), on its own context:
/// its cost against the hand-written `_fragment_r8` pass above.
fn helper_box(grey: &[u8], radius: i32, tag: &str) {
    use daedalus_gpu::{FragmentPass, GpuBackendKind, GpuOptions, GpuTexture, select_backend};
    let name = format!("{tag}_fragment_helper_r8");
    let ctx = match select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    }) {
        Ok(ctx) if ctx.wgpu().is_some() => ctx,
        _ => return kv(&name, "no wgpu context"),
    };
    let format = wgpu::TextureFormat::R8Unorm;
    let shader = format!(
        "@group(0) @binding(0) var src: texture_2d<f32>;
         @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{
             let c = vec2<i32>(pos.xy); let size = vec2<i32>(textureDimensions(src));
             {sum} return mean;
         }}",
        sum = box_sum(radius, "textureLoad(src, p, 0)")
    );
    let built = (|| {
        let pass = FragmentPass::new(&ctx, &shader, &[format])?;
        let src = GpuTexture::new(&ctx, FRAME, format, None)?;
        src.write(grey)?;
        let dst = GpuTexture::new(&ctx, FRAME, format, None)?;
        Ok::<_, daedalus_gpu::GpuError>((pass, src, dst))
    })();
    let (pass, src, dst) = match built {
        Ok(built) => built,
        Err(err) => return kv(&name, format!("unsupported: {err}")),
    };
    let run = || {
        if let Ok(ticket) = pass.run(&[&src], &[], &[&dst]) {
            ticket.wait();
        }
    };
    run();
    let mut samples: Vec<Duration> = (0..iters().min(30))
        .map(|_| {
            let start = Instant::now();
            run();
            start.elapsed()
        })
        .collect();
    let t = median(&mut samples).as_secs_f64();
    let pixels = f64::from(FRAME.0 * FRAME.1);
    kv(
        &name,
        format!("{:.2} ms, {:.2} GB/s", t * 1e3, pixels * 2.0 / t / 1e9),
    );
}
