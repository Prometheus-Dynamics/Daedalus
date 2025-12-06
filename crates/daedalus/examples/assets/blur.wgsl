struct BlurParams {
    width: u32,
    height: u32,
    radius: u32,
    kernel_len: u32,
};

@group(0) @binding(0)
var blurInput : texture_2d<f32>;

@group(0) @binding(1)
var blurSampler : sampler;

@group(0) @binding(2)
var blurOutput : texture_storage_2d<rgba8unorm, write>;

@group(0) @binding(3)
var<storage, read> blurWeights : array<f32>;

@group(0) @binding(4)
var<uniform> blurParams : BlurParams;

@compute @workgroup_size(64)
fn blur_horizontal_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let width = blurParams.width;
    let height = blurParams.height;
    let total = width * height;
    if (idx >= total) {
        return;
    }

    let x = idx % width;
    let y = idx / width;
    let radius = i32(blurParams.radius);
    var accum = vec4<f32>(0.0);

    for (var offset = -radius; offset <= radius; offset = offset + 1) {
        let kernel_index = u32(offset + radius);
        if (kernel_index >= blurParams.kernel_len) {
            continue;
        }
        let weight = blurWeights[kernel_index];
        let sample_x = clamp(i32(x) + offset, 0, i32(width) - 1);
        let sample_uv = vec2<f32>(f32(sample_x) + 0.5, f32(y) + 0.5);
        let sample = textureSampleLevel(blurInput, blurSampler, sample_uv, 0.0);
        accum = accum + sample * weight;
    }

    textureStore(blurOutput, vec2<u32>(x, y), clamp(accum, vec4<f32>(0.0), vec4<f32>(1.0)));
}
