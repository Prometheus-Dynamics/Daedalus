struct BinaryParams {
    width: u32,
    height: u32,
    _pad0: vec2<u32>,
};

struct Stats {
    mean: f32,
    count: u32,
    pad: vec2<u32>,
};

@group(0) @binding(0)
var inputTex : texture_2d<f32>;

@group(0) @binding(1)
var inputSampler : sampler;

@group(0) @binding(2)
var outputTex : texture_storage_2d<rgba8unorm, write>;

@group(0) @binding(3)
var<uniform> binaryParams : BinaryParams;

@group(0) @binding(4)
var<storage, read_write> binaryState : Stats;

@compute @workgroup_size(256)
fn binary_state_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = binaryParams.width * binaryParams.height;
    if (idx >= total) {
        return;
    }

    let x = idx % binaryParams.width;
    let y = idx / binaryParams.width;
    let pixel = textureSampleLevel(inputTex, inputSampler, vec2<f32>(f32(x) + 0.5, f32(y) + 0.5), 0.0);
    let luminance = dot(pixel.rgb, vec3<f32>(0.299, 0.587, 0.114));

    let threshold = clamp(binaryState.mean, 0.05, 0.95);
    let value = select(0.0, 1.0, luminance > threshold);
    textureStore(outputTex, vec2<u32>(x, y), vec4<f32>(value, value, value, 1.0));

    // Update running mean on GPU.
    let new_count = binaryState.count + 1u;
    let accum = binaryState.mean * f32(binaryState.count) + luminance;
    let new_mean = accum / f32(new_count);
    binaryState.mean = new_mean;
    binaryState.count = new_count;
}

