struct BinaryParams {
    width: u32,
    height: u32,
    threshold: f32,
    _pad: f32,
};

@group(0) @binding(0)
var inputTex : texture_2d<f32>;

@group(0) @binding(1)
var inputSampler : sampler;

@group(0) @binding(2)
var outputTex : texture_storage_2d<rgba8unorm, write>;

@group(0) @binding(3)
var<uniform> binaryParams : BinaryParams;

@compute @workgroup_size(256)
fn binary_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = binaryParams.width * binaryParams.height;
    if (idx >= total) { return; }

    let x = idx % binaryParams.width;
    let y = idx / binaryParams.width;
    let pixel = textureSampleLevel(inputTex, inputSampler, vec2<f32>(f32(x) + 0.5, f32(y) + 0.5), 0.0);
    let luminance = dot(pixel.rgb, vec3<f32>(0.299, 0.587, 0.114));
    let value = select(0.0, 1.0, luminance > binaryParams.threshold);
    let mask = vec4<f32>(value, value, value, 1.0);
    textureStore(outputTex, vec2<u32>(x, y), mask);
}
