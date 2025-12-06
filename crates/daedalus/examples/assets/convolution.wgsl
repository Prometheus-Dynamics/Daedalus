struct ConvolutionParams {
    width: u32,
    height: u32,
    _pad0: vec2<u32>,
    factor_bits: u32,
    bias_bits: u32,
    _pad1: vec2<u32>,
};

@group(0) @binding(0)
var<storage, read> convInput : array<u32>;

@group(0) @binding(1)
var<storage, read_write> convOutput : array<u32>;

@group(0) @binding(2)
var<uniform> convParams : ConvolutionParams;

@group(0) @binding(3)
var<storage, read> convKernel : array<f32>;

fn pack_gray(value: f32) -> u32 {
    let clamped = clamp(value, 0.0, 255.0);
    let v = u32(clamped + 0.5);
    return v | (v << 8u) | (v << 16u) | (255u << 24u);
}

fn sample_gray(x: i32, y: i32) -> f32 {
    let width = i32(convParams.width);
    let height = i32(convParams.height);
    let sx = clamp(x, 0, width - 1);
    let sy = clamp(y, 0, height - 1);
    let idx = sy * width + sx;
    return f32(convInput[u32(idx)] & 0xFFu);
}

@compute @workgroup_size(256)
fn convolution_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let width = convParams.width;
    let height = convParams.height;
    let total = width * height;
    if (idx >= total) {
        return;
    }
    let x = i32(idx % width);
    let y = i32(idx / width);
    var acc = 0.0;
    for (var ky = -1; ky <= 1; ky = ky + 1) {
        for (var kx = -1; kx <= 1; kx = kx + 1) {
            let weight = convKernel[u32((ky + 1) * 3 + (kx + 1))];
            acc = acc + sample_gray(x + kx, y + ky) * weight;
        }
    }
    let factor = bitcast<f32>(convParams.factor_bits);
    let bias = bitcast<f32>(convParams.bias_bits);
    let value = acc * factor + bias;
    convOutput[idx] = pack_gray(value);
}
