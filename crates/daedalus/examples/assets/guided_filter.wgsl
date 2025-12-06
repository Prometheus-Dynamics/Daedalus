struct GuidedParams {
    width: u32,
    height: u32,
    radius: u32,
    epsilon_bits: u32,
};

@group(0) @binding(0)
var<storage, read> guidedInput : array<u32>;

@group(0) @binding(1)
var<storage, read_write> guidedCoeffA : array<f32>;

@group(0) @binding(2)
var<storage, read_write> guidedCoeffB : array<f32>;

@group(0) @binding(3)
var<uniform> guidedParams : GuidedParams;

fn pack_gray(value: f32) -> u32 {
    let clamped = clamp(value, 0.0, 255.0);
    let v = u32(clamped + 0.5);
    return v | (v << 8u) | (v << 16u) | (255u << 24u);
}

fn sample_guided(idx: u32) -> f32 {
    return f32(guidedInput[idx] & 0xFFu) / 255.0;
}

fn compute_bounds(idx: u32, width: u32, height: u32, radius: u32) -> vec4<i32> {
    let x = i32(idx % width);
    let y = i32(idx / width);
    let r = i32(radius);
    let start_x = max(x - r, 0);
    let end_x = min(x + r + 1, i32(width));
    let start_y = max(y - r, 0);
    let end_y = min(y + r + 1, i32(height));
    return vec4<i32>(start_x, end_x, start_y, end_y);
}

@compute @workgroup_size(256)
fn guided_coeff_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = guidedParams.width * guidedParams.height;
    if (idx >= total) {
        return;
    }
    let bounds = compute_bounds(idx, guidedParams.width, guidedParams.height, guidedParams.radius);
    let start_x = bounds.x;
    let end_x = bounds.y;
    let start_y = bounds.z;
    let end_y = bounds.w;
    var sum_i = 0.0;
    var sum_ii = 0.0;
    for (var yy = start_y; yy < end_y; yy = yy + 1) {
        let row = u32(yy) * guidedParams.width;
        for (var xx = start_x; xx < end_x; xx = xx + 1) {
            let sample_idx = row + u32(xx);
            let value = sample_guided(sample_idx);
            sum_i = sum_i + value;
            sum_ii = sum_ii + value * value;
        }
    }
    let area = max((end_x - start_x) * (end_y - start_y), 1);
    let inv_area = 1.0 / f32(area);
    let mean_i = sum_i * inv_area;
    let mean_ii = sum_ii * inv_area;
    let variance = max(mean_ii - mean_i * mean_i, 0.0);
    let epsilon = bitcast<f32>(guidedParams.epsilon_bits);
    let a = variance / (variance + epsilon);
    let b = mean_i - a * mean_i;
    guidedCoeffA[idx] = a;
    guidedCoeffB[idx] = b;
}

@group(0) @binding(0)
var<storage, read> resolveInput : array<u32>;

@group(0) @binding(1)
var<storage, read> resolveCoeffA : array<f32>;

@group(0) @binding(2)
var<storage, read> resolveCoeffB : array<f32>;

@group(0) @binding(3)
var<storage, read_write> resolveOutput : array<u32>;

@group(0) @binding(4)
var<uniform> resolveParams : GuidedParams;

@compute @workgroup_size(256)
fn guided_resolve_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = resolveParams.width * resolveParams.height;
    if (idx >= total) {
        return;
    }
    let bounds = compute_bounds(idx, resolveParams.width, resolveParams.height, resolveParams.radius);
    let start_x = bounds.x;
    let end_x = bounds.y;
    let start_y = bounds.z;
    let end_y = bounds.w;
    var sum_a = 0.0;
    var sum_b = 0.0;
    for (var yy = start_y; yy < end_y; yy = yy + 1) {
        let row = u32(yy) * resolveParams.width;
        for (var xx = start_x; xx < end_x; xx = xx + 1) {
            let sample_idx = row + u32(xx);
            sum_a = sum_a + resolveCoeffA[sample_idx];
            sum_b = sum_b + resolveCoeffB[sample_idx];
        }
    }
    let area = max((end_x - start_x) * (end_y - start_y), 1);
    let inv_area = 1.0 / f32(area);
    let mean_a = sum_a * inv_area;
    let mean_b = sum_b * inv_area;
    let value = mean_a * sample_resolve(idx) + mean_b;
    resolveOutput[idx] = pack_gray(value * 255.0);
}
fn sample_resolve(idx: u32) -> f32 {
    return f32(resolveInput[idx] & 0xFFu) / 255.0;
}
