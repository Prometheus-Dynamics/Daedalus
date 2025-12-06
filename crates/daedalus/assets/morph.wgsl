struct MorphParams {
    width: u32,
    height: u32,
    radius: u32,
    norm: i32,
    op: u32,
    pad: vec3<u32>,
};

@group(0) @binding(0)
var inputTex : texture_2d<f32>;

@group(0) @binding(1)
var inputSampler : sampler;

@group(0) @binding(2)
var outputTex : texture_storage_2d<rgba8unorm, write>;

@group(0) @binding(3)
var<uniform> morphParams : MorphParams;

fn lnorm(dx: i32, dy: i32, norm: i32) -> f32 {
    if norm == 1 {
        return f32(abs(dx) + abs(dy));
    }
    let dist = sqrt(f32(dx * dx + dy * dy));
    return dist;
}

@compute @workgroup_size(256)
fn morph_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let width = morphParams.width;
    let height = morphParams.height;
    let total = width * height;
    if (idx >= total) { return; }

    let x = idx % width;
    let y = idx / width;
    let radius = i32(morphParams.radius);
    let norm = morphParams.norm;

    var best = select(1.0, 0.0, morphParams.op == 1u); // dilation => start at 0, erosion => 1
    for (var dy = -radius; dy <= radius; dy = dy + 1) {
        for (var dx = -radius; dx <= radius; dx = dx + 1) {
            if (lnorm(dx, dy, norm) > f32(radius)) { continue; }
            let sx = clamp(i32(x) + dx, 0, i32(width) - 1);
            let sy = clamp(i32(y) + dy, 0, i32(height) - 1);
            let sample = textureSampleLevel(inputTex, inputSampler, vec2<f32>(f32(sx) + 0.5, f32(sy) + 0.5), 0.0);
            let lum = dot(sample.rgb, vec3<f32>(0.299, 0.587, 0.114));
            if (morphParams.op == 1u) { // dilation
                best = max(best, lum);
            } else { // erosion
                best = min(best, lum);
            }
        }
    }
    let out = vec4<f32>(best, best, best, 1.0);
    textureStore(outputTex, vec2<u32>(x, y), out);
}

