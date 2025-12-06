// Simple texture -> sampler -> storage texture pass with a mild gamma tweak.
@group(0) @binding(0)
var input_tex : texture_2d<f32>;

@group(0) @binding(1)
var input_sampler : sampler;

@group(0) @binding(2)
var output_tex : texture_storage_2d<rgba8unorm, write>;

fn srgb_to_linear(c: vec3<f32>) -> vec3<f32> {
    return pow(c, vec3<f32>(2.2));
}

fn linear_to_srgb(c: vec3<f32>) -> vec3<f32> {
    return pow(c, vec3<f32>(1.0 / 2.2));
}

@compute @workgroup_size(64)
fn texture_sample_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let dims = textureDimensions(output_tex);
    let idx = id.x;
    let total = dims.x * dims.y;
    if (idx >= total) {
        return;
    }
    let x = idx % dims.x;
    let y = idx / dims.x;
    let uv = (vec2<f32>(f32(x) + 0.5, f32(y) + 0.5)) / vec2<f32>(vec2<u32>(dims));
    let sample = textureSampleLevel(input_tex, input_sampler, uv, 0.0);
    // Slight contrast bump in linear space to show the path works.
    let linear = srgb_to_linear(sample.rgb);
    let boosted = clamp(linear * 1.1, vec3<f32>(0.0), vec3<f32>(1.0));
    let srgb = linear_to_srgb(boosted);
    textureStore(output_tex, vec2<u32>(x, y), vec4<f32>(srgb, sample.a));
}

