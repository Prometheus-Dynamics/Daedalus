struct EdgeParams {
    width: u32,
    height: u32,
    _pad: vec2<u32>,
};

@group(0) @binding(0)
var inputTex : texture_2d<f32>;

@group(0) @binding(1)
var inputSampler : sampler;

@group(0) @binding(2)
var outputTex : texture_storage_2d<rgba8unorm, write>;

@group(0) @binding(3)
var<uniform> edgeParams : EdgeParams;

@compute @workgroup_size(256)
fn sobel_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let width = edgeParams.width;
    let height = edgeParams.height;
    let total = width * height;
    if (idx >= total) { return; }

    let x = idx % width;
    let y = idx / width;

    let kernel_x = array<vec2<i32>, 9>(
        vec2<i32>(-1, -1), vec2<i32>(0, -1), vec2<i32>(1, -1),
        vec2<i32>(-2,  0), vec2<i32>(0,  0), vec2<i32>(2,  0),
        vec2<i32>(-1,  1), vec2<i32>(0,  1), vec2<i32>(1,  1)
    );
    let kernel_y = array<vec2<i32>, 9>(
        vec2<i32>(-1, -1), vec2<i32>(-2, 0), vec2<i32>(-1, 1),
        vec2<i32>(0, -1),  vec2<i32>(0, 0),  vec2<i32>(0, 1),
        vec2<i32>(1, -1),  vec2<i32>(2, 0),  vec2<i32>(1, 1)
    );

    var gx = 0.0;
    var gy = 0.0;
    for (var i = 0u; i < 9u; i = i + 1u) {
        let offset = kernel_x[i];
        let ox = clamp(i32(x) + offset.x, 0, i32(width) - 1);
        let oy = clamp(i32(y) + offset.y, 0, i32(height) - 1);
        let sample = textureSampleLevel(inputTex, inputSampler, vec2<f32>(f32(ox) + 0.5, f32(oy) + 0.5), 0.0);
        let luminance = dot(sample.rgb, vec3<f32>(0.299, 0.587, 0.114));
        gx = gx + luminance * f32(kernel_x[i].x);
        gy = gy + luminance * f32(kernel_y[i].y);
    }
    let mag = clamp(length(vec2<f32>(gx, gy)), 0.0, 1.0);
    textureStore(outputTex, vec2<u32>(x, y), vec4<f32>(mag, mag, mag, 1.0));
}

