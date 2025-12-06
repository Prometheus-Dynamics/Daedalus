struct AdaptiveParams {
    width: u32,
    height: u32,
    radius: u32,
    _pad: u32,
    offset: f32,
    scale: f32,
    _pad1: vec2<f32>,
};

@group(0) @binding(0)
var<storage, read> adaptiveInput : array<u32>;

@group(0) @binding(1)
var<storage, read_write> adaptiveOutput : array<u32>;

@group(0) @binding(2)
var<uniform> adaptiveParams : AdaptiveParams;

fn unpack_gray(value: u32) -> f32 {
    return f32(value & 0xFFu) / 255.0;
}

fn pack_gray(value: f32) -> u32 {
    let clamped = clamp(value, 0.0, 1.0);
    let v = u32(clamped * 255.0 + 0.5);
    return v | (v << 8u) | (v << 16u) | (255u << 24u);
}

@compute @workgroup_size(256)
fn adaptive_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let width = adaptiveParams.width;
    let height = adaptiveParams.height;
    let total = width * height;
    if (idx >= total) {
        return;
    }

    let x = i32(idx % width);
    let y = i32(idx / width);
    let radius = i32(adaptiveParams.radius);

    var sum = 0.0;
    var count = 0.0;

    var dy = -radius;
    loop {
        if (dy > radius) {
            break;
        }
        var dx = -radius;
        loop {
            if (dx > radius) {
                break;
            }
            let sx = clamp(x + dx, 0, i32(width) - 1);
            let sy = clamp(y + dy, 0, i32(height) - 1);
            let sample_idx = u32(sy) * width + u32(sx);
            sum = sum + unpack_gray(adaptiveInput[sample_idx]);
            count = count + 1.0;
            dx = dx + 1;
        }
        dy = dy + 1;
    }

    if (count == 0.0) {
        adaptiveOutput[idx] = adaptiveInput[idx];
        return;
    }

    let mean = sum / count;
    let pixel = unpack_gray(adaptiveInput[idx]);
    let threshold = mean - adaptiveParams.offset;
    let value = select(0.0, 1.0, pixel > threshold);
    adaptiveOutput[idx] = pack_gray(value);
}
