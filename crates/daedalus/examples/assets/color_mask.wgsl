struct MaskParams {
    width: u32,
    height: u32,
    range_count: u32,
    _pad: u32,
};

@group(0) @binding(0)
var<storage, read> maskInput : array<u32>;

@group(0) @binding(1)
var<storage, read_write> maskOutput : array<u32>;

@group(0) @binding(2)
var<uniform> maskParams : MaskParams;

@group(0) @binding(3)
var<storage, read> maskRanges : array<vec4<f32>>;

fn unpack_rgba(value: u32) -> vec4<f32> {
    let r = f32(value & 0xFFu) / 255.0;
    let g = f32((value >> 8u) & 0xFFu) / 255.0;
    let b = f32((value >> 16u) & 0xFFu) / 255.0;
    let a = f32((value >> 24u) & 0xFFu) / 255.0;
    return vec4<f32>(r, g, b, a);
}

fn pack_gray(value: f32) -> u32 {
    let clamped = clamp(value, 0.0, 1.0);
    let v = u32(clamped * 255.0 + 0.5);
    return v | (v << 8u) | (v << 16u) | (255u << 24u);
}

fn rgb_in_ranges(color: vec3<f32>) -> f32 {
    if (maskParams.range_count == 0u) {
        return 0.0;
    }

    var idx: u32 = 0u;
    loop {
        if (idx >= maskParams.range_count) {
            break;
        }
        let base = idx * 2u;
        let low = maskRanges[base];
        let high = maskRanges[base + 1u];
        if (color.r >= low.x && color.r <= low.y && color.g >= low.z && color.g <= low.w && color.b >= high.x && color.b <= high.y) {
            return 1.0;
        }
        idx = idx + 1u;
    }
    return 0.0;
}

fn rgb_to_hsv(color: vec3<f32>) -> vec3<f32> {
    let max_val = max(max(color.r, color.g), color.b);
    let min_val = min(min(color.r, color.g), color.b);
    let delta = max_val - min_val;

    var h = 0.0;
    if (delta > 0.0) {
        if (max_val == color.r) {
            h = 60.0 * ((color.g - color.b) / delta);
            if (h < 0.0) {
                h = h + 360.0;
            }
        } else if (max_val == color.g) {
            h = 60.0 * (((color.b - color.r) / delta) + 2.0);
        } else {
            h = 60.0 * (((color.r - color.g) / delta) + 4.0);
        }
    }

    var s = 0.0;
    if (max_val > 0.0) {
        s = delta / max_val;
    }
    return vec3<f32>(h, s, max_val);
}

fn hsv_in_ranges(color: vec3<f32>) -> f32 {
    if (maskParams.range_count == 0u) {
        return 0.0;
    }

    var idx: u32 = 0u;
    loop {
        if (idx >= maskParams.range_count) {
            break;
        }
        let base = idx * 2u;
        let bounds0 = maskRanges[base];
        let bounds1 = maskRanges[base + 1u];
        if (color.x >= bounds0.x && color.x <= bounds0.y && color.y >= bounds0.z && color.y <= bounds0.w && color.z >= bounds1.x && color.z <= bounds1.y) {
            return 1.0;
        }
        idx = idx + 1u;
    }
    return 0.0;
}

@compute @workgroup_size(256)
fn rgb_range_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = maskParams.width * maskParams.height;
    if (idx >= total) {
        return;
    }

    let pixel = unpack_rgba(maskInput[idx]).rgb;
    let value = rgb_in_ranges(pixel);
    maskOutput[idx] = pack_gray(value);
}

@compute @workgroup_size(256)
fn hsv_range_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = maskParams.width * maskParams.height;
    if (idx >= total) {
        return;
    }

    let pixel = unpack_rgba(maskInput[idx]).rgb;
    let hsv = rgb_to_hsv(pixel);
    let value = hsv_in_ranges(hsv);
    maskOutput[idx] = pack_gray(value);
}
