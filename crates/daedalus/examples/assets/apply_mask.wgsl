struct ApplyParams {
    width: u32,
    height: u32,
    _pad0: u32,
    _pad1: u32,
};

@group(0) @binding(0)
var<storage, read> applyInput : array<u32>;

@group(0) @binding(1)
var<storage, read> applyMask : array<u32>;

@group(0) @binding(2)
var<storage, read_write> applyOutput : array<u32>;

@group(0) @binding(3)
var<uniform> applyParams : ApplyParams;

fn unpack_rgba(value: u32) -> vec4<f32> {
    let r = f32(value & 0xFFu) / 255.0;
    let g = f32((value >> 8u) & 0xFFu) / 255.0;
    let b = f32((value >> 16u) & 0xFFu) / 255.0;
    let a = f32((value >> 24u) & 0xFFu) / 255.0;
    return vec4<f32>(r, g, b, a);
}

fn pack_rgba(color: vec4<f32>) -> u32 {
    let clamped = clamp(color, vec4<f32>(0.0), vec4<f32>(1.0));
    let r = u32(clamped.r * 255.0 + 0.5);
    let g = u32(clamped.g * 255.0 + 0.5);
    let b = u32(clamped.b * 255.0 + 0.5);
    let a = u32(clamped.a * 255.0 + 0.5);
    return r | (g << 8u) | (b << 16u) | (a << 24u);
}

@compute @workgroup_size(256)
fn apply_mask_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = applyParams.width * applyParams.height;
    if (idx >= total) {
        return;
    }

    let color = unpack_rgba(applyInput[idx]);
    let mask = unpack_rgba(applyMask[idx]);
    if (mask.r < 0.5) {
        applyOutput[idx] = pack_rgba(vec4<f32>(0.0, 0.0, 0.0, color.a));
    } else {
        applyOutput[idx] = pack_rgba(color);
    }
}
