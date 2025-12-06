@group(0) @binding(0)
var<storage, read> r8Input : array<u32>;

@group(0) @binding(1)
var<storage, read_write> rgbaOutput : array<u32>;

@compute @workgroup_size(256)
fn r8_to_rgba_main(@builtin(global_invocation_id) id : vec3<u32>) {
    let idx = id.x;
    // Each u32 packs 4 R8 pixels from the input buffer.
    let word_index = idx >> 2u;
    let lane = (idx & 3u) * 8u;
    let packed = r8Input[word_index];
    let value = (packed >> lane) & 0xFFu;
    let rgba = value | (value << 8u) | (value << 16u) | 0xFF000000u;
    rgbaOutput[idx] = rgba;
}
