struct ClaheParams {
    width: u32,
    height: u32,
    tiles_x: u32,
    tiles_y: u32,
    tile_w: u32,
    tile_h: u32,
    max_x: u32,
    max_y: u32,
    clip_limit_bits: u32,
    _pad0: vec3<u32>,
};

@group(0) @binding(0)
var<storage, read> claheInput : array<u32>;

@group(0) @binding(1)
var<storage, read_write> claheOutput : array<u32>;

@group(0) @binding(2)
var<uniform> claheParams : ClaheParams;

@group(0) @binding(3)
var<storage, read_write> claheLuts : array<u32>;

fn unpack_gray(value: u32) -> f32 {
    return f32(value & 0xFFu) / 255.0;
}

fn pack_gray(value: f32) -> u32 {
    let clamped = clamp(value, 0.0, 1.0);
    let v = u32(clamped * 255.0 + 0.5);
    return v | (v << 8u) | (v << 16u) | (255u << 24u);
}

fn lut_value(idx: u32) -> f32 {
    return f32(claheLuts[idx]) / 255.0;
}

fn sample_lut(tx: u32, ty: u32, value: u32) -> f32 {
    let tiles_x = claheParams.tiles_x;
    let lut_idx = ((ty * tiles_x + tx) * 256u) + value;
    return lut_value(lut_idx);
}

fn tile_weight(coord: u32, tile_extent: u32, idx0: u32, idx1: u32, max_coord: u32, tiles: u32) -> f32 {
    if (idx0 == idx1 || tile_extent == 0u || tiles <= 1u) {
        return 0.0;
    }
    let start = idx0 * tile_extent;
    var end = idx1 * tile_extent;
    if (idx1 == tiles - 1u) {
        end = max_coord;
    }
    let range = max(i32(end) - i32(start), 1);
    return clamp(f32(i32(coord) - i32(start)) / f32(range), 0.0, 1.0);
}

@compute @workgroup_size(256)
fn clahe_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let width = claheParams.width;
    let height = claheParams.height;
    let total = width * height;
    if (idx >= total) {
        return;
    }

    let x = idx % width;
    let y = idx / width;

    let tile_w = max(claheParams.tile_w, 1u);
    let tile_h = max(claheParams.tile_h, 1u);

    var tx0 = x / tile_w;
    if (tx0 >= claheParams.tiles_x) {
        tx0 = claheParams.tiles_x - 1u;
    }
    var tx1 = min(tx0 + 1u, claheParams.tiles_x - 1u);

    var ty0 = y / tile_h;
    if (ty0 >= claheParams.tiles_y) {
        ty0 = claheParams.tiles_y - 1u;
    }
    var ty1 = min(ty0 + 1u, claheParams.tiles_y - 1u);

    let wx = tile_weight(x, tile_w, tx0, tx1, claheParams.max_x, claheParams.tiles_x);
    let wy = tile_weight(y, tile_h, ty0, ty1, claheParams.max_y, claheParams.tiles_y);

    let value = unpack_gray(claheInput[idx]);
    let lut_index = u32(clamp(value * 255.0 + 0.5, 0.0, 255.0));

    let tl = sample_lut(tx0, ty0, lut_index);
    let tr = sample_lut(tx1, ty0, lut_index);
    let bl = sample_lut(tx0, ty1, lut_index);
    let br = sample_lut(tx1, ty1, lut_index);

    let top = tl * (1.0 - wx) + tr * wx;
    let bottom = bl * (1.0 - wx) + br * wx;
    let blended = top * (1.0 - wy) + bottom * wy;

    claheOutput[idx] = pack_gray(blended);
}

var<workgroup> hist: array<u32, 256>;

@compute @workgroup_size(256)
fn clahe_lut_main(@builtin(local_invocation_id) local_id: vec3<u32>, @builtin(workgroup_id) group_id: vec3<u32>) {
    let bin = local_id.x;
    let tiles_x = claheParams.tiles_x;
    let tile_index = group_id.x;
    if (tile_index >= tiles_x * claheParams.tiles_y) {
        return;
    }

    let tile_w = max(claheParams.tile_w, 1u);
    let tile_h = max(claheParams.tile_h, 1u);
    let tx = tile_index % tiles_x;
    let ty = tile_index / tiles_x;
    let x0 = tx * tile_w;
    let x1 = min((tx + 1u) * tile_w, claheParams.width);
    let y0 = ty * tile_h;
    let y1 = min((ty + 1u) * tile_h, claheParams.height);
    let tile_pixels = max((x1 - x0) * (y1 - y0), 1u);

    var count: u32 = 0u;
    var y: u32 = y0;
    loop {
        if (y >= y1) {
            break;
        }
        let row_offset = y * claheParams.width;
        var x: u32 = x0;
        loop {
            if (x >= x1) {
                break;
            }
            let idx = row_offset + x;
            let value = claheInput[idx] & 0xFFu;
            if (value == bin) {
                count = count + 1u;
            }
            x = x + 1u;
        }
        y = y + 1u;
    }
    hist[bin] = count;
    workgroupBarrier();

    if (bin == 0u) {
        let clip_limit = bitcast<f32>(claheParams.clip_limit_bits);
        let avg_per_bin = f32(tile_pixels) / 256.0;
        var clip_value = tile_pixels;
        if (clip_limit > 0.0) {
            let scaled = u32(round(max(clip_limit, 1.0) * avg_per_bin));
            clip_value = max(scaled, 1u);
        }

        var excess: u32 = 0u;
        for (var i: u32 = 0u; i < 256u; i = i + 1u) {
            if (hist[i] > clip_value) {
                excess = excess + (hist[i] - clip_value);
                hist[i] = clip_value;
            }
        }

        let redistribute = excess / 256u;
        let remainder = excess % 256u;
        for (var i: u32 = 0u; i < 256u; i = i + 1u) {
            hist[i] = hist[i] + redistribute;
        }
        for (var i: u32 = 0u; i < remainder; i = i + 1u) {
            hist[i] = hist[i] + 1u;
        }

        var cumulative: u32 = 0u;
        var cdf_min: u32 = 0u;
        var found: bool = false;
        let denom_base = tile_pixels;
        for (var i: u32 = 0u; i < 256u; i = i + 1u) {
            cumulative = cumulative + hist[i];
            if (!found && cumulative != 0u) {
                cdf_min = cumulative;
                found = true;
            }
            let val = cumulative - cdf_min;
            let denom = max(denom_base - cdf_min, 1u);
            let mapped = (val * 255u) / denom;
            let lut_index = tile_index * 256u + i;
            claheLuts[lut_index] = mapped;
        }
    }
}
