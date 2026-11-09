// The image ops on GPU-resident frames (`GpuImages`): one thread per output word, four output
// bytes each, as kernels.wgsl does. Integer arithmetic only (no 64-bit values, no subgroups), so
// it runs on Bifrost (Mali-G52) as on Valhall. Each `img_*` function mirrors one scalar reference
// in `daedalus-kernels` (reference.rs) and gives the same bytes. Buffers are `array<u32>` (WGSL
// has no byte stores), little-endian.

struct Params {
    // 0 area, 1 bilinear, 2 crop, 3 flip, 4 rotate, 5 fill, 6 blend, 7 palette, 8 colour key,
    // 9 mask ROP, 10 line.
    op: u32,
    width: u32,
    height: u32,
    // Bytes per source pixel.
    channels: u32,
    out_width: u32,
    out_height: u32,
    // Output bytes, and the output words written (ceil(out_len / 4)).
    out_len: u32,
    threads: u32,
    // Op-specific: area factor (f0); crop left, top (f0, f1); flip flags (f0, f1); quarter turns
    // (f0); fill rectangle (f0..f3); blend mode and premultiplied (f0, f1); mask op (f0); colour
    // key min r, g, b and max r, g, b (f0..f5); line from x, y, the signs of the steps, |dx|,
    // |dy|, and whether x is the major axis (f0..f6, the signs as i32 bits).
    f0: u32,
    f1: u32,
    f2: u32,
    f3: u32,
    f4: u32,
    f5: u32,
    f6: u32,
    f7: u32,
}

@group(0) @binding(0) var<storage, read> src: array<u32>;
// Second input: the palette (256 RGBA8 entries), the fill colour, the blend destination or the
// second mask, by op.
@group(0) @binding(1) var<storage, read> aux: array<u32>;
@group(0) @binding(2) var<storage, read_write> dst: array<u32>;
@group(0) @binding(3) var<storage, read> params: Params;

const WG: u32 = 64u;

fn img_src(i: u32) -> u32 {
    return (src[i >> 2u] >> ((i & 3u) * 8u)) & 0xffu;
}

fn img_aux(i: u32) -> u32 {
    return (aux[i >> 2u] >> ((i & 3u) * 8u)) & 0xffu;
}

// The source byte of channel `c` of pixel (sx, sy).
fn img_pixel_byte(sx: u32, sy: u32, c: u32) -> u32 {
    return img_src((sy * params.width + sx) * params.channels + c);
}

// reference::resize_area: the rounded mean of the block of `f0` by `f0` source pixels.
fn img_area(j: u32) -> u32 {
    let ch = params.channels;
    let p = j / ch;
    let c = j % ch;
    let f = params.f0;
    let x0 = (p % params.out_width) * f;
    let y0 = (p / params.out_width) * f;
    let x1 = min(x0 + f, params.width);
    let y1 = min(y0 + f, params.height);
    var sum = 0u;
    var n = 0u;
    for (var y = y0; y < y1; y++) {
        for (var x = x0; x < x1; x++) {
            sum += img_src((y * params.width + x) * ch + c);
            n += 1u;
        }
    }
    return (sum + n / 2u) / n;
}

// reference::linear_tap: the lower source index and the 11-bit weight of output index `d`
// along an axis of `src_n` samples resized to `dst_n`.
fn img_tap(d: u32, src_n: u32, dst_n: u32) -> vec2<u32> {
    let num = (2u * d + 1u) * src_n;
    if (num < dst_n) {
        return vec2<u32>(0u, 0u);
    }
    let pos = num - dst_n;
    let unit = 2u * dst_n;
    let s = pos / unit;
    let rem = pos % unit;
    if (s >= src_n - 1u) {
        return vec2<u32>(src_n - 1u, 0u);
    }
    return vec2<u32>(s, (rem * 2048u + unit / 2u) / unit);
}

// reference::resize_bilinear: two passes of 11-bit weights, rounded back to 8 bits.
fn img_bilinear(j: u32) -> u32 {
    let ch = params.channels;
    let p = j / ch;
    let c = j % ch;
    let w = params.width;
    let h = params.height;
    let tx = img_tap(p % params.out_width, w, params.out_width);
    let ty = img_tap(p / params.out_width, h, params.out_height);
    let x0 = tx.x;
    let wx = tx.y;
    let x1 = min(x0 + 1u, w - 1u);
    let y0 = ty.x;
    let wy = ty.y;
    let y1 = min(y0 + 1u, h - 1u);
    let row0 = img_pixel_byte(x0, y0, c) * (2048u - wx) + img_pixel_byte(x1, y0, c) * wx;
    let row1 = img_pixel_byte(x0, y1, c) * (2048u - wx) + img_pixel_byte(x1, y1, c) * wx;
    let v = row0 * (2048u - wy) + row1 * wy;
    return (v + (1u << 21u)) >> 22u;
}

// reference::crop: the source pixel at (left + x, top + y).
fn img_crop(j: u32) -> u32 {
    let ch = params.channels;
    let p = j / ch;
    let x = params.f0 + p % params.out_width;
    let y = params.f1 + p / params.out_width;
    return img_pixel_byte(x, y, j % ch);
}

// reference::flip.
fn img_flip(j: u32) -> u32 {
    let ch = params.channels;
    let p = j / ch;
    var x = p % params.width;
    var y = p / params.width;
    if (params.f0 != 0u) {
        x = params.width - 1u - x;
    }
    if (params.f1 != 0u) {
        y = params.height - 1u - y;
    }
    return img_pixel_byte(x, y, j % ch);
}

// reference::rotate: the source pixel that lands at (x, y) after `f0` clockwise quarter turns.
fn img_rotate(j: u32) -> u32 {
    let ch = params.channels;
    let p = j / ch;
    let x = p % params.out_width;
    let y = p / params.out_width;
    let w = params.width;
    let h = params.height;
    let c = j % ch;
    if (params.f0 == 0u) {
        return img_pixel_byte(x, y, c);
    }
    if (params.f0 == 1u) {
        return img_pixel_byte(y, h - 1u - x, c);
    }
    if (params.f0 == 2u) {
        return img_pixel_byte(w - 1u - x, h - 1u - y, c);
    }
    return img_pixel_byte(w - 1u - y, x, c);
}

// reference::fill: the fill colour (aux, `channels` bytes) inside the rectangle, else the source.
fn img_fill(j: u32) -> u32 {
    let ch = params.channels;
    let p = j / ch;
    let x = p % params.width;
    let y = p / params.width;
    let inside = x >= params.f0 && x < params.f0 + params.f2 && y >= params.f1 && y < params.f1 + params.f3;
    if (inside) {
        return img_aux(j % ch);
    }
    return img_src(j);
}

// reference::mask_rop: AND (f0 = 0), OR (1) or XOR (2) of the two masks.
fn img_mask_rop(j: u32) -> u32 {
    let a = img_src(j);
    let b = img_aux(j);
    if (params.f0 == 0u) {
        return a & b;
    }
    if (params.f0 == 1u) {
        return a | b;
    }
    return a ^ b;
}

// reference::palette: byte `j % 4` of palette entry `index of pixel j / 4`.
fn img_palette(j: u32) -> u32 {
    return img_aux(img_src(j / 4u) * 4u + j % 4u);
}

// reference::color_key_mask: 255 where R, G and B of pixel `j` lie in the range, else 0.
fn img_color_key(j: u32) -> u32 {
    let base = j * params.channels;
    let r = img_src(base);
    let g = img_src(base + 1u);
    let b = img_src(base + 2u);
    let inside = r >= params.f0 && r <= params.f3 && g >= params.f1 && g <= params.f4 && b >= params.f2 && b <= params.f5;
    return select(0u, 255u, inside);
}

// reference::line: whether pixel (px, py) is on the segment (`on_line` in image_tests.rs, which
// checks it against the reference on a grid). The major axis is x when |dx| >= |dy|; step i along
// it lands on minor offset floor((2 * minor * i + major) / (2 * major)), rounding half up in the
// direction of the line. A segment with no extent on an axis has offset 0 there.
fn img_line_on(px: i32, py: i32) -> bool {
    let x0 = bitcast<i32>(params.f0);
    let y0 = bitcast<i32>(params.f1);
    let sx = bitcast<i32>(params.f2);
    let sy = bitcast<i32>(params.f3);
    let a = params.f4;
    let b = params.f5;
    let dx = px - x0;
    let dy = py - y0;
    if (a == 0u && b == 0u) {
        return dx == 0 && dy == 0;
    }
    if (params.f6 != 0u) {
        let i = dx * sx;
        if (i < 0 || u32(i) > a) {
            return false;
        }
        let ui = u32(i);
        return dy == sy * i32((2u * b * ui + a) / (2u * a));
    }
    let j = dy * sy;
    if (j < 0 || u32(j) > b) {
        return false;
    }
    let uj = u32(j);
    return dx == sx * i32((2u * a * uj + b) / (2u * b));
}

// reference::line: the aux colour (`channels` bytes) on the segment, else the source.
fn img_line(j: u32) -> u32 {
    let ch = params.channels;
    let p = j / ch;
    if (img_line_on(i32(p % params.width), i32(p / params.width))) {
        return img_aux(j % ch);
    }
    return img_src(j);
}

fn img_unpack(w: u32) -> vec4<u32> {
    return vec4<u32>(w & 0xffu, (w >> 8u) & 0xffu, (w >> 16u) & 0xffu, w >> 24u);
}

fn img_pack(v: vec4<u32>) -> u32 {
    return v.x | (v.y << 8u) | (v.z << 16u) | (v.w << 24u);
}

// One channel of `over` (reference::blend): straight or premultiplied, with the reference's
// rounding. Premultiplied sums saturate at 255.
fn img_mix(t: u32, u: u32, a: u32) -> u32 {
    let rest = (u * (255u - a) + 127u) / 255u;
    if (params.f1 != 0u) {
        return min(t + rest, 255u);
    }
    return (t * a + u * (255u - a) + 127u) / 255u;
}

// reference::blend's `over(top, under)`.
fn img_over(top: vec4<u32>, under: vec4<u32>) -> vec4<u32> {
    let a = top.w;
    let alpha = min(a + (under.w * (255u - a) + 127u) / 255u, 255u);
    return vec4<u32>(img_mix(top.x, under.x, a), img_mix(top.y, under.y, a), img_mix(top.z, under.z, a), alpha);
}

// reference::blend for pixel `t` (the foreground in src, the destination in aux): mode 0 is
// SrcOver, 1 Src, 2 DstOver.
fn img_blend(t: u32) -> u32 {
    let fg = img_unpack(src[t]);
    let under = img_unpack(aux[t]);
    var out = fg;
    if (params.f0 == 0u) {
        out = img_over(fg, under);
    } else if (params.f0 == 2u) {
        out = img_over(under, fg);
    }
    return img_pack(out);
}

fn img_out_byte(j: u32) -> u32 {
    switch params.op {
        case 0u: { return img_area(j); }
        case 1u: { return img_bilinear(j); }
        case 2u: { return img_crop(j); }
        case 3u: { return img_flip(j); }
        case 4u: { return img_rotate(j); }
        case 5u: { return img_fill(j); }
        case 7u: { return img_palette(j); }
        case 8u: { return img_color_key(j); }
        case 9u: { return img_mask_rop(j); }
        case 10u: { return img_line(j); }
        default: { return 0u; }
    }
}

fn img_word(t: u32) -> u32 {
    if (params.op == 6u) {
        return img_blend(t);
    }
    var word = 0u;
    for (var k = 0u; k < 4u; k++) {
        let j = t * 4u + k;
        if (j < params.out_len) {
            word |= (img_out_byte(j) & 0xffu) << (8u * k);
        }
    }
    return word;
}

@compute @workgroup_size(64)
fn image_op(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = gid.x + gid.y * groups.x * WG;
    if (t >= params.threads) { return; }
    dst[t] = img_word(t);
}
