// Minimal image invert example (matches the manifest shader harness conventions).
// Expected input: RGBA8 bytes in a storage buffer (language-side wraps it as base64 PNG in examples).

@group(0) @binding(0)
var<storage, read_write> data: array<u32>;

@compute @workgroup_size(1, 1, 1)
fn main() {
  // This fixture is intentionally tiny; real shaders should define their own buffer layouts.
  // Here we treat the first u32 as packed RGBA8 and invert RGB.
  let px: u32 = data[0];
  let r: u32 = 255u - ((px >> 0u) & 255u);
  let g: u32 = 255u - ((px >> 8u) & 255u);
  let b: u32 = 255u - ((px >> 16u) & 255u);
  let a: u32 = (px >> 24u) & 255u;
  data[0] = (r << 0u) | (g << 8u) | (b << 16u) | (a << 24u);
}

