use daedalus_macros::node;

#[node(id = "ui.blur", inputs(c), outputs(out), backend = daedalus_wgpu_author(element, c: u32, out: u32))]
const BLUR: &str = "
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    fn apply(c: u32) -> u32 { return c + src[1]; }
";

#[node(id = "ui.two", inputs(c), outputs(a, b), backend = daedalus_wgpu_author(element, c: u32, a: u32, b: u32))]
const TWO: &str = "fn apply(c: u32) -> u32 { return c; }";

fn main() {}
