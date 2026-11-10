# daedalus-wgpu-author

What `#[node]` expands to when a node's body is a shader.

## The three levels

A GPU node is written at one of three levels. Pick the lowest that does the job.

| Level | You write | What Daedalus can do with it |
| --- | --- | --- |
| Per-pixel function | One WGSL function of a pixel | Merge it with its neighbours into one shader |
| Whole shader | A compute shader with its own bindings | Share a submit with other nodes, never merge |
| Your own recording | Rust that records into the encoder | Order it, nothing more |

This crate covers all three.

## A per-pixel function

The body is a const holding one WGSL function, `apply`, of the pixel it is given.

```rust
#[node(
    id = "demo.normalize",
    inputs(c, cfg),
    outputs(out),
    backend = daedalus_wgpu_author(pixel, c: Samples, cfg: Normalize, out: Samples),
)]
const NORMALIZE: &str = "
    struct Config { mean: f32, scale: f32 }
    fn apply(c: vec4<f32>, cfg: Config) -> vec4<f32> { return (c - cfg.mean) * cfg.scale; }
";
```

- The first parameter is the pixel, and `apply` returns the pixel it makes. Each is a number, a vector of numbers, or a struct of them.
- A parameter named `cfg` is the node's config: a struct, fed by the port of that name, and zeroes when nothing is connected.
- Any other parameter is a side input, read at the same pixel from a buffer of its own.
- A pixel body sees nothing else. A global, a binding, an entry point, an atomic, a barrier or workgroup memory makes it a whole shader, and the build says so.
- The node has exactly one output.

When the backend compiles a region, pixel bodies that feed each other, with nothing else reading the values between them, become one shader: one pass, one loop over the pixels, and no buffer in between. A second consumer of a value, or a node that isn't a pixel body, ends a chain. Helpers, structs and constants in different bodies can share names: each body's top-level names are prefixed when the bodies are put together.

## A whole-shader node

The body is a `const` holding WGSL, as a string or `include_str!` of a file beside the source.

```rust
use daedalus_macros::node;

#[node(
    id = "demo.scale",
    inputs(src),
    outputs(dst),
    backend = daedalus_wgpu_author(src: Samples, dst: Samples),
)]
const SCALE: &str = include_str!("scale.wgsl");
```

```wgsl
struct Config { gain: f32, bias: f32 }
@group(0) @binding(0) var<storage, read> src: array<f32>;
@group(0) @binding(1) var<storage, read_write> dst: array<f32>;
@group(0) @binding(2) var<uniform> config: Config;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    if id.x < arrayLength(&dst) {
        dst[id.x] = src[id.x] * config.gain + config.bias;
    }
}
```

- A port is fed by the binding with the same name. The shader has no Rust signature, so the
  backend's arguments give each port its type: `src: Samples`.
- The WGSL is read when the node is compiled, with naga. Only real declarations count, so a
  `@binding` in a comment is nothing, and a `@workgroup_size` built from a `const` is read.
  Nothing parses WGSL while a graph runs.
- All bindings are in `@group(0)`.
- A written buffer has as many elements as the first array the shader reads. When the shader
  reads none, the node says how large with `bytes.<port>` in its metadata.
- A crate with shader nodes depends on `daedalus-wgpu`, `daedalus-registry` and `daedalus-types`,
  which the generated code names.

### How many workgroups

| You write | The run dispatches |
| --- | --- |
| nothing | One invocation per element of the first storage buffer the shader writes |
| `per = name` | One invocation per element of that buffer |
| `count = [x, y, z]` | Exactly those workgroups |

A module with more than one compute entry point needs `entry = "name"`.

### The binding table

| WGSL | Kind | Bound at run time |
| --- | --- | --- |
| `var<storage, read>` | Storage, read | A storage buffer |
| `var<storage, read_write>` | Storage, written | A storage buffer |
| `var<uniform>` | Uniform | The struct's bytes |
| `texture_2d<f32 \| u32 \| i32>`, `texture_depth_2d` | Sampled texture | A texture |
| `texture_storage_2d<format, access>` | Storage texture | A texture of that format |
| `sampler`, `sampler_comparison` | Sampler | The shader's own, made when it loads |

Storage texture formats are `rgba8unorm`, `r32float`, `rg32float`, `rgba16uint` and `rg32uint`.
Textures are two-dimensional and not arrays.

### What fails to compile

The limits are the ones every WebGPU device meets, so a shader that compiles runs anywhere. The
device's own, larger limits are checked when the shader loads.

- More than 8 storage buffers or 4 storage textures. The error gives the count.
- A workgroup over 256 invocations, or over 256 x 256 x 64.
- More than 16 KiB of `var<workgroup>` memory.
- A compute entry point with no `@workgroup_size`.
- A port that names no binding, or names a sampler.
- A function. A shader node is a const.

## Your own recording

When a shader with fixed bindings isn't enough, the body is a function that records into the region's encoder. It can open any passes, with pipelines of its own, and it never submits: the region does, once, for every node in it.

```rust
use daedalus_macros::node;
use daedalus_wgpu::record::{RecordBody, RecordCx};

fn record_blur(cx: &mut RecordCx<'_>) -> Result<(), String> {
    let (src, dst) = (cx.input(0).ok_or("no input")?, cx.output(0).ok_or("no output")?);
    // Open passes on cx.encoder(), using src.wgpu() and dst.wgpu().
    Ok(())
}

#[node(
    id = "demo.blur",
    inputs(src),
    outputs(dst),
    backend = daedalus_wgpu_author(record, src: Samples, dst: Samples),
)]
const BLUR: RecordBody = RecordBody::new(record_blur);
```

- Inputs and outputs are by port index, in the order the node declares them.
- An output is as large as the first input. `RecordBody::new(f).sized(|inputs, output| ..)` says otherwise.
- An error from the function faults the region's step at this node, and nothing is submitted.
- `daedalus_wgpu::fragment::FragmentPass` is a ready-made pass for per-pixel work on tile-based GPUs: one WGSL fragment function, drawn over a whole target or over rectangles of it.

## What the macro makes

A `BodyRecord` for the backend `wgpu`. Its `code` is a `daedalus_wgpu::shader::ShaderSpec`: the
entry point, the workgroup size, the binding table and the dispatch shape, all constants. Its
`needs` are the limits the shader relies on, which the backend checks before it takes the node.

Pipelines are cached per device under a key that is FNV-1a over the name, entry point, source and
binding table. The key depends on content only, so two nodes with the same shader share a pipeline
and the key means the same thing in every process.
