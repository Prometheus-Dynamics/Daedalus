# daedalus-wgpu-author

What `#[node]` expands to when a node's body is a shader.

## The three levels

A GPU node is written at one of three levels. Pick the lowest that does the job.

| Level | You write | What Daedalus can do with it |
| --- | --- | --- |
| Per-pixel function | One WGSL function of a pixel | Merge it with its neighbours into one shader |
| Whole shader | A compute shader with its own bindings | Share a submit with other nodes, never merge |
| Your own recording | Rust that records into the encoder | Order it, nothing more |

This crate is the whole-shader level. The other two arrive with their own PRs.

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

## What the macro makes

A `BodyRecord` for the backend `wgpu`. Its `code` is a `daedalus_wgpu::shader::ShaderSpec`: the
entry point, the workgroup size, the binding table and the dispatch shape, all constants. Its
`needs` are the limits the shader relies on, which the backend checks before it takes the node.

Pipelines are cached per device under a key that is FNV-1a over the name, entry point, source and
binding table. The key depends on content only, so two nodes with the same shader share a pipeline
and the key means the same thing in every process.
