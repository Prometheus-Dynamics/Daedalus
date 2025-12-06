# daedalus (facade)

Crates.io package name: `daedalus-rs` (crate name `daedalus`).

Add to your Cargo.toml:
`daedalus = { package = "daedalus-rs", version = "1.0.0" }`

User-facing crate that re-exports the primary APIs from the workspace: registry, planner, runtime, GPU helpers, macros, and engine.

## What it includes
- Re-exports from `daedalus-core`, `daedalus-data`, `daedalus-registry`, `daedalus-planner`, `daedalus-runtime`, `daedalus-gpu`, `daedalus-macros`, and optional `daedalus-engine`.
- Feature flags mirror the underlying crates: `engine`, `plugins`, `gpu`, `gpu-wgpu`, `gpu-mock`, `schema`, `proto`, `examples`.

## Typical use
- Depend on `daedalus` to get the combined API.
- Enable `engine`/`plugins` for the full runtime + convenience facade.
- Enable `gpu-wgpu` to allow GPU-preferred/required nodes to execute on a real device.
- Use the examples under `crates/daedalus/examples` as templates for building graphs and nodes.

## Examples
- CPU pipelines: `cpu_image`, `cpu_text`, `cpu_branch`, `typed_any`.
- GPU pipelines: `gpu_image`, `gpu_shader_nodes`, `gpu_segments`.
