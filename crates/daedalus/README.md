# daedalus (facade)

Crates.io package name: `daedalus-rs` (crate name `daedalus`).

Add to your Cargo.toml:
`daedalus = { package = "daedalus-rs", version = "2.0.0" }`

User-facing crate that re-exports the primary APIs from the workspace: registry, planner, runtime, optional GPU helpers, macros, and engine.

## What it includes
- Re-exports from `daedalus-core`, `daedalus-data`, `daedalus-registry`, `daedalus-planner`, `daedalus-runtime`, `daedalus-macros`, optional `daedalus-gpu`, and optional `daedalus-engine`.
- Feature flags mirror the underlying crates: `engine`, `plugins`, `gpu-types`, `gpu-runtime`, `gpu-engine`, `gpu`, `gpu-wgpu`, `gpu-async`, `gpu-mock`, `schema`, `proto`, `examples`.

## Typical use
- Depend on `daedalus` to get the combined API.
- Enable `engine`/`plugins` for the full runtime + convenience facade.
- Enable `gpu-types` when code only needs GPU handles/options, `gpu-runtime` for planner/runtime GPU wiring, or `gpu-engine` for high-level engine GPU wiring.
- Enable `gpu-wgpu` to allow GPU-preferred/required nodes to execute on a real device.
- Enable `gpu-async` for async shader dispatch/readback paths on the real backend.
- Use the workspace `examples` package as templates for building graphs and nodes.

## Examples
- Quickstart: `quickstart_typed_cpu_graph`, `quickstart_bounded_streaming_io`.
- Basics: `hello_graph`, `typed_ports`, `typed_handle_graph`.
- GPU: `gpu_node`, `upload_download`, `mixed_cpu_gpu`, `gpu_fallback`.
