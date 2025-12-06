# Daedalus

Daedalus is a modular dataflow runtime for building graphs of typed nodes that can run on CPU or GPU. The workspace is split into focused crates (registry, planner, runtime, GPU, macros, engine facade, plugins) so downstream users can pick only the layers they need.

- Fast, typed node execution with CPU/GPU affinities and sync policies.
- Planner/runtime split so graphs can be validated and optimized before execution.
- GPU path built on wgpu, with derive helpers for binding WGSL shaders.
- Macro tooling to generate node descriptors/handlers with minimal boilerplate.
- crates.io package name: `daedalus-rs` (crate name `daedalus`).

## Quick start
- Add dependency: `daedalus = { package = "daedalus-rs", version = "1.0.0" }`
- Format: `cargo fmt --all`
- Lint: `cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings`
- Tests: `cargo test --workspace --features "engine,plugins"` (add `gpu-wgpu` to hit GPU paths)
- Run a CPU example: `cargo run -p daedalus-rs --features "engine,plugins" --example cpu_image`
- Run a GPU example (needs a compatible device/driver):  
  `cargo run -p daedalus-rs --features "engine,plugins,gpu-wgpu" --example gpu_image`

## Architecture at a glance
- **core**: shared enums/IDs/metrics used everywhere.
- **data**: portable type/value model for ports (JSON/proto/schema helpers).
- **registry**: node descriptor store with validation and plugin install support.
- **planner**: turns a logical graph into an executable plan with scheduling, sync groups, and compute affinities.
- **runtime**: executes plans; manages handlers, capabilities, backpressure, telemetry, plugins, and GPU hand-off.
- **gpu**: thin wgpu-backed layer plus shader dispatch helpers and pooling.
- **macros**: `#[node]` and GPU binding/state derive macros.
- **engine**: convenience facade that wires registry → planner → runtime.
- **daedalus**: user-facing facade re-exporting the public API.
- **nodes**: demo/bundle nodes used as fixtures and examples.
- **plugins**: optional plugin crates (math, images) showing external node packs.

See crate-level READMEs in each subdirectory for deeper details.

## Core concepts
- **Node**: A function annotated with `#[node]` describing ID, ports, optional state, compute affinity (CPU/GPU preferred/required), and custom capabilities.
- **Graph**: Nodes + edges by port name; validated against the registry.
- **Planner**: Produces a runtime plan (segments, sync groups, GPU requirements) from a graph and registry.
- **Runtime**: Executes the plan with backpressure policies and telemetry; negotiates plugins and GPU resources.
- **GPU shaders**: WGSL compute shaders dispatched via derived bindings that infer layouts/workgroups.

## Config-backed inputs (example)
```rust
use daedalus::macros::{node, NodeConfig};
use daedalus::runtime::NodeError;

#[derive(Clone, Debug, NodeConfig)]
struct BlurConfig {
    #[port(default = 3, min = 1, max = 31, odd = true, policy = "clamp")]
    radius: i32,
    #[port(default = 1.0, min = 0.1, max = 5.0, policy = "error")]
    sigma: f32,
}

#[node(id = "demo:blur", inputs("image", config = BlurConfig), outputs("out"))]
fn blur(image: Vec<u8>, cfg: BlurConfig) -> Result<Vec<u8>, NodeError> {
    // use cfg.radius / cfg.sigma
    Ok(image)
}
```

## Feature flags (high level)
- `engine`: enable the engine facade and examples.
- `plugins`: enable plugin registry and macro-generated installers.
- `gpu-wgpu`: enable the real GPU backend; without it, GPU-required nodes are rejected and GPU-preferred nodes fall back to CPU.
- `gpu-mock`: deterministic mock GPU for tests; `gpu-noop` is the default fallback.
- Planner/registry/data offer `schema`/`proto`/`json`/`gpu` flags; see their READMEs for exact switches.

## Example sampler
- `cpu_image`: load → invert → save PNG (CPU).
- `cpu_text`: trim/uppercase/join strings.
- `cpu_branch`: fan-out/fan-in arithmetic.
- `typed_any`: typed payload demo.
- `gpu_image`: brighten an image via WGSL compute shader.
- `gpu_shader_nodes`: multi-stage GPU pipeline using derived shader bindings/state.

## Development workflow
- Format + lint: `cargo fmt --all` then `cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings`.
- Tests: `cargo test --workspace --features "engine,plugins"` (add `gpu-wgpu` when you can run GPU tests).
- UI/macro diagnostics: trybuild tests in `crates/nodes/tests/ui`.
- Planner/runtime golden tests live under `crates/planner/tests` and `crates/runtime/tests`.

## Repo map
- `crates/*`: core library crates (see per-crate READMEs).
- `plugins/*`: optional plugin crates.
- `crates/daedalus/examples`: end-to-end examples (CPU + GPU).
- `scripts/*`: feature matrix checks for CI.

## GPU notes
- GPU shaders are WGSL; bindings and workgroup sizes can be inferred or specified via derive attributes.
- GPU-required nodes will be rejected by planner/runtime if no compatible backend is available.
- Pooling is used for buffers/textures to reduce allocations; caches are LRU-bounded.

## License
Licensed under either of:
- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)
