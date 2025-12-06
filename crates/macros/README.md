# daedalus-macros

Procedural macros that generate node descriptors/handlers and GPU bindings with minimal boilerplate.

## Macros
- `#[node]`: annotate a function to generate a node descriptor, handler, and registration glue (IDs, ports, compute affinity, state).
- `#[derive(GpuBindings)]`: derive WGSL binding packs for GPU shaders with binding inference and workgroup hints.
- `#[derive(GpuStateful)]`: mark POD structs for persistent GPU state buffers.

## Shader binding derive (high level)
- `#[gpu(spec(src = "...", entry = "...", workgroup_size = N))]` on the binding struct points to WGSL.
- Field attributes: `#[gpu(binding = N)]`, `texture2d(format = "...", write)]`, `uniform`, `storage(read|read_write|write)`, `sampler(...)`, `state`.
- Inference: WGSL bindings/workgroup sizes inferred when not explicitly provided; compile-time validation against WGSL.

## Node macro (high level)
- Attributes for `id`, `inputs/outputs`, `compute` affinity, `shaders(...)`, `state(...)`, capabilities, and plugin glue.
- Generates descriptor + handler registry entries; integrates with registry/planner/runtime.

## Testing
- Trybuild UI tests under `crates/nodes/tests/ui` exercise diagnostics and macro outputs.
