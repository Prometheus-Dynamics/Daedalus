# daedalus

Facade crate for application users. The package is published as `daedalus-rs`; the Rust crate name is `daedalus`.

## Purpose

Use this crate when an application wants the public Daedalus API from one dependency instead of depending on each internal crate directly. It re-exports core, data, transport, registry, planner, runtime, macros, optional engine, optional GPU, and plugin helpers.

## Feature Selection

- `engine`: high-level execution facade.
- `plugins`: plugin registry and plugin macro installation.
- `gpu-types`: GPU handles and type surface.
- `gpu-runtime`: registry/planner/runtime GPU wiring.
- `gpu-engine`: engine GPU wiring.
- `gpu-wgpu`: real `wgpu` backend.
- `gpu-async`: async `wgpu` shader dispatch/readback helpers.
- `gpu-mock`: deterministic mock GPU backend.
- `schema` and `proto`: optional export surfaces.

For most host applications, start with `engine,plugins`. Add GPU features only when the host actually needs GPU planning or execution.
