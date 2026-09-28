# daedalus

Facade crate for application users. The package is published as `daedalus-rs`; the Rust crate name is `daedalus`.

## Purpose

Use this crate when an application wants the public Daedalus API from one dependency instead of depending on each internal crate directly. It re-exports core, data, transport, registry, planner, runtime, macros, optional engine, optional GPU, and plugin helpers.

## Feature Selection

- `engine-full`: `engine` + `executor-pool` + `metrics`; the recommended application preset.
- `engine`: lean high-level execution facade (no worker pool, no metrics).
- `executor-pool`: Rayon worker pool for parallel/adaptive runtime modes.
- `metrics`: executor telemetry collection.
- `embedded`: `engine` + `plugins` without pool or metrics, for constrained hosts.
- `plugins`: plugin registry and `#[plugin]`/`declare_plugin!` installation.
- `dylib-plugins`: native Rust plugin `cdylib`s: `export_plugin!` on the plugin side,
  `PluginLibrary` to load them at runtime; see `docs/dynamic-plugins.md`.
- `gpu-types`: GPU handles and type surface.
- `gpu-runtime`: registry/planner/runtime GPU wiring.
- `gpu-engine`: engine GPU wiring.
- `gpu-wgpu`: real `wgpu` backend.
- `gpu-async`: async `wgpu` shader dispatch/readback helpers.
- `gpu-mock`: deterministic mock GPU backend.
- `schema` and `proto`: optional export surfaces.

Macros (`node`, `plugin`, `type_key`, `adapt`, `device`, and the derives) are re-exported at the
crate root and under `daedalus::macros`; see the `daedalus-macros` item docs for their arguments.

For most host applications, start with `engine-full,plugins`; constrained hosts can use `embedded`. Add GPU features only when the host actually needs GPU planning or execution.
