# Documentation

This directory is the repository-level documentation index. Crate-specific details live beside each crate.

## Start Here

- [../README.md](../README.md): workspace overview, feature sets, examples, and validation commands.
- [development.md](development.md): repo layout, dependency policy, runtime defaults, observability, and production API conventions.
- [testing.md](testing.md): local, feature, release, Docker, and extended test surfaces.
- [../testing/README.md](../testing/README.md): short testing checklist.

## Runtime And Engine

- [../crates/daedalus/README.md](../crates/daedalus/README.md): facade crate and feature selection.
- [../crates/core/README.md](../crates/core/README.md): common ids, clocks, channels, policies, and errors.
- [../crates/transport/README.md](../crates/transport/README.md): payload identity, access, residency, adapters, and lifecycle.
- [../crates/data/README.md](../crates/data/README.md): value/type model and serialization helpers.
- [../crates/registry/README.md](../crates/registry/README.md): capability and node declaration registry.
- [../crates/planner/README.md](../crates/planner/README.md): graph validation, lowering, adapter resolution, and scheduling.
- [../crates/runtime/README.md](../crates/runtime/README.md): executor, host bridge, streaming, state, and telemetry.
- [runtime-diagnostics.md](runtime-diagnostics.md): release/debug flow for runtime telemetry, host bridge diagnostics, stream workers, and FFI workers.
- [host-bridge-lock-granularity.md](host-bridge-lock-granularity.md): release review note for host bridge shared-state locking.
- [../crates/engine/README.md](../crates/engine/README.md): high-level host facade and warm reuse model.

## Optional Layers

- [../crates/gpu/README.md](../crates/gpu/README.md): GPU backends, handles, shader helpers, and async dispatch.
- [../crates/wgsl-infer/README.md](../crates/wgsl-infer/README.md): lightweight WGSL metadata inference.
- [../crates/macros/README.md](../crates/macros/README.md): proc macro surface.
- [../crates/nodes/README.md](../crates/nodes/README.md): built-in/demo node bundles.
- [../crates/daemon/README.md](../crates/daemon/README.md): optional long-lived engine process.

## Plugins And FFI

- [../examples/plugins/README.md](../examples/plugins/README.md): native Rust plugin examples.
- [../crates/ffi/README.md](../crates/ffi/README.md): FFI workspace overview.
- [ffi-ergonomics.md](ffi-ergonomics.md): release review note for FFI package install and invocation ergonomics.
- [../crates/ffi/core/README.md](../crates/ffi/core/README.md): schema, backend, package, and worker protocol contracts.
- [../crates/ffi/sdk-authoring.md](../crates/ffi/sdk-authoring.md): target SDK authoring shape.
- [../crates/ffi/feature-matrix.md](../crates/ffi/feature-matrix.md): cross-language FFI feature tracking.
- [../examples/08_ffi/README.md](../examples/08_ffi/README.md): FFI showcase examples.
