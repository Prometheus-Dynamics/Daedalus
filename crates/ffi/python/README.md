# Daedalus FFI Python

Python worker and packaging integration target for Daedalus FFI plugins.

## Target Model

Python plugins use `BackendRuntimeModel::PersistentWorker`: load the module once, perform
the worker protocol handshake, advertise supported node ids, and handle repeated `InvokeRequest`
messages without respawning Python for every node call.

## Package Shape

Python packages should emit or lower into:

- `PluginSchema` for node and port shape
- per-node `BackendConfig` with `backend = python`, `runtime_model = persistent_worker`,
  `entry_module`, `entry_symbol`, and `executable`
- `PluginPackage` artifacts for source files under `_bundle/src/`

Large bytes and image-shaped values should use `WireValue` JSON first, then move to
raw payload or handle paths where the host transport supports them.

## Wire Values

The SDK's `to_wire(value, ty=None)` and `from_wire(wire)` convert between Python values and
`WireValue` JSON objects. Annotate unsigned 64-bit ports with `daedalus_ffi.u64` (`int` is `i64`):
`to_wire` writes ints on `u64` ports, and any int above `i64::MAX`, as `{"kind":"uint"}`, and
rejects values outside `i64`/`u64` range.

## Current Status

This crate is the home for Python persistent worker code, packaging helpers, and SDK validation
against the shared schema. Inline Python embedding is not part of the baseline execution
model.
