# Daedalus FFI Node

Node.js worker and packaging integration target for Daedalus FFI plugins.

## Target Model

Node plugins use `BackendRuntimeModel::PersistentWorker`: import modules once, negotiate
the worker protocol, advertise supported node ids, and dispatch repeated `InvokeRequest` messages.

## Package Shape

Node packages should emit or lower into:

- `PluginSchema` for node and port shape
- per-node `BackendConfig` with `backend = node`, `runtime_model = persistent_worker`,
  `entry_module`, `entry_symbol`, and `executable`
- `PluginPackage` artifacts for JavaScript or bundled TypeScript output under `_bundle/src/`

TypeScript compilation and Node bundle generation should remain explicit build steps or feature
paths. The default package format should record the generated runtime artifact rather than requiring
the host to run language build tools at load time.

## Current Status

This crate is the home for Node persistent worker code, packaging helpers, TypeScript build
validation, and schema validation.
