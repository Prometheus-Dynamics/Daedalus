# Daedalus FFI C/C++

C and C++ ABI and packaging integration target for Daedalus FFI plugins.

## Target Model

C and C++ plugins should stay on `BackendRuntimeModel::InProcessAbi` when they expose trusted shared
libraries with stable symbols. They should not be routed through the persistent worker pool.

## Trust And Safety

The C/C++ path is an in-process native ABI. It is intended for trusted shared libraries that are
built against the expected Daedalus FFI header and loaded from verified packages. This path is not a
sandbox: native plugin code has the same process permissions as the host, and ABI mismatches can
cause undefined behavior if they are not rejected before registration.

C/C++ package validation should happen before the library is loaded. Runtime loading should then
check required symbols, ABI version metadata, and package integrity before installing handlers into
the host registry.

## Package Shape

C/C++ packages should emit:

- `PluginSchema` for node and port shape
- per-node `BackendConfig` with `backend = c_cpp`, `runtime_model = in_process_abi`,
  `entry_module` pointing at the shared library, and `entry_symbol`
- `PluginPackage` artifacts for shared libraries under `_bundle/native/<platform>/`

Generated schema metadata from C/C++ libraries is future work. Until that lands, C/C++ package
builders must provide explicit package descriptors for node declarations.

## Current Status

This crate is the home for C/C++ ABI helpers, package APIs, and validation that do not require a
language worker process.
