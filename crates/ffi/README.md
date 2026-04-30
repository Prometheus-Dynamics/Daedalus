# Daedalus FFI Workspace

This directory contains the FFI/plugin crates. The old manifest crate has been removed; new work
targets the split schema, backend, package, and worker surfaces directly.

## Architecture

The surface is built around three artifacts:

- `PluginSchema`
  Schema-only plugin metadata: plugin identity, nodes, ports, type keys, feature flags, boundary
  contracts, and required host capabilities.
- `BackendConfig`
  Runtime-only backend metadata: backend kind, runtime model, entry module/class/symbol, executable,
  args, classpath, native library paths, working directory, environment, and backend options.
- `PluginPackage`
  Physical package metadata: schema, backend configs, bundled artifacts, dependency metadata,
  lockfile, manifest hash, signature, and integrity hashes.

Host/backend traffic uses `InvokeRequest`, `InvokeResponse`, `InvokeEvent`, and `WireValue`. The
baseline worker protocol is JSON-lines `WorkerMessage`; persistent workers should implement that
before adding an optimized binary transport.

## Crates

- `core/`
  Contract crate. Owns schema/backend/package descriptors, worker protocol types, wire values,
  lockfiles, package integrity, and deterministic bundle path rewriting.
- `host/`
  Shared host installer and runner orchestration. Owns schema-to-registry declaration generation,
  package install planning, runner pools, response decoding, state sync, persistent worker process
  handling, and entrypoint validation.
- `python/`
  Python worker and SDK integration.
- `node/`
  Node.js worker and SDK integration.
- `java/`
  Java worker and packaging helpers, including classpath, jar/classes directory, Maven/Gradle
  metadata, native library packaging metadata, and Java worker launch arguments.
- `cpp/`
  C/C++ ABI and package helpers.

## Runtime Models

- `in_process_abi`
  Rust dynamic plugins and C/C++ shared libraries. These run in process and should not go through
  the persistent worker pool.
- `persistent_worker`
  Python, Node, Java, and future out-of-process languages. Workers load code once, negotiate the
  worker protocol, advertise supported nodes, and handle repeated invocations.

Use `in_process_abi` when the plugin is trusted native code that benefits from direct calls and
shared process memory. Use `persistent_worker` for language runtimes, isolated execution, stateful
nodes, classpath/module loading, crash isolation, and deterministic startup negotiation.

## Package Flow

Package APIs build `PluginSchema + BackendConfig + PluginPackage`, rewrite artifacts into
deterministic `_bundle/...` paths, stamp integrity hashes, and write package lockfiles. Backends
must refer to bundled paths after rewriting.

Normal package generation writes wrapper sources and bundles under
`target/ffi-generated/<language>/<out-name>/`. Repo examples are demos only, not the source of truth
for generated plugin wrappers.

## Registry Schema Export

`ffi-host` can export schema JSON from installed registry metadata with
`export_registry_plugin_schema_json`. This uses `PluginManifest` and `NodeDecl` as the source of
truth, preserving node ports, type keys, feature flags, boundary contracts, required host
capabilities, and registry metadata. The split language crates expose validation helpers so Python,
Node, Java, and C/C++ package builders can check their emitted `PluginSchema + BackendConfig`
against the same core contract.

## Troubleshooting

- Worker startup fails:
  Check `BackendConfig.executable`, `args`, `working_dir`, and `env`. For persistent workers, verify
  that the worker emits `WorkerHello` and supports the host protocol version.
- Node id is rejected before invoke:
  The runner advertised `supported_nodes`, and the package requested a node not in that list. Fix the
  package schema/backend mapping or worker startup registration.
- Java class cannot be found:
  Ensure all jars/classes directories are listed in `BackendConfig.classpath` after bundle rewriting
  and that `java_worker_launch` receives the rewritten `_bundle/java/...` paths.
- Java native library fails to load:
  Ensure native libraries are recorded as package artifacts under `_bundle/native/<platform>/` and
  are present in `BackendConfig.native_library_paths`.
- Python or Node module cannot be imported:
  Ensure source or bundled runtime artifacts are present under `_bundle/src/` and that
  `entry_module` refers to the bundled path/module expected by the worker.
- Malformed response:
  Validate `InvokeResponse.protocol_version`, correlation id, output names, and `WireValue` shapes.
  Use `ffi-host` response decoding helpers so all languages report conversion failures the same way.

## Feature Shape

GPU shader and image payload support stay feature-gated inside the appropriate host/core or language
crates until they prove they need standalone crates. `pyo3`, broad image codecs, WGPU integration,
Java bridge compilation, and Node bridge generation should stay behind explicit features in the
crates that actually need them.

## Tooling Direction

Package and SDK library APIs come before CLI workflows. CLI commands such as `plugin new`, `plugin
check`, `plugin build`, and `plugin run` are useful later, but the first stable surface should be the
Rust and language SDK APIs that generate, validate, package, and run the artifacts.
