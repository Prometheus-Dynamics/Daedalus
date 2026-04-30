# Daedalus FFI Core

Core schema, backend runtime, package, and wire protocol contracts for Daedalus FFI plugins.

This crate intentionally avoids language runtimes, GPU backends, image codecs, and plugin host
execution. Higher-level FFI crates build on these types.

## Contract Stability

The contract is split into three artifacts.

### Stable Fields

`PluginSchema` is the stable schema surface:

- `schema_version`
- `plugin.name`
- `plugin.version`
- `plugin.description`
- `dependencies`
- `required_host_capabilities`
- `feature_flags`
- `boundary_contracts`
- `nodes`
- node `id`
- node `backend`
- node `entrypoint`
- node `label`
- node `stateful`
- node `feature_flags`
- node `inputs`
- node `outputs`
- port `name`
- port `ty`
- port `type_key`
- port `optional`
- port `access`
- port `residency`
- port `layout`
- port `source`
- port `const_value`

`BackendConfig` is the stable runtime surface:

- `backend`
- `runtime_model`
- `entry_module`
- `entry_class`
- `entry_symbol`
- `executable`
- `args`
- `classpath`
- `native_library_paths`
- `working_dir`
- `env`

`InvokeRequest`, `InvokeResponse`, `InvokeEvent`, and `WireValue` are the stable host/backend
transport surface for JSON workers.

### Experimental Fields

These fields are intentionally available for package and backend experiments, but callers should
not rely on long-term semantics without an explicit stability note:

- `plugin.metadata`
- node `metadata`
- `BackendConfig.options`
- `PluginPackage.metadata`
- package artifact `metadata`
- `ImagePayload`
- handle-based or resident payload behavior layered through `Payload`

### JSON-Oriented Fields

`ByteEncoding::Base64` exists for JSON/package descriptors, logs, snapshots, and workers that do not
support raw transport payloads. Host paths that can carry raw bytes should use `ByteEncoding::Raw`
and `WireValue::into_payload`.

## Worker Transport Format

JSON-lines is the baseline worker protocol. Every worker implementation should support JSON-lines
before adding an optimized format. This keeps diagnostics readable, package fixtures portable, and
cross-language conformance tests straightforward.

Every line is one serialized `WorkerMessage`:

```json
{"protocol_version":1,"correlation_id":"req-1","payload":{"type":"invoke","payload":{}}}
```

The `payload.type` variants are:

- `hello`
  Worker startup metadata. Carries the worker protocol range, backend kind, supported node ids,
  capability summary, and optional backend metadata.
- `ack`
  Host acknowledgement after version negotiation. Carries the negotiated protocol version and
  optional worker id.
- `invoke`
  Host request. Carries `InvokeRequest`: protocol version, node id, correlation id, input args,
  optional state, and context metadata.
- `response`
  Worker response. Carries `InvokeResponse`: protocol version, correlation id, output map, optional
  state, and structured events.
- `event`
  Out-of-band diagnostic event using the same `InvokeEvent` shape as response events.
- `error`
  Out-of-band worker error with a stable code, message, and metadata map.

### Handshake

Persistent workers start by sending `WorkerHello`. The host validates the advertised protocol range
and replies with `WorkerProtocolAck::from_hello`. The negotiated version is the lower of the host
version and the worker's maximum version, as long as the host version is within the worker's
supported range.

Workers should fill `supported_nodes` before accepting invokes. Host installation can use that list
to reject a package before the first invocation if the runner does not advertise every node assigned
to it.

### Invocation

After the handshake, the host sends `WorkerMessagePayload::Invoke` messages. The message
`correlation_id` and the inner `InvokeRequest.correlation_id` should match. Workers must echo that
correlation id in `InvokeResponse` so the host can pair responses with requests and produce stable
diagnostics.

Inputs and outputs use `WireValue`. JSON workers should support all scalar and
structured variants. Large bytes and image-shaped values may start with JSON encoding, then upgrade
to raw `Payload` or future handle-based paths where the host/backend transport supports them.

`InvokeRequest::validate_contract`, `InvokeResponse::validate_contract`, `InvokeEvent::validate_contract`,
and `WireValue::validate_contract` enforce the stable host/backend transport shape after protocol
version checks. Use `validate_against_node` when a request or response should be checked against a
specific `NodeSchema` port surface.

### State And Events

Stateful workers may keep state internally and return a state handle, or may return an explicit
`WireValue` state payload in `InvokeResponse.state`. Host-side state sync is intentionally separate
from output decoding so tests and debugging can export/import state deterministically.

## Conformance Fixtures

`generate_scalar_add_fixtures` produces the first canonical generated fixture set from one spec:
Rust, Python, Node, Java, and C/C++ scalar add. Each generated fixture includes `PluginSchema`,
per-node `BackendConfig`, a canonical `InvokeRequest`, the expected `InvokeResponse`, and minimal
language source files.

`generate_scalar_add_package_fixtures` wraps those fixtures in `PluginPackage` descriptors with
deterministic `_bundle/src/...` artifacts, so package validation and integrity stamping can run for
every supported language from the same source fixture.

Future fixture kinds should extend `CanonicalFixtureKind` and generate from the same spec path
instead of hand-writing separate language manifests.

Workers should place node diagnostics in `InvokeResponse.events` when they are tied to a request.
`WorkerMessagePayload::Event` is reserved for out-of-band process/runtime diagnostics such as
startup warnings or background health information.

Binary protocols such as CBOR or MessagePack are upgrade candidates only after benchmarks show that
JSON serialization is the bottleneck. A binary worker protocol should not become the default unless
all of these are true for at least two out-of-process backends:

- warm invoke latency improves by at least 20 percent for scalar and small structured payloads
- 1 MB byte payload throughput improves by at least 30 percent, excluding process startup time
- 10 MB byte payload throughput improves by at least 30 percent, excluding process startup time
- worker CPU time spent in encode/decode drops by at least 25 percent
- diagnostics still include protocol version, correlation id, backend kind, node id, and typed
  error payloads

Large bytes and images should prefer `WireValue::into_payload`, `Payload`, or future boundary
handles when the host path can carry them directly. Base64 JSON remains the portable path for
language SDKs, package descriptors, logs, snapshots, and workers that do not support raw transport
payloads.

## Package Layout

`PluginPackage` is the package descriptor. It records the schema, per-node backend configs,
artifacts, dependency metadata, lockfile path, optional signature, and package integrity hashes.
The descriptor is intentionally separate from the files it describes so packages can be unpacked,
verified, and installed without repo-relative paths.

Package paths are always package-relative. Absolute paths and `..` components are rejected by
validation. A package loader should treat the directory containing the package descriptor as the
package root.

### Bundle Path Rewriting

Package builders should call `PluginPackage::rewrite_artifact_paths_for_bundle` before stamping or
writing descriptors. This rewrites every artifact path into the deterministic bundle layout used by
`bundled_artifact_path`:

- `SourceFile` -> `_bundle/src/<file>`
- `CompiledModule` -> `_bundle/modules/<file>`
- `Jar` and `ClassesDir` -> `_bundle/java/<file-or-dir-name>`
- `SharedLibrary` and `NativeLibrary` -> `_bundle/native/<platform>/<file>`
- `ShaderAsset` -> `_bundle/shaders/<file>`
- `Lockfile` -> `_bundle/locks/<file>`
- `Other` -> `_bundle/assets/<file>`

Native paths include a platform directory derived from `PackagePlatform`: `os-arch-abi`, with
missing fields replaced by `any`. For example, a Linux x86_64 GNU native library is bundled under
`_bundle/native/linux-x86_64-gnu/`.

Backends must refer to the rewritten bundle paths after packaging. For example:

- Java classpath entries should point at `_bundle/java/*.jar` or `_bundle/java/<classes-dir>`.
- Java native libraries should point at `_bundle/native/<platform>/*`.
- Python and Node source entry modules should point at `_bundle/src/*`.
- C/C++ in-process libraries should point at `_bundle/native/<platform>/*`.
- Shader assets should point at `_bundle/shaders/*`.

### Integrity And Lockfiles

After files are copied into their bundle paths, package builders should call
`PluginPackage::stamp_integrity`. This records per-artifact SHA-256 hashes and the package manifest
hash. Loaders can then use `PluginPackage::read_descriptor_and_verify` to read the descriptor and
verify that all bundled files exist and still match their recorded hashes.

`PluginPackage::generate_lockfile` derives a deterministic lockfile view from the package,
including backend runtime metadata and sorted artifact metadata. Use `write_lockfile` when a
package should carry a separate lockfile artifact.
