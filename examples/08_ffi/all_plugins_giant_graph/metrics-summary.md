# Giant Graph Telemetry Summary

The all-language graph now separates two concerns:

- `coverage`: example-local structural proof that every language exposes the required node shapes.
- `telemetry`: shared `FfiTelemetryReport` emitted by the runtime/FFI host instrumentation.

Example-specific code should not define metric counters. The only example-local report is coverage;
package, backend, worker, payload, adapter, and ABI metrics come from the shared telemetry model.

## Coverage

| Metric | Value |
| --- | ---: |
| Languages | 5 |
| Plugin packages | 5 |
| Nodes invoked | 100 |
| Required node categories per language | 20 |
| Minimum requested categories covered per language | 12 |
| Adapter declarations | 10 |
| Internal adapter lanes | 5 |
| External adapter lanes | 5 |
| GPU nodes | 5 |
| Stateful nodes | 5 |
| Zero-copy nodes | 5 |
| Shared-reference nodes | 5 |
| Copy-on-write nodes | 5 |
| Mutable nodes | 5 |
| Owned/move nodes | 5 |
| Cross-language category-chain edges | 80 |
| Cross-language payload-ref edges | 8 |
| Cross-language adapter edges | 8 |
| Telemetry collector edges | 100 |
| Total graph edges in `giant_graph.rs` | 196 |

## Shared Telemetry

The executable output includes `telemetry`, a `FfiTelemetryReport` with these sections:

| Section | Source |
| --- | --- |
| `packages` | `install_package_with_ffi_telemetry` records validation/load/install metadata. |
| `backends` | `RunnerPool` and in-process ABI hooks record starts, reuses, invokes, bytes, ABI calls, and failures. |
| `workers` | `PersistentWorkerRunner` records handshakes, protocol bytes, encode/decode time, malformed responses, stderr events, typed errors, and raw events. |
| `payloads` | Payload lease and worker request handling record handle creation/resolution, access modes, and ownership modes. |
| `adapters` | Host adapter hooks record adapter id, source/target type keys, origin, calls, duration, and failures. |

## Field Semantics

| Field | Meaning | Exactness |
| --- | --- | --- |
| `packages.*.validation_duration` | Time spent validating package/schema/backend descriptors. | Measured by host wall clock. |
| `packages.*.load_duration` | End-to-end package install path around validation, lowering, and registry install. | Measured by host wall clock. |
| `packages.*.artifact_checks` | Number of declared package artifacts checked by the host package path. | Exact counter. |
| `backends.*.runner_starts` | Runner instances started for persistent-worker backends. | Exact counter. |
| `backends.*.runner_reuses` | Runner lookups/reuses before invoke/state operations. | Exact counter. |
| `backends.*.invokes` | Successful host invokes through `RunnerPool` or in-process ABI hooks. | Exact counter for instrumented paths. |
| `backends.*.invoke_duration` | Host-side duration of a runner invoke call. | Measured by host wall clock. |
| `backends.*.bytes_sent` / `bytes_received` | JSON-wire byte estimate for host request/response values. | Estimated by serialized JSON length. |
| `backends.*.abi_call_duration` | Host-side duration of an in-process ABI call hook. | Measured where ABI hook is used. |
| `workers.*.handshake_duration` | Persistent worker process startup/hello/ack time. | Measured by host wall clock. |
| `workers.*.request_bytes` / `response_bytes` | JSON-lines protocol byte estimates. | Estimated by serialized JSON length. |
| `workers.*.encode_duration` / `decode_duration` | Host-side JSON protocol encode/decode time. | Measured by host wall clock. |
| `payloads.handles_created` | Payload handles leased by the host. | Exact counter. |
| `payloads.handles_resolved` | Payload handles resolved by host or worker request telemetry. | Exact counter for instrumented paths. |
| `payloads.zero_copy_hits` | View-mode payload resolutions. | Exact host-side classification. |
| `payloads.shared_reference_hits` | Read/shared payload resolutions. | Exact host-side classification. |
| `payloads.cow_materializations` | Worker-declared COW payload materializations. | Exact when worker request metadata declares COW. |
| `payloads.mutable_in_place_hits` | Mutable payload resolutions that use modify access. | Exact host-side classification. |
| `payloads.owned_moves` | Move/owned payload resolutions. | Exact host-side classification. |
| `adapters.*.calls` | FFI adapter calls recorded by host adapter hooks. | Exact counter for instrumented adapter hooks. |
| `adapters.*.duration` | Host-side adapter duration. | Measured where adapter hook is used. |

## Per Language Coverage

| Language | Nodes | Adapters | GPU | Stateful | Zero Copy | Shared Ref | COW | Mutable | Owned | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Rust | 20 | 2 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | Native ownership, borrow, COW, and payload contracts are the baseline. |
| Python | 20 | 2 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | Uses memoryview/mmap transport declarations and persistent worker handle tests. |
| Node/TypeScript | 20 | 2 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | Uses Buffer/shared-memory declarations and persistent worker handle tests. |
| Java | 20 | 2 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | Uses direct ByteBuffer/mmap declarations and persistent worker handle tests. |
| C/C++ | 20 | 2 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | Pointer/length ABI is covered through in-process resolver tests. |

## Run

```bash
cargo run --release -p daedalus-ffi-host --example ffi_all_plugins_giant_graph
```

## Benchmark Commands

Use release-mode benchmark runs for throughput claims:

```bash
cargo bench -p daedalus-ffi-host --bench ffi_overhead -- --sample-size 10 --warm-up-time 1 --measurement-time 1
```

The giant graph smoke executable proves field population and cross-language coverage; it is not a
formal benchmark suite. Debug runs are valid for regression shape, but release runs are the only
numbers that should be used for overhead comparisons.

## Latest Release Overhead Check

Captured with:

```bash
CARGO_TARGET_DIR=/tmp/daedalus-ffi-telemetry-target cargo bench -p daedalus-ffi-host --bench ffi_overhead -- --sample-size 10 --warm-up-time 1 --measurement-time 1 'ffi_(warm_invoke|worker_warm_invoke|payload_handle_ref|cross_process_payload_handle_ref)'
CARGO_TARGET_DIR=/tmp/daedalus-ffi-telemetry-target cargo run --release -p daedalus-ffi-host --example ffi_all_plugins_giant_graph
```

The current telemetry-on budget is:

| Path | Budget | Latest Release Result |
| --- | ---: | ---: |
| Direct in-process warm invoke | <= 100 ns | Rust 63.144 ns, C/C++ 65.787 ns |
| Runner-pool warm invoke | <= 2 us | Python 1.1859 us, Node 1.2237 us, Java 1.2776 us |
| Worker warm invoke | <= 2 us | Python 1.1407 us, Node 1.1804 us, Java 1.3386 us |
| Host payload handle ref, 1 MiB | <= 3 us | 1.8112 us |
| Host payload handle ref, 10 MiB | <= 3 us | 1.7589 us |
| Cross-process payload handle, 1 MiB | <= 150 us | Python 108.10 us, Node 113.80 us, Java 96.827 us |
| Cross-process payload handle, 10 MiB | <= 150 us | Python 107.44 us, Node 110.15 us, Java 97.025 us |
| Giant graph package telemetry load total | <= 500 us | 323.826 us across 5 packages |

The payload-handle throughput numbers are high because the handle benchmark measures handle
resolution and protocol metadata, not a full byte copy. Embedded-byte benchmarks remain the copy
path comparison.

## Latest Release Smoke Check

Captured after building the release example:

```bash
target/release/examples/ffi_all_plugins_giant_graph
```

30 process-level runs on this machine reported:

| Metric | Value |
| --- | ---: |
| Min elapsed | 3512.82 us |
| p50 elapsed | 3982.90 us |
| p95 elapsed | 4516.62 us |
| Max elapsed | 5008.95 us |

The emitted shared telemetry reported 5 package records, 98 backend records, 95 backend invokes,
60 runner starts, 60 runner reuses, 12,140 request bytes, 10,268 response bytes, and 36,361 ns of
in-process ABI call time. These are smoke-run measurements of the synthetic transcript graph; use
Criterion suites for formal per-operation overhead budgets.

## Latest Telemetry Overhead Criterion Pass

Captured with:

```bash
cargo bench -p daedalus-ffi-host --bench ffi_overhead -- --sample-size 10 --warm-up-time 1 --measurement-time 1 ffi_telemetry_overhead
```

| Benchmark | Plain | Telemetry | Delta |
| --- | ---: | ---: | ---: |
| Warm invoke | 1.1713 us | 1.8506 us | +0.6793 us |
| 1 MiB payload handle invoke | 1.6883 us | 2.4440 us | +0.7557 us |

The payload handle benchmark measures handle metadata dispatch and response handling in the host
fixture; it does not copy 1 MiB per iteration. Throughput values from this fixture therefore reflect
handle path overhead, not memory bandwidth.
