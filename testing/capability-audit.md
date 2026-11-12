# Capability audit against dev

Every capability `dev` has, one row each, with the PR that replaces it on `next` or the reason it
is dropped. `scripts/check-capability-audit.sh` checks this file: every row's last cell is a list
of PR ids that have a brief in `docs/prs/`, or `dropped: <reason>`, and every crate directory on
`dev` is named here.

A row says where a capability went, not that the PR has merged. `python3 scripts/pr-status.py`
shows what is merged.

## Where dev was read

- crates-at: 967479653e9283877d9e1f1497d54f7a2a3fd55e

That is `origin/dev` when this audit was written. Its crate directories:

- crates: accel accelerator bench-counters core daedalus daemon data engine ffi frame-bench gpu host-wrapper kernels macros mcu nodes planner registry runtime surface transport wgsl-infer

## Planning and graph model

| Capability | On dev | Replaced by |
|---|---|---|
| Graph model and its JSON format | `crates/core`, `crates/planner` | A6 |
| Type keys, type expressions, ids, errors | `crates/core` | A4 |
| Registry: records, joining bodies, capabilities | `crates/registry` | A8 |
| Node groups and defaults | `crates/planner`, `crates/nodes` | B1 |
| Ports and typecheck | `crates/planner` | B2 |
| Const inputs and config | `crates/planner` | B3 |
| Conversions, exclusive access, host input policy | `crates/planner` | B4 |
| Rewrite hook (dev's lowerings) | `crates/planner` | B5 |
| Rates and ordering | `crates/planner` | B6 |
| Plan hashing, plan cache and the schema drift guard | `crates/planner`, `crates/engine` | B12 |
| Planner API and forms | `crates/planner` | B13 |
| Lint, diagnostics, explain a plan | `crates/planner` | B11 |
| Graph patches and port refresh | `crates/planner` | B14 |
| `#[node]` macro | `crates/macros` | A9 |
| Macro UI (trybuild) tests | `crates/macros`, `crates/daedalus/tests` | A9 |
| Derive macros for config and types | `crates/macros` | A13 |

## Solver and assignment

| Capability | On dev | Replaced by |
|---|---|---|
| Device assignment | `crates/daedalus/tests/device_assignment.rs` | B7 |
| A region that is refused and assigned again | `crates/planner` | B8 |
| Stitching: transfers, fences, slots, the step list | `crates/planner`, `crates/runtime` | B9 |

## Runtime executor

| Capability | On dev | Replaced by |
|---|---|---|
| Serial runner | `crates/runtime` | C1 |
| Fences and pending steps | `crates/runtime` | C2 |
| Host IO: feed, step, take | `crates/runtime` | C3 |
| Rates, joins, backpressure, edge policies | `crates/runtime` | C4 |
| Edge queues and blocking backpressure | `crates/runtime` | C15, C16 |
| Parallel lanes | `crates/runtime` | C6 |
| Cancellation and progress | `crates/runtime` | C7 |
| Output reuse | `crates/runtime` | C8 |
| State slots and config cache | `crates/runtime` | C9 |
| Async nodes and streams | `crates/runtime`, `examples/04_async` | C13 |

## Failure handling

| Capability | On dev | Replaced by |
|---|---|---|
| Failure rule: backoff, containment, manual off | `crates/daedalus/tests/dylib_retry.rs`, `retry_by_value.rs`, `input_dropped.rs` | C5 |
| Fault reports and manual unmerged mode | `crates/runtime` | C12 |
| Device loss | `crates/gpu` | E11 |

## Engine and host

| Capability | On dev | Replaced by |
|---|---|---|
| Load, start, reload with state carried | `crates/engine`, `crates/engine/tests` | C10 |
| The facade crate | `crates/daedalus` | C14 |
| Host wrapper and host tests | `crates/host-wrapper`, `crates/daedalus/tests/it/host_*.rs` | C10, C14 |
| Host listener | `crates/daemon` | H3 |

## Metrics and telemetry

| Capability | On dev | Replaced by |
|---|---|---|
| Telemetry and metrics | `crates/runtime` (metrics feature) | C11 |
| Hardware counters for benches | `crates/bench-counters` | I2 |

## Values and recycling

| Capability | On dev | Replaced by |
|---|---|---|
| Values, slots, the recycle pool | `crates/data`, `crates/daedalus/tests/recycled_by_value.rs`, `recycled_identity.rs` | A5 |

## Transport and wire

| Capability | On dev | Replaced by |
|---|---|---|
| Values across the wire | `crates/transport` | A5, G1 |
| The transport crate itself | `crates/transport` | dropped: replaced by the value model (A5) and the worker wire (G1) |

## Surface and shared mode

| Capability | On dev | Replaced by |
|---|---|---|
| The C surface | `crates/surface` | G10 |
| Host wrapper shared mode, a safe host over the C surface | `crates/host-wrapper` | G11 |

## Frames

| Capability | On dev | Replaced by |
|---|---|---|
| A frame type and frame library | `crates/daedalus/tests/dylib_frames.rs`, plugin `frames.rs` | dropped: Daedalus has no frame type; a type's owner defines it |
| Moving large values | `crates/data` | A5, A14, E17 |
| Values owned across a plugin boundary | `crates/ffi` | G3 |
| dma-buf into the GPU | `crates/gpu` | E14, E15 |
| Large-value overhead benchmark | `crates/frame-bench` | I3 |
| Colorimetry and pixel conversions | `crates/gpu` | dropped: they belong to whoever owns the type |

## Stable ABI and dylib plugins

| Capability | On dev | Replaced by |
|---|---|---|
| Step ABI and handshake | `crates/ffi` | A3 |
| Loader | `crates/ffi`, `examples/plugins/stable_*` | G2 |
| Stable plugin path and codecs | `crates/daedalus/tests/dylib_stable*.rs`, `dylib_export_macro.rs`, `dylib_external_types.rs` | G3 |
| Reload race | `crates/daedalus/tests/dylib_reload_race.rs` | G2 |
| Stable ABI allocations | `crates/daedalus/tests/dylib_stable_allocations.rs` | I2 |

## Node groups in plugins

| Capability | On dev | Replaced by |
|---|---|---|
| Node groups | `crates/nodes` | B1 |
| Plugin node groups | `examples/plugins/node_groups*` | G2 |
| The bundled starter and demo nodes | `crates/nodes` | dropped: the examples replace them |

## Process workers and languages

| Capability | On dev | Replaced by |
|---|---|---|
| Process backend (new: dev has none) | none | G4 |
| Python, Node, Java, C and C++, Rust SDKs | `crates/ffi` | G5, G6, G7, G8, G9 |
| A dylib plugin run in a worker | none | G12 |

## GPU

| Capability | On dev | Replaced by |
|---|---|---|
| wgpu context and device choice | `crates/gpu` | E1 |
| Buffers, textures, pools | `crates/gpu` | E2 |
| Transfers | `crates/gpu` | E3 |
| Whole-shader nodes and WGSL inference | `crates/gpu`, `crates/wgsl-infer` | E4 |
| Shared submit | `crates/gpu` | E5 |
| Element-wise merge | `crates/gpu` | E6 |
| Recording code | `crates/gpu` | E7 |
| Fences and bounded waits | `crates/gpu` | E8 |
| dma-buf kernel calls | `crates/gpu` | E9 |
| Multi-device | `crates/gpu` | E10 |
| The GPU fallback test | `crates/daedalus/tests/gpu_fallback.rs` | E11, E12 |
| The Khronos validation layer run | `scripts/ci.sh vvl` | E12 |
| Pi board tests | `scripts/ci.sh pi` | E9 (its board run) |

## Kernels and accelerators

| Capability | On dev | Replaced by |
|---|---|---|
| Acceleration inventory and backend matrix | `crates/accel`, `crates/kernels` | dropped: nothing in Daedalus calls a byte kernel |
| WGSL inference | `crates/wgsl-infer` | E4 |
| The accelerator interface | `crates/accelerator` | dropped: the Backend trait (A7) covers it, and I1 is the NPU case |

## CPU and chips

| Capability | On dev | Replaced by |
|---|---|---|
| CPU backend | `crates/runtime` | D1 |
| CPU merging | `crates/runtime` | D2 |
| Blob form | `crates/mcu` | D3 |
| Source form | `crates/mcu` | D4 |
| Chips and the firmware API | `crates/mcu` | D5 |
| Scalars and wire on a chip | `crates/mcu` | D6 |
| Chip examples | `examples/mcu_*` | D7 |
| no_std core | `crates/core`, `crates/data`, `crates/runtime` | C1, A3, A4, A5, A6 |

## CUDA

| Capability | On dev | Replaced by |
|---|---|---|
| The gpu-cuda feature and its test | `crates/gpu`, `crates/daedalus/tests/device_cuda.rs` | dropped: there is no CUDA backend in the design |

## Lean preset

| Capability | On dev | Replaced by |
|---|---|---|
| The lean preset | the `lean-preset` job | dropped: there are no presets; telemetry is one feature, tested by C11 |

## Daemon

| Capability | On dev | Replaced by |
|---|---|---|
| The daemon | `crates/daemon` | H4 |
| Control client | `crates/daemon` | H5 |
| Wire protocol | `crates/daemon` | H1 |
| Client library | `crates/daemon` | H2 |
| Packaging | none | H6 |

## Examples and docs

| Capability | On dev | Replaced by |
|---|---|---|
| Examples 00 to 03 and 09 | `examples/00_quickstart` to `examples/03_transport`, `examples/09_hosts` | I4 |
| Async and metrics examples | `examples/04_async`, `examples/06_metrics_debugging` | I4 |
| GPU example | `examples/07_gpu` | I8 |
| FFI example | `examples/08_ffi` | G10, G11 |
| Plugin examples | `examples/plugins` | G2, G3 |
| wasm examples | `examples/wasm_*` | D9 |
| no_std smoke example | `examples/nostd_smoke` | D10 |
| Surface image example | `examples/surface_image` | dropped: there is no surface crate and no image type |
| Documentation | `docs/*` | I5 |

## Benchmarks

| Capability | On dev | Replaced by |
|---|---|---|
| The bench workflow | `.github/workflows/bench.yml` | I2 |
| Frame benches | `crates/frame-bench` | I2, I3 |

## Gate and CI

Each `scripts/ci.sh` step and each workflow job on `dev`, and the PR whose gate step or workflow
replaces it.

| Step or job on dev | Replaced by |
|---|---|
| `ci.sh lints`: file sizes, workspace deps, GPU async lints, rustfmt | A1, A2, E8 |
| `ci.sh features`: release feature surfaces | I7 |
| `ci.sh clippy` | A1 |
| `ci.sh link`: build every library with all features | A1 |
| `ci.sh test`: workspace tests and dylib plugin tests | A1, G2 |
| `ci.sh macro-ui` | A9 |
| `ci.sh smoke`: run the CPU-only example binaries | I4 |
| `ci.sh doc` | I5 |
| `ci.sh aarch64` | D10 |
| `ci.sh lean` | dropped: there are no presets; telemetry is one feature, tested by C11 |
| `ci.sh nostd` | A2, D10 |
| `ci.sh mcu` | D5, D7 |
| `ci.sh wasm` | D9 |
| `ci.sh semver` | I7 |
| `ci.sh package` | I7 |
| `ci.sh bench` | I2 |
| `ci.sh pi` | E9 (its board run) |
| `ci.sh vvl` | E12 |
| `ci.sh quick` | dropped: the gate is one script, and a step is run by name with `scripts/ci.sh <step>` |
| `ci.yml` job `workspace` | A1, I4 |
| `ci.yml` job `facade` | G2, A9 |
| `ci.yml` job `docs-and-lints` | A1, I5, I7 |
| `ci.yml` job `package-surface` | I7 |
| `ci.yml` job `semver` | I7 |
| `ci.yml` job `aarch64` | D10 |
| `ci.yml` job `lean-preset` | dropped: there are no presets; telemetry is one feature, tested by C11 |
| `ci.yml` job `portability` | D9, D10, D5 |
| `bench.yml` job `bench` | I2 |

## Crates on dev

Every crate directory on `dev`, and the rows above that cover it.

| Crate | Replaced by |
|---|---|
| `crates/accel` | D8, D11, D14 |
| `crates/accelerator` | dropped: the Backend trait (A7) covers it |
| `crates/bench-counters` | I2 |
| `crates/core` | A4, A6 |
| `crates/daedalus` | C14 |
| `crates/daemon` | H1, H2, H3, H4, H5 |
| `crates/data` | A5 |
| `crates/engine` | C10 |
| `crates/ffi` | A3, G2, G3 |
| `crates/frame-bench` | I2, I3 |
| `crates/gpu` | E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11 |
| `crates/host-wrapper` | C10, G11 |
| `crates/kernels` | D8 |
| `crates/macros` | A9, A13 |
| `crates/mcu` | D3, D4, D5, D6 |
| `crates/nodes` | B1 |
| `crates/planner` | B1, B2, B3, B4, B5, B6, B7, B8, B9 |
| `crates/registry` | A8 |
| `crates/runtime` | C1, C2, C3, C4, C6, C7, C8, C9 |
| `crates/surface` | G10 |
| `crates/transport` | A5, G1 |
| `crates/wgsl-infer` | E4 |
