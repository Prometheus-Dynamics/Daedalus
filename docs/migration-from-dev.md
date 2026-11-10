# From dev to next

One table: each crate on `dev`, and what replaced it on `next`. It is for reading `dev` beside
`next`, not a guide to using either. [`testing/capability-audit.md`](../testing/capability-audit.md)
has the same thing by capability, one row each, and is checked by the gate.

`next` is not `dev` with changes. It was built from nothing against [design.md](design.md), and
`dev` was only read. Nothing on `next` keeps a `dev` API for compatibility.

| On `dev` | On `next` | PRs |
|---|---|---|
| `crates/core` | `core/types` (type keys, ids, errors), `core/graph` (the graph model and its JSON) | A4, A6 |
| `crates/data` | `core/value` (a value is a type key, a memory space and an owner) | A5, A14 |
| `crates/registry` | `core/registry` (one entry a node id, several bodies) | A8 |
| `crates/macros` | `core/macros` (`#[node]`, `#[transfer]`), `core/derive` (`NodeConfig`, `Type`) | A9, A13, A15 |
| `crates/planner` | `core/planner`, and `core/plan` for the stored step list | B1 to B15, A10 |
| `crates/runtime` | `core/runner` (walks the step list, no heap), `core/host` (feed, step, take, reload, telemetry) | C1 to C24 |
| `crates/engine` | `core/host`: `LoadedHost`, with the planner called directly | C10 |
| `crates/daedalus` | `core/daedalus`, the facade | C14 |
| `crates/host-wrapper` | `core/host`, and the C surface that is planned | C10, G10, G11 |
| `crates/transport` | Gone. Values are `core/value`, and values between processes are `core/wire` | A5, G1 |
| `crates/gpu` | `backends/wgpu`, `backends/wgpu-author`, `backends/vk-sys`, `backends/dma-sys`, `backends/dmabuf` | E1 to E17 |
| `crates/wgsl-infer` | Inside `backends/wgpu-author` | E4 |
| `crates/accelerator` | Gone. The `Backend` trait in `core/backend` is the interface | A7, I1 |
| `crates/accel`, `crates/kernels` | `backends/kernels-sys`, `testing/crates/accel`, `testing/crates/inventory` | D8, D11, D14, D15, D16 |
| `crates/mcu` | The CPU backend's blob, source and chip parts in `backends/cpu` | D3, D4, D5, D6 |
| `crates/ffi` | `core/abi` (the step interface), `core/loader` (dylib plugins), `backends/process` and `languages/*` (workers) | A3, G2 to G9, G12 |
| `crates/surface` | Planned as `core/capi` | G10 |
| `crates/nodes` | Gone. Node groups are in the planner, and the examples replace the demo nodes | B1 |
| `crates/daemon` | `core/wire` (the service protocol), `core/client`, `core/host` (the listener), `service/daedalusd`, `service/daedalus-ctl` | H1 to H6 |
| `crates/frame-bench` | `testing/crates/budget-harness` | I2, I3 |
| `crates/bench-counters` | Gone from CI. Instruction counts are taken with callgrind | I2 |

## What has no successor

- A frame type, a frame library, colorimetry and pixel conversions. A type belongs to whoever
  wrote the nodes that use it.
- A CUDA backend.
- Presets and configuration from environment variables.
- The Rust-ABI plugin path, plugin fingerprints, and every versioned plugin handshake. A plugin
  and a host compare one layout hash.
- JSON and base64 on the worker path, and the payload handles and leases that went with them.
- The separate MCU macro and node types. A chip uses the same `#[node]`.

## What changed shape

- A backend is asked what it can run and compiles what it is given. There is no device
  assignment pass that knows about GPUs.
- A solved graph is a flat list of steps with numbered value slots. The runner walks it and
  knows nothing of the planner or the registry.
- Failure handling is one rule everywhere: never give up, back off to a cap, turn nothing off
  automatically.
