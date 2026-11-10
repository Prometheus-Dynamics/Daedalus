# TODO

Every planned PR into `next`. `python3 scripts/pr-status.py` shows the live state; a merged PR is one whose `pr/<id>` branch merged. Briefs are in [`docs/prs/`](docs/prs/). PRs don't edit this file.

## A. Foundation

The core with nothing running yet. A3 to A9 are the shared interfaces everyone builds on, so they land first.

- [x] [A1](docs/prs/A1.md) Clean slate and skeleton (wave 1; needs nothing)
- [x] [A2](docs/prs/A2.md) Boundary checks in the gate (wave 2; needs A1)
- [x] [A3](docs/prs/A3.md) daedalus-abi: Step, StepEnd, Slots, StepCx, Fence, Status, handshake, layout hash (wave 2; needs A1)
- [x] [A4](docs/prs/A4.md) daedalus-types: type keys, type expressions, ids, errors (wave 2; needs A1)
- [x] [A5](docs/prs/A5.md) daedalus-value: value, memory space, owner, slots, inline small values, recycle pool (wave 3; needs A3, A4)
- [x] [A6](docs/prs/A6.md) daedalus-graph: graph model, JSON format, schema, groups (wave 3; needs A4)
- [x] [A7](docs/prs/A7.md) daedalus-backend: the Backend trait, Offer, Region, Compiled, Refusal, MemorySpace, Transfer (wave 4; needs A5, A6)
- [x] [A8](docs/prs/A8.md) daedalus-registry: records, joining bodies, conversions, capabilities (wave 5; needs A7)
- [x] [A9](docs/prs/A9.md) #[node] macro and the authoring-crate contract (wave 6; needs A8)
- [x] [A10](docs/prs/A10.md) daedalus-plan: step list format and Blob encoding (wave 4; needs A3, A5)
- [x] [A11](docs/prs/A11.md) backends/sim: fake backend with two memory spaces, transfers, merging, refusal (wave 5; needs A7)
- [x] [A12](docs/prs/A12.md) Backend conformance suite in testing/crates (wave 6; needs A11)
- [x] [A13](docs/prs/A13.md) Derive macros: NodeConfig and Type (new in this group) (wave 7; needs A4, A8, A9)
- [x] [A14](docs/prs/A14.md) Foreign values: owner and layout hash (wave 4; needs A5)
- [x] [A15](docs/prs/A15.md) #[transfer]: a memory handler in one function (wave 7; needs A7, A9)

## B. Planner

Solve the graph, ask the backends, assign, compile, stitch. It runs on a host or in `build.rs`, and may allocate.

- [x] [B1](docs/prs/B1.md) Solve: expand groups, defaults, hydrate (wave 6; needs A8)
- [x] [B2](docs/prs/B2.md) Solve: ports, typecheck (wave 7; needs B1)
- [x] [B3](docs/prs/B3.md) Solve: const inputs and config (wave 8; needs B2)
- [x] [B4](docs/prs/B4.md) Solve: conversions, exclusive access, host input policy (wave 8; needs B2)
- [x] [B5](docs/prs/B5.md) Solve: rewrite hook (formerly lowerings) (wave 7; needs B1)
- [x] [B6](docs/prs/B6.md) Solve: order, rates, sync groups (wave 9; needs B2, B4)
- [x] [B7](docs/prs/B7.md) Assignment: offers, forced placement, fewest crossings, priority (wave 10; needs A11, B6)
- [x] [B8](docs/prs/B8.md) Regions: formation, rules, compile, refusal round (wave 11; needs B7)
- [x] [B9](docs/prs/B9.md) Stitch: memory spaces, transfers, fences, slots, step list (wave 12; needs A10, B8)
- [x] [B10](docs/prs/B10.md) Control regions: if, for-each, until (wave 13; needs A6, B9)
- [x] [B11](docs/prs/B11.md) Lint, diagnostics, explain a plan (wave 13; needs B9)
- [x] [B12](docs/prs/B12.md) Plan hashing, plan cache, schema drift guard (wave 13; needs B9)
- [x] [B13](docs/prs/B13.md) Planner public API and forms: plan, form, write (wave 13; needs B9)
- [x] [B14](docs/prs/B14.md) Graph patches and port refresh (wave 6; needs A8)
- [x] [B15](docs/prs/B15.md) Shared upstream across graphs (wave 6; needs A8)

## C. Runner and Host

Walk the step list with no heap, and the Host around it: feed, step, take, reload.

- [x] [C1](docs/prs/C1.md) Serial runner: tick loop, dirty bits, slots, no heap (wave 5; needs A3, A5, A10; board run)
- [x] [C2](docs/prs/C2.md) Fences and pending steps (wave 6; needs A3, C1)
- [x] [C3](docs/prs/C3.md) Host IO: feed, step, take (wave 6; needs A4, C1; board run)
- [x] [C4](docs/prs/C4.md) Joins and fire policies: required inputs, fire all, held inputs, fan-in (wave 7; needs A10, C3)
- [x] [C5](docs/prs/C5.md) Failure rule: backoff, containment, opt-in auto-off, manual off (wave 6; needs A8, C1)
- [x] [C6](docs/prs/C6.md) Parallel lanes (std, threads) (wave 7; needs A10, C2)
- [x] [C7](docs/prs/C7.md) Cooperative cancellation and node progress (wave 7; needs C2)
- [x] [C8](docs/prs/C8.md) Output reuse for pure nodes (wave 7; needs A5, A9, C3)
- [x] [C9](docs/prs/C9.md) State slots and config cache (wave 6; needs A8, C1)
- [x] [C10](docs/prs/C10.md) Host: load, start, reload with state carried or reported (wave 14; needs B13, C3, C9)
- [x] [C11](docs/prs/C11.md) Telemetry and metrics (wave 6; needs C1)
- [x] [C12](docs/prs/C12.md) Fault reports and manual unmerged mode (wave 12; needs B8, C5)
- [x] [C13](docs/prs/C13.md) Async nodes and streams (wave 7; needs C2, C3)
- [x] [C14](docs/prs/C14.md) The daedalus facade crate (wave 15; needs A9, C10)
- [x] [C15](docs/prs/C15.md) Edge queues: pressure and freshness policies (wave 7; needs A5, C3)
- [x] [C16](docs/prs/C16.md) Blocking backpressure and strategy overrides (wave 8; needs C15)
- [x] [C17](docs/prs/C17.md) Sync groups for multi-input feeds (wave 8; needs C3, C4)
- [x] [C18](docs/prs/C18.md) Activity masks for skipping steps (wave 6; needs C1)
- [x] [C20](docs/prs/C20.md) Host waits, inbound readiness and drive loops (wave 7; needs C3)
- [x] [C21](docs/prs/C21.md) Execution domain: linked graphs ticked together (wave 15; needs C3, C10)
- [x] [C22](docs/prs/C22.md) Payload inspection and value serializers (wave 7; needs A4, C3)
- [x] [C23](docs/prs/C23.md) Host introspection, const patches and node control (wave 15; needs C9, C10)
- [x] [C24](docs/prs/C24.md) tick-path overhead probe, diagnostics JSON and allocation probe (wave 7; needs C3, C11; board run)

## D. CPU backend and chips

The first backend, merging, the Blob and Source forms, chips, the kernels, wasm and Windows.

- [x] [D0](docs/prs/D0.md) Per-step cells: a step sees its own inputs then outputs (needs B9, B10, C1)
- [x] [D1](docs/prs/D1.md) CPU backend: Steps form for #[node] functions (wave 14; needs A9, B13, C1)
- [x] [D2](docs/prs/D2.md) CPU merging: a region's nodes run as direct calls (wave 15; needs B8, D1; board run)
- [x] [D3](docs/prs/D3.md) Blob form and its small runner (wave 15; needs A10, D1)
- [x] [D4](docs/prs/D4.md) Source form: generated Rust for a solved CPU region (wave 15; needs B13, D1)
- [x] [D5](docs/prs/D5.md) Chip runtime: generated firmware API, no heap, five-node budget (wave 16; needs D4)
- [x] [D6](docs/prs/D6.md) Scalars, parameters and wire encoding on a chip (wave 17; needs D5)
- [x] [D7](docs/prs/D7.md) Chip blink example: compiled, tunable, loaded and fan-in (wave 18; needs D3, D5, D6)
- [x] [D8](docs/prs/D8.md) Kernels library: float, copy and codec kernels (wave 2; needs A1)
- [x] [D9](docs/prs/D9.md) WebAssembly: embedded preset smoke modules and Node runs (wave 15; needs C10, D1)
- [x] [D10](docs/prs/D10.md) No-heap tier, cross-target CPU runs and Windows (wave 15; needs C1, C10, D1)
- [x] [D11](docs/prs/D11.md) Acceleration inventory engine (wave 2; needs A1)
- [x] [D12](docs/prs/D12.md) Chip board example: Raspberry Pi Pico (RP2040) (wave 19; needs D7; board run)
- [x] [D13](docs/prs/D13.md) Shared nodes: one #[node] for the host and the chip (wave 17; needs C14, D3, D4, D5)
- [x] [D14](docs/prs/D14.md) Acceleration matrix: generator, asm check and gate (wave 3; needs D11)
- [x] [D15](docs/prs/D15.md) CPU kernel inventory rows, evidence and backend matrix (wave 4; needs D8, D14, D16; board run)
- [x] [D16](docs/prs/D16.md) CRC-32 kernels: slice-by-8, carry-less and CRC-instruction paths (wave 3; needs D8; board run)

## E. GPU backend

wgpu as a backend: whole-shader nodes, shared submits, merged per-pixel functions, importing external memory, and a dma-buf memory backend.

- [x] [E1](docs/prs/E1.md) wgpu context: instance, device, adapter choice, opens on request (wave 5; needs A7; board run)
- [x] [E2](docs/prs/E2.md) Buffers, textures, memory spaces, pools (wave 6; needs E1; board run)
- [ ] [E3](docs/prs/E3.md) CPU-to-GPU and GPU-to-CPU transfers (wave 15; needs D1, E2; board run)
- [ ] [E4](docs/prs/E4.md) Whole-shader nodes, WGSL inference, authoring crate (wave 7; needs A9, E2)
- [ ] [E5](docs/prs/E5.md) Shared submit across nodes in a region (wave 12; needs B8, E4; board run)
- [ ] [E6](docs/prs/E6.md) Per-pixel functions merged into one shader (wave 13; needs E5, E7; board run)
- [ ] [E7](docs/prs/E7.md) Your own recording code (wave 8; needs E4, E13; board run)
- [ ] [E8](docs/prs/E8.md) Fences, bounded waits, uncaptured errors (wave 8; needs C2, E4; board run)
- [ ] [E9](docs/prs/E9.md) dma-sys: dma-buf kernel calls in one place (wave 7; needs E2; board run)
- [ ] [E10](docs/prs/E10.md) Multi-device and foreign-memory handoff (wave 16; needs E3)
- [ ] [E11](docs/prs/E11.md) Device loss and the failure rule (wave 9; needs C5, E8; board run)
- [ ] [E12](docs/prs/E12.md) GPU conformance on lavapipe and the GPU budgets (wave 14; needs E6; board run)
- [x] [E13](docs/prs/E13.md) Raw Vulkan boundary: backends/vk-sys (wave 6; needs E1; board run)
- [ ] [E14](docs/prs/E14.md) GPU import: external memory as a sampled texture (wave 8; needs A15, E9, E13; board run)
- [ ] [E15](docs/prs/E15.md) images into storage and sampled textures (wave 9; needs E14; board run)
- [ ] [E16](docs/prs/E16.md) GPU kernel ops on device-resident data (wave 8; needs D8, E4; board run)
- [ ] [E17](docs/prs/E17.md) dmabuf memory backend: a memory space, a pool, no nodes (wave 8; needs A7, E9; board run)

## G. Plugins and languages

Dylib plugins carrying nodes and backends, the process backend, and an SDK per language.

- [x] [G1](docs/prs/G1.md) Worker wire protocol: framing, values, worker messages (wave 4; needs A5)
- [x] [G2](docs/prs/G2.md) Loader: dylib plugins carrying nodes and backends (wave 7; needs A3, A9; board run)
- [x] [G3](docs/prs/G3.md) Plugin-owned values, shared bytes, plain-data types and conversions (wave 8; needs A8, G2)
- [x] [G4](docs/prs/G4.md) Process backend: workers, supervision, the canonical worker fixtures (wave 7; needs A11, C2, G1; board run)
- [ ] [G5](docs/prs/G5.md) Python SDK: node declarations and worker loop (Python) (wave 8; needs G4)
- [ ] [G6](docs/prs/G6.md) Node SDK: node declarations and worker loop (Node) (wave 8; needs G4)
- [ ] [G7](docs/prs/G7.md) Java SDK: annotated nodes and worker loop (Java) (wave 8; needs G4)
- [ ] [G8](docs/prs/G8.md) C and C++ SDK: worker API, typed values, arena outputs (C and C++) (wave 8; needs G4)
- [ ] [G9](docs/prs/G9.md) Rust SDK: worker serve loop, context, panic containment (wave 8; needs A9, G4)
- [ ] [G10](docs/prs/G10.md) C surface: core/capi, run a graph from a shared library (wave 15; needs C10, G2)
- [ ] [G11](docs/prs/G11.md) Rust host over the C surface, safe crate over a sys crate (wave 16; needs G10)
- [x] [G12](docs/prs/G12.md) Plugin worker: isolate a dylib plugin's nodes in the process backend (wave 8; needs G2, G4)

## H. Service

`daedalusd`, its protocol, its client and `daedalus-ctl`.

- [ ] [H1](docs/prs/H1.md) Service wire protocol: daedalusd requests, responses, watch events (wave 5; needs A5, G1)
- [ ] [H2](docs/prs/H2.md) Service client: connect, pipeline, watch, reconnect (wave 6; needs H1)
- [ ] [H3](docs/prs/H3.md) Host listener: one Host, many clients, unix socket, TCP loopback and stdio (wave 15; needs C10, H1)
- [ ] [H4](docs/prs/H4.md) daedalusd: the daemon binary, config, plugin loading, shutdown (wave 16; needs G2, H3)
- [ ] [H5](docs/prs/H5.md) daedalus-ctl: command line client for daedalusd (wave 17; needs H2, H4)
- [ ] [H6](docs/prs/H6.md) Packaging the service: unit files, socket activation, hardening (wave 17; needs H4)

## I. Proof and finish

A third-party backend that proves the interface, the budget harness, examples, documentation, and the audit that nothing was lost.

- [ ] [I1](docs/prs/I1.md) A third-party NPU backend shipped as a plugin (wave 8; needs A12, G2)
- [ ] [I2](docs/prs/I2.md) Budget harness: instruction counts, allocations, sections (wave 17; needs C1, D5; board run)
- [ ] [I3](docs/prs/I3.md) Large-value handoff benchmark (wave 18; needs A14, I2; board run)
- [ ] [I4](docs/prs/I4.md) CPU and host examples (wave 16; needs C11, C14, D1)
- [ ] [I5](docs/prs/I5.md) Documentation and the doc gate (wave 16; needs C14)
- [ ] [I6](docs/prs/I6.md) Capability audit against dev (wave 1; needs nothing)
- [ ] [I7](docs/prs/I7.md) Finish: package surface, semver, lockfile, ready for other repositories (wave 20; needs A2, A13, B3, B5, B10, B11, B12, B14, B15, C6, C7, C8, C12, C13, C16, C17, C18, C20, C21, C22, C23, C24, D2, D9, D10, D12, D13, D15, E10, E11, E12, E15, E16, E17, G3, G5, G6, G7, G8, G9, G11, G12, H5, H6, I1, I3, I5, I8)
- [ ] [I8](docs/prs/I8.md) GPU examples on lavapipe (wave 17; needs E3, E6, I4)
