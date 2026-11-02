# TODO

Open work first, done work with its commits below. Each open item says **what** it is, **why** it
matters, **done when** (a checkable test, gate, measurement or board run), **blocked by**, and
**measured** (current numbers, or that none exist). **[board]** items need hardware; "written,
untested on board" means the code exists and has only run under qemu, lavapipe, stand-ins or a
desktop GPU. Audited against the code and CI on 2026-11-01, at 03f55c5; CI items updated at 519b022.

The user alone decides when anything is released and what blocks a release. `dev` stays on the
`3.0.1-dev` scheme. Board work is tested on boards before anything ships, unless the user says
otherwise.

Scope (user decision, 2026-10-30): full support on every backend, no half implementations. GPU on
every target with a usable driver (V3D, Mali-G610, Mali-G52, desktop), the CPU path everywhere,
builds and tests for x86_64, aarch64, armv7 and riscv64. GPU is an explicit backend the consumer
chooses, per node or per graph; Daedalus does no cost-based CPU/GPU selection. Acceleration rule
(user, 2026-10-31): everything that can use RVV, NEON (64 and 32), x86 SIMD, ISA extensions, the
GPU or a fixed-function unit does. "Impossible" means physically impossible only; slower variants
are built, measured per device and kept selectable. `docs/backend-matrix.md` is the evidence and
`ci.sh accel` the gate.

## Open

### CI on `dev`

- [ ] **What:** the `semver` job stays red. It is non-blocking (`continue-on-error`), so the `CI`
      run is green without it.
      **Why:** `cargo semver-checks` judges breaks against the version number. `dev` is `3.0.1-dev`,
      a patch over `v3.0.0`, so every listed break fails it whether or not CHANGELOG explains it;
      it has no allowlist. It passes only once the crates carry a major version.
      **Done when:** the release version bump, which is the user's call (see Release steps); then the
      job is made blocking with `SEMVER_BASELINE` on the new tag.
      **Blocked by:** the user's decision.
      **Measured (519b022):** the report finishes over 20 crates; 324 listed items, each named in
      CHANGELOG `[Unreleased]` or `docs/api-review.md` (an identifier match, ab7cadd).

### Raze (CM5, V3D)

- [ ] **What:** confirm the stale-graph refresh on the Raze (`refresh_graph_ports_with` +
      `drop_stale_consts`, bb07e3b, 0398d4a).
      **Why:** HeliOS's stored rev-215 graph failed to plan (`PortExtra` on folded Eidos knobs).
      **Done when:** HeliOS dev on the Raze plans and runs the default pipeline from the stored
      rev-215 graph and logs the dropped consts.
      **Blocked by:** a HeliOS deploy on the Raze, which the HeliOS coordinator owns. HeliOS `dev`'s
      `backend/Cargo.lock` pins b148918, which carries bb07e3b; what the Raze runs was not checked.
      **Measured:** planner tests only (`planner/src/refresh/tests.rs`); no Raze run is recorded.
- [ ] **What:** `ci.sh vvl` on the Raze exits cleanly under the validation layer.
      **Why:** VVL is how new Eidos kernels get validated. This was ticked as accepted on 2026-10-30,
      but its done-when (`ci.sh vvl` exits 0) was never met.
      **Done when:** `./scripts/ci.sh vvl` on the Raze exits 0, with no layer errors from our code.
      **Blocked by:** a properly built aarch64 layer. Today's is cross-built with zig and segfaults
      in its own static destructors at process exit, even for a bare `vkCreateInstance` +
      `vkDestroyInstance`. A distro arm64 package needs the user's OK for the download.
      **Measured:** the runs recorded in `docs/testing.md` print no validation message before the
      exit crash.
### Boards

- [ ] **[board] What:** armv7 RV1106, CPU-only (`armv7-unknown-linux-musleabihf`): the CPU graph
      tests and the kernel conformance binary on the board. Written, untested on board.
      **Why:** the user's platform list (2026-10-30); the RV1106 has no compute-capable GPU.
      **Done when:** `ci.sh cross`'s CPU graph tests and the `+neon` conformance binary pass on the
      board, and the evidence is committed.
      **Blocked by:** a board. The target is Flame, our own RV1106G2 module; the Luckfox Pico Pro
      (same chip, off the shelf, Gaia is imaging it) stands in for it and is where it gets tested.
      **Measured:** qemu only: 91 + 5 + 2 tests pass in `ci.sh cross` at 03f55c5; `+neon` kernels
      pass under qemu cortex-a7.
- [ ] **[board] What:** decide `crc32()`'s ARMv7 default from a measurement of
      `crc32::neon_vmull_p8` (50093a9) against slice-by-8. Written, untested on board.
      **Why:** selection follows measurements. `crc32()` stays on slice-by-8 on ARMv7 until then.
      **Done when:** `conformance crc32_throughput -- --ignored --nocapture` has run on the Luckfox
      Pico Pro (Flame's stand-in) and the default follows the numbers.
      **Blocked by:** the Luckfox board. The Raze cannot run AArch32 binaries.
      **Measured:** conformance under qemu only; no timing exists.
- [ ] **[board] What:** K230D, CPU-only (`riscv64gc-unknown-linux-gnu` and `-musl`): the CPU graph
      tests and the kernel conformance binary on the board. Written, untested on board.
      **Why:** the K230D has no compute-capable GPU, and qemu cannot show a real VLEN.
      **Done when:** both pass on a K230D and the evidence, with its VLEN, is committed.
      **Blocked by:** a K230D board.
      **Measured:** qemu only: 91 + 5 + 2 tests in `ci.sh cross` at 03f55c5; `+v` kernels at VLEN
      128, 256 and 512, plus a run without Zvbc.
- [ ] **[board] What:** RK3588S (Mali-G610): `gpu_probe`, the `ci.sh pi` hardware tests, `gpu_bench`
      and the `GpuKernels` evidence on the shipped driver (libmali or PanVK). Written, untested on
      board: `GpuDeviceProfile` (Mali Valhall, warp 16; b095018), the GPU kernels and fragment
      passes, the AFBC modifier helpers, the checklist in `docs/testing.md`.
      **Why:** proper hardware support on every board.
      **Done when:** the reports are in `docs/testing.md`, the matrix has Mali evidence, and the
      hardware tests pass or each gap is documented, with the copy fallback in place.
      **Blocked by:** a board (the PCB dev board) and Gaia's driver choice. Both are as recorded on
      2026-10-30; neither can be checked from this repo.
      **Measured:** nothing on Mali. The same code runs on lavapipe, desktop GPUs and V3D.
- [ ] **[board] What:** RK3576, RK3566 and RK3562 (Mali-G52): the same reports, or a confirmed
      clean CPU fallback where the driver has no usable Vulkan compute. Written, untested on board
      (Mali Bifrost, warp 8 on G52).
      **Why:** as above.
      **Done when:** as above, per chip.
      **Blocked by:** boards and Gaia's driver choice.
      **Measured:** nothing on Mali.
- [ ] **[board] What:** the Rockchip RGA fixed-function cells in `daedalus-rga`, the crate shared
      with Styx. Route (b) is V4L2 mem2mem (`rgb_to_rgba`, `rgba_to_rgb`); route (a) is `Librga`
      (2f7c880; `gray_to_rgba`, `rgba_red`). Both written, untested on board.
      **Why:** these are the matrix's 4 untested cells. `rgba_red` relabels the frame as gray and
      scales it down by nearest, so it yields red only if the hardware takes the first of every four
      samples; the headers do not promise that.
      **Done when:** `crates/rga/tests/{v4l2,librga}.rs` pass on a Rockchip board and the cells read
      pass, or `rgba_red` is recorded as failing.
      **Blocked by:** a Rockchip board.
      **Measured:** no RGA hardware run. Route (b) passes end to end on `vim2m` with `dma_heap`
      dmabufs; route (a) passes against software stand-ins and a C stub built from the real header.

### Upstream and low priority

- [ ] **What:** dmabuf `Timeline` fence mode blocks the next submission on Mesa and trips
      `VUID-vkQueueSubmit-pWaitSemaphores-03238`.
      **Why:** wgpu-hal chains submissions with binary semaphores, which Mesa accepts only once the
      previous wait-before-signal submission reached the kernel. `Timeline` is opt-in; `Auto` tries
      `SyncFd` first and does not produce it.
      **Done when:** `Timeline` neither blocks nor violates the VUID under `ci.sh vvl`.
      **Blocked by:** wgpu-hal relaying with timeline semaphores; 30.0.1 still uses binary
      `RelaySemaphores`.
      **Measured:** 100 ms blocked with a 100 ms timeout (`crates/gpu/README.md`).
- [ ] **What:** flash the MCU firmwares on a real board (with a HAL: ADC input, GPIO output) and
      measure stack use and tick time in each mode.
      **Why:** sizes are linked and measured, but the firmware has never run on hardware.
      **Done when:** the stack and tick numbers per mode are in `docs/mcu.md`.
      **Blocked by:** an MCU board and a HAL.
      **Measured (`ci.sh mcu`, 2026-11-02), flash / static RAM, M4F and M0+:** compiled 2988 / 3316 B
      and 112 B; tunable 4424 / 5488 B and 128 B; loaded 11060 / 12024 B and 584 B. Stack and tick
      time: none.
- [ ] **What:** a chunked blob receive helper for the MCU (feature-gated framing + CRC-32 into a
      buffer or flash partition).
      **Why:** only if applications keep writing the same transport code.
      **Done when:** a test receives a plan blob in chunks and loads it.
      **Blocked by:** a consumer needing it; none is recorded.
      **Measured:** none. `daedalus_mcu::loaded::crc32` exists; the framing does not.
- [ ] **What:** fan-in and user adapters on the MCU device (adapter calls for `#[adapt]` functions
      that are plain `no_std` code).
      **Why:** today lowering rejects several edges into one input (`crates/mcu/build/src/lower.rs`),
      and only builtin numeric widening adapts.
      **Done when:** a blink-style graph with fan-in and a user adapter runs in each mode.
      **Blocked by:** nothing.
      **Measured:** none.
- [ ] **What:** shared nodes with the full runtime: generate both a `#[node]` handler and the device
      glue from one function.
      **Why:** so a node crate serves both.
      **Done when:** one function runs under both, with a test in each.
      **Blocked by:** nothing.
      **Measured:** none.
- [ ] **What:** decide whether Windows matters. Checkouts need `core.symlinks` for the 14 symlinks
      under `crates/` (`build.rs`, `portable.rs`, `trace.rs`).
      **Why:** without it those crates do not build from a Windows checkout.
      **Done when:** the user has decided, and the symlinks are replaced or the limit is documented.
      **Blocked by:** the user's decision.
      **Measured:** 14 symlinks.
- [ ] **What:** a CUDA backend: a second `GpuBackend`, a residency key per backend so CUDA and wgpu
      device memory are distinct port types, upload/download adapters, CUDA↔Vulkan interop
      (`cudaImportExternalMemory`) if both run in one graph, and `#[device]` implementations keyed by
      backend.
      **Why:** only if one is wanted; no request is recorded.
      **Done when:** a graph runs a node on CUDA, with tests.
      **Blocked by:** a decision to do it.
      **Measured:** none.
- [ ] **What:** signing and registry work.
      **Why:** pinned for later by the user (2026-10-30).
      **Done when:** the user has scoped it.
      **Blocked by:** the user's decision.
      **Measured:** none.

### Release steps

These run only when the user calls a release. None of them is pending until then.

- [ ] **What:** the version bump (4.0.0 was the user's choice on 2026-10-30), the CHANGELOG
      `[Unreleased]` section cut, `SEMVER_BASELINE` set to the new tag, the CI `semver` job made
      blocking, `dev` merged into `main`, and the tag. The `v3.0.0` tag stays as it is.
      **Why:** `dev` breaks public API since `v3.0.0` (`GpuCapabilities`, `NodePortRefresh` and
      others); downstream pins by commit and reads the CHANGELOG.
      **Done when:** the tag exists, every crate carries the version, the plugin-ABI and stable-ABI
      constants are unchanged or bumped with a reason, and `ci.sh full`, `ci.yml` and `bench.yml` are
      green.
      **Blocked by:** the user's decision.
      **Measured:** `dev` is `3.0.1-dev`; `docs/migration-4.0.md` is written (b422ec0) but its check
      against the semver report predates the 319 items above; CI as above.

## Done

Each entry cites the commit that did the work, as far as history can attribute it. Where one symbol
was searched for, the commit is the one that introduced it.

### GPU and hardware support

- [x] `gpu_bench` and `gpu_probe` report on the Raze CM5: e56840d (`docs/testing.md`, "CM5 run
      (2026-10-31)").
- [x] Compute limits and features in `GpuCapabilities`: fb5ba91;
      `mock_and_noop_backends_report_compute_caps`.
- [x] Explicit device assignment, resolved device in `explain_plan()`: 74b8ef4, b24bde0; test
      `device_assignment`.
- [x] `GpuContextHandle::wgpu()`: 8865fcf.
- [x] `#[device]` transfers can take the GPU context: f22a58c.
- [x] `GpuStorage`, persistent node-owned storage: 8358890; `tests/gpu_storage.rs`.
- [x] `GpuTicket` / `GpuInFlight`, non-blocking completion: 3ce1b9a; `tests/gpu_ticket.rs`.
- [x] Fragment-pass nodes, `FragmentPass` + `GpuTexture`: e26be2b. On the Raze the helper matches the
      hand-written pass: box3x3 r8 2.83 vs 2.84 ms, box5x5 7.05 vs 7.06 ms.
- [x] `GpuStorage::read_into` / `start_read` with a staging ring: e526090.
- [x] `device_round_trips()`: 154b8c5.
- [x] `GpuTimer` / `GpuTimed`: bb618bc. v3dv uses timestamp queries (evidence "Timestamps").
- [x] LINEAR dmabuf as a storage buffer, `GpuStorage::import_dmabuf`: fd455a4; passes on RADV and
      the Raze.
- [x] On-disk pipeline cache: 8fbc780, engine-owned in 3d34c9e, `pipeline_cache_path_for` in 03f55c5.
      On V3D 12 pipelines build in 52.0 → 5.7 ms with the cache loaded, Mesa's cache off.
- [x] `GpuStoragePool`, `SimpleBufferPool` size fix: ee86872; `storage_pool_reuses_within_its_cap`.
- [x] GPU nodes guide and `gpu_fragment_node`: f626d74.
- [x] Desktop GPUs (RADV, NVIDIA): d033fc7 (`docs/testing.md`, "Desktop GPUs").
- [x] CI lane for the CPU-only boards: 501de01, de16ef3. The on-board runs stay open above.
- [x] Raw Vulkan access for external SPIR-V kernels: 29ae351; 8-bit storage: c572945.
- [x] Cached readback on v3dv through a dma-heap buffer: 602b14b.
- [x] Stable-ABI plugin path without per-call allocations: 5f5c51c.
- [x] `ci.sh pi` re-run on the Raze at 65fd3a5 (2026-11-02): 16 dmabuf and raw Vulkan hardware tests
      ok, `gpu_probe` unchanged (`docs/testing.md`, "CM5 run (2026-11-02)").
- [x] Frame interface v3 (`SharedFrame`, colorimetry, dylib reload): 36d2af6, 2b09654, fad4b99,
      3b14534, d543c6c. The open "v3 follow-ups" item is removed: it held no work, only a wait for a
      consumer request, and none is open.

### Acceleration inventory

- [x] `daedalus-accel`, registry, evidence, matrix and gate: 7b85ad7, 6f9bd4b, 002d1f4, d5b55f7.
- [x] `daedalus-kernels` on every backend, wired into every call site: 6f9bd4b, 002d1f4, d5b55f7,
      c27f9c9, c797a47, 3196a4e, 1a6e8de. Committed matrix: pass 112, impossible 18, untested 4.
- [x] The `portable_simd` column on nightly-2026-10-15: 80eb8cb.
- [x] NEON32 CRC-32 by `vmull.p8` folding: 50093a9. Its default is open above.
- [x] Core-tuned variants as their own matrix columns: 03cb4a8.
- [x] `GpuKernels` on V3D, compute and fragment forms: d5b55f7, a08550b, evidence refreshed at
      f4ea43a. All bit-exact. Delivered cost, all copies included, against the A76: compute 3.7x
      (`f16_to_f32`) to 93x (`crc32`) slower; fragment 1.2x (`f16_to_f32`, 14.3 vs 11.9 ms) to 9x
      slower, and 1.1-3.5x faster than compute. The pass alone, without its copies, beats the A76 only
      on `f16_to_f32` (8.1 vs 11.9 ms) and `rgb_to_rgba` (8.05 vs 8.40 ms). The copy engine is 1.09 vs 1.27 ms per 4 MiB.

### CI on `dev` (2026-11-01)

The `CI` run on `dev` was red from 3196a4e to ab7cadd; green again at 519b022 (`semver` stays red,
non-blocking, see above).

- [x] `package-surface`: the `stable_abi_counted` fixture excluded from packaging, 8bfeb5e.
- [x] `accel`: CI's qemu 8.2.2 (ubuntu-24.04) did not report Zvbc through `riscv_hwprobe` (absent
      from qemu's `linux-user/syscall.c` at v8.2.2, present at v9.0.0). The job runs on ubuntu-26.04
      with `qemu-user-binfmt` 10.2.1: a1d615f, 519b022. The GitHub job renders pass 112,
      impossible 18, untested 4, the committed matrix.
- [x] `ci.sh semver` finishes over every crate: new crates excluded, the baseline's libcamera
      feature dropped, the unnamed breaks named in CHANGELOG: ab7cadd.
- [x] Stale lines in `docs/migration-4.0.md` and `docs/development.md`: 249bf6e.

### Earlier release mechanics

- [x] Internal API dedup pass: 9561d9b..f6299ad.
- [x] Migration guide and first semver report: b422ec0, 4d24649.

### Host integration

- [x] Native Rust `cdylib` plugin loading: 3b4abe8.
- [x] Declarative feature classification (`[package.metadata.daedalus]`): 8f9968e.
- [x] `GraphDocument` and its JSON Schema: f181802, 4420c46.
- [x] Host graph APIs (typed ports, inspection, `drive`): 1e38a37, 9ba8b01.
- [x] Lean presets; executor pool opt-in: 5856708.
- [x] `FfiHost` one-call package host: af12814.
- [x] dmabuf zero-copy GPU import, fences and NV12: 007a200, 384eea1.
- [x] `external_frame_source` example: 1209795.
- [x] Daedalus never depends on a camera library; library-owned integration features: 603b56b.
- [x] Deterministic port keys for foreign types, boundary type check: 1832ae7.
- [x] Foreign (host-owned) types, `foreign_interface!`: 11fe8de.
- [x] Generic runtime key lookups, `type_index`: de1d95e.
- [x] Optional inputs: 26fbe0f.
- [x] Cross-tick joins, `fire = "all"`: 5133cc0.
- [x] Typed host ports in embedded graphs: 0cd2d82.
- [x] Stable plugin handler path: a852ed1.
- [x] Graph JSON Schema drift, enum lists from `ALL`: 0c55a31.
- [x] Enum config ports: 5d2e5a1.
- [x] Ids from other macros: c57d28e.
- [x] Macro leaf keys through the installing registry: c1b1902.
- [x] Dynamic plugin dependencies: 9d183ef.
- [x] One key per builtin number: 632a82e.
- [x] Direct routes deliver const inputs: c487ae3.
- [x] Typed nodes with three reference parameters: d267d3b.
- [x] `export_plugin!` boundary contracts in the schema: commit not attributed (the field is older
      than the item, 509283e).

### Performance

- [x] Process-global boundary contract registry removed: 3aa0f30, 233f947.
- [x] Host bridge per-port state, latest-only slots: f881d93.
- [x] Executor direct bridge handles, allocation-free ticks (`hot_path_allocations`): 3aa0f30.
- [x] Shared `PolicyQueue<T>`: 9ee678c.
- [x] Generated handlers allocate nothing per frame: d75e60f.
- [x] Java and C++ SDK integer widths: 20dfb33.
- [x] `u64` on the FFI wire: 73d6524.
- [x] Owned constants decode once: 8c884d5.
- [x] Benchmarks and `bench.yml`: f881d93, 595a1b0.

### CI, examples and daemon

- [x] CI on pushes to `dev`, with its cache: 7c70fed.
- [x] `ci.sh smoke` and the `09_hosts` examples: f32c804, a3581e0.
- [x] Docs match the tree: 17685f6.
- [x] Daemon tests: commit not attributed (the tooling is 24283de).

### Code health

- [x] `parking_lot` outside `core`/`transport`: 59e1e70.
- [x] Oversized files split, strict file-size check: 81df42c, 47fe6e9.
- [x] Duplicate code consolidated: 825c007, 6dfc13b, 349674c.
- [x] Dead code removed: b523513, 62e7757.
- [x] `--all-features` workspace builds link: dada0fb.
- [x] CI aarch64 check, lean-preset tests, `ci.sh` subcommands: 595a1b0.
- [x] Portability tier 1 and the `portability` job: f088918, add2024, 2c37564.
- [x] Bugs fixed: schema round trip 6dfc13b, `Coalesce` 96cb7af. The other bugs in that entry
      (unknown wgpu formats, shared freshness watermarks, CI feature checks) are not attributed.

### Validation and tooling

- [x] v3dv validation on a CM5: 1512568, abe7ad4, d02276e.
- [x] Frame-path overhead on the CM5: 4939e42, 3367fb2 (`docs/benchmarks-log.md`). Detector
      `graph_overhead` p50 7.2 µs unfused, 6.8 µs fused.
- [x] First GitHub Actions run of `ci.yml`: run 40175816440.
- [x] `bench.yml` on `dev`, baseline on the same runner: 595a1b0 and later; see the workflow.
- [x] Tags `v2.0.0` (8946223) and `v3.0.0` (e7e88fc); dev versioning.
- [x] Faster local builds and `ci.sh -j`: f7edb7d.
- [x] False bench flags on shared runners: a flagged bench is re-run twice more on both commits,
      alternating, and `bench` fails only if the median of three still regresses (bf07300).
      Docs-only commits never ran `bench` (its path filters). The one failure in 15 runs, 0e620c2's
      `telemetry_clone_costs/clone/Basic` +16.7%, was noise: that commit does not touch what the
      bench clones, and a local re-run overlaps (196-212 ns before, 212-217 ns after, ±10% per run).
- [x] Public API review: 38cbdd9, b25b304 and the series around them (`docs/api-review.md`).
- [x] GPU-side fence wait with a timeout, compressed modifiers: 6b5ec57, 9dfeda1.
- [x] Configurable fence wait, `SyncFd` by default: a03a1e0.
- [x] Hardware tests under the validation layers: fc50a8a.
- [x] Parallel test runs segfault; driver setup serialized: documented in 05633f0.
- [x] `NodeRecovery::Retry` with by-value inputs: 89836d1.
- [x] GPU fallback with `GpuBackend::Cpu`: 4939d8a.
- [x] Host-created Vulkan instances, `with_driver_lock`: 888aaca.

### Portability (tier 2)

Tier 1 and tier 2 are done; see "Portability" in `docs/development.md`.

- [x] Lock backend for runtime and engine: bf3b085.
- [x] `threads` feature: 19b6925.
- [x] Clock injection, lineage clock: 7d0bbc8.
- [x] Non-blocking host bridge without `std`: 9ba8b01 (`InboundWaiter`), bf3b085.
- [x] `alloc`-only runtime: 2c37564.
- [x] Targets without compare-and-swap, runtime and engine included: d81cee9.
- [x] wasm host glue: 624bab5.

### MCU profile

- [x] Host plan compiler, device crate, blink example, size budgets: c7b3989.
- [x] Live parameters: 0cb75f6, 1bfd08c.
- [x] Runtime-loaded plans: 0cb75f6, 1bfd08c; blob CRC-32 in c06a35e.
- [x] Smaller tunable and loaded code: the `f64` feature (default) gates `f64` conversions, and
      `f64` to `f32` narrows in integer arithmetic, so the blink firmwares link no `__aeabi_d*`.
      Tunable 5012 → 4424 B (M4F), 5952 → 5488 B (M0+); loaded 11672 → 11060 B, 12488 → 12024 B;
      budgets 6 KiB and 13 KiB. Commit: daa7d22.
