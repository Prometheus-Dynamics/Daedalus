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

## Alpha (Raze)

The next release is an alpha that covers the Raze (CM5, V3D 7.1) hardware only (user, relayed by
HeliOS, 2026-11-02). These items come first. The other boards (Rockchip, K230D, RV1106), the MCU
and CUDA follow after it, under Open. When to release, and what blocks it, stays the user's call.

## Open

### Current list (HeliOS coordinator, 2026-11-03)

Now, in this order. Items not listed here keep their place below. Nothing new is taken on.

- [x] **What:** the allocation flake: `detector_graph_frame_allocation_budget` read
      `Parallel/Hardware: 9.05` against a budget of 9.
      **Done (2026-11-03):** 7c48145, CI run 41787751284. No budget changed; 200 of 200 runs under
      load, 10 of 200 failing before.
- [x] **What:** the Profile and Trace adapter edge to 0 allocations per frame.
      **Done (2026-11-03):** the adapter edge allocates 0; the detector frame went from 75 to 9 at
      Profile, serial and parallel, and Trace is 9. `AdapterPathReport` did not need to change.
      Adaptive rows at those levels have no budget.
- [ ] **What:** the payload allocations per frame at Basic: 9, now 8.
      **Why:** they are on the default frame path, under the zero-allocation rule.
      **Done when:** 0 at Basic.
      **Blocked by:** a design change, which waits for the user's go. A per-output `PayloadSlot`
      took it to 2 but broke unique handoff (`gpu_fallback`'s `drain_owned`), so it was reverted.
      Candidates: small plain-data outputs held inline in the payload, or a return buffer the
      consumer hands back.
      **Measured (2026-11-05):** 8 per frame, all payload `Arc`s the runtime wraps: six node outputs
      (`Roi` x2, `Histogram`, `Report`, `Stats`, `Track`) and two `FrameInfo` adapter outputs. The
      ninth was `exposure`'s owned `String` parameter, the node's: it borrows it now (b9df7ac).
- [x] **What:** dma-heap cleanup: `crates/frame-bench/src/buffer.rs`,
      `crates/gpu/examples/gpu_bench/linux/frame_import.rs` and `crates/gpu/examples/gpu_probe.rs`
      moved onto `lemnos_linux::dma`. `crates/rga/tests/v4l2.rs` stays until `crates/rga` goes to
      Lemnos.
      **Done (2026-11-04):** allocation, sync and mapping go through Lemnos. Two things stay:
      `gpu_probe`'s per-heap report still opens each heap read-write itself, because that open is
      what it reports and Lemnos's `open_path` is read-only; and `frame-bench`'s dma-buf mapping is
      now writable, because Lemnos's `DmaBuf::map` is. `frame_chain` was not re-measured.
- [x] **[board]** **What:** the stable-path bench on Raze #1 (CM5): cost per constant, a
      `Detections`-shaped crossing at 0, 4 and 16 tags, a prototype plugin-local handle, and a
      12-node chain.
      **Done (2026-11-05):** 52f1199, one run; numbers against the estimates in
      `docs/benchmarks-log.md` ("Stable plugin path"). Handles on 12 edges cost 18.7 µs; constants
      0.27-0.36 µs each; `Detections` through `Value` 38 µs per crossing at 4 tags, as a handle
      2.5 µs at any count.

#> 2026-11-05 (user, via HeliOS): Lemnos goes first. Anything that depends on Lemnos APIs (the
> rga/vglite hand-off, surfaces on `lemnos-abi`, dma-heap or device changes) waits until HeliOS says
> Lemnos is ready. Daedalus-only items continue.

## Decided, starts when the user says

Decided, not started. Agents finish the current list, then pause (user, relayed by HeliOS,
2026-11-03).

1. Stable path gains adapters and serializers (C function pointers keyed by type name), typed
   `repr(C)` values and views, plugin-local handles, a telemetry sink, constants sent once, and
   codecs for the catalog nodes.
2. `FrameLease` adapter cached per frame on the stable path.
3. Host-side GPU ops and the C vtable for plugins, per
   `/mnt/ssd-samsung/helios-notes/gpu-seam.md`.
4. One real Eidos node on the stable path as a trial, then Eidos as a `cdylib` plugin.
5. Whole-subgraph crossing: only if the numbers justify it.
6. Use `lemnos-abi` primitives; hand `crates/rga` and `crates/vglite` to Lemnos, then repoint
   `crates/kernels/src/registry/fixed.rs` and delete ours.
7. Shared libraries: static and shared modes, with core, surface and wrapper per dependency, and
   plugin linking. Design in `docs/shared-library.md` (2026-11-05). Started; blocked by Lemnos's
   `lemnos-abi`. In-image node crates link into the surface `cdylib`; only separate plugins use the
   stable ABI. Gaia's A/B: per-node calls across a library cost 26% on the mirror bench, so the
   surface is per frame.
8. Lemnos's kernel module and its Rust userspace drivers: Daedalus's fixed-function nodes and
   accelerator implementations sit on them.

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

### Boards

- [ ] **[board] What:** armv7 RV1106, CPU-only (`armv7-unknown-linux-musleabihf`): the CPU graph
      tests and the kernel conformance binary on the board. Written, untested on board.
      **Why:** the user's platform list (2026-10-30); the RV1106 has no compute-capable GPU.
      **Done when:** `scripts/board.sh flame root@<luckfox>` passes (the CPU graph tests and the
      `+neon` conformance binary) and its evidence is committed.
      **Blocked by:** a board. The target is Flame, our own RV1106G2 module; the Luckfox Pico Pro
      (same chip, off the shelf, Gaia is imaging it) stands in for it and is where it gets tested.
      **Measured:** qemu only: 91 + 5 + 2 tests pass in `ci.sh cross` at 03f55c5; `+neon` kernels
      pass under qemu cortex-a7.
- [ ] **[board] What:** decide `crc32()`'s ARMv7 default from a measurement of
      `crc32::neon_vmull_p8` (50093a9) against slice-by-8. Written, untested on board.
      **Why:** selection follows measurements. `crc32()` stays on slice-by-8 on ARMv7 until then.
      **Done when:** `crc32_throughput` has run on the Luckfox Pico Pro (Flame's stand-in; the
      `flame` profile of `scripts/board.sh` runs it) and the default follows the numbers.
      **Blocked by:** the Luckfox board (not imaged or reachable, Gaia 2026-11-03). The Raze
      cannot run AArch32 binaries.
      **Measured (qemu-arm `-cpu cortex-a7`, 2026-11-03, provisional):** slice-by-8 454 / 963 /
      970 / 954 MB/s, `neon_vmull_p8` 404 / 43 / 47 / 47 MB/s at 64 B / 1 KiB / 64 KiB / 4 MiB.
      Raze #2 (CM5, HeliOS dev.904, kernel 7.2.9-v8-16k, 2026-11-03): the kernel does not execute
      AArch32 binaries (the static armv7 musl bench fails exec and the shell reads it as a
      script), so the A76 cannot stand in; no COMPAT on the 16K-page kernel.
      Decision until a board run: slice-by-8 stays the ARMv7 default. QEMU TCG emulates
      `vmull.p8` in software, so this says nothing about the A7's NEON; only the board number
      can move the default.
- [ ] **[board] What:** K230D (`riscv64gc-unknown-linux-gnu` and `-musl`): the CPU graph tests
      and the kernel conformance binary on the board, and `crates/vglite/tests/vglite.rs` (the
      VGLite cells, each op's measured difference). Written, untested on board.
      **Why:** the K230D has no compute-capable GPU, and qemu cannot show a real VLEN.
      **Done when:** `scripts/board.sh k230d root@<k230d>` passes and its evidence, with the board's
      VLEN, is committed.
      **Blocked by:** a K230D board. The image is ready: Gaia feb262c ships `libvg_lite.so`,
      `/dev/dma_heap/linux,cma`; Gaia 6e4ede8: a 16 MiB CMA pool shared with VGLite's staging
      (no `libmmz`: contiguous frames come through Lemnos's heap API).
      **Measured:** qemu only: 91 + 5 + 2 tests in `ci.sh cross` at 03f55c5; `+v` kernels at VLEN
      128, 256 and 512, plus a run without Zvbc.
- [ ] **[board] What:** RK3588S (Mali-G610): `gpu_probe`, the `ci.sh pi` hardware tests, `gpu_bench`
      and the `GpuKernels` evidence on the shipped driver (libmali or PanVK). Written, untested on
      board: `GpuDeviceProfile` (Mali Valhall, warp 16; b095018), the GPU kernels and fragment
      passes, the AFBC modifier helpers, the checklist in `docs/testing.md`.
      **Why:** proper hardware support on every board.
      **Done when:** `scripts/board.sh rockchip` has run; the reports are in `docs/testing.md`, the
      matrix has Mali evidence, and the
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
- [ ] **[board] What:** (Lemnos's: Lemnos owns hardware blocks, and `crates/rga` goes to it when
      the user says, item 6 above. Lemnos's Rust userspace drivers decide the routes from then on.)
      The Rockchip RGA fixed-function cells in `daedalus-rga`, the crate shared with Styx. Route (b)
      is V4L2 mem2mem (`rgb_to_rgba`, `rgba_to_rgb`); route (a) is `Librga` (2f7c880;
      `gray_to_rgba`, `rgba_red`). Both written, untested on board.
      **Why:** these are the matrix's 4 untested cells. `rgba_red` relabels the frame as gray and
      scales it down by nearest, so it yields red only if the hardware takes the first of every four
      samples; the headers do not promise that.
      **Done when:** `crates/rga/tests/{v4l2,librga}.rs` pass on a Rockchip board (the `rockchip`
      profile of `scripts/board.sh` runs them) and the cells read
      pass, or `rgba_red` is recorded as failing. The same run measures the resize, palette and
      colour-key cells (`daedalus.image.*`, max difference printed): 2x and 4x area on NV12 and
      RGB (does 4x equal two 2x passes within 1?), and whether colour key `INVERTED` keeps the
      in-range pixels; the turn direction; whether `IM_ROP_XOR` (0xf6) is plain XOR; the blend
      rounding; the results go into `daedalus_rga::caps` as `Exact` or `Tolerance(n)`.
      Eidos's matrix cells for its 2x area lowering wait on it (nostd, 2026-11-02).
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
      **Measured:** 100 ms blocked with a 100 ms timeout (`crates/gpu/README.md`). The next
      submission behind a pending acquire (a producer job finishing 200 ms later, `fence_cost`):
      v3dv (Raze #2, UIF): `SyncFd` 0.2 ms, `Timeline` 197 ms; RADV: 0.2 ms and 196 ms
      (2026-11-03). `Auto`, the default, resolves to `SyncFd` on both, and no consumer (Styx,
      Eidos, HeliOS) selects `Timeline` or a timeout; their V3D frames are storage imports, whose
      fence is waited on the CPU. wgpu-hal 30.0.1 still relays with binary semaphores.
- [ ] **What:** flash the MCU firmwares on a real board (with a HAL: ADC input, GPIO output) and
      measure stack use and tick time in each mode.
      **Why:** sizes are linked and measured, but the firmware has never run on hardware.
      **Done when:** the stack and tick numbers per mode are in `docs/mcu.md`.
      **Blocked by:** an MCU board. The HAL side is written: `examples/mcu_board` runs all three
      modes on a Raspberry Pi Pico (`rp2040-hal`: ADC0 in, GPIO25 out) and reports tick time
      and the stack high-water mark per mode over defmt-RTT (2026-11-03); flashing it needs a
      Pico and a probe.
      **Measured (`ci.sh mcu`, 2026-11-02), flash / static RAM, M4F and M0+:** compiled 2988 / 3316 B
      and 112 B; tunable 4424 / 5488 B and 128 B; loaded 11496 / 12572 B and 584 B. Stack and tick
      time: none.
- [x] **What:** a chunked blob receive helper for the MCU (feature-gated framing + CRC-32 into a
      buffer or flash partition).
      **Done (2026-11-03):** `daedalus_mcu::loaded::receive` (feature `receive`): `encode_chunk` on
      the sender, `BlobReceiver` over a `BlobSink` (`[u8]` or a flash partition) on the device;
      a CRC-32 per chunk, duplicates ignored, a gap answered with `resend_from`. The tests receive
      a plan blob in chunks (with a corrupted frame, a retransmission and a gap) and load and run
      it. Untested on a board.
- [x] **What:** Windows. The user said to make it work (2026-11-03, relayed by HeliOS).
      **Done (2026-11-03):** the 14 symlinks are copies checked by
      `scripts/check-shared-sources.sh` (in `ci.sh lints`), checkouts are LF everywhere, and CI's
      `windows` job runs clippy and the workspace tests on `windows-latest` (green on
      `ci/windows`, run 41707584216). Linux-only paths are compiled out;
      the lifecycle tests' fake workers are the test binary (`tests/worker_lifecycle.rs`, no shell);
      two Java payload tests stay ignored there (JVM-mapped files that Windows cannot delete);
      `docs/development.md`, "Windows".
- [x] **What:** CUDA interop and backend-aware planning: CUDA↔Vulkan memory sharing
      (`cudaImportExternalMemory`) so a wgpu buffer can reach a CUDA node without a host copy, and a
      `Residency::Gpu` request that picks its device key by its neighbours.
      **Why:** the CUDA backend (done below) keeps the two residencies distinct; these two are what a
      mixed graph would need.
      **Done when:** a graph with a wgpu node feeding a CUDA node (and back) runs on the RTX 4060
      with checked values, and a planner test shows a `@gpu` request never picks a CUDA device.
      **Status (2026-11-03, branch `cuda-graph`):** done, measured.
      - Planner: a bare `Residency::Gpu` request picks the neighbour's device key
        (`residency_tests.rs`: a CUDA producer inserts 0 adapter steps and keeps `@cuda`; a wgpu
        producer keeps `@gpu`; a host producer follows its one consumer's device key).
      - Graph: `crates/runtime/tests/it/executor_shared.rs` plans gen (wgpu) -> scale (CUDA) -> addone
        (wgpu) -> sink with the planner and runs three ticks on the RTX 4060. The two device edges
        are cost-0 `SharedView` adapters (`share.gpu_to_cuda`, `share.cuda_to_gpu`, residency `Gpu`),
        so the plan's only converter steps are those two, and both transfer counters stay at 0. Values
        are checked on each tick. The memory is one `interop::SharedValue`: a Vulkan allocation with a
        `wgpu::Buffer` view (`create_buffer_from_hal`, externally owned), a CUDA import, and a timeline
        semaphore. The wgpu side is ordered by a host fence, the CUDA side by the timeline.
      - Buffer interop (`cuda_interop.rs`): a device-local Vulkan buffer exported as an opaque fd,
        imported into CUDA and scaled in place, with the transfer counters unchanged.
      - Timeline semaphores (`cuda_interop.rs::a_timeline_semaphore_hands_a_buffer_between_vulkan_and_cuda`):
        ordered on the GPU, values checked. Host fence against semaphore round trip, RTX 4060, release
        build, 2000 round trips: fence 215-217 µs median, semaphore 198-199 µs (`docs/benchmarks-log.md`).
      - CUDA images (`cuda_device.rs`).
      Refused by the driver: dma-buf import, `cuImportExternalMemory` returns `CUDA_ERROR_UNKNOWN` (999)
      for a dma-buf fd.
      **Limits:** wgpu cannot take caller semaphores (`wgpu-hal` `Queue::submit`), so the wgpu boundaries
      are host fences. A wgpu view must be `from_raw_externally_owned`: `from_raw` makes wgpu-hal call
      `vkDestroyBuffer` on drop. The executor holds one GPU handle (wgpu here), so the CUDA backend is
      captured by the adapters and the CUDA node.
      **Measured:** the counters above; the release-build latency above.
- [ ] **What:** signing and registry work.
      **Why:** pinned for later by the user (2026-10-30).
      **Done when:** the user has scoped it.
      **Blocked by:** the user's decision.
      **Measured:** none.

- [x] **What:** per-executor port buffer pools without `std`, for multi-threaded `no_std` hosts
      (Eidos saw 15/80 runs of 5 parallel graphs allocate, cad62f3).
      **Done (2026-11-03):** without `std` each executor owns its pool (`io::PortPool`, carried
      in `NodeIoEnv`; a snapshot gets its own). `examples/nostd_smoke/tests/parallel_executors.rs`
      ticks 4 engine graphs on 4 threads with every crate's `std` off: 2 allocations per tick on
      each thread, the same as one thread alone (8000 over 4000 ticks). With the old
      process-wide pool the same test counted 8515 to 12725 per thread (3/3 runs failed).

- [ ] **What:** (As of 2026-11-03 these nodes sit on Lemnos's drivers and open no devices
      themselves.) Fixed-function nodes for Eidos on the RGA and the K230D Image 2D / 2.5D GPU
      (VGLite), with a per-SoC capability descriptor. Requested by Eidos 2026-11-02 (Eidos
      100cc356, `docs/accel/fixed-function-audit.md`). Daedalus owns the nodes and Lemnos the
      units. Eidos lowers its ops onto these nodes and never drives the units.
      **Order:** (1) area and bilinear resize, for pyramids (Eidos `geometry.resize_area`,
      `resize_linear`, `features.feature_pyramid`, `filter.build_pyramid`); (2) palette lookup and
      colour-key range mask; (3) crop, flip, copy (partly in `daedalus-rga`); (4) blend with mask
      alpha, fill, ROP, line draw. With (1): the capability descriptor (RGA2, RGA3, mainline V4L2
      vs librga, K230 Image 2D, VGLite; ops and an exactness class per unit and SoC).
      **Constraints:** today's routes: mainline `rockchip-rga` (V4L2) reaches scale, crop,
      conversion, flip, 90/180/270 rotation and background fill only; blend, colour key, palette and
      ROP need librga (vendor kernels). Lemnos's kernel module and Rust userspace drivers may change
      what each unit reaches. RGA scaling exactness is undocumented: tolerance until a board
      measures it.
      K230: VGLite through `libvg_lite.so` (`daedalus-vglite`); no board yet.
      **Done when:** each node has a scalar reference, a fixed-function kernel cell in the registry,
      and a test on `vim2m` where the path allows; board cells marked written, untested on board.
      **Blocked by:** Lemnos's drivers and the `crates/rga` and `crates/vglite` hand-off (items 6
      and 8 above); boards for measurement.
      **Progress:** (1) written: `Filter::{Area, Bilinear}` on both routes, `resize_luma` through
      NV12 on mainline, `caps::caps(soc)`, ops `daedalus.image.resize_{area,bilinear}` (scalar and
      fixed_function rows only, `Op::only_on`); vim2m checks the plumbing. Board cells written,
      untested on board; exactness `Unmeasured`. Open in (1): a GRAY8 dmabuf through NV12M with a
      shared dummy chroma plane (mainline 7.2 lists NV12M on RGA2 and RGA3): not wanted, Eidos
      chains NV12 levels (nostd, 2026-11-02). (2) written: librga palette and colour key, ops
      `daedalus.image.{palette,color_key_mask}`, `caps` `FixedOp`; written, untested on board.
      (3) written: crop on both routes (full frame = copy), flip and rotate on librga, ops
      `daedalus.image.{crop,flip,rotate}`; mainline flip and rotate through Lemnos's
      `set_control` (01939e9), flips checked exactly on vim2m. (4) written: librga fill (boxes as
      four fills), mask AND/OR/XOR and Porter-Duff blend, ops
      `daedalus.image.{fill,mask_rop,blend}`; mainline has none (7.2 never programs
      `BG_COLOR`); RGA2 and RGA3 have no line mode (vendor drivers, radxa 6.1). K230 design note
      written (`docs/k230-fixed-function.md`). `daedalus-vglite` written (nostd, 2026-11-02: the
      board is not a blocker): the `vglite` variant on resize (area 2x only, as a half-phase
      bilinear blit), crop, flip, rotate, fill, blend (straight alpha), palette and
      `daedalus.image.line` (new op, Bresenham reference; the one fixed-function line route);
      scattered dmabufs and byte buffers staged through a contiguous buffer, the cost returned
      and in `caps` (`Staging`). Written, untested on board.
      **Measured:** n/a.

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

## Dependency layers

The DMA-buffer move moved up into Alpha (Raze) on 2026-11-02 and is done. The user slated the rest
for after the release (HeliOS architecture-overhaul f2a15c8f), then on 2026-11-03 said to do the
whole list now (relayed by HeliOS): the shared device and the accelerator interface below are
under way. The split: Daedalus
decides whether and how data moves; a compute device's own copies stay in its driver (GPU, RGA,
later NPU); Lemnos owns the DMA buffer basics and bus transfers. Overlap checked against the code
on 2026-11-02, at f7e05ac.

- [x] **What:** one Vulkan device per process, shared with Styx's gpuisp.
      **Why:** two devices in one process duplicate pipelines and memory and need cross-device
      sync.
      **Daedalus side, done (2026-11-03):** `shared_context` / `process_context` /
      `devices_created`; every engine with `GpuBackend::Device` takes the process's device
      (`tests/shared_device.rs`, `daedalus/tests/gpu_shared_device.rs`: two engines and a library,
      one device).
      **Styx side, done (2026-11-03):** Styx's `gpu_isp::context()` takes `shared_context` and
      builds `GpuContext::from_daedalus` on it; every submit on it goes through `RawSubmitter`,
      under `submit_lock()`.
      **Done when:** a Styx + Daedalus process creates exactly one device, asserted in a Styx
      test, and it passes on the Raze under VVL.
      **Measured:** Styx aa95fb4 takes `shared_context`. `crates/gpuisp/tests/one_device.rs` on
      Raze #2's V3D: `devices_created()==1`, 0 validation messages after Styx's `convert.comp`
      fix (2026-11-03). Synthetic frame. Sync validation confirmed on V3D with Debian's layer
      1.4.309: `sync_hazard_control` reports one `SYNC-HAZARD-WRITE-AFTER-WRITE` with it on and
      none with it off; `one_device` reports no hazard and no VUID, exit 0, one run each. The
      control uses fill buffers, so it shows sync validation runs, not that it catches every
      compute hazard.

- [ ] **What:** an accelerator interface (load program, import buffers, submit, fence) for
      NPUs and custom accelerators.
      **Why:** the GPU path's residency, `#[device]` transfers and completion API should extend to
      other accelerators without a backend per device type.
      **Interface, done (2026-11-03):** `daedalus-accelerator` (`Accelerator`, handles with
      deferred release, `BufferSource::DmaBuf` with an acquire fence, `Fence` with `sync_file`
      export, `Wait`, `validate_bindings`) and `docs/accelerator-interface.md`; the contract test
      runs on an accelerator defined in the test.
      **Open:** the design's graph integration (residency key, `ExecutionContext` capability,
      fences between nodes) with the first implementation.
      **Blocked by:** the user's go for an implementation (RKNPU, K230 KPU, Coral Edge TPU), on
      Lemnos's kernel module and Rust userspace drivers, not vendor runtimes.
      **Measured:** n/a.

## Done

Each entry cites the commit that did the work, as far as history can attribute it. Where one symbol
was searched for, the commit is the one that introduced it.

- [x] `daedalus-rga`'s V4L2 mem2mem session is Lemnos's `M2mSession` (Lemnos fdbb0c4: crop in
      `QueueSetup`, `v4l2::query`, `find_m2m`). `crates/rga/tests/v4l2.rs` passes on `vim2m`
      (conversion, scaling, dmabuf in and out, a refused crop); the crop success path waits for an
      RGA board. One RGA binding, in Daedalus; Styx keeps V4L2 M2M for codecs (agreed 2026-11-02).

### GPU and hardware support

- [x] A CUDA backend (`gpu-cuda`, `daedalus_gpu::CudaBackend`): the driver API loaded at runtime
      (`libcuda.so.1`, no toolkit at build time), PTX kernels compiled by the driver, device memory,
      streams and events. A residency key per backend: CUDA values are `…@cuda` port types, wgpu's
      `…@gpu`, so the two never meet. `#[device]` transfers keyed `@cuda` run through the executor's
      context. Tests: `crates/gpu/tests/cuda_device.rs` (upload, `scale_f32`, download, 1000 values
      checked exactly; a download waits for the kernel queued before it), `executor_cuda.rs` (a graph
      whose GPU node runs upload, kernel, download and whose sink checks the values) and
      `device_cuda.rs` (the `@cuda` transfers). All ran on an RTX 4060, driver 595.104.02, and skip
      with the reason under `CUDA_VISIBLE_DEVICES=-1` (2026-11-03). Images, buffer interop and
      neighbour-based planning came after it (see Open for what remains).
- [x] The engine and a plugin `cdylib` on one device share its submit lock (alpha item): the
      lock is the context's (700be30, `GpuContextHandle::submit` / `submit_lock`), and
      `crates/gpu/tests/plugin_submit.rs` (9861b18) loads a plugin `cdylib` with its own copy of
      `daedalus-gpu`: it sees the host's lock, waits while the host holds it, and 200 host
      `RawSubmitter` fills alongside 200 plugin round trips stay correct. Raze #2 (CM5, V3D) at
      c919b14 under the Debian arm64 validation layer 1.4.309 with sync validation: 5 of 5 runs,
      no validation message (2026-11-03). Eidos's own direct submits move onto `ctx.submit` on
      its side.
- [x] The stale-graph refresh confirmed on the Raze (alpha item; `refresh_graph_ports_with` +
      `drop_stale_consts`, bb07e3b, 0398d4a): Raze #1 on HeliOS dev.890 (469cb6f3) logs "stored
      graph ports refreshed from the plugins" for each Eidos node of the stored default graph,
      drops the stale settings, and plans and runs it (state running, revision 218; reported by
      the HeliOS coordinator, 2026-11-02).
- [x] The dmabuf fence-timing tests pass under plain multi-threaded `cargo test` (alpha item):
      the non-blocking import margins are 300 ms, the late producer's spin is calibrated on the
      fastest of three runs, and a check the producer loses (its job done before the check needs it
      running) is rerun with a doubled job, up to 4 times; a consumer that really waits still
      fails. RADV: 5 of 5 multi-threaded `--include-ignored` runs and a single-threaded run, 97 of
      97 (before: 2 of 3 and 1 of 3 runs failed).
- [x] Readback timing on the Raze after the DMA-buffer move onto Lemnos (alpha item): unchanged.
      Same-session A/B, `gpu_bench` at 5e97c0d (before 803f310) against 6a0c73f, interleaved,
      12 runs each: 16 MiB host-cached read median 3.83 -> 3.79 ms, 1 MiB 0.270 -> 0.275 ms
      (`docs/testing.md`, "CM5 run (2026-11-02, alpha)").
- [x] The gpu crate's DMA-buffer basics on Lemnos's (alpha item): 803f310, Lemnos 63374c6
      (`DmaBufRef`, the frame identity and the acquire-fence wait with Lemnos fdbb0c4 and
      2610455); the Raze readback is unchanged (above).
- [x] Zero-copy import of Styx's software-ISP frames on the Raze (alpha item): Styx's
      `daedalus_import_check` (Styx 4c6e9b7): an NV12 1280x800
      `softisp_dmabuf` frame imported with `GpuStorage::import_dmabuf` in 95 to 97 us, the GPU's
      read equal to the frame's 1536000 bytes, and a CPU-written sentinel read back by the GPU
      (same memory, no copy); 3 of 3 runs.
- [x] The GPU path on the CM5 at 6a0c73f (alpha item): `board.sh raze` (graph, kernels, the GPU
      crate's 96 tests with the dmabuf and raw Vulkan hardware tests, `gpu_probe`, `gpu_bench`,
      `GpuKernels` evidence) and `driver_stress` pass; numbers in `docs/testing.md`, "CM5 run
      (2026-11-02, alpha)".
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

- [x] Zero allocations per frame on the raw GPU submit path: `RawSubmitter`, a ring of command buffers and fences that shares one submit lock with every Daedalus submit. 0 allocations over 100 frames of submit and wait, against 17 per frame through `submit_raw` before (1700 over 100). fd18808; `tests/raw_submitter_allocations.rs`. The validation layer is not installed here, and Styx's own test and the Raze runs are still to do.

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
- [x] `scripts/board.sh`, one command per board run (profiles flame, k230d, rockchip, raze):
      2680686. Proven on the Raze (2026-11-02); the other profiles are written, untested on
      board.
- [x] False bench flags on shared runners: a flagged bench is re-run twice more on both commits,
      alternating, and `bench` fails only if the median of three still regresses (bf07300).
      Docs-only commits never ran `bench` (its path filters). The one failure in 15 runs, 0e620c2's
      `telemetry_clone_costs/clone/Basic` +16.7%, was noise: that commit does not touch what the
      bench clones, and a local re-run overlaps (196-212 ns before, 212-217 ns after, ±10% per run).
- [x] Public API review: 38cbdd9, b25b304 and the series around them (`docs/api-review.md`).
- [x] GPU-side fence wait with a timeout, compressed modifiers: 6b5ec57, 9dfeda1.
- [x] Configurable fence wait, `SyncFd` by default: a03a1e0.
- [x] Hardware tests under the validation layers: fc50a8a.
- [x] The validation layer on the Raze, cleanly: Debian trixie's arm64 layer 1.4.309 (download
      approved by the user, 2026-11-02) loads, the dmabuf (13) and raw Vulkan (3) tests and
      `gpu_probe` exit 0 with no validation message and no exit crash, after fixing the one error it
      found (a dedicated dmabuf allocation, VUID-...-02965): 8fbea48.
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
- [x] Fan-in and user adapters (`#[daedalus_mcu::adapt]`) in compiled, tunable and loaded modes,
      blob format 3: 730edf7. `examples/mcu_blink/graph_fanin.json` runs natively in all three modes
      (no firmware of its own, so not size-measured); the blink loaded firmware grew to 11496 /
      12572 B. `FanIn<T>` (indexed ports) stays unsupported.
- [x] Shared nodes: one function is the device node and a full-runtime `#[node]`
      (`runtime = "feature"`), with the same id, ports, state and fire mode; the runtime NodeDecl
      matches the device derivation in a test, and one graph runs under the runtime, compiled and
      loaded. `examples/mcu_shared`; the blink firmwares are unchanged: 25777dc.
