# Benchmark log

Device and host-specific runs of the frame path, kept out of the generic benchmark guide
([development.md](development.md#benchmarks)). The bench commands and the x86_64 numbers stay
there; this file records what was measured on a Raspberry Pi CM5 (aarch64) and the run
conditions behind the comparisons.

Run conditions used in the notes below:

- Before/after comparisons were benched back to back on the `helios-integration` tree (the build
  before the change), so differences under about 10% are noise.
- Device runs were taken with PhotonVision stopped, so the measurement core was not shared.
- The reference point for the detector-shaped graph is a real AprilTag graph measured on the
  same CM5 (about 16-17 us `graph_overhead` p50, group about equal to flat, no copies).

## Group node against the per-stage template

Group node against the per-stage template. On the CM5 the group measured 35 µs against the
template's 27 µs. Runtime overhead explained little of it: the expanded group runs the same five
nodes and eleven edges (asserted) and cost 2% more instructions before this pass (the per-node
host-bridge metadata lookup over the group's larger metadata maps, now a mask), the same after.
The rest is measurement order: the reference bench ticks the group right after the bare stages
(about 1 ms of image processing that evicts the runtime's code and data from L1/L2) and the
template right after the group, warm. Replaying that loop on x86 (a 8 MiB memory sweep, then
graph A, then graph B, per frame) gave, before this pass, 6.4 µs for whichever graph ran first
against 5.5-5.8 µs for the second (5.2 / 5.0 µs without the sweep); after it, 4.0-4.2 µs first
against 3.8-4.0 µs second (3.5 / 3.5 µs without). Fewer instructions and less data touched per
tick shrink the cold-cache penalty with them; on the CM5's smaller caches the first-run graph
still pays more, so compare graphs in alternating order or one per run.

**CM5 projection (an estimate, not a measurement).** Scaling the CM5 rows by the x86 ratios
(`graph_overhead` p50 x0.66, p99 x0.47; instructions x0.50), the per-stage template's 27 µs
`graph_overhead` p50 would be about 14-18 µs and its 36-41 µs p99 about 17-20 µs, and the
group, run cold-first as in the reference bench, about 3-5 µs above that rather than 8; with no
node allocations (the 384 B per frame are gone), the CM5 should gain at least as much as x86
does. Measure on the device to confirm.

## Fusion

**CM5 measurement (Raspberry Pi CM5, kernel 7.2.9 with 4 KiB pages, `bcc9f33`, static aarch64-musl,
`FRAME_BENCH_CPU=2`, system services stopped, 2000 frames, fusion off/on alternated and repeated).**
The tick-cost pass and fusion together beat the projections above: the detector mirror's
`graph_overhead` p50 is 7.2 µs unfused (27-35 µs before the tick-cost pass), and flat and group
now cost the same. For comparison, the reference graph measures about 16-17 µs `graph_overhead` p50 on the
CM5 for its real AprilTag graph (group about equal to flat), with no copies and about zero
allocations per tick.

| CM5 detector mirror | flat, unfused | flat, fused | group, unfused | group, fused |
| --- | --- | --- | --- | --- |
| instructions per frame | 32 439 | 32 046 | 32 480 | 32 087 |
| push + tick + take p50, recording off (repeat) | 8 593 (8 648) | 8 463 (8 611) | 8 611 (8 667) | 8 463 (8 555) |
| inputs p50 | 1 800 | 1 520 | 1 800 | 1 540 |
| node_io p50 | 2 260 | 2 170 | 2 280 | 2 190 |
| dispatch p50 | 1 280 | 1 260 | 1 280 | 1 270 |
| `graph_overhead` p50 / p99 | 7 220 / 7 410 | 6 830 / 7 040 | 7 240 / 7 520 | 6 850 / 7 020 |

| CM5 `frame_chain` (`interface` feed, 640x480 GRAY8 on dma-heap) | unfused | fused |
| --- | --- | --- |
| instructions per frame, 1 / 4 / 16 stages | 7 432 / 13 606 / 38 302 | 7 432 / 13 109 / 35 717 |
| frame p50, 1 / 4 / 16 stages | 1 871 / 3 222 / 8 241 ns | 1 907 / 3 130 / 7 407 ns |
| frame p50 fit | 1 485 ns + 423 ns per stage | 1 602 ns + 364 ns per stage |
| `graph_overhead` p50 fit | 1 194 ns + 655 ns per stage | 1 342 ns + 498 ns per stage |

Fusion saves about 5.5% of the detector's `graph_overhead` and 24% per fused chain stage;
nothing copies or allocates in either mode.

## Shared preprocessing

**CM5 measurement (same run as the fusion rows above, fusion on, recording off).** Push + domain
tick + take p50: 2 detectors 17 666 separate, 17 611 shared, 17 685 structural ns; 4 detectors
35 630 separate, 30 852 shared (-13%), 30 944 structural ns. Over 2000 frames with 4 detectors
the domain avoided 6000 graph runs and 12 000 node runs.

## CUDA handoff: host fence against timeline semaphore

Vulkan→CUDA→Vulkan round trips on the RTX 4060 (driver 595.104.02, `CudaBackend` device 0, the
wgpu device on the same GPU), 1000 `f32` in a shared device-local buffer, one `scale_f32` by 1.0
per round trip. Each path does two Vulkan submits and one kernel per round. The host-fence path
waits on each side before the next side starts; the semaphore path (`SharedSemaphore`,
`RawSubmitter::submit_synced`, `CudaSemaphore`) orders the steps on the GPU and the host waits once
at the end. The two paths alternate within each run. Measured by
`crates/gpu/tests/cuda_interop.rs::handoff_latency_host_fence_vs_semaphore` (ignored test),
2000 round trips after 200 warm-up, release build (`cargo test --release`), three runs:

| run | fence median | fence p99 | fence mean | semaphore median | semaphore p99 | semaphore mean |
|---|---|---|---|---|---|---|
| 1 | 256.4 µs | 1274.8 µs | 322.1 µs | 215.6 µs | 1202.8 µs | 273.3 µs |
| 2 | 217.1 µs | 252.4 µs | 225.0 µs | 198.6 µs | 227.3 µs | 207.8 µs |
| 3 | 215.2 µs | 254.7 µs | 222.0 µs | 197.6 µs | 217.8 µs | 205.1 µs |

In runs 2 and 3 the semaphore path is about 18 µs faster per round trip (8% at the median). Run 1
is an outlier on both paths (p99 above 1 ms, the first run after the build), so read its p99 with
care. The gap is the host round trips the fence path adds; the Vulkan submits and the kernel launch
with its event are common to both and were not timed separately.

Earlier debug (unoptimized test profile) figures, for comparison: fence median 221 µs, semaphore
median 201 µs.

## Stable plugin path: constants, struct values and handles

Raze #1 (CM5, kernel 7.2.9-v8-16k, HeliOS services running, load average 0.2), 2026-11-05, one
run at 52f1199's bench on 1930539, release, aarch64 glibc. Unpinned: the board has no `taskset`.
`run.sh` from the cross-build, held under `/run/lock/raze-gpu.lock` for 13 s. Static and stable
installs alternate within the run; p50 over 101 repeats. Commands:
`cargo test -p daedalus-rs --features engine,dylib-plugins --release --test dylib_stable_bench --
--ignored --nocapture` and `--test dylib_stable_bench_allocations`.

The fixture (`examples/plugins/stable_bench`) mirrors the shape of Eidos's `Detections` (per tag:
id, rotation, hamming, four integer corners, four float sub-pixel corners), but it is not Eidos's
code: Eidos has no way back from a `Value`, so the fixture writes the `from_value` it would need.
Its constants are fixed values and a fixed 17-byte string, and the staged frame is a 64x32 host
frame whose stages pass the handle on, so the staged rows measure edge cost only.

| case | static p50 | stable p50 | stable extra |
| --- | --- | --- | --- |
| node with 0 / 8 / 16 / 49 scalar constants | 2.0 / 3.0 / 3.7 / 7.7 µs | 3.2 / 6.2 / 9.3 / 26.2 µs | 1.2 / 3.3 / 5.6 / 18.5 µs |
| fit, scalars / quarter strings | 116 / 162 ns per constant | 476 / 533 ns per constant | 360 / 370 ns per constant |
| `Detections` through `Value`, 0 / 4 / 16 tags | 2.9 / 3.0 / 3.2 µs | 7.0 / 40.6 / 141.7 µs | 4.2 / 37.6 / 138.5 µs |
| `Detections` as a plugin-local handle, 0 / 4 / 16 tags | 2.6 / 2.6 / 2.8 µs | 5.2 / 5.2 / 5.4 µs | 2.7 / 2.5 / 2.7 µs |
| 12 stages, handle in and out plus a frame, 0 constants | 14.3 µs | 33.0 µs | 18.7 µs |
| the same, 10 constants per stage | 38.3 µs | 89.6 µs | 51.3 µs |

Allocations per tick (host / plugin): `Detections` through `Value` at 4 tags 132 / 276, at 16
tags 508 / 1052 (static: 3); as a handle 1 / 4 at every tag count. Each string constant costs the
plugin two per call (49 constants with a quarter strings: 24). The 12-stage chain allocates 13 on
the host on the stable path against 3 statically: one payload wrapper per handle edge.

Against the estimates in the audit of running Eidos on the stable path:

- Handles on every edge, 20-50 µs per frame: 18.7 µs for 12 stages, about 1.6 µs per stage, at
  the low end. Not the zero-allocation rule's 0: one host allocation per handle edge.
- Constants sent on every call, 10-80 µs more: about 0.27 µs per constant in the chain (32.6 µs
  for 120) and 0.36 µs in isolation. Eidos's tracked stages hold 310 if all run, so 85-110 µs:
  above the estimate.
- Structs through `Value`, several hundred µs per frame: 38 µs per crossing at 4 tags and 139 µs
  at 16, so 20 struct edges cost about 0.75 ms at 4 tags. Confirmed, and it allocates per tag.
- A handle costs the same at 0, 4 and 16 tags.

## Stable plugin path: `Detections` as `StablePod` structs (x86-64)

x86-64 desktop, NOT the CM5: 2026-11-06, release, load average about 62 (other builds running),
so treat the timings as rough. One run of `cargo test -p daedalus-rs --features
engine,dylib-plugins --release --test dylib_stable_bench -- --ignored --nocapture` and
`--test dylib_stable_bench_allocations`. Case `detections_pod`: the same tags as the `Detections`
fixture as a `Vec<TagPod>` (`#[derive(StablePod)]`, 72 bytes per tag), crossing as a typed byte
view (stable ABI 5).

| case | static p50 | stable p50 | host / plugin allocations per tick (stable) |
| --- | --- | --- | --- |
| through `Value`, 0 / 4 / 16 tags | 2.8 / 2.9 / 3.3 µs | 5.8 / 33.1 / 128.0 µs | 7 / 12, 133 / 276, 509 / 1052 |
| plugin-local handle, 0 / 4 / 16 tags | 2.5 / 2.6 / 2.9 µs | 4.4 / 4.5 / 5.2 µs | 2 / 2, 2 / 4, 2 / 4 |
| `StablePod` structs, 0 / 4 / 16 tags | 2.7 / 2.7 / 2.4 µs | 4.8 / 4.9 / 4.0 µs | 1 / 1, 1 / 4, 1 / 4 |

The plain-data crossing costs about what a handle does and does not grow with the tag count at
these sizes (one copy of 72 bytes per tag each way); through `Value` it grows by about 8 µs per
tag here. The CM5 row is still to be measured.

## Inline plain payloads (208 B) and stable ABI 3 on the CM5

Raze #1 (CM5, kernel 7.2.9-v8-16k, HeliOS services running), 2026-11-06. Release, aarch64 glibc,
unpinned (no `taskset`), held under `/run/lock/raze-gpu.lock`. 671715b (`Payload` 184 B, stable ABI
2) against ceacfc6 (`Payload` 208 B with `Payload::plain`, stable ABI 3 with constants sent once),
alternated base, new, base, new, base, new; median of three runs. Load average rose from 1.1 to 2.9
over the session. Base was built with the same `metrics` dev-dependency ceacfc6 adds, so both
measure the same features. The A/B bundles both changes; the rows below say which one moves.

| row | 671715b | ceacfc6 | change |
| --- | --- | --- | --- |
| 12 stages, handle and frame, 0 constants: static / stable | 17.9 / 36.7 µs | 18.0 / 37.1 µs | +0.4% / +1.1% |
| 12 stages, 10 constants per stage: static / stable | 42.1 / 94.7 µs | 42.1 / 62.1 µs | +0.1% / −34% |
| node with 49 scalar constants, stable | 27.2 µs | 10.4 µs | −62% |
| node with 49 quarter-string constants, stable | 30.5 µs | 12.2 µs | −60% |
| stable cost per constant, scalars / quarter strings | 484 / 551 ns | 128 / 164 ns | |
| `Detections` through `Value`, 16 tags, stable | 170 µs | 171 µs | +0.4% |
| graph frame, serial, metrics Off / Basic | 26.6 / 30.1 µs | 26.2 / 29.1 µs | within noise |
| graph frame, adaptive, metrics Off | 27.9 µs | 27.8 µs | within noise |

- The 208 B payload shows no cost above run-to-run noise on the rows that isolate it: static
  paths, serial and adaptive graph frames, allocation counts. The parallel graph rows were bimodal
  under the rising load on the base runs and are not used.
- Two zero-constant stable rows rose 4-7% (`scalars n=0`, `Detections through Value` at 0 tags)
  while their static twins did not; this run cannot tell the new `set_consts` check from the payload
  size apart.
- Constants: the stable path now pays 128 ns per scalar constant in isolation, against 115 ns
  statically. Plugin allocations for constants fell (49 quarter strings: 24 to 12; the 10-constant
  chain: 172 to 28). Host allocations are unchanged; the 12-stage chain is still 14 on the stable
  host against 4 static (one per handle edge, see the TODO).

### Bisect of the zero-constant stable rows (2026-11-06)

The 4-7% on two zero-constant stable rows above, split over 671715b (A), 9ac2d6d (B: stable ABI 3,
`Payload` 184 B) and ceacfc6 (C: `Payload` 208 B). Raze #1, pinned with a `sched_setaffinity`
helper: the stable bench to CPU 3, the graph bench to CPUs 1-3 (CPU 0 keeps IRQs and services).

- Pinned, the parallel graph frame is steady at 37.3 / 37.3 / 36.8 µs (A / B / C); unpinned it had
  been bimodal at 44-62 µs. Every graph row is within ±1.3% across the three.
- B against A (three runs each): no shift on the zero-constant stable rows. The `set_consts` check
  on that path is one `Option` compare on the host and one `None` clone in the plugin.
- C against B (nine runs each, Mann-Whitney): the stable-minus-static extra rises about 300 ns on
  `scalars n=0` (p < 0.001) and `Detections` through `Value` at 0 tags (p = 0.001); the handle row
  and the 12-stage chain do not move. The static twins move 1-4.5% in both directions between the
  two binaries.
- Instructions (x86, `perf stat`, pinned, one stable zero-constant node per tick): B 13,461, C 13,393
  per tick. C does no extra work; the remaining gap is code layout or caching, not the payload
  size. Cycles on the shared desktop were too noisy to compare. Raze #1 has no `perf`.

### The same rows on the A76's counters (2026-11-06)

`daedalus-bench-counters` (`perf_event_open`, user space, one group, no multiplexing;
`perf_event_paranoid` is 2 on Raze #1), `DAEDALUS_BENCH_COUNTERS=1`, pinned to CPU 3, B (9ac2d6d)
and C (ceacfc6) alternated five times each; medians per call.

| row | cycles B / C | instructions B / C | branch misses B / C |
| --- | --- | --- | --- |
| `scalars n=0`, static | 5,992 / 5,961 | 9,262 / 9,376 | 5.7 / 3.7 |
| `scalars n=0`, stable | 9,242 / 9,522 | 12,654 / 12,807 | 8.7 / 12.5 |
| `Detections` through `Value`, 0 tags, static | 8,977 / 9,007 | 13,340 / 13,487 | 12.4 / 15.7 |
| `Detections` through `Value`, 0 tags, stable | 20,141 / 20,422 | 29,100 / 29,320 | 34.8 / 46.6 |

The stable-only part of `scalars n=0` (stable minus static) grows 311 cycles but only 39
instructions; the ~115-instruction growth both paths share is the payload and telemetry change.
Branch misses rise on the stable path (L1D misses were too noisy to read). Closed as layout: no
extra work on the A76 either.

## Foreign handles held inline: the payload layout on the CM5 (2026-11-06)

Raze #1 (CM5), release aarch64, `stable_bench_counters` (`daedalus-bench-counters`,
`DAEDALUS_BENCH_COUNTERS=1`), pinned to CPU 3, every run inside one `flock -w 600
/run/lock/raze-gpu.lock` hold, arms alternated five times, 15 repeats of 20,000 calls each; medians
per call. A: 3dcdf2b (`Payload` 208 B, a foreign handle in an `Arc`'d storage). B: c1b42d5 (the
handle held inline in the payload body: `Body` 56 B, `Payload` 216 B). C: B with the `Residency`
moved into the body's tag word (`Payload` 208 B again), which is what landed. Load average 0.4 to
1.0. The rows have no foreign handle: they measure what the layout costs every payload.

| row | cycles A / B / C | instructions A / B / C |
| --- | --- | --- |
| `scalars n=0`, static | 6,105 / 6,131 / 5,965 | 9,221 / 9,431 / 9,322 |
| `scalars n=0`, stable | 9,737 / 9,789 / 9,411 | 12,805 / 13,067 / 12,858 |
| `Detections` through `Value`, 0 tags, static | 8,810 / 8,688 / 8,427 | 12,945 / 13,001 / 12,744 |
| `Detections` through `Value`, 0 tags, stable | 20,304 / 20,174 / 20,091 | 28,920 / 28,836 / 28,499 |

- B against A, first session (A and B only, five alternations): `scalars n=0` +2.3% cycles
  static and +2.2% stable, tracking +2.3-2.6% instructions (about 240-300 more per call); the
  `Detections` rows moved −1.0% / +0.6%. L1D misses stayed at noise level. On x86 the instruction
  growth reproduced (+91 to +350 per call), and boxing the handle (a test build, `Payload` 208 B,
  same code paths) took `scalars n=0` back to A's count: the extra instructions are the larger
  payload's moves, not the added match arms.
- C against A: 1-4% fewer cycles on all four rows; instructions +0.4-1.1% on `scalars n=0`,
  −1.5% on `Detections`. A frame still allocates nothing.

## Acquire fence on a dma-buf frame: a real GPU sync_file on the CM5 (2026-11-06)

Raze #1 (CM5, kernel 7.2.9-v8-16k, V3D 7.1.10.2 through v3dv), release aarch64, under `flock -w 600
/run/lock/raze-gpu.lock`, load average 0.3. `frames::fence_board` in `daedalus-surface` (ignored;
`surface_tests --ignored --nocapture fence`): a Vulkan job of buffer copies (no shader), calibrated
to about 250 ms, signals a semaphore exported as a `SYNC_FD`; the `sync_file` is pushed with a
system-heap dma-buf frame into `frame` -> `frame_info` (the node maps the frame and reads its bytes,
so `plane_data` and `DMA_BUF_IOCTL_SYNC` start). Five rounds each, three runs:

| | push + tick, min / median / max |
| --- | --- |
| with the GPU's `sync_file` | 245-260 / 246-262 / 250-266 ms |
| without a fence (the same job running) | 0.090 / 0.098-0.104 / 0.109-0.126 ms |
| the GPU job (submit to fence wait) | 245-260 / 249-263 / 260-282 ms |

- With the fence the tick returns only after the job: the `sync_file` is unsignalled at the push
  and signalled when the tick returns, every round, and a poll of the dma-buf
  (`DMA_BUF_IOCTL_EXPORT_SYNC_FILE` for a read, Lemnos's `export_fence`) shows the job's write
  pending between the push and the tick.
- Without it the tick takes 0.1 ms while the job is still running (its `sync_file` unsignalled
  when the tick returns): the wait is the fence's, not the GPU's.
- The same binary passed the other frame tests on the board, including the release tests
  (descriptor and mapping counts back to baseline). On the x86 desktop (RADV, RX 6800 XT) the
  test passes the same way (154 ms job, 154 ms fenced tick, 0.65 ms without).

## C surface: static, static through the stable ABI, and shared (2026-11-06)

Raze #1 (CM5), release aarch64, 5091e67, pinned to CPU 3, every run inside one
`flock -w 600 /run/lock/raze-gpu.lock` hold, load average 0.13. A three-node graph from
`examples/plugins/stable_abi`, three arms alternated five times: `static` (Rust-ABI install),
`static-stable` (the same `HostGraph`, the plugin installed through `InstallPath::Stable`, as the
surface does) and `shared` (`daedalus-host` over `daedalus-surface`). Counters from
`daedalus-bench-counters`; medians.

| arm | scalar frame | bytes frame | instructions per scalar frame |
| --- | --- | --- | --- |
| static | 4.24 µs | 6.15 µs | 17,176 |
| static-stable | 6.89 µs | 11.04 µs | 25,001 |
| shared | 7.32 µs | 11.46 µs | 26,469 |

- The surface adds 0.43 µs a frame (about 1,470 instructions): 200-280 instructions per push or
  take (lock, port lookup by name, `PortId` clone, panic guard) and 200 on tick. Allocations are
  the engine's own; the wrapper allocates nothing.
- The stable plugin path is the rest: about 3,100 instructions per node per tick, 86-92% of the
  gap. In shared mode, node crates built into the image link into the surface and do not pay it
  (`docs/shared-library.md`); separately built plugins do.
- Fixes queued: ports resolved to indices at compile, a batch push taking the lock once, no
  zero-fill in the wrapper's `take_bytes`, and reuse of the bytes port's spare vector.

## C surface with in-image nodes, and the per-call fixes (2026-11-06)

Raze #1 (CM5), release aarch64, pinned to CPU 3, every run inside one `flock -w 600
/run/lock/raze-gpu.lock` hold; load average 1.49 at the start and 1.69 at the end (higher than the
0.13 of the run above). The same three-node graph from `examples/plugins/stable_abi` and the same
scratch harness, with a fourth arm: `shared-in-image`, `daedalus-host` over an `export_surface!`
image (`examples/surface_image`) whose hosts have the plugin installed in-image, through the Rust
path. Two builds alternated five times, every arm in each: `before` (c82f568 plus `export_surface!`
alone) and `after` (8b7b993: ports by index, the batch push, interned port ids, the bytes port's
payload slot, no zero-fill in `take_bytes`). `after` adds `-batch` arms: ports resolved once, one
`push_batch` per frame, takes by index. Medians; instructions per frame from
`daedalus-bench-counters`.

| arm | scalar frame before / after | bytes frame before / after | instr. per scalar frame |
| --- | --- | --- | --- |
| static | 4.28 / 4.18 µs | 6.28 / 6.19 µs | 17,176 / 17,176 |
| static-stable | 6.93 / 6.70 µs | 11.35 / 10.85 µs | 25,001 / 25,001 |
| shared | 7.13 / 7.09 µs | 11.52 / 11.24 µs | 26,467 / 26,355 |
| shared-in-image | 4.67 / 4.74 µs | 6.76 / 6.67 µs | 18,642 / 18,530 |
| shared, batch | - / 7.13 µs | - / 11.43 µs | - / 25,995 |
| shared-in-image, batch | - / 4.61 µs | - / 6.48 µs | - / 18,170 |

- In-image nodes remove the stable-ABI cost: `shared-in-image` runs a scalar frame in 4.6-4.7 µs
  against 7.1 µs for `shared`, 0.4-0.5 µs over `static`. What is left over static is the surface
  itself: 1,466 instructions per scalar frame before, 1,354 after by name, 994 with the batch push.
- The per-call fixes are small. By name, the wrapper now resolves the index and the surface skips
  the UTF-8 check, the name match and the reference count: 112 instructions per scalar frame (six
  calls), within timing noise. One batch instead of two or three pushes saves the lock and guard
  of the others: 472 instructions per scalar frame; in time, about 0.1 µs in-image and none
  measurable for `shared`, where the stable path dominates. Bytes frames save 254 instructions by
  name and 761 with the batch (`shared`), 339 and 846 in-image.
- Allocations per warm frame: scalar frames 0 everywhere. Bytes frames: the surface now allocates
  1 (the engine's, in the tick) instead of 2: the bytes port reuses its payload allocation as well
  as its vector (the vector was already reused). The static graph allocates 2 (it boxes each pushed
  `SharedBytes`). The wrapper allocates nothing.
- `static` and `static-stable` run the same code in both builds (identical instructions); their
  0.1-0.5 µs differences between builds are run-to-run noise under this load.
