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
