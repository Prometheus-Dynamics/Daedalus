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

