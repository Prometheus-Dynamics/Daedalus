# Board run: worker values by reference (#268)

Status: run on CM5 2026-11-14.

## Check

The same program and method as [worker-shm.md](worker-shm.md): `process-timings WORKER 101
MEGABYTES` hands a payload of 1 or 4 MiB to the worker fixture's `fixture.raw_io` node and back,
one run per operation, 101 runs after a warm-up of 8. The node's own work, reversing the bytes, is
timed separately on the same board, and the transport is the hand-over less it. Three ways,
interleaved, three rounds each, all from #268's binaries:

- **by reference:** as #268 ships. The input goes as a place in the host's buffer, the worker
  reverses it straight into a value buffer, and the output cell holds it there.
- **shared:** `DAEDALUS_FIXTURE_NO_REFERENCE=1`: whole packets in lent memory, #265's way.
- **plain:** `DAEDALUS_FIXTURE_PLAIN=1`: one socket, every value in its packet.

Then `tests/by_reference.rs`, cross-built and run against the cross-built fixture
(`DAEDALUS_WORKER_FIXTURE`), with buffers from the board's system dma-heap as well as a memfd.

## Result

Run on the CM5 2 GB (raze1): aarch64, 4 cores, kernel 7.2.9, dev OS image. Binaries were
cross-built in release on NVMe against glibc 2.36. CPU only, run from one directory under /tmp
and removed after. No GPU was used, so no GPU lock was taken. Each figure is the median of the
three rounds.

| Size | Way | Hand-over, median | Hand-over, p99 | Node's own work | Transport |
|---|---|---|---|---|---|
| 1 MiB | by reference | 0.22 ms/MB | 0.56 ms/MB | 0.05 ms/MB | 0.17 ms/MB |
| 1 MiB | shared | 2.13 ms/MB | 4.01 ms/MB | 0.05 ms/MB | 2.08 ms/MB |
| 1 MiB | plain | 3.39 ms/MB | 5.45 ms/MB | 0.05 ms/MB | 3.34 ms/MB |
| 4 MiB | by reference | 0.38 ms/MB | 0.69 ms/MB | 0.28 ms/MB | 0.08 ms/MB |
| 4 MiB | shared | 2.63 ms/MB | 3.84 ms/MB | 0.28 ms/MB | 2.35 ms/MB |
| 4 MiB | plain | 3.56 ms/MB | 4.82 ms/MB | 0.28 ms/MB | 3.27 ms/MB |

Allocations in steady state: 0 per 8 runs on the host's thread, every way and size. Descriptor
sends: 0 (these are `bytes` values, not dma-bufs).

**1 MiB:** by reference cuts the transport by 92% against whole packets in shared memory, and by
95% against plain.

**4 MiB:** the hand-over is the node's own work plus 0.01 to 0.16 ms/MB across the rounds. That
is within the noise of timing the work separately. What is left is the worker writing its output
once, which is the node's work itself.

Against [#265's run](worker-shm.md), the hand-over is 2.06 → 0.22 ms/MB at 1 MiB, and 2.69 →
0.38 at 4 MiB.

All seven tests in `tests/by_reference.rs` pass on the board. On the system heap, which is
cached on arm64 so `DMA_BUF_IOCTL_SYNC` does cache maintenance there, they confirm:
- A dma-buf reaches the node mapped `r--s` with no copy.
- Each read is bracketed by the sync calls.
- Every START has its END after a failing node too.
- One descriptor is sent per buffer, and eviction stays bounded.
- A plain worker gets a copy.

Raw lines: one per run of the program, as printed.

```text
hand_over transport by_reference megabytes 1 median_ns 259129 p99_ns 755704 min_ns 220074 max_ns 940000 ns_per_megabyte 259129 p99_ns_per_megabyte 755704 node_work_ns_per_megabyte 46278 transport_ns_per_megabyte 212851 runs 101 allocations_per_8_runs 0 placed_in 117 placed_out 117 fd_sends 0
hand_over transport shared megabytes 1 median_ns 2126704 p99_ns 4247556 min_ns 1894666 max_ns 4576982 ns_per_megabyte 2126704 p99_ns_per_megabyte 4247556 node_work_ns_per_megabyte 48315 transport_ns_per_megabyte 2078389 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport plain megabytes 1 median_ns 3387593 p99_ns 5334426 min_ns 3044296 max_ns 6246055 ns_per_megabyte 3387593 p99_ns_per_megabyte 5334426 node_work_ns_per_megabyte 47370 transport_ns_per_megabyte 3340223 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport by_reference megabytes 1 median_ns 185667 p99_ns 334055 min_ns 174778 max_ns 337260 ns_per_megabyte 185667 p99_ns_per_megabyte 334055 node_work_ns_per_megabyte 47611 transport_ns_per_megabyte 138056 runs 101 allocations_per_8_runs 0 placed_in 117 placed_out 117 fd_sends 0
hand_over transport shared megabytes 1 median_ns 2090907 p99_ns 4011685 min_ns 1897241 max_ns 4715667 ns_per_megabyte 2090907 p99_ns_per_megabyte 4011685 node_work_ns_per_megabyte 46370 transport_ns_per_megabyte 2044537 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport plain megabytes 1 median_ns 3445704 p99_ns 5811815 min_ns 2932778 max_ns 6583814 ns_per_megabyte 3445704 p99_ns_per_megabyte 5811815 node_work_ns_per_megabyte 46278 transport_ns_per_megabyte 3399426 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport by_reference megabytes 1 median_ns 220926 p99_ns 561759 min_ns 198556 max_ns 639167 ns_per_megabyte 220926 p99_ns_per_megabyte 561759 node_work_ns_per_megabyte 46278 transport_ns_per_megabyte 174648 runs 101 allocations_per_8_runs 0 placed_in 117 placed_out 117 fd_sends 0
hand_over transport shared megabytes 1 median_ns 2181259 p99_ns 3381426 min_ns 1951056 max_ns 5187648 ns_per_megabyte 2181259 p99_ns_per_megabyte 3381426 node_work_ns_per_megabyte 46259 transport_ns_per_megabyte 2135000 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport plain megabytes 1 median_ns 3347166 p99_ns 5447649 min_ns 2926963 max_ns 5781629 ns_per_megabyte 3347166 p99_ns_per_megabyte 5447649 node_work_ns_per_megabyte 46278 transport_ns_per_megabyte 3300888 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport by_reference megabytes 4 median_ns 1527018 p99_ns 2767149 min_ns 1160982 max_ns 3775519 ns_per_megabyte 381754 p99_ns_per_megabyte 691787 node_work_ns_per_megabyte 301606 transport_ns_per_megabyte 80148 runs 101 allocations_per_8_runs 0 placed_in 116 placed_out 116 fd_sends 0
hand_over transport shared megabytes 4 median_ns 10328889 p99_ns 15359796 min_ns 9056723 max_ns 15606389 ns_per_megabyte 2582222 p99_ns_per_megabyte 3839949 node_work_ns_per_megabyte 272634 transport_ns_per_megabyte 2309588 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport plain megabytes 4 median_ns 14184278 p99_ns 19282111 min_ns 11806111 max_ns 19737130 ns_per_megabyte 3546070 p99_ns_per_megabyte 4820528 node_work_ns_per_megabyte 279930 transport_ns_per_megabyte 3266139 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport by_reference megabytes 4 median_ns 1709426 p99_ns 3567130 min_ns 1326537 max_ns 3635352 ns_per_megabyte 427356 p99_ns_per_megabyte 891782 node_work_ns_per_megabyte 264430 transport_ns_per_megabyte 162926 runs 101 allocations_per_8_runs 0 placed_in 116 placed_out 116 fd_sends 0
hand_over transport shared megabytes 4 median_ns 10512297 p99_ns 14509500 min_ns 9062667 max_ns 15019593 ns_per_megabyte 2628074 p99_ns_per_megabyte 3627375 node_work_ns_per_megabyte 265616 transport_ns_per_megabyte 2362458 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport plain megabytes 4 median_ns 14439759 p99_ns 18948796 min_ns 12023926 max_ns 19262945 ns_per_megabyte 3609940 p99_ns_per_megabyte 4737199 node_work_ns_per_megabyte 287996 transport_ns_per_megabyte 3321944 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport by_reference megabytes 4 median_ns 1414407 p99_ns 2173759 min_ns 1241407 max_ns 2376778 ns_per_megabyte 353602 p99_ns_per_megabyte 543440 node_work_ns_per_megabyte 341820 transport_ns_per_megabyte 11782 runs 101 allocations_per_8_runs 0 placed_in 116 placed_out 116 fd_sends 0
hand_over transport shared megabytes 4 median_ns 10600278 p99_ns 15509963 min_ns 9117834 max_ns 17454815 ns_per_megabyte 2650070 p99_ns_per_megabyte 3877491 node_work_ns_per_megabyte 297532 transport_ns_per_megabyte 2352537 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
hand_over transport plain megabytes 4 median_ns 14224519 p99_ns 20029463 min_ns 11801296 max_ns 22203648 ns_per_megabyte 3556130 p99_ns_per_megabyte 5007366 node_work_ns_per_megabyte 294287 transport_ns_per_megabyte 3261843 runs 101 allocations_per_8_runs 0 placed_in 0 placed_out 0 fd_sends 0
```
