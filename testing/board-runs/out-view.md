# Board run: output views into a worker's lent memory

Status: run on CM5 2026-11-14.

## Check

`process-timings fanout daedalus-sweep-worker 301` runs one CPU producer whose list goes to a CPU
reader, to a worker, or to both, for two shapes: a `list<f32>`, and a list of 28-byte records (five
`f32` and two `i32`, a keypoint's shape). Each shape runs with two producers:

- **vec:** the node returns a `Vec<T>`.
- **view:** the node writes through an `OutView<'_, T>`: `items_mut` for the numbers, and
  `reserve_bytes` then `extend` for the records. With a worker reading, the planner places the
  output in `process.lent`, and the producer writes into the worker's value buffer.

Four graphs per shape and size: `producer` alone, `host` (producer and CPU reader), `worker`
(producer and the worker's reader), and `fan` (producer, CPU reader and worker). Three rounds;
each figure is the median of the three rounds' medians.

The f32 graphs at 1 MiB were also run under `counters`, a static helper (the `daedalus-counters`
crate in `testing/crates/counters`) that counts
user-space cycles, instructions, cache misses, and last-level-cache loads and misses through
`perf_event_open` over the program and its worker. The image has no `perf`, and
`perf_event_paranoid` is 2, which allows counting one's own processes in user space. Each graph was
run for 201 and for 1201 ticks. The difference over the extra 1000 ticks gives the counts per tick,
with no setup in them. `DAEDALUS_FAN_WORKER_FIRST=1` puts the worker's reader before the CPU reader,
to see whether order matters. The script is `scripts/board/out-view.sh`.

Run on the CM5 2 GB (raze1): aarch64, 4 cores, 2 MiB L3, kernel 7.2.9, dev OS image. Binaries were
cross-built in release on NVMe against glibc 2.36. CPU only, run from one directory under /tmp and
removed after. No GPU was used, so no GPU lock was taken.

## Result

Allocations per tick, at every size:

| Graph | vec | view |
|---|---|---|
| producer alone | 2 | 0 |
| producer, CPU reader | 1 | 0 |
| producer, worker | 1 | 0 |
| fan, `list<f32>` | 1 | 0 |
| fan, records | 2 | 1 |

The one allocation left in the records' fan is the CPU reader's decode of the wire form, which
`explain_plan` lists.

`list<f32>`, µs per tick, median:

| Bytes | Graph | vec | view |
|---|---|---|---|
| 4096 | producer | 0.5 | 0.5 |
| 4096 | host | 0.9 | 0.9 |
| 4096 | worker | 18.6 | 18.3 |
| 4096 | fan | 19.1 | 16.9 |
| 65536 | producer | 3.8 | 4.0 |
| 65536 | host | 5.7 | 5.6 |
| 65536 | worker | 28.0 | 20.4 |
| 65536 | fan | 30.9 | 26.0 |
| 1048576 | producer | 70.9 | 56.4 |
| 1048576 | host | 95.3 | 86.8 |
| 1048576 | worker | 309.0 | 109.9 |
| 1048576 | fan | 425.0 | 169.0 |

Records, µs per tick, median:

| Bytes | Graph | vec | view |
|---|---|---|---|
| 4096 | producer | 0.6 | 0.6 |
| 4096 | host | 0.9 | 0.9 |
| 4096 | worker | 46.2 | 45.5 |
| 4096 | fan | 47.1 | 46.6 |
| 65536 | producer | 4.6 | 4.4 |
| 65536 | host | 6.4 | 6.4 |
| 65536 | worker | 48.5 | 37.1 |
| 65536 | fan | 62.6 | 51.9 |
| 1048576 | producer | 63.8 | 68.5 |
| 1048576 | host | 102.6 | 94.9 |
| 1048576 | worker | 603.1 | 292.4 |
| 1048576 | fan | 944.8 | 600.6 |

The cost of handing 1 MiB on, with the producer's own time taken out, in ms/MB:

| Path | vec | view |
|---|---|---|
| to a CPU reader | 0.024 | 0.030 |
| to a worker | 0.238 | 0.054 |
| to a CPU reader and a worker | 0.354 | 0.113 |

A CPU reader alone costs the same either way: the list goes to it by reference with no copy in both
cases.

## Why a CPU reader is slower beside a worker

The f32 CPU reader's share of each tick at 1 MiB: `host - producer` alone, and `fan - worker` beside
a worker. Counts per tick:

| Reader | cycles | instructions | cache misses | LLC loads | LLC misses |
|---|---|---|---|---|---|
| alone, vec | 67,688 | 278,458 | 9,893 | 31,150 | 3,590 |
| beside, vec | 589,676 | 278,388 | 17,837 | 34,124 | 24,274 |
| beside, vec, worker first | 670,726 | 278,387 | 17,091 | 33,714 | 24,477 |
| alone, view | about 0 | 278,602 | 8,848 | 25,044 | 277 |
| beside, view | 373,259 | 278,506 | 7,832 | 34,434 | 10,090 |
| beside, view, worker first | 370,786 | 278,506 | 9,204 | 34,435 | 10,097 |

The reader runs the same instructions in every case. What changes is where its list is when it
reads it:

- **Alone,** the list the producer just wrote is still in the caches.
- **Beside a worker, with vec,** about every one of the 16,384 lines of the 1 MiB list misses the
  last-level cache. The producer writes the list, the step writes its wire form, and that is copied
  into the worker's memory: 3 MiB written per tick through a 2 MiB L3. The list is evicted before
  the reader gets to it.
- **With the reader after the worker,** nothing changes. Order is not the cause; the volume is.
- **With view,** the producer writes once, into the worker's lent memory, and nothing is copied.
  The reader's LLC misses drop from 24,274 to 10,090 and its cycles by 37%. What is left is the
  worker reading the same lines from another core.

A first run of the same check found the view producer slower than vec when only CPU nodes read its
list (133.7 µs against 56.4 at 1 MiB). It filled each list with zeros before the node wrote it, and
alternated between two lists because the output's cell still held the last one. The step now
releases the cell before the node runs, and `items_mut` leaves the numbers to the node. Writing
records one `push` at a time also cost more than the list; `extend` now writes them into the room in
one pass. The tables above are from the second run.
