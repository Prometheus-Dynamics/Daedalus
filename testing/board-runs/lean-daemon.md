# Board run: the daemon with and without the planner

Status: run 2026-11-17 on a CM5 (4 GB) at `next` 4f638036 with `scripts/board/run.sh cm5-measure
--go`. The judge passed. Both daemons were measured in the same pass.

## What it measures

The daemon built with the planner, and the lean daemon (`daedalusd --no-default-features`), which
has no planner and runs a made plan (`docs/weak-hardware.md`, "Plan somewhere else"). The full
daemon is given a graph and plans it. The lean daemon is given the made plan the full daemon's
`make-plan` wrote for the same graph at bundle time, with `put-plan`. Then both do the same start,
feed, step and take, 200 round trips, and hold 32 connections. Resident memory is read from
`/proc/<pid>/status` at each point.

## The board

- The dev OS image 2026.2.0-dev.993+e1097ec7, kernel 7.2.9-v8-16k (16 KiB pages), aarch64,
  governor `performance`, 4.1 GB of memory, 3.86 GB available.
- CPU only. No GPU lock was taken. The run wrote under one directory in `/tmp` and removed it.
- A rehearsal on the development machine (`run.sh local-measure`) passed at the same revision
  first.

## Result

Static musl release builds for aarch64.

| | With the planner | Lean |
|---|---|---|
| Binary | 2,454,744 bytes | 1,317,640 bytes |
| Idle: resident | 3,148 kB | 2,952 kB |
| Graph planned, or plan loaded: resident | 3,976 kB | 3,032 kB |
| After the first step: resident | 4,048 kB | 3,072 kB |
| After 200 round trips: resident (peak) | 4,180 kB | 3,184 kB |
| 32 connections: resident | 5,504 kB | 4,532 kB |
| Idle: address space | 206,308 kB | 203,084 kB |
| 32 connections: address space | 2,112,156 kB | 2,111,044 kB |
| Threads, 0 and 32 connections | 2 and 66 | 2 and 66 |

- The lean daemon's peak is 996 kB lower, 24 % of the full daemon's.
- Against a 64 MB budget for a whole stack on a small board: the full daemon's peak is 6.4 % of
  it and the lean daemon's 4.9 %. With 32 connections held they are 8.4 % and 6.9 %.
- The address space with 32 connections is reserved stacks and glibc's arenas, not memory in use.
  This run did not set `MALLOC_ARENA_MAX`; the packaged service does.
- The time from start to the first answer reads 10 ms for both: the image's clock source for this
  figure has 10 ms resolution, so it is not a measurement of the difference.

## Not measured

- The lean daemon on a board smaller than a CM5.
- The same pass with `MALLOC_ARENA_MAX=2`.
