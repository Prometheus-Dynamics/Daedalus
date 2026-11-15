# Board run: a CPU node's value to a worker, and the threshold without any-type

Status: run on CM5 2026-11-14.

## Check

Three modes of `process-timings`, from one build:

- `bytes daedalus-worker-fixture 101`: a `bytes` payload handed to `fixture.raw_io`, which
  reverses it into its output, from 512 bytes to 4 MiB, doubling. Three ways: **by reference**,
  a worker that agreed values by reference for bytes alone (`BY_REFERENCE` without `ANY_TYPE`);
  **whole packet**, a worker that agreed shared memory for whole packets alone; and **socket**,
  a worker that agreed nothing. Both shared ways place from 0 bytes (`shared_from(0)`), so each
  size shows what the way costs there. Three rounds; each figure is the median of the rounds.
- `fanout daedalus-sweep-worker 101`: one CPU producer of a list, read by a CPU node, by a worker,
  or both, timed by the tick. The CPU reader takes `&[f32]`, viewed where its bytes are, or
  `&[Point]` of 28-byte records, which it decodes from the wire form. The CPU reader's cost is
  `producer + CPU reader - producer` with no worker, and `fan-out - producer + worker` beside one.
  Two rounds.
- `sweep daedalus-sweep-worker 101`: the any-type sweep of
  [worker-any-type.md](worker-any-type.md), again on this build, for the main path's cost per MiB.

Page faults are this process's minor faults (`/proc/self/stat`), memory touched for the first
time; allocations are this thread's, once warm.

## Result

Run on the CM5 2 GB (raze1): aarch64, 4 cores, kernel 7.2.9, dev OS image. Binaries were
cross-built in release on NVMe against glibc 2.36. CPU only, from one directory under /tmp,
removed after. No service was stopped, nothing was loaded, nothing written outside it.

### Bytes and whole packets, µs per run

| Size | by reference | whole packet | socket | by reference per MiB | whole packet per MiB | socket per MiB | faults per 1000 runs (ref / packet / socket) |
|---|---|---|---|---|---|---|---|
| 512 B | 10.9 | 11.2 | 10.9 | 22.34 ms | 23.02 ms | 22.26 ms | 26 / 17 / 9 |
| 1 KiB | 10.9 | 11.3 | 15.4 | 11.11 ms | 11.59 ms | 15.74 ms | 0 / 0 / 9 |
| 2 KiB | 11.0 | 11.8 | 12.1 | 5.63 ms | 6.04 ms | 6.19 ms | 9 / 9 / 17 |
| 4 KiB | 11.2 | 12.5 | 13.6 | 2.86 ms | 3.19 ms | 3.48 ms | 9 / 35 / 35 |
| 8 KiB | 11.5 | 13.8 | 15.6 | 1.47 ms | 1.77 ms | 1.99 ms | 9 / 52 / 43 |
| 16 KiB | 12.1 | 16.6 | 19.8 | 0.78 ms | 1.06 ms | 1.27 ms | 17 / 61 / 96 |
| 32 KiB | 13.4 | 21.7 | 30.6 | 0.43 ms | 0.70 ms | 0.98 ms | 35 / 191 / 252 |
| 64 KiB | 16.4 | 50.7 | 102.1 | 0.26 ms | 0.81 ms | 1.63 ms | 70 / 235 / 496 |
| 128 KiB | 23.0 | 123.0 | 198.1 | 0.18 ms | 0.98 ms | 1.59 ms | 139 / 1026 / 1296 |
| 256 KiB | 39.6 | 270.2 | 527.6 | 0.16 ms | 1.08 ms | 2.11 ms | 278 / 1426 / 1409 |
| 512 KiB | 83.1 | 788.1 | 1421.3 | 0.17 ms | 1.58 ms | 2.84 ms | 557 / 2835 / 3913 |
| 1 MiB | 228.0 | 2197.8 | 3462.9 | 0.23 ms | 2.20 ms | 3.46 ms | 1104 / 6861 / 6687 |
| 2 MiB | 564.6 | 4990.3 | 7081.0 | 0.28 ms | 2.50 ms | 3.54 ms | 22339 / 13678 / 13365 |
| 4 MiB | 1396.4 | 10546.4 | 14429.0 | 0.35 ms | 2.64 ms | 3.61 ms | 4452 / 27313 / 26722 |

### A CPU producer, a CPU reader and a worker, µs per tick

| Shape | Size | producer | producer + CPU reader (`cpu.host`) | producer + worker | fan-out | CPU reader alone | CPU reader beside the worker | decodes listed | faults per 1000 ticks (worker / fan-out) | allocations per tick (host / worker / fan-out) |
|---|---|---|---|---|---|---|---|---|---|---|
| f32 | 4 KiB | 0.5 | 0.9 | 18.1 | 19.3 | 0.3 | 1.2 | 0 | 0 / 0 | 1 / 1 / 1 |
| f32 | 64 KiB | 3.8 | 5.7 | 28.5 | 31.6 | 1.9 | 3.2 | 0 | 0 / 0 | 1 / 1 / 1 |
| f32 | 1 MiB | 59.3 | 96.0 | 303.5 | 423.2 | 36.7 | 119.7 | 0 | 0 / 0 | 1 / 1 / 1 |
| point28 | 4 KiB | 0.6 | 0.9 | 46.0 | 52.6 | 0.4 | 6.7 | 1 | 0 / 0 | 1 / 1 / 2 |
| point28 | 64 KiB | 4.4 | 6.5 | 49.0 | 63.0 | 2.1 | 13.9 | 1 | 0 / 0 | 1 / 1 / 2 |
| point28 | 1 MiB | 63.5 | 110.4 | 617.3 | 958.6 | 46.9 | 341.3 | 1 | 0 / 0 | 1 / 1 / 2 |

### The main path: any type by reference, per MiB

| Shape | Size | view | view per MiB | decode per MiB | socket per MiB |
|---|---|---|---|---|---|
| f32 | 64 KiB | 18.3 | 0.29 ms | 5.25 ms | 5.16 ms |
| f32 | 128 KiB | 22.2 | 0.18 ms | 5.20 ms | 5.29 ms |
| f32 | 256 KiB | 32.7 | 0.13 ms | 5.59 ms | 6.39 ms |
| f32 | 512 KiB | 55.9 | 0.11 ms | 6.06 ms | 7.09 ms |
| f32 | 1 MiB | 115.3 | 0.12 ms | 6.15 ms | 7.35 ms |
| f32 | 2 MiB | 364.8 | 0.18 ms | 6.22 ms | 7.40 ms |
| f32 | 4 MiB | 1061.2 | 0.27 ms | 8.32 ms | 9.63 ms |
| point28 | 64 KiB | 26.7 | 0.43 ms | 9.09 ms | 8.69 ms |
| point28 | 128 KiB | 39.5 | 0.32 ms | 9.02 ms | 9.17 ms |
| point28 | 256 KiB | 66.4 | 0.27 ms | 13.75 ms | 14.46 ms |
| point28 | 512 KiB | 127.6 | 0.26 ms | 17.38 ms | 18.02 ms |
| point28 | 1 MiB | 290.1 | 0.29 ms | 18.88 ms | 19.83 ms |
| point28 | 2 MiB | 1119.7 | 0.56 ms | 18.54 ms | 19.89 ms |
| point28 | 4 MiB | 2912.0 | 0.73 ms | 14.21 ms | 19.81 ms |

## What it decided

- **The threshold without any-type is 2 KiB** (`daedalus_process_shm::THRESHOLD`, was 64 KiB).
  It is the smallest size from which both shared ways were no slower than the socket at every
  larger size in all three rounds: by reference from 512 bytes in each, whole packets from 512
  bytes in two rounds and 2 KiB in one. Above it both are faster: at 64 KiB a whole packet takes
  half the socket's time and a value by reference a sixth.
- **What each path costs per MiB**, at 1 MiB: any type by reference, read in place, 0.12 ms
  (`list<f32>`) and 0.29 ms (28-byte records); `bytes` by reference 0.23 ms; a whole packet in
  shared memory 2.2 ms; the socket 3.5 ms for `bytes`, 7.4 ms for a typed list, where the
  worker's decode is most of it.
- **A CPU output a worker reads costs no allocation and no page fault a tick** once warm: the
  wire form is written over the buffer it went in last time, once no reader holds it. Before,
  each tick made a buffer and a copy of it, and the producer-and-worker graph took 1.44 ms at
  1 MiB with 248 thousand page faults on a desktop; on the same desktop it now takes 0.15 ms
  with 2 thousand. `backends/process/tests/wire_steady.rs` holds that steady state.
- **The CPU reader beside a worker.** A `&[f32]` reads the wire form where it is: 1.2 µs at
  4 KiB and 3.2 µs at 64 KiB, against 0.3 and 1.9 µs in `cpu.host`. At 1 MiB it is 120 µs
  against 37 µs. No decode runs there; the likely difference is the cache, since the list has
  just been copied into the worker's buffer before the CPU reader reads it. A list of records is decoded, and
  `explain_plan` lists it: 6.7, 13.9 and 341 µs, against 0.4, 2.1 and 47 µs.
