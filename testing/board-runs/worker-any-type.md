# Board run: values by reference of any type

Status: run on CM5 2026-11-14.

## Check

`process-timings sweep daedalus-sweep-worker 101` hands a list to a Rust worker and takes back a
checksum, so only the list crosses. It sweeps sizes from 512 bytes to 4 MiB, doubling, for two
shapes: a `list<f32>`, and a list of 28-byte records (five `f32` and two `i32`, a keypoint's
shape). Each size runs three ways, 101 timed runs each after a warm-up:

- **view:** the list goes as a place (`shared_from(0)`). The node's `&[f32]`, or `&[Point]` of a
  `Flat` record, reads the items where the host put them.
- **decode:** the list goes as a place, and the node decodes it into a value first, as an SDK with
  no view does.
- **socket:** the list goes in the run's packet (`shared_from(usize::MAX)`), which is what a value
  below the threshold does.

Three rounds. Each figure is the median of the three rounds' medians.

## Result

Run on the CM5 2 GB (raze1): aarch64, 4 cores, kernel 7.2.9, dev OS image. Binaries were
cross-built in release on NVMe against glibc 2.36. CPU only, run from one directory under /tmp
and removed after. No GPU was used, so no GPU lock was taken.

`list<f32>`, µs per run, median (p99):

| Bytes | view | decode | socket |
|---|---|---|---|
| 516 | 13.5 (32.8) | 19.5 (20.7) | 19.2 (24.0) |
| 1028 | 16.4 (20.3) | 21.9 (22.6) | 21.5 (36.2) |
| 2052 | 16.6 (18.2) | 23.9 (47.8) | 25.9 (50.6) |
| 4100 | 16.8 (23.2) | 37.2 (38.0) | 33.0 (48.3) |
| 8196 | 16.6 (17.7) | 57.5 (72.9) | 50.3 (83.0) |
| 16388 | 17.8 (18.6) | 97.7 (116.4) | 86.2 (101.6) |
| 32772 | 18.9 (19.7) | 174.5 (220.5) | 160.4 (212.5) |
| 65540 | 17.8 (20.5) | 338.0 (424.6) | 317.8 (470.6) |
| 131076 | 21.7 (27.5) | 664.1 (1002.6) | 645.7 (840.0) |
| 262148 | 40.2 (94.0) | 1417.8 (2459.9) | 1541.4 (2451.1) |
| 524292 | 54.5 (114.2) | 3111.9 (4966.7) | 3537.6 (5792.7) |
| 1048580 | 114.3 (143.9) | 6232.8 (8878.4) | 7507.1 (10556.9) |
| 2097156 | 349.9 (754.2) | 12712.7 (15859.5) | 14938.9 (18132.9) |
| 4194308 | 1122.3 (2876.3) | 33875.4 (37513.6) | 37852.8 (40993.3) |

`list of 28-byte records`, µs per run, median (p99):

| Bytes | view | decode | socket |
|---|---|---|---|
| 508 | 18.5 (31.3) | 21.2 (41.9) | 17.2 (32.7) |
| 1012 | 17.4 (25.8) | 24.8 (29.2) | 20.4 (26.4) |
| 2048 | 17.5 (18.1) | 29.9 (38.3) | 26.9 (28.1) |
| 4092 | 18.0 (18.9) | 48.4 (50.0) | 40.1 (52.4) |
| 8180 | 15.6 (19.6) | 80.9 (89.7) | 66.5 (113.7) |
| 16384 | 17.4 (35.8) | 142.5 (236.7) | 130.7 (196.0) |
| 32764 | 24.0 (27.2) | 271.7 (494.8) | 260.1 (547.1) |
| 65524 | 26.8 (52.6) | 551.8 (748.7) | 536.7 (829.3) |
| 131072 | 44.5 (72.9) | 1124.1 (2143.2) | 1115.6 (1841.9) |
| 262140 | 74.0 (138.6) | 3464.6 (6098.2) | 3577.6 (6334.6) |
| 524276 | 125.8 (288.9) | 8564.1 (11870.5) | 8977.6 (12167.9) |
| 1048576 | 281.9 (725.1) | 18806.6 (21356.1) | 19745.4 (21973.7) |
| 2097148 | 1155.8 (1924.8) | 37692.7 (40924.8) | 39457.2 (43298.9) |
| 4194292 | 2923.3 (5509.7) | 56396.9 (59991.9) | 79380.9 (83723.7) |

- **view against the socket:** view beats the socket from about 1 KiB for both shapes, and in
  every round from 2 KiB. At 4 KiB it takes half the socket's time. At 64 KiB it is 18× faster for
  floats and 20× for records. At 1 MiB it is 66× and 70×.
- **decode against the socket:** decode is 5 to 20% slower than the socket from 4 KiB to 128 KiB,
  and faster from 256 KiB. Decoding into a value costs far more than either transport, so a place
  helps a node only when the node reads the items where they are.
- **placed counts:** at 1 MiB and 4 MiB, one run in 230 per size went in the packet. That was the
  first run at a new size, while the host lent a larger pool, as designed.

## The default

The default comes from the view crossover, about 1 KiB, with a margin: 4 KiB, where view takes
half the socket's time for both shapes in every round. It applies to a worker that agreed values
of any type by reference (`ANY_TYPE_THRESHOLD`). Any other worker keeps 64 KiB (`THRESHOLD`),
which this run didn't measure. `WorkerSpec::shared_from` sets either. An SDK that agrees
`ANY_TYPE` gives its nodes views, so the decode row is a gap to close in an SDK without views, not
a reason to keep values on the socket.
