# Board run: worker values in lent shared memory (#265)

Status: run on CM5 2026-11-14.

## Check

`process-timings WORKER 101 MEGABYTES` hands a payload of 1 or 4 MiB to the worker fixture's
`fixture.raw_io` node and back, one run per operation, 101 runs after a warm-up of 8. The node
reverses the bytes; that work is timed separately in the same program on the same board, so the
transport's share is the hand-over less it. Three ways, interleaved, three rounds each:

- **before:** `next` at 7cfbd119, the commit before #265: pipes, every value in its packet.
- **plain:** #265 with `DAEDALUS_FIXTURE_PLAIN=1`: one socket, the worker doesn't ask for memory.
- **shared:** #265 as it ships: values of 64 KiB or more in lent memfd buffers.

## Result

Run on 2026-11-14 on the CM5 2 GB (raze1), aarch64, 4 cores, the dev OS image
2026.2.0-dev.993, kernel 7.2.9. Binaries cross-built in release on NVMe (`aarch64-unknown-linux-gnu`
against glibc 2.36), with the p99 field added to `process-timings` for both builds. CPU only, run
from one directory under /tmp, removed after. Each figure is the median of the three rounds.

| Size | Way | Hand-over, median | Hand-over, p99 | Node's own work | Transport |
|---|---|---|---|---|---|
| 1 MiB | before | 3.91 ms/MB | 6.32 ms/MB | 0.05 ms/MB | 3.86 ms/MB |
| 1 MiB | plain | 3.37 ms/MB | 5.19 ms/MB | 0.05 ms/MB | 3.32 ms/MB |
| 1 MiB | shared | 2.06 ms/MB | 3.76 ms/MB | 0.05 ms/MB | 2.01 ms/MB |
| 4 MiB | before | 4.10 ms/MB | 5.07 ms/MB | 0.28 ms/MB | 3.82 ms/MB |
| 4 MiB | plain | 3.45 ms/MB | 4.98 ms/MB | 0.28 ms/MB | 3.14 ms/MB |
| 4 MiB | shared | 2.69 ms/MB | 3.62 ms/MB | 0.28 ms/MB | 2.41 ms/MB |

Allocations in steady state: 0 per 8 runs on the host's thread, every way and size.

So the transport is 48% less at 1 MiB and 37% less at 4 MiB than before. Part of that is the
socket alone (plain is 14% and 18% less than before): the rest is the bytes not going through
it. What is left is copies: the host encodes a run into its buffer and then into the lent one,
and the worker's SDK decodes the bytes into a `Vec`. Those go with values by reference (the next
PR), which is measured the same way.

Raw lines: one per run of the program, as printed.

```text
hand_over transport before megabytes 1 median_ns 3910018 p99_ns 6122203 min_ns 3468463 max_ns 6773537 ns_per_megabyte 3910018 p99_ns_per_megabyte 6122203 node_work_ns_per_megabyte 47519 transport_ns_per_megabyte 3862499 allocations_per_8_runs 0
hand_over transport plain megabytes 1 median_ns 3307741 p99_ns 5173352 min_ns 3017963 max_ns 5797593 ns_per_megabyte 3307741 p99_ns_per_megabyte 5173352 node_work_ns_per_megabyte 47037 transport_ns_per_megabyte 3260704 allocations_per_8_runs 0
hand_over transport shared megabytes 1 median_ns 2057611 p99_ns 3761648 min_ns 1898092 max_ns 4444259 ns_per_megabyte 2057611 p99_ns_per_megabyte 3761648 node_work_ns_per_megabyte 46963 transport_ns_per_megabyte 2010648 allocations_per_8_runs 0
hand_over transport before megabytes 1 median_ns 3787352 p99_ns 6732667 min_ns 3431259 max_ns 6821722 ns_per_megabyte 3787352 p99_ns_per_megabyte 6732667 node_work_ns_per_megabyte 47055 transport_ns_per_megabyte 3740297 allocations_per_8_runs 0
hand_over transport plain megabytes 1 median_ns 3372093 p99_ns 5188166 min_ns 2937704 max_ns 5445129 ns_per_megabyte 3372093 p99_ns_per_megabyte 5188166 node_work_ns_per_megabyte 47630 transport_ns_per_megabyte 3324463 allocations_per_8_runs 0
hand_over transport shared megabytes 1 median_ns 2020074 p99_ns 4039000 min_ns 1860797 max_ns 4349519 ns_per_megabyte 2020074 p99_ns_per_megabyte 4039000 node_work_ns_per_megabyte 47037 transport_ns_per_megabyte 1973037 allocations_per_8_runs 0
hand_over transport before megabytes 1 median_ns 4004722 p99_ns 6316555 min_ns 3507926 max_ns 9073908 ns_per_megabyte 4004722 p99_ns_per_megabyte 6316555 node_work_ns_per_megabyte 47185 transport_ns_per_megabyte 3957537 allocations_per_8_runs 0
hand_over transport plain megabytes 1 median_ns 3394482 p99_ns 5248852 min_ns 2955723 max_ns 6059203 ns_per_megabyte 3394482 p99_ns_per_megabyte 5248852 node_work_ns_per_megabyte 46797 transport_ns_per_megabyte 3347685 allocations_per_8_runs 0
hand_over transport shared megabytes 1 median_ns 2111240 p99_ns 3724667 min_ns 1891777 max_ns 4772407 ns_per_megabyte 2111240 p99_ns_per_megabyte 3724667 node_work_ns_per_megabyte 47019 transport_ns_per_megabyte 2064221 allocations_per_8_runs 0
hand_over transport before megabytes 4 median_ns 16432926 p99_ns 20288445 min_ns 14139592 max_ns 20444389 ns_per_megabyte 4108232 p99_ns_per_megabyte 5072111 node_work_ns_per_megabyte 273435 transport_ns_per_megabyte 3834796 allocations_per_8_runs 0
hand_over transport plain megabytes 4 median_ns 13678981 p99_ns 19926611 min_ns 11714611 max_ns 20038370 ns_per_megabyte 3419745 p99_ns_per_megabyte 4981653 node_work_ns_per_megabyte 279454 transport_ns_per_megabyte 3140292 allocations_per_8_runs 0
hand_over transport shared megabytes 4 median_ns 10850685 p99_ns 14674574 min_ns 9259018 max_ns 15197000 ns_per_megabyte 2712671 p99_ns_per_megabyte 3668644 node_work_ns_per_megabyte 286630 transport_ns_per_megabyte 2426042 allocations_per_8_runs 0
hand_over transport before megabytes 4 median_ns 16384297 p99_ns 19991296 min_ns 14256741 max_ns 20966537 ns_per_megabyte 4096074 p99_ns_per_megabyte 4997824 node_work_ns_per_megabyte 280625 transport_ns_per_megabyte 3815449 allocations_per_8_runs 0
hand_over transport plain megabytes 4 median_ns 13889445 p99_ns 20302963 min_ns 11919722 max_ns 21078500 ns_per_megabyte 3472361 p99_ns_per_megabyte 5075741 node_work_ns_per_megabyte 272944 transport_ns_per_megabyte 3199417 allocations_per_8_runs 0
hand_over transport shared megabytes 4 median_ns 10770352 p99_ns 14479389 min_ns 9246333 max_ns 15687130 ns_per_megabyte 2692588 p99_ns_per_megabyte 3619847 node_work_ns_per_megabyte 251514 transport_ns_per_megabyte 2441074 allocations_per_8_runs 0
hand_over transport before megabytes 4 median_ns 16291463 p99_ns 20390815 min_ns 14270778 max_ns 20767112 ns_per_megabyte 4072866 p99_ns_per_megabyte 5097704 node_work_ns_per_megabyte 395681 transport_ns_per_megabyte 3677185 allocations_per_8_runs 0
hand_over transport plain megabytes 4 median_ns 13809833 p99_ns 18548722 min_ns 12159000 max_ns 19222870 ns_per_megabyte 3452458 p99_ns_per_megabyte 4637180 node_work_ns_per_megabyte 307819 transport_ns_per_megabyte 3144639 allocations_per_8_runs 0
hand_over transport shared megabytes 4 median_ns 10615055 p99_ns 13821370 min_ns 9258315 max_ns 15559667 ns_per_megabyte 2653764 p99_ns_per_megabyte 3455342 node_work_ns_per_megabyte 284310 transport_ns_per_megabyte 2369454 allocations_per_8_runs 0
```
