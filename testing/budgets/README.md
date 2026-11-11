# Budgets

[`budgets.toml`](budgets.toml) is the one table of budgets. `scripts/ci.sh budgets` measures its
rows with [`testing/crates/budget-harness`](../crates/budget-harness) and fails on a row above its
limit. A PR that makes a row worse changes the limit in `budgets.toml` and says why in its commit
message.

## The design's table, and where each row is checked

| Measure | `dev` | Checked by |
|---|---|---|
| Per-stage overhead, CM5, unfused | 655 ns | Board run [`I2`](../board-runs/I2.md). A time is not a CI row. |
| Per-stage overhead, CM5, fused | 498 ns | Board run `I2`. The instruction count of the same chain is the row `fused_chain_16_tick`. |
| 16-stage idle tick | 27,064 instructions | `idle_tick_16`, under callgrind, in the gate where valgrind is installed and in the budgets workflow. |
| Stable-ABI node | about 3,100 instructions per tick | `plugin_node_tick`, the same way. |
| Detector mirror graph | about 7 µs per frame | Board run `I2` for the time. `detector_frame` for its instruction count, and `alloc_detector_frame` for its allocations. |
| 8 GPU ops, RX 6800 XT | 0.37 ms with 8 submits, 0.15 ms with 1 | Board run `I2` for the times. The submit count is `gpu_region_submits`, counted on lavapipe by `scripts/gpu-lavapipe.sh` in the GPU workflow. |
| 8 GPU ops, V3D | 1.30 ms with 8 submits, 1.32 ms with 1 | Board run `I2`. |
| Five-node firmware | 3-12 KiB flash, 112-584 B RAM | `firmware_compiled`, `firmware_tunable` and `firmware_loaded`, from ELF section sizes on `thumbv7em-none-eabihf` and `thumbv6m-none-eabi`. |

Steady-state running allocates nothing: `alloc_fused_tick`, `alloc_plugin_node_tick`, `alloc_plugin_handover_tick`,
`alloc_large_handle_tick`, `alloc_large_handle_moved_tick`, `alloc_detector_frame` and the handoff
chains passed by handle or as shared bytes are all zero. A CPU output is kept for reuse by its
producer, as `docs/design.md` says, host-read outputs included.

## The fields of a row

- `name`: what the harness and the check call it.
- `measure`: what is measured, in words.
- `tool`: `callgrind`, `alloc`, `elf` or `gpu-lane`.
- `dev`, or `dev_callgrind` and `dev_perf`: the value on `dev`. For an instruction row,
  `dev_callgrind` is `dev` measured with callgrind on the same benchmark, and `dev_perf` is the
  figure `dev` reported from `perf stat -e instructions:u`, kept for reference.
- `limit`: the value the row may not exceed. For an instruction row it is `dev_callgrind`.
- `next` and `tolerance_percent`: an instruction row also fails when it is more than the
  tolerance above what this branch measured when the row was last set. The limit from `dev` is
  far above what this branch costs, and this is what catches a regression before it gets there.
- `board_run`: the board run that records a timing for the same thing.

## How each tool measures

- **Instructions.** `valgrind --tool=callgrind` on the release build of the harness. A count does
  not depend on how busy the machine is. The harness runs a benchmark a given number of times,
  the check runs it for 1,000 and for 3,000, and the row is the difference in `Ir` divided by
  2,000, so building the graph cancels out. `perf stat` is not used.
- **Allocations.** The harness installs a counting global allocator and counts 1,000
  steady-state ticks after a warm-up. Planning is not counted, and neither is making the value
  that is fed.
- **Flash and RAM.** `readelf -SW` on the firmware built with the `chip` profile. Flash is
  `.vector_table + .text + .rodata + .data`, and static RAM is `.data + .bss + .uninit`.
- **GPU submits.** Counted by `testing/crates/gpu-budgets` on lavapipe. `scripts/ci.sh budgets`
  does not run it: it needs a Vulkan driver, and has its own workflow.

A row that needs valgrind is reported as not run where valgrind is not installed. The budgets
workflow (`.github/workflows/bench.yml`) installs it and sets `BUDGETS_REQUIRE_ALL=1`, which
makes a row that cannot be run a failure.

## The dev baselines

`dev` was measured at `967479653e92` with valgrind 3.27.1, on the machine this was written on:

| Row | How | `Ir` |
|---|---|---|
| `idle_tick_16` | `crates/frame-bench/examples/idle_tick.rs` with 2,000 and 4,000 ticks | 25,988 a tick |
| `plugin_node_tick` | the `scalars n=0` case of `crates/daedalus/tests/support/stable_bench.rs`, 1,000 and 3,000 ticks, on the stable path | 14,425 a tick (10,655 with the node installed statically) |
| `detector_frame` | `crates/frame-bench/examples/detector.rs` with `DETECTOR_ONLY=flat` and `FRAME_CHAIN_TICKS` of 1,000 and 3,000, whose warm-up is a fifth of that again | 34,395 a frame |

`perf stat -e instructions:u` on the same idle tick gave 27,033, against the design's 27,064.
The stable path's case has no entry point that runs one path a given number of times, so a
ten-line test that does was added to a scratch checkout of `dev` for the measurement. `dev`
itself is unchanged.

## The planted regressions

Every run of the check also measures two fixtures that must fail: a chain with a stage that
allocates once a tick, against `alloc_fused_tick`, and a chain with a loop of extra work in one
stage, against a limit set to what the plain chain costs. `scripts/ci.sh budgets --self-test`
runs only those.

## Timings

`cargo bench -p daedalus-budget-harness` times the same benchmarks with criterion. The budgets
workflow uploads the result and compares it with the previous run on the same branch using
`scripts/bench-compare.py`. A slower timing is a warning and never a failure. The timings that
count are board runs.

## Large-value handoff (I3)

The `handoff_*` and `alloc_handoff_*` rows are three ways to pass a large value on the same
graphs, from `testing/crates/handoff-bench`. A value read through a reference costs no
allocation on any path. A value taken and handed on through 8 stages costs no allocation once
warm, as a handle or as shared bytes: each stage refills cells of its own. A
vector chain allocates the one vector each stage makes for itself, 8 a tick. `docs/prs/I3.md`
has the table.
