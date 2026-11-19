# Testing

## Everything

```sh
scripts/gate.sh
```

This is what CI runs. It needs the bare-metal and WebAssembly targets, Node, and
`wasm-bindgen-cli` at the version `scripts/wasm-bindgen-version.sh` prints. Without valgrind the
instruction rows of the budget table are reported as not run.

## One part

```sh
cargo test -p daedalus-planner-tests        # the planner, against the simulated backend
cargo test -p daedalus-cpu --all-features   # the CPU backend
cargo test -p daedalus-host --features listen --test listen
scripts/ci.sh budgets                       # the budget table
scripts/ci.sh smoke                         # the examples
scripts/gpu.sh                              # the GPU backend, on a software adapter unless told otherwise
scripts/gpu-lavapipe.sh                     # the GPU lane, on Mesa's software Vulkan
```

## GPU adapters

Tests, benches and `cargo run` see only software adapters by default. The repo's
`.cargo/config.toml` sets `DAEDALUS_GPU_ADAPTERS=software`, so a plain `cargo test --workspace` on
a machine with a GPU does not run on that GPU. A software adapter is one whose device type is CPU:
lavapipe, llvmpipe and SwiftShader are examples.

To run on the GPU on purpose, set the variable in your shell:

```sh
DAEDALUS_GPU_ADAPTERS=hardware cargo test -p daedalus-wgpu
```

`hardware` keeps every adapter that is not a CPU one. `any` keeps all of them, and unset means
`any` for a program run directly. A shell setting overrides the repo's default, because cargo does
not replace a variable that is already set. Any other value is an error that names the variable.
When the rule hides every adapter, the open fails and says how many it hid, so a skipped test
explains itself.

The board scripts set the variable themselves. `scripts/board/batch-on-board.sh` sets `hardware`,
and `scripts/board/batch.sh rehearse` sets `software` on lavapipe. A binary built for a board is run
directly, not through cargo, so the repo's default does not reach it.

## Where tests live

- A crate's own tests are in its `tests/`. Where a crate has more than one such file, they are
  modules of one test binary, `tests/all.rs`, which is one link instead of one per file. A file
  that needs a process to itself (it installs a global allocator, counts the process's file
  descriptors, runs trybuild, or includes generated source that names the crate root) is a
  `[[test]]` of its own in the crate's `Cargo.toml`, with the reason beside it. Such a crate sets
  `autotests = false`, and the boundary check refuses a test file that is in neither. To run one
  file's tests: `cargo test -p <crate> --test all -- <file>::`.
- A core crate cannot depend on a backend, so tests that need both are in `testing/crates/`:
  `planner` (the planner with the simulated backend), `conformance` (the rules every backend
  keeps), `worker-conformance` (the fixtures every language's worker serves).
- `testing/fixtures/` holds things built on their own: the dylib plugin the loader, the daemon
  and the command line client load.
- Compile-fail cases are trybuild tests in the macro and authoring crates.

## Kinds of proof

- **Allocation.** A test installs the counting allocator from `daedalus_abi::counting` and
  asserts that a warm path allocates nothing.
- **Planted failures.** A check that can fail is shown to: the boundary checks, the budget
  rows, the capability audit, the packaging check, the link check and the smoke step each have
  a planted case that must fail.
- **Processes.** Tests that start a daemon or a worker use a directory and a socket of their
  own under the target's `tmp/`, stop the processes they started, and wait on conditions, never
  on a fixed delay.
- **Fake clocks.** A backoff is tested by handing the code a clock or a sleep function, so no
  test waits for real time.

## Steady state

A graph that is running must not allocate, and must not copy what it was handed. These are
counting tests, in the gate's test stage, and each fails on the first allocation after its
warm-up. None is a benchmark.

| What | Test | Where |
|---|---|---|
| The runner, ten thousand ticks | `ten_thousand_warm_ticks_allocate_nothing` | `core/runner/tests/no_alloc.rs` |
| The runner's parallel lanes | `warm_parallel_ticks_allocate_nothing_on_any_thread` | `core/runner/tests/lanes_alloc.rs` |
| The runner with its counters | `warm_ticks_with_counters_allocate_nothing` | `core/runner/tests/telemetry.rs` |
| A host's feed, step and take | `a_steady_feed_step_take_loop_allocates_nothing_after_warm_up` | `core/host/tests/host_io.rs` |
| The same with each step's time and bytes kept | `a_probe_counts_the_bytes_each_step_publishes_and_the_loop_still_allocates_nothing` | `core/host/tests/host_io.rs` |
| The CPU backend's steps | `a_thousand_runs_allocate_nothing` | `backends/cpu/tests/no_alloc.rs` |
| A plugin's step, across the library boundary | `a_plugin_tick_allocates_nothing_after_warm_up` | `core/loader/tests/alloc.rs` |
| A megabyte a plugin hands over, taken by the host: no allocation, no byte copied | `a_steady_state_take_of_a_handed_megabyte_allocates_nothing` | `core/loader/tests/handover.rs` |
| A worker's region, on the host's thread | `a_steady_state_run_allocates_nothing_on_the_host_thread` | `backends/process/tests/alloc.rs` |
| Every backend's steps, as a conformance check | `steps_do_not_allocate` | `testing/crates/conformance/src/checks.rs` |

Copies are decided when a graph is planned, not while it runs: the planner refuses a reader that
takes a shared value by value unless a copy is planned, so a plan with no copy step copies
nothing (`a_single_move_consumer_needs_no_copy`, `a_shared_output_replays_by_handle_with_no_copy`,
`a_host_that_takes_its_output_by_value_gets_it_moved_with_no_copy_or_allocation`). Where the
host does copy, it is counted (`copies_are_counted_when_the_host_copies`).

A worker is the exception, and by construction: it is another process reached through a pipe,
so a value's bytes are written into the pipe and read out of it, once each way. The host's side
adds no allocation to that. What the worker's own process allocates is its SDK's and its
language's, and is not Daedalus's to promise.

## What CI cannot run

- Timings. A hosted runner's clock is not a measurement. The `Budgets` workflow compares
  timings with the previous run and never fails on them.
- Boards. Each PR that needs one has its procedure and record in `testing/board-runs/<id>.md`.
  The record's first line is a `Status:` line: `run on CM5 <date>`, `partly run on CM5 <date>`
  (what ran and what did not), or `not run` with the reason.

## Measured and left as they are

- The trybuild tests run one after another. trybuild builds every crate's compile tests in one
  shared directory, `<build directory>/tests/trybuild`, naming each case's binary `trybuild000`,
  `trybuild001` and so on, so two at once overwrite each other's builds; cargo's lock on that
  directory would serialize them anyway. A directory of their own each would let them run at once,
  but after a change to a core crate each directory builds that crate again, up to six times the
  work of the one shared build (2026-11-18: the seven took 25 s and 60 CPU-s after a core change,
  most of it that one rebuild, and 8 s warm). Putting all the cases in one trybuild run saves only
  each run's fixed cost, about 0.2 s of the 8; the rest is compiling the cases, which is the test.
  The runner starts them first, as its longest job, so they add about 2 s to the test step.
- Each transcript of the driver suite (`testing/crates/drive-conformance`) starts the driver
  afresh: 35 starts per suite, about 3 CPU-s a gate for Node, 5 for Java and 1 for Python
  (2026-11-18). One driver process per transport would save that, but it changes the driver
  contract in five languages and gives up a fresh process per transcript, which keeps each
  transcript clear of what an earlier one left in the driver. Not done; this is the measurement.

## Workflows

| Workflow | Runs |
|---|---|
| `Next` | `scripts/gate.sh` |
| `Portability` | The CPU backend on other targets under qemu, and clippy and the tests on Windows |
| `GPU` | The GPU lane on lavapipe, under the Vulkan validation layer |
| `Budgets` | The budget table with valgrind, and the timing comparison |
