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
scripts/gpu.sh                              # the GPU backend, on this machine's adapter
scripts/gpu-lavapipe.sh                     # the GPU lane, on Mesa's software Vulkan
```

## Where tests live

- A crate's own tests are in its `tests/`.
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

## What CI cannot run

- Timings. A hosted runner's clock is not a measurement. The `Budgets` workflow compares
  timings with the previous run and never fails on them.
- Boards. Each PR that needs one has its procedure in `testing/board-runs/<id>.md`, marked
  `untested on board` until someone runs it.

## Workflows

| Workflow | Runs |
|---|---|
| `Next` | `scripts/gate.sh` |
| `Portability` | The CPU backend on other targets under qemu, and clippy and the tests on Windows |
| `GPU` | The GPU lane on lavapipe, under the Vulkan validation layer |
| `Budgets` | The budget table with valgrind, and the timing comparison |
