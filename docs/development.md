# Development

## Layout

```text
core/        abi, types, value, graph, backend, registry, plan, planner, runner, macros, derive,
             host, wire, client, loader, capi, made, integrity, daedalus (the facade)
backends/    cpu, cpu-author, dmabuf, process, process-shm, sim, wgpu, wgpu-author, and *-sys crates
languages/   python, node, java, cpp, cpp-sdk, rust: SDKs for nodes run by the process backend
             rust-capi (a safe Rust host over the C surface) and rust-capi-sys (its loader)
service/     daedalusd, daedalus-ctl, daedalus-lock
testing/     testing/crates/ (author-fixture, budget-harness, conformance, counters, cpu-timings,
             fake-npu, fake-npu-author, fake-npu-plugin, gpu-budgets, handoff-bench, planner,
             process-timings, wgpu-conformance, worker-conformance, worker-fixture), fixtures/,
             board-runs/, budgets/
examples/    each directory directly inside is a crate
```

Every directory directly inside `core/`, `backends/`, `languages/`, `service/`,
`testing/crates/` and `examples/` is a crate, and the workspace picks them up by glob. Adding a
crate is making its directory.

## Rules the gate enforces

- `core/*` depends on nothing in `backends/`, `languages/` or `service/`.
- Nothing in `core/` is named after hardware or media.
- `unsafe` only in `core/abi`, `core/capi`, `core/loader`, `core/runner`, `examples/wasm_smoke`,
  `examples/wasm_bindgen_host`, `examples/chip_board`, and crates named `*-sys`, with a
  `SAFETY` comment on each use. Every other crate forbids it.
- The heap-free crates build for the three bare-metal targets with no allocator.
- No Rust file is over 800 lines.
- Third-party dependencies are declared once, in the root `Cargo.toml`, and used with
  `workspace = true`.
- A library prints nothing.

`python3 scripts/boundaries.py` runs the boundary checks alone.

## The gate

`scripts/gate.sh` is what every change must pass, and what CI runs. It runs every step in
`scripts/ci.d/`, in four lanes at once: the cargo steps for this machine one after another, the
steps whose builds never replace what those run (the daemon built with other features, in
`variants` inside the build directory, and the budget table), the steps that build only for other
targets, and the checks that build nothing. A new step must be
named in one of the lanes, or the gate refuses to run. `scripts/ci.sh <step>` runs one by name:

| Step | What it checks |
|---|---|
| `format`, `clippy`, `test` | `cargo fmt`, `cargo clippy -D warnings`, and every test binary `cargo test --workspace --all-targets` builds, run side by side by `scripts/test-parallel.py` |
| `boundaries` | The rules above |
| `nostd` | The heap-free crates on the bare-metal targets |
| `chip` | The firmware builds and their flash and RAM |
| `wasm` | The WebAssembly builds, run under Node |
| `gpu-async-blocking`, `gpu-driver-lock` | Two rules of the GPU backend |
| `budgets` | The budget table: instructions, allocations, firmware sizes |
| `capability` | The audit of what the earlier branch could do |
| `packaging` | The daemon's unit files and sample config |
| `package` | Every publishable crate lists its files with no warning and has the registry metadata |
| `semver` | Semantic versioning against the `SEMVER_BASELINE` tag; reports and never fails the gate |
| `smoke` | The examples run and print what they should |
| `doc` | The API docs build with no warning, and every link in `docs/` resolves |
| `tools` | `scripts/affected.py`, `scripts/prune-targets.py` and `scripts/seed-target.py` against planted cases |

To add a check, add an executable `scripts/ci.d/<step>.sh`. A check that needs an environment of
its own gets a workflow of its own: `Portability` (other targets under qemu, and Windows), `GPU`
(lavapipe) and `Budgets` (valgrind) are those.

Where lavapipe is installed, the steps run on it unless the caller names a Vulkan driver, so a
gate never loads the machine's own GPU.

### While working

`scripts/check.sh` is a quick answer for a change, never a replacement for the gate. It runs
`format`, `clippy` and the file-size rule as the gate does, and the gate's own test build, from
which only the binaries of the packages the change can reach run: the changed packages, every
package that depends on them, and `daedalus`, whose tests read the whole tree. A change to a file
in no package runs every binary. A red there is a red at the gate; a green is not a green there.

A build directory belongs to one checkout, and `scripts/ci.sh` refuses one another checkout built
in: cargo would reuse those builds, and they carry the other checkout's paths. Each directory
records its checkout. `scripts/prune-targets.py DIR...` lists the build directories under `DIR`
whose checkout is finished with: gone, or merged into `origin/next`, clean and unused for two
days. `--delete` deletes them. A directory with no record is never touched.

A new checkout's first build can start from the dependencies another one built. `scripts/seed-target.py
save SEED` keeps this checkout's build directory, less the workspace's own builds, as `SEED`; with
`DAEDALUS_TARGET_SEED=SEED`, a build directory that doesn't exist yet starts as a copy of it.
The workspace's own builds never go in a seed: they carry the paths of the checkout that built
them. Cargo checks every seeded build against the source, so an old seed only means more to build.
The build directories inside it, trybuild's and the test fixtures' under `tmp`, are seeded the same way.

## Adding a backend

A backend is two crates: a small authoring crate that nodes depend on, which exports the body
macro `#[node]` calls, and the crate that implements `Backend`. Nothing in `core/` changes.
`backends/sim` is the smallest backend, and `testing/crates/conformance` is the suite a backend
runs to show it keeps the rules.

## Shared interfaces

`core/abi`, `core/types`, `core/value`, `core/graph`, `core/backend`, `core/registry`,
`core/macros`, `core/derive` and `core/plan` are what everything else is built against. A change
to one is a change of its own, with the page under `docs/` that describes it updated in the same commit.

## Conventions

- Failure: never give up, wait at most 8 s between attempts, turn nothing off automatically.
- Running allocates nothing in Daedalus's own code. Planning may.
- A number that is a budget is in [`testing/budgets/budgets.toml`](../testing/budgets/budgets.toml),
  and a change that makes one worse changes it there and says why.
- Hardware that was not reached has a record in `testing/board-runs/`, whose `Status:` line says
  what ran and what did not.
