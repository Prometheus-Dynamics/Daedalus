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
`scripts/ci.d/`, in seven lanes at once: the cargo steps for this machine one after another; the API
documentation (built in `doc-build` inside the build directory); the firmware (in `chip`); what a release ships (the crates
packaged, the daemon's packaging files and the examples run, built in `ship`); the steps whose
builds never replace what the first lane runs (the daemon built with other features and the budget
table, in `variants`); the steps that build only for other targets (in `cross`); and the checks
that build nothing. No two lanes build in one directory, so no lane waits on another's lock. A new step must be
named in one of the lanes, or the gate refuses to run.

In the gate, the `test` step builds every binary as the workspace builds it, then runs only the
packages whose tests have not passed on exactly these inputs (`scripts/lane-reuse.py`). A package's
key is taken after the build: what its tests run and load (their binaries, and the programs and
shared libraries of the packages they build), each by what the loader maps of it (its allocated
ELF sections, without the build-id note; a file that isn't whole ELF is hashed whole), and every
file of the checkout its tests may read as they run: of the files of each workspace package they
build, the root's and cargo's config, all but those they were compiled from. A comment or a
function nothing calls reruns nothing; a comment above code moves a panic's location, and reruns.
Every key also holds the lock file's entry of every package the tests build, the root's files, the
scripts that run and judge the test step, cargo's configuration, the toolchain, the programs and
Vulkan drivers a test may load, and the environment a build or test reads. Other files outside the
packages (other steps' scripts, the lander's tools, fixtures that are not packages) are read by no
test but those of the packages below; the self-test checks that every path the test step's scripts
name is keyed or listed with why it isn't. The gate holds a narrowly keyed package to its key: its
tests run under bwrap seeing only the files of the checkout the key holds, so a test that reads a
file it was compiled from fails, named (one that must is listed, with why, in `READ_AS_IT_RUNS`,
and keyed by content), and every unit it builds must have compiled only from the
paths of the packages it builds, by rustc's dep-info and what build scripts declare
(`lane-reuse.py reads`). A package that starts reading outside itself fails the test step, named,
and nothing is recorded; one that must is listed in `WHOLE_TREE`. With no bwrap, nothing is reused
or recorded. The files a key holds are one read-only mount: hard links to the checkout's own files
in a directory on the checkout's file system (the build directory when it is there, else beside
the checkout, never in it), laid out once per set of files (side by side, before any
binary runs, so no binary's time or bound counts it), shared by every binary
that sees it, checked to be exactly the key's files (the same inodes) before it is mounted, and
removed when the run ends, red or stopped. One bind per file cost bwrap 7 s a binary for the
1263 paths of a package keyed by its sources (0.06 s staged). A file is never copied: with nowhere
on the checkout's file system to lay them out, the files are bound one by one, and the run's
summary line says how many sets were. The step also says each of its phases in wall and CPU
seconds (`phases:` the build, the fixtures, the keys, the layout, the run; then `after the run:` the
reads check and the records), so a slow step shows where its time went. Either
way nothing can be written into the checkout but into the live directories and the build
directory, and a file seen empty is an empty, read-only regular file of the gate's own (in the
stage, checked to be one: one link, not the checkout's; bound one by one, one such file on each;
never /dev/null, which a bind mounts nodev, where it can't be opened to be read). What a package's tests read beyond the packages they build is declared in
`scripts/lane-reuse.py`: `EXTRA_READS` lists paths of the checkout (headers, a corpus, the published
schema), which its tests see and its key holds by content; the fixtures its tests build (the
names in `scripts/test-parallel.py` `FIXTURES` its sources give) are built before the keys, and
each key holds their outputs by what the loader maps (a test's own build of one is answered as
fresh, and the fixture's sources are not in what it sees); `TARGET_READS` lists the files beside the
test binary its tests read (a sibling shared library or staticlib, the binary run again), each an
artifact the key holds, or the plan fails. The build directory is not sandboxed, so that list is
reviewed, not enforced. A package whose sources name a path out of it anywhere else (a `..` path,
a walk up from its manifest, the running binary, cargo building no fixture, the build directory)
is keyed by the whole tree. One whose tests run trybuild, which compiles its cases from source as
the test runs, is keyed by its sources: every file it sees, by content; a path an `include!`, `include_str!`, `include_bytes!` or
`#[path]` literal names is resolved first, and one inside the package is no way out of it. Such a
package is keyed by the whole tree but the
planning notes, by content. Every binary is still built; only the run is skipped. The gate's last
line, `Gate-Lanes:`, says what each lane did (`passed`, `passed(K-of-N-reused)`, or `not-run(WHY)` for a
lane `scripts/dev/land.sh` left out), and the commit carries it. On CI, or with
`GATE_LANE_REUSE=0`, nothing is reused. `lane-reuse.py --explain PACKAGE ARTIFACTS` lists a key's
inputs after a build (cargo's messages, as the test step keeps them).

A step outside the test lane can be reused the same way: `scripts/step-reuse.py` keys it by the
parts every key holds, its own script, and every file of the packages it builds (their closure
without dev-dependencies). After each run, rustc's dep-info for what it compiled must stay inside
that key, or the step fails, named, and records nothing. A step that passed before on its key is
not run again, and its token on the line reads `passed(reused:KEY)`. `nostd` is the first such
step. `vvl` and `vvl-wgpu` (scripts/vvl-run.py) are keyed the same way by what their test binaries
build, and by every file of the validation layer. When reuse is on, each binary runs under bwrap
and sees only the paths of the checkout its key holds, as the test step's narrowly keyed packages do.

The `tools` step's longer self-tests are reused the same way (`scripts/tools-reuse.py`): each is
keyed by the parts every key holds, the files it reads and the programs it starts, and for those
that read the tree (the reuse keys' own and the budget table's), every manifest, the lock file,
the names of the checkout's files and which packages are keyed by the whole tree. A comment in a
package's source reruns none of them.

The budget table (`budgets`) is measured in the local gate only when its build directory has
measured these inputs before. When the change reaches what the measured release binaries are built
from, or the build directory is cold, the step builds nothing. It leaves the table to the budgets
workflow, which runs it with valgrind and `BUDGETS_REQUIRE_ALL=1`, and the line carries
`budgets=not-run(ci-only:release+callgrind+sizes)`. `scripts/release-check.sh COMMIT` refuses a
release while any commit since the last tag carries that and the budgets job has not passed on the
exact commit. The firmware (`chip`) is always checked locally.

`scripts/ci.sh <step>` runs one by name:

| Step | What it checks |
|---|---|
| `format`, `clippy`, `test` | `cargo fmt`, `cargo clippy -D warnings`, and every test binary `cargo test --workspace --all-targets` builds, run side by side by `scripts/test-parallel.py` |
| `boundaries` | The rules above |
| `nostd` | The heap-free crates on the bare-metal targets |
| `chip` | The firmware builds and their flash and RAM |
| `wasm` | The WebAssembly builds, run under Node |
| `gpu-async-blocking`, `gpu-driver-lock` | Two rules of the GPU backend |
| `vvl` | The `vk-sys` tests on lavapipe under the Khronos validation layer pinned in `scripts/vvl.pin` (built by `scripts/vvl-build.sh`, found at `DAEDALUS_VVL_DIR`, refused unless its hash is the pinned one). It runs the binaries the `test` step built (`scripts/vvl-run.py`). The planted call has to be named first; then any validation line fails. With no layer it says `not run` and fails, except on a CI runner, which has none |
| `budgets` | The budget table: instructions, allocations, firmware sizes. A measurement is kept in the build directory under a key of everything it was measured from, and used again while that is unchanged; `BUDGETS_FRESH=1` measures again |
| `capability` | The audit of what the earlier branch could do |
| `packaging` | The daemon's unit files and sample config |
| `package` | Every publishable crate lists its files with no warning and has the registry metadata |
| `semver` | Semantic versioning against the `SEMVER_BASELINE` tag; reports and never fails the gate |
| `smoke` | The examples run and print what they should |
| `doc` | The API docs build with no warning. Every link in `docs/` resolving is a `daedalus` `docs::` test, which the `test` step runs |
| `tools` | `scripts/affected.py`, `scripts/prune-targets.py`, `scripts/seed-target.py`, `scripts/fixture-locks.sh`, `scripts/lane-reuse.py` (when it changed) and `scripts/bounded.py` against planted cases; the bounds against a planted sleeper and an orphan that holds the output, in `scripts/ci.sh` and `scripts/test-parallel.py` |
| `vvl-wgpu` | The `daedalus-wgpu` tests, every one that opens a device, the same way as `vvl`, on the binaries the `test` step built, after it in the host lane |

Every step runs under a hard bound, written in its own file as `# bound: N s`: three times its
slowest green run, and never under a minute (the three scripts the scripts lane runs after its
steps carry one too). `scripts/bounded.py` runs each in a process group of its own. Past its bound
the group gets TERM, then KILL, and the step fails, named. A step that ends but leaves something
running in its group has that stopped and named. Each test binary `scripts/test-parallel.py` runs
is bounded the same way, at three times the slowest it has passed in, never under a minute, and
600 s for a binary with no passing run on record. The slowest is the larger of the build directory's
times file and `scripts/test-slowest.json`, each binary's slowest green run in cold gates on fresh
slots, committed: a warm build directory's own record can be one quick pass; its output goes to a file, so a child left holding it can't block the run.
`DAEDALUS_BOUND_SCALE` multiplies every bound; the hosted CI runners set it to 4. A step whose
slowest green run grows past a third of its bound gets a new bound in the same change. The slowest
run is a cold one: a fresh checkout with an empty build directory, as a gate on a new machine runs.

Each step also runs with a `TMPDIR` of its own, an empty directory made in the gate's and removed
when the step ends. A step that leaves anything in it fails, and the names are listed: leftovers in
a `TMPDIR` every gate run shares pile up until unrelated steps fail. A cache meant to outlive one
run (the budget fixture's copies) goes in `DAEDALUS_GATE_TMPDIR`, the gate's own `TMPDIR`.

Time a step, or the gate, in the environment the merge gate runs it in: `CARGO_INCREMENTAL=0`, the
same job count and cores, and the seed it starts from (or none, for a cold build). An incremental
build in a warm checkout is a different measurement, and a cut that only holds there is not one.

To add a check, add an executable `scripts/ci.d/<step>.sh` with its `# bound:` line. A check that needs an environment of
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

A build directory that doesn't exist yet is made as a btrfs subvolume of its own where the file
system allows it (`scripts/seed-target.py root`, and `use` for a seeded one): compilers that share
one subvolume with every other build on the disk queue on its locks.

A new checkout's first build can start from the dependencies another one built. `scripts/seed-target.py
save SEED` keeps this checkout's build directory, less the workspace's own builds, as `SEED`; with
`DAEDALUS_TARGET_SEED=SEED`, a build directory that doesn't exist yet starts as a copy of it.
The workspace's own builds never go in a seed: they carry the paths of the checkout that built
them. Cargo checks every seeded build against the source, so an old seed only means more to build.
The build directories inside it, trybuild's and the test fixtures' under `tmp`, are seeded the same way.
A seed is copied by reflink, so a copy costs only the blocks a build then changes, and nothing
writes into a seed once it is saved. `save` records the key its builds were made at (the lock file,
the toolchain, cargo's configuration, `scripts/home-remap.sh`, the build profiles, `rustc -vV` and the cargo homes' paths); `stale SEED` exits 1 when
this checkout's key differs, which is when a new seed is worth building, from an empty build
directory and apart from any gate that uses one. The key never decides what is fresh: a seed made
at an older lock file was shown to rebuild exactly the dependency that changed and the packages
that depend on it.

The test fixtures build on their own, outside the workspace, and their lock files aren't committed.
`scripts/fixture-locks.sh` gives a fixture with none a lock made from the root one, before the gate's
lanes start, so a fixture builds the workspace's versions of what they share.

## The working area

`scripts/dev/setup.sh` makes the lander's working area from nothing (`DAEDALUS_WORK`, default
`/mnt/nvme-wd/daedalus`): `wt/` the clones (`work`, `gate`, `seeder`), `target/` a build directory
for each clone, `cache/` what can be thrown away (the seed, the validation layer, `TMPDIR`) and
`logs/` the gate logs. `source scripts/dev/env.sh` inside a clone sets what the tools read.
`scripts/dev/land.sh` gates and lands a change, running only what it can affect: notes no check
reads cost no gate, and a change to `docs/` alone runs the steps that read it. It writes the gate's
summary into the commit. `setup.sh --timer` installs the idle job that keeps the seed current
(`scripts/dev/reseed.sh`); `--board` builds the board's validation layer. The validation layer is
built from the sources `scripts/vvl.pin` names, with its paths mapped, so it has the pinned hash
wherever it is built.

The lander's gates run offline in cargo homes of their own, never in `~/.cargo`: there, a step
waits on the package-cache lock whenever another program on the machine resolves or downloads, and
makes no progress of its own while it does. Before a gate, `scripts/dev/cargo-home.sh` (run by
`land.sh` and `reseed.sh`) fetches every committed lock file's crates into `cache/cargo-home`, the
only step that goes online, and says each lock file's sha256, the sources, the number of crate files
and a sha256 over their sums (`logs/fetch-ID.log`). Each lane then runs in a copy of its own,
`cache/cargo-home.lanes/LANE` (reflinks, so the copies cost no blocks until a lane unpacks a crate):
cargo takes the package-cache lock for writing while it resolves, even offline, so lanes sharing one
home would wait on each other. The paths never change, since a dependency's source path is in what it
builds and in the test step's reuse keys; a new fetch is copied into each lane's home, and nothing is
removed from one. Outside the lander (CI, a developer's build) `CARGO_HOME` is the caller's.

Whatever `scripts/ci.sh` builds, in every profile, names the cargo home's sources as `/cargo/...`
rather than by the home's path (`scripts/home-remap.sh`: `--remap-path-prefix` in
`CARGO_BUILD_RUSTFLAGS`), so its bytes don't depend on who built it or where: the daemon's debug
builds held the builder's home 185 times. `scripts/check-home-remap.sh` builds the daemon in two
homes, debug and release, and compares the bytes (`--without-remap` shows they differ without it).
It builds every dependency four times, so it is run by hand, not by a gate. A build outside
`ci.sh` (a consumer's, `cargo build` in a shell) passes the flag itself, or names its home.

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
