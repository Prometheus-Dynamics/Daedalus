# Working on the Daedalus `next` branch

`next` is a clean-slate rebuild of Daedalus. It started empty on purpose. `dev` is the frozen
reference, and nothing here changes it. The work is a pre-planned list of PRs into `next`.

## If you are picking this up

1. Read [`docs/design.md`](docs/design.md). It is the design every PR builds. Its "Settled details"
   section wins over anything a brief says.
   [`docs/architecture.md`](docs/architecture.md) is the longer walkthrough of the same design: worked
   examples, the reasoning, where each `dev` crate goes, the groups and waves. `docs/design.md` wins
   if they differ. [`docs/architecture.html`](docs/architecture.html) is that document with diagrams.
2. Run `python3 scripts/pr-status.py` to see what's merged, open, ready and blocked.
3. Take a PR marked ready and read its brief at `docs/prs/<id>.md`. The brief is the whole
   assignment: what it needs, what it creates, which files on `dev` to read, what to build and what
   proves it. A bullet starting "Settled:" wins over an earlier bullet in the same brief.

[`TODO.md`](TODO.md) is the full list.

Group A is merged. Groups B and C are being built in local sessions, one owner per group. No PR
runs in the cloud until the user says so, whatever a brief's `Runs:` line says.

## Working a PR

- Base branch `next`, working branch `pr/<id>` (for example `pr/A2`), one PR into `next`.
- Read `dev` as a reference: `git fetch origin dev:refs/remotes/origin/dev`, then
  `git show origin/dev:<path>`. Bring in only
  what the brief lists. The paths under "Read on dev" are from the repository root on `dev`.
- Stay inside the paths your PR creates. Don't edit the root `Cargo.toml`, `TODO.md`, or a changelog.
  `Cargo.lock` is committed since I7. Keeping to this is what lets many sessions run at once without
  conflicts.
- The one exception: a PR that adds a module to a crate from an earlier PR adds its own `mod` line
  to that crate's `lib.rs`. On a conflict there, keep both lines.
- Add a crate by creating its directory under the right folder. Workspace members are globs. Inherit
  third-party dependencies with `workspace = true`. Refer to another Daedalus crate by path
  (`daedalus-value = { path = "../value" }`).
- CI works one way, whatever a brief calls it. Add a step by creating an executable
  `scripts/ci.d/<step>.sh`: the gate runs every step in that folder. A check that needs its own
  environment gets its own workflow file instead. Where a brief says to add a `ci.sh` subcommand or
  edit a named job in `ci.yml`, do one of those two. I7 may also edit `.gitignore` and `scripts/ci.d/`.
- A crate that may use `unsafe` replaces `[lints] workspace = true` with its own `[lints.rust]` and
  `[lints.clippy]` tables, as `core/abi/Cargo.toml` does, because the workspace table forbids it.
- If your PR needs a shared interface changed, stop and say so in the PR description. Shared
  interfaces (`core/abi`, `core/types`, `core/value`, `core/graph`, `core/backend`, `core/registry`,
  `core/macros`, `core/derive`, `core/plan`) change only through A-series PRs.
- A PR merges when `scripts/gate.sh` passes. Paste the proof named in the brief into the PR
  description.

## Rules

- Daedalus connects and structures. Nothing in `core/` may know what a GPU, shader, NPU, frame, image
  or chip is. Those belong to backends and to whoever wrote the nodes.
- Planning allocates. Running doesn't: steady-state running in Daedalus's own code allocates nothing.
- Failure rule everywhere: never give up, backoff capped at 8 s, no automatic turning-off by default.
  Manual off exists, and automatic off is only a policy someone opts into.
- Each budget in the design must be matched or beaten. A PR that makes one worse says so.
- `unsafe` only where the boundary check allows it, with one operation per block and a `SAFETY`
  comment.
- No dependency on Lemnos.
- No Rust file over 800 lines.
- Remove legacy outright: no compatibility shims and no code paths for old callers.
- Hardware you can't reach is marked `untested on board`, with the check written to
  `testing/board-runs/<id>.md`. A PR is not held back for a board run.
- Never claim something works without having run it. Report failures with their output.
- Breaking other repositories is fine. They aren't built here.
- Never push tags. Never force-push `next` or `dev`. Don't change `dev`. Don't bump versions.
- Don't estimate how long anything will take. Describe size by scope and risk.
- Product names appear only in the planning files (this file, `TODO.md`, and everything in
  `docs/` that describes the rebuild).

## Board runs

These need a real board, so they run in a local session on the user's machine after a PR merges.
Timings are CM5 only, except rows in the design's budget table that name another machine (the
RX 6800 XT is the desktop, V3D is the CM5). Builds there go on NVMe, never on the Github HDD.
