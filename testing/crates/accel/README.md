# daedalus-accel

The acceleration inventory: which data-parallel op has which backend (scalar, portable SIMD,
x86 SSE2/AVX2, AArch64 NEON, ARMv7 NEON, RVV, GPU), and the CI evidence that each one matches
its scalar reference.

No dependencies, and `no_std` unless the `std` feature is on. The crate forbids `unsafe`: it is
a testing crate, and the boundary gate allows `unsafe` only in the kernel and backend `-sys`
crates.

- **Register.** A crate (or a registry crate next to it) exports const `Registry` tables: every
  `Op` with its scalar reference, and per backend a `Kernel`: implemented (source `path:line`,
  conformance test, optional `symbol` for compiler-vectorized code), `Gap`, `Impossible`,
  `NotDataParallel` or `Deferred` (possible but postponed), each with a reason. An entry with
  `with_extensions` decides that extension column, so an `Op::could_use` candidate that cannot be
  done is a `missing(.., Impossible(..))` entry with its extensions. Tables nest through
  `children`. A backend without an entry is a gap.
- **Prove.** Conformance tests call `conform(op, backend, || kernel == scalar)` (feature `std`).
  With `ACCEL_RESULTS_DIR` set it appends a JSON line: op, backend, variant, adapter, target
  (`ACCEL_TARGET`), runner (`ACCEL_RUNNER`), RVV `vlen` (from `ACCEL_RVV_VLEN`, which the runner
  sets to the vector length it runs at), rev, result. A backend the process cannot run is skipped
  (ARMv7 NEON and RVV only count when compiled in: stable Rust cannot detect them at runtime).
- **Dump.** `dump` and `dump_json` write the registries as JSON for the renderer.
- **Generate and gate.** The `generator` feature adds the matrix renderer and gate (`accel-matrix`).
  It combines the registry dumps with the evidence into `docs/backend-matrix.{md,csv}`.

A cell is **pass** only with passing CI evidence (RVV at VLEN 128, 256 and 512; compiler-
vectorized kernels also need the asm record); **fail** when any record failed; **untested** when
implemented without that evidence; **gap**; **deferred**; or **impossible** with its reason.
