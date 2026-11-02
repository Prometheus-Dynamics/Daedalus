# daedalus-accel

The acceleration inventory shared by Daedalus, Eidos and Styx: which data-parallel op has
which backend (scalar, portable SIMD, x86 SSE2/AVX2, AArch64 NEON, ARMv7 NEON, RVV, GPU,
fixed-function), and the CI evidence that each one matches its scalar reference.

No dependencies and `no_std` by default-off `std`; Eidos uses it as a dev-dependency only.

- **Register.** A crate (or a registry crate next to it) exports const `Registry` tables:
  every `Op` with its scalar reference, and per backend a `Kernel`: implemented (source
  `path:line`, conformance test, optional `symbol` for compiler-vectorized code), `Gap`,
  `Impossible`, `NotDataParallel` or `Deferred` (possible but postponed), each with a reason.
  An entry with `with_extensions` decides that extension column, so an `Op::could_use`
  candidate that cannot be done is a `missing(.., Impossible(..))` entry with its extensions.
  Tables nest through `children`. A backend without an entry is a gap, unless the op is
  `Op::only_on` some backends (a fixed-function node whose CPU kernels and callers are another
  repo's): the other columns are n/a and the required reason is listed with the op.
- **Prove.** Conformance tests call `conform(op, backend, || kernel == scalar)` (`std`). With
  `ACCEL_RESULTS_DIR` set it appends a JSON line: op, backend, variant, adapter, target
  (`ACCEL_TARGET`), runner (`ACCEL_RUNNER`), RVV `vlen` (read from `vlenb`), rev, result.
  A backend the process cannot run is skipped (ARMv7 NEON and RVV only count when compiled in:
  stable Rust cannot detect them at runtime).
- **Check the code is vector code.** `accel-matrix asm --binary <test binary> --target <triple>`
  disassembles (`rust-objdump`, from `llvm-tools`) every kernel `symbol` and records whether it
  contains that ISA's vector instructions.
- **Render and gate.** `accel-matrix render --registry <dump.json> --results <dir>
  --baseline docs/backend-matrix.csv --scan <repo root> --out docs/` writes
  `backend-matrix.md` and `backend-matrix.csv` (`repo,op,backend,cell,reason,evidence`).

A cell is **pass** only with passing CI evidence (RVV at VLEN 128, 256 and 512; compiler-
vectorized kernels also need the asm record); **fail** when any record failed; **untested**
when implemented without that evidence; **gap**; **deferred**; or **impossible** with its reason. The totals count each separately.

The gate fails on registry errors, failing cells, a source file with SIMD or GPU markers
(`core::arch`, `#[target_feature]`, `asm!`, `multiversion!`, WGSL, pipeline creation) that no
kernel or exemption names, and pass cells lost against the baseline. `--release` also fails on
every gap, untested or deferred cell. GPU cells marked impossible on per-element ops are listed for
review (`--strict-review` fails on them).
