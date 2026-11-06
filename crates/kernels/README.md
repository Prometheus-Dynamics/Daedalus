# daedalus-kernels

Daedalus's data-parallel CPU kernels: pixel layout conversions (RGB/RGBA/gray), GPU readback
float conversions (LE f32, binary16, unorm8), byte and row copies, lowercase hex and standard
base64. Each is vectorized per ISA: SSE2 and runtime-selected AVX2 on x86_64, NEON on AArch64,
NEON on ARMv7 built with `-C target-feature=+neon`, and RVV built with `-C target-feature=+v`
(stable Rust cannot detect those two at runtime, so the build decides).

`no_std` + `alloc`. `ACCEL` registers every kernel in the acceleration inventory; the
conformance tests check each backend against the scalar references in `reference`, and
`docs/backend-matrix.md` shows the evidence.

The fixed-function rows (Rockchip's RGA and the K230's VGLite) name Lemnos's routes at its pinned
revision (`lemnos-drivers-rga`, `lemnos-drivers-vglite`; `src/registry/fixed.rs`). Their tests run in
Lemnos, so those cells stay untested here until evidence is recorded. A Linux-only test ties each
row to Lemnos's capability table (`caps::route_for`): every implemented row is served by its route,
and the rows Lemnos does not serve (VGLite lines, the Y-only output of `rgba_red`) have no route.
