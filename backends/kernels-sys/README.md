# daedalus-kernels-sys

The CPU kernels any backend can call: float conversions for readbacks (`f32_from_le`,
`f16_to_f32` with its F16C path, `f32_to_unorm8`), byte and row copies, hex and base64. Each
kernel has one entry per ISA (SSE2, SSE4.1 and AVX2 on x86_64, NEON on AArch64, NEON on ARMv7
under `+neon`, RVV under `+v`) and a baseline `v` build. The public functions dispatch to the best
entry for the machine.

`src/reference.rs` holds the scalar references. `tests/conformance.rs` runs every kernel on every
ISA this process can run against them. `portable` (`core::simd`) is built only with
`--cfg daedalus_portable_simd` on nightly.

This crate uses `unsafe` for the vector paths, so it is one of the crates the boundary gate allows
(`backends/*-sys`). Each block carries a `SAFETY` comment. It has no dependencies. It builds with
`no_std` and `alloc`; the default feature `std` enables AVX2 runtime detection on x86.
