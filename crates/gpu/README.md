# daedalus-gpu

Lightweight GPU facade and shader helpers built on wgpu. Provides opaque handles, pooling, and dispatch utilities so GPU-capable nodes can run without exposing backend types.

## What it offers
- **Backends**: noop (default), mock (tests), real wgpu (`gpu-wgpu` feature). Optional async API (`gpu-async`).
- **Handles**: `GpuContextHandle`, buffer/image handles, and backend selection with skip reasons.
- **Shader helpers** (`shader` module): derive-friendly WGSL binding inference, storage/texture helpers, workgroup derivation, readback paths, pooling, and caching.
- **Pooling/caching**: LRU-bounded pipeline/bind-group caches and temp buffer/texture pools.

## Feature flags
- `gpu-wgpu`: enable the real wgpu backend and shader module.
- `gpu-mock`: deterministic mock backend.
- `gpu-noop`: always-available fallback (default).
- `gpu-async`: async dispatch/readback APIs.

## Key modules
- `shader`: WGSL dispatch helpers, derive-friendly `GpuBindings`, `GpuState`, readback, and caching.
- `wgpu_backend`: actual wgpu device/queue management (feature-gated).
- `convert`: payload helpers for CPU↔GPU images/buffers.
- `handles`: opaque buffer/image IDs and allocation helpers.

## Typical use
- Select a backend: `GpuContextHandle::select_backend`.
- Derive bindings: annotate a struct with `#[derive(GpuBindings)]` and `#[gpu(spec(...))]`.
- Dispatch: create `ShaderContext`, call `ctx.dispatch_bindings`/`dispatch_auto`.
- Readback: use `ShaderRunOutput` helpers to interpret buffers/textures or get `GpuImageHandle`.

## Testing
- No-GPU path works everywhere; mock backend available with `gpu-mock`.
- GPU tests/examples require `--features gpu-wgpu` and a compatible device.
