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
- Select a backend: `select_backend_async` from async applications, or `select_backend` from sync callers.
- Derive bindings: annotate a struct with `#[derive(GpuBindings)]` and `#[gpu(spec(...))]`.
- Dispatch: create `ShaderContext`, call `ctx.dispatch_bindings_async`/`dispatch_auto_async` from async applications, or `ctx.dispatch_bindings`/`dispatch_auto` from sync callers.
- Readback: use `ShaderRunOutput` helpers to interpret buffers/textures or get `GpuImageHandle`.

## Sync vs async GPU paths
- `select_backend` and `WgpuBackend::new` are synchronous compatibility APIs. They block the current thread while wgpu enumerates adapters and requests a device.
- Prefer `select_backend_async` or `WgpuBackend::new_async` from async applications so backend creation does not park an executor worker thread.
- Synchronous shader dispatch and readback wait for GPU submission/readback completion on the current thread. Use the `gpu-async` feature and the `*_async` shader helpers for async runtimes; async dispatch submits without the synchronous hard-throttle wait and async readback polls cooperatively.
- When no `GpuContextHandle` is supplied, sync shader helpers create the fallback wgpu context with a blocking path; async helpers use the non-blocking fallback path.
- Shader dispatch throttles queued GPU submissions per device to avoid unbounded in-flight work. Use `daedalus_gpu::shader::set_max_inflight_submissions_per_device` at startup to tune that limit for the process; `max_inflight_submissions_per_device` reports the current effective value.
- Async wgpu readback reuses staging buffers through `WgpuStagingPoolConfig`. Use
  `WgpuBackend::new_with_staging_pool_config[_async]` for explicit limits, or set
  `DAEDALUS_WGPU_STAGING_MAX_SIZE_CLASSES`, `DAEDALUS_WGPU_STAGING_MAX_BUFFERS_PER_SIZE`, and
  `DAEDALUS_WGPU_STAGING_MAX_BYTES` before constructing the backend. `staging_pool_stats()` reports
  the active limits and current pool usage.

## Testing
- No-GPU path works everywhere; mock backend available with `gpu-mock`.
- GPU tests/examples require `--features gpu-wgpu` and a compatible device.
