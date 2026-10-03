# daedalus-gpu

GPU-facing handles, backend selection, and shader helpers.

## Owns

- GPU buffer/image handles and ids,
- memory location, usage, format, and capability descriptors,
- noop, mock, and `wgpu` backend selection,
- buffer pools and transfer statistics,
- optional async backend trait,
- WGSL shader dispatch, staging, readback, and resource helpers behind `gpu-wgpu`.

Use `gpu-mock` for deterministic tests. Use `gpu-wgpu` only where hardware and drivers are available. Planner/runtime GPU behavior is enabled from facade/runtime/engine features, not by this crate alone.

## Sync and async GPU paths

The synchronous `wgpu` entry points are compatibility APIs for callers that do not own an async
runtime:

- `WgpuBackend::new` and `WgpuBackend::new_with_staging_pool_config` create the backend by
  blocking on their async constructors.
- `select_backend` can reach that same blocking path when it probes the real `wgpu` backend.
- Synchronous shader helpers create the fallback shader context on first use with a blocking wait,
  then perform any requested readback on the current thread.

Async hosts should prefer `WgpuBackend::new_async`, `WgpuBackend::new_with_staging_pool_config_async`,
`select_backend_async`, shader helpers that use `ctx_async`, and async dispatch/readback APIs. Those
paths avoid blocking executor worker threads while the fallback context is created or GPU readbacks
are mapped.

## Async poll worker limits

Async `wgpu` readback maps are driven by a small blocking poll pool so executor threads do not park
inside `Device::poll(Wait)`. By default the pool starts with two workers and a bounded queue sized
to `worker_limit * 64`. When that queue is full, Daedalus may run a poll job on a bounded overflow
thread; the default overflow limit is two. If the queue is saturated and all overflow slots are in
use, the readback future returns an error instead of running blocking polling work inline.

Hosts that need different limits can configure the process before the first async readback:

- `shader::set_async_poll_worker_limit(limit)` sets the shared worker count used when the pool is
  first initialized.
- `shader::set_async_poll_overflow_thread_limit(limit)` sets the maximum temporary overflow thread
  count; use `0` to reject saturated jobs without overflow workers.
- `shader::async_poll_worker_limit`, `shader::async_poll_overflow_thread_limit`, and
  `shader::active_async_poll_overflow_threads` expose the effective configured limits and current
  overflow pressure for diagnostics.

## Importing external frames (dmabuf)

Frames that already live in GPU-importable memory, such as camera buffers on a Raspberry Pi 5 /
CM5 (Mesa `v3dv`) or any Linux dmabuf producer, can become GPU images without a CPU round trip:

```rust,ignore
use daedalus_gpu::{DrmFourcc, ExternalFrameDescriptor, ExternalPlane, DRM_FORMAT_MOD_LINEAR};

if ctx.supports_dmabuf_import() {
    let plane = ExternalPlane::from_borrowed(buffer_fd.as_fd(), offset, stride)?; // dup()s the fd
    let image = ctx.import_dmabuf(
        ExternalFrameDescriptor::single_plane(width, height, DrmFourcc::XRGB8888, plane)
            .with_modifier(DRM_FORMAT_MOD_LINEAR)
            .with_acquire_fence(request_fence) // optional sync_file from the producer
            .with_keepalive(Arc::new(camera_request)), // released when the GPU is done
    )?;
    // `image` is a normal GpuImageHandle: sample it, dispatch shaders on it, read it back.
} else {
    tracing::info!(reason = ?ctx.dmabuf_import_support().reason(), "falling back to CPU upload");
}
```

- **Capability query:** `dmabuf_import_support()` / `supports_dmabuf_import()` on
  `GpuContextHandle` and `GpuBackend` never panic and give a reason when unsupported (no GPU,
  non-Vulkan adapter, missing extensions, non-Linux, or `gpu-dmabuf` not built).
- **Backends:** `gpu-mock` validates and records imports (`MockBackend::imported_frames`), waits
  for the acquire fence, and holds the fds/keepalive for the handle's lifetime, so planner/runtime
  paths are testable anywhere; `MockBackend::without_dmabuf_import()` exercises fallbacks. Noop
  returns `Unsupported`. The real path is `gpu-dmabuf` (implies `gpu-wgpu`, Linux + Vulkan only):
  the device is created with `VK_KHR_external_memory_fd`, `VK_EXT_external_memory_dma_buf`, and
  `VK_EXT_image_drm_format_modifier` (plus wgpu's `TEXTURE_FORMAT_NV12`) when the adapter has them,
  and the dmabuf is bound to a `VkImage` with the explicit per-plane layout, then wrapped as a
  `wgpu::Texture`.
- **Formats:** single-plane `R8`, `GR88`, `XRGB8888`/`ARGB8888` (`Bgra8Unorm`), and
  `XBGR8888`/`ABGR8888` (`Rgba8Unorm`); the `X` variants leave alpha undefined. `modifier: None`
  means linear. Single-plane images are sampleable and copy sources; request
  `GpuUsage::{UPLOAD, STORAGE, RENDER_TARGET}` only if you intend to write into the producer's
  buffer.
- **NV12:** pass two planes (Y, then interleaved UV; both in one dmabuf at different offsets, or
  in two dmabufs, which needs a modifier with `DISJOINT` support) and get one
  `GpuFormat::Nv12` image. It is sample-only (wgpu 29 has no NV12 copies, storage, or render
  targets, so `read_texture` returns `Unsupported`): bind `texture_plane_views(&texture)` (Y as
  `R8Unorm`, UV as `Rg8Unorm`) and convert YUV to RGB in the shader. Devices without
  `TEXTURE_FORMAT_NV12` return `UnsupportedFormat`; there, import each plane on its own (`R8`
  for Y, `GR88` for UV) using its offset and stride. Three-plane `YU12` is always imported per
  plane. Drivers constrain explicit plane layouts (RADV rejects a 128-byte `LINEAR` pitch that
  256 bytes satisfies); a rejected layout surfaces as `UnsupportedFormat` naming
  `vkCreateImage`.
- **Explicit sync:** `with_acquire_fence(fd)` takes a `sync_file` (from V4L2/libcamera, a GPU
  producer, or `DMA_BUF_IOCTL_EXPORT_SYNC_FILE`). For producers that only fence the dmabuf itself,
  `with_implicit_fence()` exports its implicit fences (`export_dmabuf_fence` is the standalone
  helper; Linux 6.0+). The wait is **CPU-side**: the import polls the fence (bounded by
  `acquire_timeout`, default 1 s, then `ExternalImportError::FenceTimeout`) before creating the
  Vulkan image, so the calling thread blocks until the producer is done. A GPU-side wait (import
  as a `SYNC_FD` semaphore and make the next submission wait on it) is not expressible through
  wgpu-hal 29, whose queue exposes signal semaphores only.
- **Layout handoff caveat:** there is no queue-family-foreign acquire; wgpu's first barrier
  transitions from `UNDEFINED`, which keeps contents on drivers without compression metadata for
  the imported modifier (v3dv, and RADV/ANV for `LINEAR`). Do not import compressed or
  aux-plane modifiers.
- **Ownership:** planes and the fence take `OwnedFd`s (use `ExternalPlane::from_borrowed` to
  `dup`). The import keeps the dmabuf referenced for the image's lifetime, but that does not stop
  the producer from recycling it, so pass the producer's buffer lease as the keepalive. It is
  dropped only after the handle and all clones are gone and no submitted GPU work uses the image.
  Held images hold producer buffers; drop them promptly.
- **Errors:** `ExternalImportError::{Unsupported, InvalidDescriptor, UnsupportedFormat,
  FenceTimeout, ImportFailed}`, convertible to `GpuError`.

Hardware tests (`#[ignore]`d; need a Vulkan GPU and a readable `/dev/dma_heap/*`, e.g. membership
in the `video` group) cover single-plane import, plane offsets, fences (exported implicit fence,
timeout), and NV12 (single dmabuf and disjoint), skipping NV12 cases the device cannot import:

```bash
CARGO_BUILD_JOBS=4 cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored dmabuf
# On a Pi, pick a heap explicitly if needed:
DAEDALUS_DMA_HEAP=/dev/dma_heap/system cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored dmabuf
```
