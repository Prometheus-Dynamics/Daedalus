# daedalus-gpu

GPU-facing handles, backend selection, and shader helpers.

## Owns

- GPU buffer/image handles and ids,
- memory location, usage, format, and capability descriptors,
- noop, mock, and `wgpu` backend selection,
- buffer pools and transfer statistics,
- optional async backend trait,
- WGSL shader dispatch, staging, readback, and resource helpers behind `gpu-wgpu` (Vulkan, plus
  Metal on Apple and DX12 on Windows; `gpu-gles` adds the OpenGL/GLES backend),
- `image` crate bridges (`Compute<DynamicImage>`, `DeviceBridge` for `image` buffers,
  `ShaderRunOutput` image readbacks) behind `gpu-image`.

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
  for the acquire fence on the CPU, and holds the fds/keepalive for the handle's lifetime, so
  planner/runtime paths are testable anywhere; `MockBackend::without_dmabuf_import()` exercises fallbacks. Noop
  returns `Unsupported`. The real path is `gpu-dmabuf` (implies `gpu-wgpu`, Linux + Vulkan only):
  the device is created with `VK_KHR_external_memory_fd`, `VK_EXT_external_memory_dma_buf`, and
  `VK_EXT_image_drm_format_modifier` (plus `VK_KHR_external_semaphore_fd`,
  `VK_EXT_queue_family_foreign`, and wgpu's `TEXTURE_FORMAT_NV12`) when the adapter has them,
  and the dmabuf is bound to a `VkImage` with the explicit per-plane layout, then wrapped as a
  `wgpu::Texture`.
- **Formats:** single-plane `R8`, `GR88`, `XRGB8888`/`ARGB8888` (`Bgra8Unorm`), and
  `XBGR8888`/`ABGR8888` (`Rgba8Unorm`); the `X` variants leave alpha undefined. `modifier: None`
  means linear. Single-plane images are sampleable and copy sources; request
  `GpuUsage::{UPLOAD, STORAGE, RENDER_TARGET}` only if you intend to write into the producer's
  buffer.
- **NV12:** pass two planes (Y, then interleaved UV; both in one dmabuf at different offsets, or
  in two dmabufs, which needs a modifier with `DISJOINT` support) and get one
  `GpuFormat::Nv12` image. It is sample-only (wgpu 30 has no NV12 copies, storage, or render
  targets, so `read_texture` returns `Unsupported`): bind `texture_plane_views(&texture)` (Y as
  `R8Unorm`, UV as `Rg8Unorm`) and convert YUV to RGB in the shader. Devices without
  `TEXTURE_FORMAT_NV12` return `UnsupportedFormat`; there, import each plane on its own (`R8`
  for Y, `GR88` for UV) using its offset and stride. Three-plane `YU12` is always imported per
  plane. Drivers constrain explicit plane layouts (RADV rejects a 128-byte `LINEAR` pitch that
  256 bytes satisfies); a rejected layout surfaces as `UnsupportedFormat` naming
  `vkCreateImage`. Tiled NV12 modifiers take two memory planes like `LINEAR`.
- **Modifiers:** tiled and compressed modifiers import like `LINEAR`: pass one plane per
  *memory* plane of the modifier (the format planes, then aux planes such as AMD DCC metadata),
  each with the offset and pitch the producer reported (`vkGetImageSubresourceLayout` with
  `MEMORY_PLANE_i`, or the GBM/KMS plane list), up to `MAX_MEMORY_PLANES`. They become the
  explicit Vulkan plane layouts as given. Planes in one dmabuf share one allocation; separate
  dmabufs need a modifier with `DISJOINT`. On RADV every renderable `R8`/`XRGB8888` modifier,
  including DCC with 2 and 3 memory planes, round-trips between devices with correct pixels, and
  the tiled `NV12` modifiers import and sample.
- **Explicit sync:** `with_acquire_fence(fd)` takes a `sync_file` (from V4L2/libcamera, a GPU
  producer, or `DMA_BUF_IOCTL_EXPORT_SYNC_FILE`). For producers that only fence the dmabuf itself,
  `with_implicit_fence()` exports its implicit fences (`export_dmabuf_fence` is the standalone
  helper; Linux 6.0+). A fence that already signaled costs nothing. How a pending one is waited
  for is an `AcquireFenceMode`: the backend default (`GpuOptions::acquire_fence_mode` through
  `select_backend`, or `WgpuBackend::set_acquire_fence_mode`; `Auto` unless set), which one import
  can override with `with_acquire_fence_mode(mode)`. The mode resolves to one of the waits the
  device has (`AcquireFenceWait`):
  - `SyncFd` (devices with `VK_KHR_external_semaphore_fd`; RADV, v3dv, lavapipe): the `sync_file`
    is imported into a binary semaphore the GPU waits on. The kernel orders the GPU work behind
    it, so no thread blocks on kernel drivers (lavapipe still holds back the next submission), but
    no timeout applies: a fence that never signals stalls the queue.
  - `Timeline` (devices with Vulkan 1.2 timeline semaphores): the GPU waits on a timeline
    semaphore value that one watcher thread per device signals once the fence signals or
    `acquire_timeout` (default 1 s) passes, so a stuck producer stalls the GPU for at most the
    timeout. The import does not wait for its fence, and `GpuImageHandle::acquire_status()`
    reports `Pending`, `Ready`, or `TimedOut` (the GPU went ahead; the contents are undefined).
    The watcher hop adds about 10 us. On Mesa drivers the *next* submission to the device
    (another import, a dispatch, a readback) blocks its thread until the wait is released (100 ms
    measured with a 100 ms timeout), because Mesa runs wait-before-signal submissions on a submit
    thread and wgpu chains submissions with binary semaphores: the CPU wait moves from the import
    to the next submission, still bounded by the timeout. The validation layer reports that chain
    as `VUID-vkQueueSubmit-pWaitSemaphores-03238`.
  - `Cpu` (always available; the only wait of `gpu-mock`): the import polls the fence, bounded by
    `acquire_timeout` (then `ExternalImportError::FenceTimeout`), before creating the Vulkan
    image.

  | Mode | Wait | Without it |
  |---|---|---|
  | `Auto` (default) | `SyncFd`, else `Timeline`, else `Cpu` | |
  | `SyncFd` | `SyncFd` | `Cpu` |
  | `Timeline` (hard timeout) | `Timeline`; `SyncFd` when `acquire_timeout` is `Duration::MAX` | `Cpu` |
  | `Cpu` | `Cpu` | |

  A fence fd that is not a `sync_file` cannot be imported for `SyncFd`; `Auto` and `Timeline` hand
  it to the timeline watcher where the device has one, otherwise it is waited for on the CPU.
  `dmabuf_import_support()` reports the default mode, the wait it resolves to, and every wait the
  device has (`ExternalImportSupport::Supported { acquire_fence, acquire_fence_mode, fence_waits }`,
  also `acquire_fence_wait()`, `acquire_fence_mode()`, `fence_waits()`).
- **Layout handoff:** every import submits a queue family ownership acquire from
  `VK_QUEUE_FAMILY_FOREIGN_EXT` (`VK_EXT_queue_family_foreign`, else `VK_QUEUE_FAMILY_EXTERNAL`),
  `GENERAL -> SHADER_READ_ONLY_OPTIMAL`, and registers the texture with wgpu in that state, so wgpu
  never transitions it from `UNDEFINED` (which would let the driver discard the contents and
  reinitialize compression metadata). `GENERAL` in the foreign family is the convention of Mesa's
  WSI and of compositors for dmabufs; RADV keeps compressed modifier images readable that way.
  When the last handle is dropped, the image is released back to the foreign family
  (`SHADER_READ_ONLY_OPTIMAL -> GENERAL`) after all GPU work using it, and the keepalive is dropped
  only once that release executed, so a producer that reuses the buffer (and a Vulkan producer
  that acquires it from `FOREIGN`) sees a complete handoff. Textures obtained from the backend
  must not be used after their handle is gone.
- **Ownership:** planes and the fence take `OwnedFd`s (use `ExternalPlane::from_borrowed` to
  `dup`). The import keeps the dmabuf referenced for the image's lifetime, but that does not stop
  the producer from recycling it, so pass the producer's buffer lease as the keepalive. It is
  dropped only after the handle and all clones are gone and no submitted GPU work uses the image.
  Held images hold producer buffers; drop them promptly.
- **Errors:** `ExternalImportError::{Unsupported, InvalidDescriptor, UnsupportedFormat,
  FenceTimeout, ImportFailed}`, convertible to `GpuError`.

Hardware tests (`#[ignore]`d; need a Vulkan GPU and a readable `/dev/dma_heap/*`, e.g. membership
in the `video` group) live in `src/wgpu_backend/dmabuf/tests/`: `LINEAR` single-plane import and
plane offsets (`linear.rs`); fences in every wait mode the device has, never-signaling and late
fences, the watcher's latency and in-order release, a timeout shorter than a real GPU producer
job, the mode selection (default, per-import override, non-`sync_file` fallback), and dropping
the backend with a stuck fence (`fences.rs`; with lavapipe as the consumer the
queues are independent, so a missing wait shows as stale pixels); NV12 in one dmabuf and disjoint
(`nv12.rs`); and every renderable modifier exported by a raw Vulkan producer, rendered through
Daedalus, released, and read back on a second device, plus tiled NV12 imports (`modifiers.rs`).
Cases the device cannot run print a `skipping:` reason:

```bash
CARGO_BUILD_JOBS=4 cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored dmabuf
# On a Pi, pick a heap explicitly if needed:
DAEDALUS_DMA_HEAP=/dev/dma_heap/system cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored dmabuf
```

### Probing a device

The `gpu_probe` example prints a paste-friendly `key: value` report of the selected adapter and
driver, `dmabuf_import_support()` with the default fence mode, the wait it resolves to (`sync_fd`,
`timeline`, `cpu`) and which waits the device has, NV12 support (`TEXTURE_FORMAT_NV12`, the `LINEAR` modifier and whether
it needs disjoint planes), the modifiers advertised for `R8`, `XRGB8888`, `XBGR8888` and `NV12`
(memory planes, aux planes, features, and which the hardware tests round-trip), the kernel, the
dma-heaps, and whether `DMA_BUF_IOCTL_EXPORT_SYNC_FILE` works. It never panics on missing
hardware.

```bash
cargo run -p daedalus-gpu --features gpu-dmabuf --example gpu_probe
./scripts/ci.sh pi   # the hardware tests above, then the probe
```

See "Validating on a Raspberry Pi 5" in [docs/testing.md](../../docs/testing.md) for setup and
what each line means, and "Vulkan Validation Layers" there for `./scripts/ci.sh vvl`, which runs
the hardware tests and the probe under `VK_LAYER_KHRONOS_validation` with synchronization
validation.

### Concurrency

Creating wgpu instances, enumerating adapters and opening devices is serialized process-wide
inside `daedalus-gpu`: the Vulkan loader crashes when two threads set up instances at once with
some ICD combinations (seen with the NVIDIA ICD installed next to Mesa). Instances are also
created without wgpu's `DEBUG` flag (object names and labels) unless `WGPU_DEBUG=1`: the loader's
`VK_EXT_debug_utils` terminators, which wgpu calls on every submission with that flag, race with
instance and device creation in other threads. Applications that create their own wgpu or Vulkan
instances on other threads are not covered by the lock.
