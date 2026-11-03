# Accelerator Interface

`daedalus-accelerator` is the interface Daedalus nodes use to drive an NPU or a custom
accelerator: load a program, allocate or import buffers, submit, and wait on a fence. It is the
interface only. No accelerator implements it yet; RKNPU (RK3588S / RK3576), the K230 KPU and the
Coral Edge TPU are the expected first ones, and each needs the user's go. They will be our own
drivers, submitting at the register or ioctl level over Lemnos's transports, not vendor runtimes
(HeliOS TODO "AI accelerators: our own stack rather than vendor runtimes").

The split is the same as for the GPU (TODO "Dependency layers"):
- Daedalus decides whether and how data moves between devices.
- A device's own copies stay in its driver.
- Lemnos owns DMA buffer basics and bus transfers.

## Shape

```text
Accelerator (object safe, Arc<dyn Accelerator>)
  id()            -> AcceleratorId        stamped on every handle it makes
  info()          -> AcceleratorInfo      kind, program formats, dmabuf import, sync_fd export
  load_program(ProgramDesc{format, image, options}) -> ProgramHandle
  program_info(&ProgramHandle)            -> ProgramInfo{inputs, outputs: [SlotInfo{slot, name, len}]}
  buffer(BufferSource)                    -> BufferHandle
      Allocate{len} | Copy(&[u8]) | DmaBuf{fd, offset, len, acquire: Option<sync_file>}
  read(&BufferHandle, offset, &mut [u8])
  submit(Submission{program, bindings: [Binding{slot, buffer, access}], waits: [Wait]})
                                          -> FenceHandle (Arc<dyn Fence>)
Fence: is_signaled(), wait(timeout) -> Result, export_sync_fd() -> Option<OwnedFd>
Wait:  Fence(FenceHandle) | SyncFd(OwnedFd)
```

## Decisions

- **Handles, not generics.** The trait is object safe, so a node holds `Arc<dyn Accelerator>`
  and plugins built separately can call it.
  - Handles are `Handle<Program>` and `Handle<Buffer>`. Each carries its owner's
    `AcceleratorId`, the owner's object id, a length, and a release callback that runs once
    when the last clone drops.
  - Nothing is downcast through `Any`; the GPU path avoids `Any` for the same reason, since it is
    unreliable across dynamic libraries.
  - An accelerator refuses another instance's handles (`ForeignHandle`).
- **Release is deferred to the device.** When the last handle drops, the release callback runs.
  The implementation frees the object only after work using it completes, because a submission
  keeps clones of its bindings' handles until its fence signals (as the contract test's
  accelerator does).
- **Zero-copy import is a dmabuf plus an acquire fence.**
  - `BufferSource::DmaBuf` hands over the fd and an optional `sync_file`. This mirrors
    `GpuBackend::import_dmabuf`, so a camera frame (`SharedFrame` planes, Styx buffers) reaches
    the NPU as it reaches the GPU.
  - The accelerator waits on the acquire fence on the device where it can. Otherwise it waits
    on the CPU before the first submission that reads the buffer.
  - The interface does not invent a frame type: a node maps a frame plane to a `DmaBuf` source.
- **Completion is a fence, and fences cross devices.**
  - `submit` returns as soon as the work is queued.
  - A `Fence` can be polled, waited on with a timeout, or exported as a `sync_file`. The GPU
    path's `AcquireFenceWait::SyncFd` imports that file, so NPU → GPU needs no CPU wait, and
    `Wait::SyncFd` covers GPU → NPU.
  - A failed submission or a failed upstream wait surfaces as the fence's `wait` result.
- **Validation is shared.** `validate_bindings` holds the checks every accelerator makes:
  - each slot is bound exactly once;
  - the buffer is large enough;
  - inputs are read and outputs are written.
- **Programs are opaque bytes plus a format tag.**
  - The tag is one of `AcceleratorInfo::program_formats`: our own compiled command-stream format
    for each device, produced by our own compiler, not a vendor model file.
  - Loading is done once, when the graph is built.

## In a graph

These parts are not built yet; they are for the first implementation.

- **Residency.** The accelerator gets its own residency key, so its buffers are a distinct port
  type, the same way the CUDA backend keys CUDA memory apart from wgpu memory (TODO "CUDA
  backend"). Upload and download adapters are `buffer(Copy)` and `read`.
- **Device selection.** A node gets the accelerator as an `ExecutionContext` capability, and the
  engine opens one instance per device per process, as `shared_context` does for the GPU.
- **Fences between nodes.** A node passes a `FenceHandle` alongside its output buffer. The
  consumer uses it as a `Wait` instead of blocking, and only a CPU consumer waits.

## Mapping onto the expected devices

Each implementation is our own driver behind the trait. Lemnos provides the transport: DRM ioctls,
MMIO register access, USB, and the DMA buffers. No vendor runtime (`librknnrt`, nncase,
`libedgetpu`) is linked or loaded.

- **RKNPU (RK3588S / RK3576), through the kernel's NPU DRM driver.**
  - `load_program`: validates our command-stream image for the NPU's cores.
  - `buffer`: the driver's GEM buffer objects, created through Lemnos's DRM ioctls. `DmaBuf`
    imports a PRIME fd, so a camera frame reaches the NPU without a copy.
  - `submit`: the driver's submit ioctl, with the task list and the bound buffer objects.
  - Completion: a DRM syncobj where the driver signals one, exported as a `sync_file`.
    Otherwise a completion thread waits on the job and signals a `CpuFence`.
- **K230 KPU, through its registers.**
  - Lemnos maps the KPU's MMIO window and the shared memory it reads from.
  - `load_program`: places our instruction stream and its constants in that memory.
  - `buffer`: carves input and output tensors from the same memory. `DmaBuf` is accepted where
    the buffer is physically contiguous and visible to the KPU.
  - `submit`: writes the start registers.
  - Completion: the KPU's done interrupt, through Lemnos's interrupt or uio wait, signals a
    `CpuFence`.
- **Coral Edge TPU, over Lemnos's USB.**
  - `load_program`: our compiled instruction bitstream and parameters, sent as the device
    protocol's bulk transfers.
  - `buffer`: host memory staged per transfer (`Copy` / `Allocate`). The TPU cannot read a
    dmabuf in place, so `info().dmabuf_import` is `false`.
  - `submit`: queues the input bulk-out and output bulk-in transfers.
  - Completion: the output transfer's completion signals a `CpuFence`.

## Testing

`crates/accelerator/tests/contract.rs` defines a small accelerator inside the test (it adds two
byte vectors on a thread) and checks the contract:
- a submission waits for its upstream fence and signals its own;
- a timeout returns `Timeout`, and an upstream failure fails the submission;
- read-back works, and buffers are released with their last handle;
- every refusal is covered: unknown format, bad image, unbound or twice-bound slot, short
  buffer, wrong access, unknown slot, foreign handle, out-of-range read, unsupported dmabuf.

A real implementation should pass the same cases on its device.
