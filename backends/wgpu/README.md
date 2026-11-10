# daedalus-wgpu

The GPU backend, on wgpu.

- `WgpuBackend::open(options)` finds an adapter and opens a device the first time something runs on it. Planning opens nothing.
- `WgpuBackend::adopt(handle)` uses a device someone else set up. Whoever made that device owns the rule below.
- Every instance and device is made under one process-wide lock (`driver_lock`). The Vulkan loader crashes when two threads set up drivers at once with an NVIDIA ICD installed beside Mesa. `scripts/check-gpu-driver-lock.sh` fails on any creation outside `src/driver.rs`.
- One device per process by default. A second opens only when `Options::adapter` names one.
- The backend doesn't read the environment. A host that honours `WGPU_BACKEND` parses it with `Apis::from_names` and passes the result in. wgpu itself reads `WGPU_DEBUG=1`, which turns debug labels on for a capture; they're off otherwise because labels race driver setup.

Tests skip when the machine has no adapter. With `DAEDALUS_GPU_REQUIRED=1`, as in `.github/workflows/ci-gpu.yml`, a missing adapter is a failure.
