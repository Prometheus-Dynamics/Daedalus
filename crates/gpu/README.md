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
