# daedalus-engine

Application-facing facade over registry, planner, and runtime.

## Owns

- `EngineConfig` and runtime/planner configuration wiring,
- `Engine` construction,
- plan preparation, runtime-plan build, and execution entry points,
- cache metrics and cache lifecycle,
- `HostGraph` helpers for retained host input/output bindings,
- high-level run results and error normalization.

Use this crate when an application wants the normal end-to-end host path. Use planner/runtime directly when you need lower-level control over declarations, plan construction, or executor behavior.

## Reuse Model

Create one `Engine`, keep registry and handler state alive, and reuse the engine across related runs. Cache hits depend on graph shape, relevant config, and registry-derived planning input. Call `clear_caches` only when the host intentionally wants to drop warm state.

## Runtime Configuration

Prefer owned `EngineConfig` values over environment-only configuration in release-facing hosts. The runtime section carries backpressure, metrics, host event recording, stream worker idling, pool sizing, and debug flags per engine instance:

```rust
use std::time::Duration;

use daedalus_engine::EngineConfig;
use daedalus_runtime::{BackpressureStrategy, MetricsLevel, RuntimeDebugConfig};

let config = EngineConfig::default()
    .with_backpressure(BackpressureStrategy::BoundedQueues)
    .with_metrics_level(MetricsLevel::Detailed)
    .with_host_event_recording(true)
    .with_host_event_limit(Some(1024))
    .with_stream_idle_sleep(Duration::from_millis(2))
    .with_pool_size(4)
    .with_runtime_debug_config(RuntimeDebugConfig {
        node_cpu_time: true,
        pool_size: Some(4),
        ..RuntimeDebugConfig::default()
    });
```

Environment parsing via `EngineConfig::from_env` is available behind the `config-env` feature for process-level defaults. Hosts that need different behavior per engine should construct `EngineConfig` directly and pass it into `Engine::new`.
