# daedalus-runtime Plan

Purpose: turn planner output into an executable runtime plan and host orchestrator/scheduler/edge policies. GPU-aware segments run via `daedalus-gpu` contexts; CPU-only path remains first-class. Focus on swappable policies and deterministic behavior.

Deliverables
- Types/modules: plan builder (ExecutionPlan -> RuntimePlan), orchestrator/scheduler, edge policy types, state/context, metrics, GPU segment executor, snapshot support.
- Policies: pluggable edge policies (FIFO/bounded FIFO/newest-wins/broadcast/priority), scheduler hooks, backpressure options.
- Context: focused node execution context (state, channels, tokens, metadata, optional GPU handle) with snapshot/metrics hooks behind features.
- GPU integration: per-segment executors using `daedalus-gpu` contexts; CPU fallback path is first-class.

Milestones / Task List
- [x] Phase 0 (skeleton): module tree + docs with contracts; stub RuntimePlan builder; feature flags (gpu/metrics/snapshots/plugins/ffi) documented.
- [x] Phase 1 (plan builder)
  - [x] Add runtime plan goldens (cpu-only, gpu-segment grouping) from ExecutionPlan input; ensure metadata deterministic.
  - [x] Expose debug/serde helper for RuntimePlan (pretty JSON) similar to planner.
- [x] Phase 2 (edge policies + scheduler)
  - [x] Implement policy trait + baseline policies (FIFO, bounded FIFO, newest-wins, broadcast) with unit tests.
  - [x] Wire policy selection into plan builder; carry policy metadata. (default policy in plan, applied to edges)
  - [x] Scheduler/orchestrator wiring with backpressure hooks; golden/unit for ordering under policies. (schedule_order emits compute-priority ordering; backpressure flag present)
- [x] Phase 3 (node context + state)
  - [x] Define execution context (state, channels, tokens, metadata, optional GPU handle) with feature-gated metrics/snapshots. (state store + execution context scaffold added)
  - [x] Snapshot/restore hooks behind feature; deterministic warning/error surfaces. (snapshot manager + test added)
- [x] Phase 4 (GPU integration)
  - [x] Segment executor using `daedalus-gpu` contexts; mock backend coverage; CPU fallback validation.
  - [x] Telemetry on GPU usage (behind feature).
- [x] Phase 5 (telemetry/ops)
  - [x] Runtime warnings/metrics export behind feature; structured errors compatible with planner diagnostics.
  - [x] Integration smoke tests (CPU + gpu-mock) executing a small graph; deterministic ordering for metrics/warnings.

Hardening / Follow-ups
- [x] Edge policies: drop Priority variant for now; `EdgePolicyKind` is `#[non_exhaustive]` and ready for per-edge overrides when planner emits them.
- [x] Scheduler/backpressure: replace the bool with a BackpressureStrategy enum (captured in RuntimePlan metadata); bounded/enforced queues remain a future hook.
- [x] GPU gating: ensure `gpu` feature guards compile (ExecutionContext alias, executor) and feature-matrix script for `--no-default-features`, `--features gpu,gpu-mock`; added gpu-mock executor smoke.
- [x] Executor concurrency: add a parallel runner that respects segment dependencies derived from edges; keep serial as default for determinism.
- [x] Telemetry export: derive Serialize/Deserialize on ExecutionTelemetry and add a gpu-mock executor smoke test to validate GPU-preferred and GPU-required paths.
- [x] State ergonomics: surface typed accessors/errors for StateStore (typed get/set helpers).
- [x] Dependency hygiene: drop unused deps (e.g., `daedalus-data`).
- [x] GPU feature gating: ensure executor compiles without `gpu` (guard `GpuContextHandle` uses) and keep gpu-mock test passing; re-run feature matrix.
- [x] Enforce edge policies/backpressure: add per-edge queues (bounded ring buffer for `Bounded`, drop-oldest for `NewestWins`, fan-out for `Broadcast`) and route executor through them; honor `BackpressureStrategy` metadata.
- [x] Parallel executor determinism/perf: honor `schedule_order` when spawning segments; avoid extra node clones where possible (shared handler/state).
- [x] Error ergonomics: replace `Result<(), String>` in `NodeHandler` with a structured, non_exhaustive error for diagnostics.
- [x] Telemetry polish: deduplicate warnings.
- [x] Real data flow: executor uses per-edge payload queues (bounded, newest-wins overwrite, broadcast/fifo append) so policies apply to payloads, not just tokens.
- [x] Threading model: cap parallel executor concurrency to available cores and prefer `schedule_order` when spawning, reducing thread explosion while keeping determinism.
- [x] Error context: attach node IDs to `ExecuteError::HandlerFailed`.
- [x] Feature checks: keep feature-matrix script; executor builds with/without `gpu` and gpu-mock test path remains covered.
- [x] State/telemetry robustness: make StateStore return errors on lock/serde failure (instead of swallowing) and include node/segment ids in telemetry warnings.
- [x] Queue payload types: replace placeholder payloads with typed `EdgePayload` enum so backpressure applies to real data without JSON allocations.
- [x] Threading/backend option: add a feature-gated thread pool/rayon backend for the parallel executor to avoid spawning unbounded threads; keep deterministic ordering via rank tie-breaks.
- [x] Diagnostics polish: include segment ids alongside node ids in `ExecuteError`/telemetry warnings for clearer traces.
- [x] Performance hygiene: use smallvec for warnings; queues remain lightweight; revisit ring buffers if profiling shows a hotspot.
- [x] Policy exhaustiveness: make `apply_policy` exhaustively match `EdgePolicyKind` (no silent `_`) and add an explicit unreachable for future variants.
- [x] StateStore symmetry: make `set` return `Result<(), String>` (like `set_typed`/`dump_json`) so lock errors surface consistently.
- [x] Payload future-proofing: add an `Any` placeholder variant for future typed payloads to avoid JSON in hot paths.
- [x] Pool reuse: optionally reuse a rayon pool (feature-gated) to avoid rebuilding per call if runtime hot-paths call `run_parallel_pool` often.
- [x] Strict StateStore API: add `get_result`/`set_result` (or make `get` return Result) so lock/serde errors are never swallowed; keep `get` as a convenience wrapper if desired.
- [x] Pool configurability: allow pool size override via env/CLI when `executor-pool` is enabled; default to available parallelism.
- [x] Payload generalization: add runtime-owned `NodeIo` and replace `EdgePayload` placeholder with a real generic/`Any` carrier; update handlers/tests so policies/backpressure act on real data.
- [x] Perf follow-up: add bounded/drop-oldest queue implementation; ring-buffer swap can be introduced if profiling demands further optimization.
- [x] Refine payload type: decide on a single efficient carrier (`Arc<[u8]>`); remove unused variants to avoid confusion and cloning overhead.
- [x] NodeIo per-port API: expose inputs/outputs keyed by port name and ensure push supports per-port selection; add tests that enforce policy behavior per port.
- [x] Ring buffer optimization: swap bounded queue implementation to a fixed-size ring buffer for better cache behavior.
- [x] Node/ID borrowing: avoid cloning node IDs in executor paths; store nodes in `Arc<[RuntimeNode]>` and pass `&str` for diagnostics/warnings to reduce allocations.
- [x] Test matrix run: execute `scripts/check-runtime-features.sh`, `cargo test -p daedalus-runtime`, and `cargo test -p daedalus-runtime --features gpu-mock,executor-pool` to validate gating and new I/O surface.
- [x] Enforce BackpressureStrategy: executor/NodeIo should honor `RuntimePlan.backpressure` (drop-with-warning vs block/error) so bounded mode is observable; add targeted overflow tests.
- [x] Drain semantics: let NodeIo expose all pending payloads per port (iter/drain API) instead of just the first item; add port-aware tests to prevent silent drops under bursty producers.
- [x] Parallel invariants: add tests/property checks that serial, scoped, and pooled executors yield identical policy behavior/order (using `schedule_order` as tie-break) to guard regressions.
- [x] Payload extensibility (feature-gated): allow swapping `EdgePayload` to a typed carrier (e.g., data `ValueRef`) to avoid `Arc<[u8]>` cloning when richer data is available; keep default lean.
- [x] Add error-mode backpressure: introduce `ErrorOnOverflow` to surface bounded-queue overflow explicitly; test coverage in backpressure suite.
- [x] Executor modularization: split executor into focused modules (mod/errors/handler/payload/queue/telemetry/serial/parallel/pool) to keep files under ~300 lines and isolate concerns.
- [x] Queue storage options: add optional `lockfree-queues` feature using crossbeam ArrayQueue for bounded edges; default is per-edge mutex storage.
Testing/Quality
- Unit tests for edge policies, scheduler decisions, snapshot store, GPU mock path.
- Integration smoke: CPU runtime path, GPU mock path, snapshot/restore; deterministic ordering for metrics/warnings where applicable.

Notes
- GPU backend selection is pipeline-owned; runtime accepts injected backend options (env/CLI) and propagates them to GPU segments.

Definition of Done
- RuntimePlan builder is pure/deterministic with golden coverage; edge policies and scheduler decisions are unit-tested.
- CPU and gpu-mock paths execute smoke graphs; snapshot/metrics features compile on/off cleanly.
- Node context exposes only the documented surfaces (state/channels/tokens/metadata/GPU handle) with structured runtime diagnostics.

Backlog / Considerations
- [x] Add feature-matrix compile check script for runtime similar to planner’s.
- Policy-aware scheduler may need priority/segment hints from planner; adjust when available.
