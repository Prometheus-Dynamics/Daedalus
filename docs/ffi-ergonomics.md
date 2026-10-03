# FFI Ergonomics

The common path for hosting FFI plugins is one call to install a package and one call per invoke.
The lower-level pieces stay public for callers that need custom lifecycle control.

## Recommended Path: `FfiHost`

`daedalus_ffi_host::FfiHost` validates a `PluginPackage`, installs its schema into a
`CapabilityRegistry`, starts its persistent-worker runners, and invokes nodes by id:

```rust
use daedalus_ffi_host::{FfiHost, PersistentWorkerRunnerFactory};

let factory = PersistentWorkerRunnerFactory::new();
let host = FfiHost::install_package(&mut registry, &package, &factory)?;
let response = host.invoke("demo.add", request)?;
```

- `invoke(node_id, request)` resolves the node's backend through the install plan and calls
  `RunnerPool::invoke`. An empty `request.node_id` is filled in; a different one is rejected.
  `invoke_batch` and `health` work the same way.
- Errors are typed (`FfiHostError`): `UnknownNode`, `InProcessNode`, `RunnerNotStarted`,
  `RequestNodeMismatch`, `DuplicateNode`, `Runner { node_id, source }`, `Install(HostInstallError)`,
  and `Shutdown`.
- `add_package(&mut registry, &package, &factory)` installs further packages into the same host.
  They share the runner pool and telemetry; a package whose backend config matches a running
  runner reuses it. Duplicate node ids are rejected before the registry is touched.
- Installs are atomic: when validation, registry install, or runner startup fails, the package's
  registry entries are removed and runners started for that package are shut down. Deferred
  startup rolls back the same way: `start_runners` removes a package whose runners fail to start
  and leaves earlier (running) and later (still pending) packages alone.
- `shutdown()` attempts every runner and reports all failures in one `RunnerShutdownError`.
  Dropping the host also stops every runner through `RunnerPool`'s `Drop`.
- Accessors: `telemetry()`, `plans()`, `plan(plugin_id)`, `plan_for_node(node_id)`,
  `backend(node_id)`, `runner_key(node_id)`, `node_ids()`, `pool()`, and `pool_mut()`.

Options go through the builder:

```rust
let telemetry = FfiHostTelemetry::new();
let mut host = FfiHost::builder()
    .telemetry(telemetry.clone())
    .pool_options(RunnerPoolOptions::default())
    .defer_runner_startup(true)
    .build();
let factory = host.persistent_worker_factory();
host.add_package(&mut registry, &package, &factory)?;
// The registry is populated; workers start here.
host.start_runners(&mut registry, &factory)?;
```

`PersistentWorkerRunnerFactory` builds the built-in `PersistentWorkerRunner` for Python, Node,
Java, and other `persistent_worker` backends. `FfiHost::persistent_worker_factory()` returns one
that shares the host telemetry and pool runner limits. Any other `BackendRunnerFactory` works too.

`in_process_abi` backends are registered in the registry but never started by `FfiHost`, and
invoking them returns `FfiHostError::InProcessNode`. Native Rust `cdylib` plugins are loaded by the
facade `dylib-plugins` feature; see [`dynamic-plugins.md`](dynamic-plugins.md).

## Advanced Path: Plan, Pool, and Telemetry

`FfiHost` is a thin composition over the low-level pieces, which stay available for callers that
manage runner lifetime themselves:

```rust
let telemetry = FfiHostTelemetry::new();
let plan = install_package_with_ffi_telemetry(&mut registry, &package, &telemetry)?;
let mut pool = RunnerPool::new().with_ffi_telemetry(telemetry.clone());
install_plan_runners(&mut pool, &plan, &factory)?;
let response = pool.invoke(&plan.backends["demo.add"], request)?;
```

`install_plan_runners` shuts down the runners it started when a later one fails, but this path
does not roll the registry back on runner failure and invokes by `BackendConfig` rather than node
id.

## Design Guidance

- Keep package/schema helpers as the default entry point. Do not make users manually walk
  `PluginSchema`, backend maps, runner keys, and node declarations for normal installs.
- Keep `RunnerPool` responsible for runner lifecycle, health checks, telemetry, and payload leases.
- Keep `HostInstallPlan` as the boundary object between static package validation and runtime
  runner creation.
- Keep `FfiHostTelemetry` shareable. Callers should be able to create it once and pass clones
  through install, pool, and runner layers.
- Prefer typed ids already exposed by the runtime (`HostAlias`, `PortId`, `RunnerKey`, `TypeKey`)
  over raw strings for stored state. Public APIs can still accept strings through `Into` or `AsRef`
  for ergonomics.
