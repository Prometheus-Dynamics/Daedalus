# FFI Ergonomics Review

This release keeps the low-level FFI pieces explicit, but the common path should stay short:

1. Build or read a `PluginPackage`.
2. Install its schema into a `PluginRegistry`.
3. Install supported runners into a `RunnerPool`.
4. Invoke through `RunnerPool::invoke` and inspect `FfiHostTelemetry` only when needed.

## Current Simple Path

Use `install_package_with_ffi_telemetry` when callers need registry installation plus runtime diagnostics from the same telemetry handle. Use `install_plan_runners` only when the caller has a custom `BackendRunnerFactory` or wants to defer runner startup.

The intended release shape is:

```rust
let telemetry = FfiHostTelemetry::new();
let plan = install_package_with_ffi_telemetry(&mut registry, &package, &telemetry)?;
let mut pool = RunnerPool::new().with_ffi_telemetry(telemetry.clone());
install_plan_runners(&mut pool, &plan, &factory)?;
let response = pool.invoke(&backend_config, request)?;
```

That is still more verbose than the eventual one-call host API, but it keeps responsibilities understandable: package validation and registry state are separate from process lifetime and payload lease lifetime.

## Release Guidance

- Keep package/schema helpers as the default entry point. Do not make users manually walk `PluginSchema`, backend maps, runner keys, and node declarations for normal installs.
- Keep `RunnerPool` responsible for runner lifecycle, health checks, telemetry, and payload leases.
- Keep `HostInstallPlan` as the boundary object between static package validation and runtime runner creation.
- Keep `FfiHostTelemetry` shareable. Callers should be able to create it once and pass clones through install, pool, and runner layers.
- Prefer typed ids already exposed by the runtime (`HostAlias`, `PortId`, `RunnerKey`, `TypeKey`) over raw strings for stored state. Public APIs can still accept strings through `Into` or `AsRef` for ergonomics.

## Follow-Up API Shape

The next simplification should be an opt-in convenience wrapper that owns the registry install plan and runner pool together:

```rust
let host = FfiHost::install_package(&mut registry, package, factory)?;
let response = host.invoke("node:id", request)?;
```

That wrapper should be a thin composition over the current `HostInstallPlan`, `RunnerPool`, and telemetry types rather than a second lifecycle system.
