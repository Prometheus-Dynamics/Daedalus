//! Host-side FFI plugin installation and runner orchestration.
//!
//! This crate owns the shared installer, runner pool, persistent worker process runner, response
//! decoding, state synchronization, and registry schema export surface.
//!
//! [`FfiHost`] is the recommended entry point: it installs a `PluginPackage` into a
//! `CapabilityRegistry`, starts its persistent-worker runners, and invokes nodes by id.

mod conformance;
mod ffi_host;
mod installer;
mod process;
mod response;
mod runner_pool;
mod schema_export;
mod state;

#[cfg(test)]
mod payload_ownership_tests;
#[cfg(test)]
mod test_support;

pub use conformance::{
    FixtureHarnessError, FixtureHarnessReport, run_canonical_generated_fixture_harness,
    run_generated_fixture_harness, run_scalar_add_generated_fixture_harness,
};
pub use daedalus_ffi_core as core;
pub use ffi_host::{FfiHost, FfiHostBuilder, FfiHostError, PersistentWorkerRunnerFactory};
pub use installer::{
    BackendRunnerFactory, HostInstallError, HostInstallPlan, install_language_package,
    install_language_package_with_ffi_telemetry, install_language_schema, install_package,
    install_package_with_ffi_telemetry, install_plan_runners, install_schema,
    install_schema_with_backends, node_decl_from_schema, node_decls_from_schema,
    plugin_manifest_from_schema, port_decl_from_schema,
};
pub use process::PersistentWorkerRunner;
pub use response::{DecodedInvokeResponse, ResponseDecodeError, decode_response};
pub use runner_pool::{
    BackendRunner, FfiHostTelemetry, PayloadLease, PayloadLeaseScope, PayloadLeaseTable,
    RunnerHealth, RunnerKey, RunnerLimits, RunnerPool, RunnerPoolError, RunnerPoolOptions,
    RunnerPoolTelemetry, RunnerRestartPolicy, RunnerShutdownError, RunnerShutdownFailure,
};
pub use schema_export::{
    SchemaExportError, export_registry_plugin_schema, export_registry_plugin_schema_json,
    export_snapshot_plugin_schema, node_schema_from_decl, plugin_schema_from_manifest,
};
pub use state::{
    StateSyncError, StateSyncPolicy, StateSyncResult, export_runner_state, sync_response_state,
};
