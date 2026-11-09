//! The canonical FFI fixtures (`daedalus-ffi-core`) run against the Rust SDK's worker: the host
//! starts this binary as a persistent worker (`--worker conformance`) and invokes each fixture's
//! node twice, checking outputs and events against the fixture's expected response.
//!
//! The fixtures this worker serves are listed in [`SERVED`]; the harness prints the rest of the
//! canonical Rust fixtures as not served. A served fixture missing from the canonical set fails the
//! run. Run with `cargo test -p daedalus-ffi-rust --test conformance`.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use daedalus::macros::node;
use daedalus::runtime::NodeError;
use daedalus::{PluginRegistry, declare_plugin};
use daedalus_ffi_core::{
    BackendConfig, BackendKind, FixtureLanguage, InvokeEventLevel, generate_canonical_fixtures,
};
use daedalus_ffi_host::{
    HostInstallPlan, PersistentWorkerRunnerFactory, RunnerPool, install_plan_runners,
};
use daedalus_ffi_rust::{Worker, emit_event};
use std::collections::BTreeMap;

/// Canonical fixtures this worker serves, by fixture name.
const SERVED: &[&str] = &[
    "scalar_add",
    "bytes_echo",
    "multi_output",
    "raw_io",
    "typed_failure",
];

#[node(id = "add", inputs("a", "b"), outputs("out"))]
fn scalar_add(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a + b)
}

#[node(id = "run", inputs("payload"), outputs("out"))]
fn bytes_echo(payload: Vec<u8>) -> Result<Vec<u8>, NodeError> {
    Ok(payload)
}

#[node(id = "split", inputs("value"), outputs("double", "label"))]
fn multi_output(value: i64) -> Result<(i64, String), NodeError> {
    Ok((value * 2, format!("value:{value}")))
}

#[node(id = "event", inputs("payload"), outputs("out"))]
fn raw_io_event(payload: Vec<u8>) -> Result<Vec<u8>, NodeError> {
    let bytes = payload.len();
    emit_event(
        InvokeEventLevel::Info,
        "raw payload accepted",
        BTreeMap::from([("bytes".into(), serde_json::json!(bytes))]),
    );
    Ok(payload)
}

#[node(id = "run", inputs("value"), outputs("ok"))]
fn typed_failure(value: i64) -> Result<bool, NodeError> {
    if value < 0 {
        emit_event(
            InvokeEventLevel::Error,
            "value must be non-negative",
            BTreeMap::from([
                ("code".into(), serde_json::json!("invalid_argument")),
                ("field".into(), serde_json::json!("value")),
            ]),
        );
        return Ok(false);
    }
    Ok(true)
}

declare_plugin!(ScalarAddPlugin, "ffi.conformance.scalar_add", [scalar_add]);
declare_plugin!(BytesEchoPlugin, "ffi.conformance.bytes_echo", [bytes_echo]);
declare_plugin!(
    MultiOutputPlugin,
    "ffi.conformance.multi_output",
    [multi_output]
);
declare_plugin!(RawIoPlugin, "ffi.conformance.raw_io", [raw_io_event]);
declare_plugin!(
    TypedFailurePlugin,
    "ffi.conformance.typed_failure",
    [typed_failure]
);

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--worker") {
        if let Err(err) = worker() {
            panic!("conformance worker failed: {err}");
        }
        return;
    }
    run_harness();
}

fn worker() -> std::io::Result<()> {
    let mut registry = PluginRegistry::new();
    for install in [
        |r: &mut PluginRegistry| r.install(&ScalarAddPlugin::new()),
        |r: &mut PluginRegistry| r.install(&BytesEchoPlugin::new()),
        |r: &mut PluginRegistry| r.install(&MultiOutputPlugin::new()),
        |r: &mut PluginRegistry| r.install(&RawIoPlugin::new()),
        |r: &mut PluginRegistry| r.install(&TypedFailurePlugin::new()),
    ] {
        install(&mut registry).map_err(std::io::Error::other)?;
    }
    Worker::new(registry)
        .map_err(std::io::Error::other)?
        .with_worker_id("rust-conformance")
        .serve()
}

fn run_harness() {
    let exe = std::env::current_exe().expect("test binary path");
    let fixtures = generate_canonical_fixtures().expect("canonical fixtures");
    let canonical_names: Vec<String> = fixtures
        .iter()
        .filter(|fixture| fixture.language == FixtureLanguage::Rust)
        .map(|fixture| fixture_name(&fixture.schema.nodes[0].id))
        .collect();
    let mut served = Vec::new();
    for mut fixture in fixtures
        .into_iter()
        .filter(|fixture| fixture.language == FixtureLanguage::Rust)
    {
        let node = fixture.schema.nodes[0].clone();
        let name = fixture_name(&node.id);
        if !SERVED.contains(&name.as_str()) {
            continue;
        }
        // The canonical Rust fixture is in-process; this run serves it from a persistent worker.
        let backend = BackendConfig::persistent_worker(
            BackendKind::Rust,
            exe.display().to_string(),
            node.entrypoint.clone(),
        )
        .with_args(["--worker", "conformance"]);
        fixture.backends = BTreeMap::from([(node.id.clone(), backend.clone())]);
        let plan = HostInstallPlan::from_schema_and_backends(&fixture.schema, &fixture.backends)
            .expect("install plan");
        let mut pool = RunnerPool::new();
        install_plan_runners(&mut pool, &plan, &PersistentWorkerRunnerFactory::new())
            .expect("start the worker");
        for round in ["first", "second"] {
            let response = pool
                .invoke(&backend, fixture.request.clone())
                .unwrap_or_else(|err| panic!("{name} {round} invoke: {err}"));
            assert_eq!(
                response.outputs, fixture.expected_response.outputs,
                "{name} {round} outputs"
            );
            assert_eq!(
                response.events, fixture.expected_response.events,
                "{name} {round} events"
            );
        }
        pool.shutdown(&backend).expect("shutdown");
        println!("conformance {name} ... ok");
        served.push(name);
    }
    let skipped: Vec<String> = canonical_names
        .iter()
        .filter(|name| !SERVED.contains(&name.as_str()))
        .cloned()
        .collect();
    let mut missing: Vec<&str> = SERVED
        .iter()
        .copied()
        .filter(|name| !served.iter().any(|served| served == name))
        .collect();
    missing.sort_unstable();
    assert!(
        missing.is_empty(),
        "served fixtures not found in the canonical set: {missing:?}"
    );
    println!(
        "conformance: {} served, {} not served by this worker: {}",
        served.len(),
        skipped.len(),
        skipped.join(", ")
    );
}

/// The fixture name of a node id: `ffi.conformance.<name>:<node>`.
fn fixture_name(node_id: &str) -> String {
    node_id
        .strip_prefix("ffi.conformance.")
        .and_then(|rest| rest.split(':').next())
        .unwrap_or(node_id)
        .to_string()
}
