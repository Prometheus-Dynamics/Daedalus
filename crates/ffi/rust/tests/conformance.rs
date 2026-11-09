//! The canonical FFI fixtures (`daedalus-ffi-core`), all 18 of them, run against the Rust SDK's
//! worker. The host starts this binary as a persistent worker (`--worker conformance`) and invokes
//! each fixture's node twice, checking outputs, events and state against the fixture's expected
//! response.
//!
//! The worker's nodes live in `conformance_nodes`. A fixture whose expected value is the CPU form of
//! a GPU or artifact computation (the shader fixtures) is served by that CPU form; `conformance_nodes`
//! says so where it applies. Run with `cargo test -p daedalus-ffi-rust --test conformance`.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod conformance_nodes;

use std::collections::BTreeMap;

use daedalus::PluginRegistry;
use daedalus_ffi_core::{
    BackendConfig, BackendKind, FixtureLanguage, GeneratedLanguageFixture,
    generate_canonical_fixtures,
};
use daedalus_ffi_host::{
    HostInstallPlan, PersistentWorkerRunnerFactory, RunnerPool, install_plan_runners,
};
use daedalus_ffi_rust::Worker;

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
    conformance_nodes::install_all(&mut registry).map_err(std::io::Error::other)?;
    Worker::new(registry)
        .map_err(std::io::Error::other)?
        .with_worker_id("rust-conformance")
        .with_state(
            "ffi.conformance.stateful_counter:run",
            conformance_nodes::decode_counter,
        )
        .serve()
}

fn run_harness() {
    let exe = std::env::current_exe().expect("test binary path");
    let fixtures: Vec<GeneratedLanguageFixture> = generate_canonical_fixtures()
        .expect("canonical fixtures")
        .into_iter()
        .filter(|fixture| fixture.language == FixtureLanguage::Rust)
        .collect();
    let total = fixtures.len();
    let mut served = Vec::new();
    for mut fixture in fixtures {
        let node = fixture.schema.nodes[0].clone();
        let name = fixture_name(&node.id);
        // The canonical Rust fixture is in-process (and some specs name another backend); this run
        // serves every node from the persistent Rust worker started below.
        let backend = BackendConfig::persistent_worker(
            BackendKind::Rust,
            exe.display().to_string(),
            node.entrypoint.clone(),
        )
        .with_args(["--worker", "conformance"]);
        // Some specs declare another backend (Python for the image spec, Shader for the shader and
        // package-artifact specs); this run serves them from the Rust worker, so the schema says so.
        fixture.schema.nodes[0].backend = BackendKind::Rust;
        fixture.backends = BTreeMap::from([(node.id.clone(), backend.clone())]);
        let plan = HostInstallPlan::from_schema_and_backends(&fixture.schema, &fixture.backends)
            .expect("install plan");
        let mut pool = RunnerPool::new();
        install_plan_runners(&mut pool, &plan, &PersistentWorkerRunnerFactory::new())
            .expect("start the worker");
        for round in ["first", "second"] {
            check(&mut pool, &backend, &fixture, &name, round);
        }
        pool.shutdown(&backend).expect("shutdown");
        println!("conformance {name} ... ok");
        served.push(name);
    }
    assert_eq!(
        served.len(),
        total,
        "every canonical Rust fixture is served"
    );
    println!(
        "conformance: {} of {total} canonical fixtures served by the Rust worker",
        served.len()
    );
}

fn check(
    pool: &mut RunnerPool,
    backend: &BackendConfig,
    fixture: &GeneratedLanguageFixture,
    name: &str,
    round: &str,
) {
    let response = pool
        .invoke(backend, fixture.request.clone())
        .unwrap_or_else(|err| panic!("{name} {round} invoke: {err}"));
    let expected = &fixture.expected_response;
    assert_eq!(response.outputs, expected.outputs, "{name} {round} outputs");
    assert_eq!(response.events, expected.events, "{name} {round} events");
    if expected.state.is_some() {
        assert_eq!(response.state, expected.state, "{name} {round} state");
    }
}

/// The fixture name of a node id: `ffi.conformance.<name>:<node>`.
fn fixture_name(node_id: &str) -> String {
    node_id
        .strip_prefix("ffi.conformance.")
        .and_then(|rest| rest.split(':').next())
        .unwrap_or(node_id)
        .to_string()
}
