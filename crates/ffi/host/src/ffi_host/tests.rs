use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use daedalus_ffi_core::{BackendKind, NodeSchema, PluginSchema, WireValue};
use daedalus_registry::ids::NodeId;

use super::*;
use crate::test_support::{EchoFactory, int_port, python_worker, rust_in_process};

fn node(id: &str, backend: BackendKind) -> NodeSchema {
    NodeSchema::new(
        id,
        backend,
        id,
        vec![int_port("value")],
        vec![int_port("out")],
    )
}

fn worker_backend(executable: &str) -> BackendConfig {
    python_worker(executable, "demo.py", "run")
}

fn package(plugin: &str, nodes: Vec<(NodeSchema, BackendConfig)>) -> PluginPackage {
    let (nodes, backends): (Vec<_>, BTreeMap<_, _>) = nodes
        .into_iter()
        .map(|(node, backend)| {
            let id = node.id.clone();
            (node, (id, backend))
        })
        .unzip();
    PluginPackage::new(
        PluginSchema::new(plugin, Some("1.0.0".into()), nodes),
        backends,
    )
}

fn python_package(plugin: &str, node_id: &str, executable: &str) -> PluginPackage {
    package(
        plugin,
        vec![(
            node(node_id, BackendKind::Python),
            worker_backend(executable),
        )],
    )
}

fn request(node_id: &str) -> InvokeRequest {
    InvokeRequest {
        protocol_version: daedalus_ffi_core::WORKER_PROTOCOL_VERSION,
        node_id: node_id.into(),
        correlation_id: Some("c1".into()),
        args: BTreeMap::from([("value".into(), WireValue::Int(2))]),
        state: None,
        context: BTreeMap::new(),
    }
}

fn out(response: &InvokeResponse) -> &WireValue {
    response.outputs.get("out").expect("out output")
}

#[test]
fn installs_package_and_invokes_by_node_id() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();

    let host = FfiHost::install_package(
        &mut registry,
        &python_package("demo.plugin", "demo.echo", "python"),
        &factory,
    )
    .expect("host installs");

    assert!(registry.plugin_manifest("demo.plugin").is_some());
    assert!(registry.node_decl(&NodeId::new("demo.echo")).is_some());
    assert_eq!(host.node_ids().collect::<Vec<_>>(), vec!["demo.echo"]);
    assert_eq!(host.plans().len(), 1);
    assert!(host.plan("demo.plugin").is_some());
    assert_eq!(
        host.plan_for_node("demo.echo").unwrap().plugin.id,
        "demo.plugin"
    );
    assert!(host.runner_key("demo.echo").is_some());
    assert_eq!(host.health("demo.echo").unwrap(), RunnerHealth::Ready);

    let response = host.invoke("demo.echo", request("")).expect("invoke");
    assert_eq!(out(&response), &WireValue::String("demo.echo".into()));
    assert_eq!(response.correlation_id.as_deref(), Some("c1"));

    let batch = host
        .invoke_batch("demo.echo", vec![request("demo.echo"), request("")])
        .expect("batch");
    assert_eq!(batch.len(), 2);

    assert_eq!(factory.builds(), 1);
    assert_eq!(host.pool().telemetry().invokes, 3);
    let report = host.telemetry().snapshot();
    assert!(report.packages.contains_key("demo.plugin"));
    let key = host.runner_key("demo.echo").unwrap().as_str();
    assert_eq!(report.backends[key].invokes, 3);
}

#[test]
fn rejects_unknown_node_and_mismatched_request() {
    let mut registry = CapabilityRegistry::default();
    let host = FfiHost::install_package(
        &mut registry,
        &python_package("demo.plugin", "demo.echo", "python"),
        &EchoFactory::default(),
    )
    .expect("host installs");

    assert!(matches!(
        host.invoke("demo.missing", request("")),
        Err(FfiHostError::UnknownNode { node_id }) if node_id == "demo.missing"
    ));
    assert!(matches!(
        host.invoke("demo.echo", request("demo.other")),
        Err(FfiHostError::RequestNodeMismatch { .. })
    ));
    assert!(host.backend("demo.missing").is_none());
}

#[test]
fn shares_pool_and_telemetry_across_packages() {
    let telemetry = FfiHostTelemetry::new();
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let mut host = FfiHost::builder()
        .telemetry(telemetry.clone())
        .install_package(
            &mut registry,
            &python_package("demo.a", "demo.a.echo", "python"),
            &factory,
        )
        .expect("first package");

    // Same backend config: the running worker is reused.
    host.add_package(
        &mut registry,
        &python_package("demo.b", "demo.b.echo", "python"),
        &factory,
    )
    .expect("second package");
    // Different backend config: a new worker is started.
    host.add_package(
        &mut registry,
        &python_package("demo.c", "demo.c.echo", "python3"),
        &factory,
    )
    .expect("third package");

    assert_eq!(factory.builds(), 2);
    assert_eq!(host.pool().len(), 2);
    assert_eq!(host.plans().len(), 3);
    assert_eq!(
        host.runner_key("demo.a.echo"),
        host.runner_key("demo.b.echo")
    );
    for node_id in ["demo.a.echo", "demo.b.echo", "demo.c.echo"] {
        assert!(registry.node_decl(&NodeId::new(node_id)).is_some());
        let response = host.invoke(node_id, request("")).expect("invoke");
        assert_eq!(out(&response), &WireValue::String(node_id.into()));
    }
    let report = telemetry.snapshot();
    assert_eq!(report.packages.len(), 3);
}

#[test]
fn duplicate_node_is_rejected_without_touching_registry() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let mut host = FfiHost::install_package(
        &mut registry,
        &python_package("demo.a", "demo.echo", "python"),
        &factory,
    )
    .expect("first package");
    let before = registry.clone();

    let err = host
        .add_package(
            &mut registry,
            &python_package("demo.b", "demo.echo", "python3"),
            &factory,
        )
        .expect_err("duplicate node");

    assert!(matches!(err, FfiHostError::DuplicateNode { node_id } if node_id == "demo.echo"));
    assert_eq!(registry, before);
    assert_eq!(factory.builds(), 1);
}

#[test]
fn runner_failure_restores_registry_and_stops_started_runners() {
    let mut registry = CapabilityRegistry::default();
    let failing = EchoFactory {
        fail_executable: Some("python"),
        ..Default::default()
    };
    let err = FfiHost::install_package(
        &mut registry,
        &python_package("demo.plugin", "demo.echo", "python"),
        &failing,
    )
    .err()
    .expect("runner startup fails");
    assert!(matches!(
        err,
        FfiHostError::Install(HostInstallError::Runner { .. })
    ));
    assert!(registry.plugin_manifest("demo.plugin").is_none());

    // The runner is built but does not advertise the node: it never joins the pool.
    let unsupported = EchoFactory {
        supported_nodes: Some(vec!["other".into()]),
        ..Default::default()
    };
    let mut host = FfiHost::new();
    let err = host
        .add_package(
            &mut registry,
            &python_package("demo.plugin", "demo.echo", "python"),
            &unsupported,
        )
        .expect_err("entrypoint rejected");
    assert!(matches!(
        err,
        FfiHostError::Install(HostInstallError::UnsupportedRunnerEntrypoint { .. })
    ));
    assert!(registry.plugin_manifest("demo.plugin").is_none());
    assert!(host.pool().is_empty());
    assert!(!host.contains_node("demo.echo"));
    assert_eq!(unsupported.builds(), 1);

    // The first worker starts, the second fails: the first is shut down again.
    let partial = EchoFactory {
        fail_executable: Some("python3"),
        ..Default::default()
    };
    let err = host
        .add_package(
            &mut registry,
            &package(
                "demo.plugin",
                vec![
                    (
                        node("demo.a", BackendKind::Python),
                        worker_backend("python"),
                    ),
                    (
                        node("demo.b", BackendKind::Python),
                        worker_backend("python3"),
                    ),
                ],
            ),
            &partial,
        )
        .expect_err("second runner fails");
    assert!(matches!(
        err,
        FfiHostError::Install(HostInstallError::Runner { node_id, .. }) if node_id == "demo.b"
    ));
    assert!(registry.plugin_manifest("demo.plugin").is_none());
    assert!(host.pool().is_empty());
    assert_eq!(partial.builds(), 1);
    assert_eq!(partial.counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn in_process_abi_nodes_are_registered_but_not_started() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let host = FfiHost::install_package(
        &mut registry,
        &package(
            "demo.mixed",
            vec![
                (
                    node("demo.worker", BackendKind::Python),
                    worker_backend("python"),
                ),
                (node("demo.native", BackendKind::Rust), rust_in_process()),
            ],
        ),
        &factory,
    )
    .expect("host installs");

    assert!(registry.node_decl(&NodeId::new("demo.native")).is_some());
    assert_eq!(factory.builds(), 1);
    assert!(host.runner_key("demo.native").is_none());
    assert!(host.backend("demo.native").is_some());
    assert!(matches!(
        host.invoke("demo.native", request("")),
        Err(FfiHostError::InProcessNode { .. })
    ));
    host.invoke("demo.worker", request(""))
        .expect("worker invoke");
}

#[test]
fn deferred_startup_installs_registry_first() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let mut host = FfiHost::builder()
        .defer_runner_startup(true)
        .install_package(
            &mut registry,
            &python_package("demo.plugin", "demo.echo", "python"),
            &factory,
        )
        .expect("host installs");

    assert!(registry.plugin_manifest("demo.plugin").is_some());
    assert!(host.has_pending_runners());
    assert!(host.pool().is_empty());
    assert!(matches!(
        host.invoke("demo.echo", request("")),
        Err(FfiHostError::RunnerNotStarted { .. })
    ));

    let keys = host
        .start_runners(&mut registry, &factory)
        .expect("runners start");
    assert_eq!(keys.len(), 1);
    assert!(!host.has_pending_runners());
    host.invoke("demo.echo", request("")).expect("invoke");
    assert!(
        host.start_runners(&mut registry, &factory)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn deferred_startup_failure_rolls_back_only_the_failing_package() {
    let mut registry = CapabilityRegistry::default();
    let mut host = FfiHost::builder().defer_runner_startup(true).build();
    let first = python_package("demo.a", "demo.a.echo", "python-a");
    // The first worker of this package starts, the second fails.
    let failing = package(
        "demo.b",
        vec![
            (
                node("demo.b.first", BackendKind::Python),
                worker_backend("python-b"),
            ),
            (
                node("demo.b.second", BackendKind::Python),
                worker_backend("python"),
            ),
        ],
    );
    let last = python_package("demo.c", "demo.c.echo", "python-c");
    let factory = EchoFactory {
        fail_executable: Some("python"),
        ..Default::default()
    };
    for package in [&first, &failing, &last] {
        host.add_package(&mut registry, package, &factory)
            .expect("deferred install");
    }

    let err = host
        .start_runners(&mut registry, &factory)
        .expect_err("second package fails");
    assert!(matches!(
        err,
        FfiHostError::Install(HostInstallError::Runner { node_id, .. }) if node_id == "demo.b.second"
    ));

    // The failing package is gone from the registry and the host; its started runner is stopped.
    let mut expected = CapabilityRegistry::default();
    for package in [&first, &last] {
        crate::install_package(&mut expected, package).expect("expected registry");
    }
    assert_eq!(registry, expected);
    assert!(!host.contains_node("demo.b.first"));
    assert!(host.plan("demo.b").is_none());
    assert_eq!(factory.builds(), 2);
    assert_eq!(factory.counters.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(host.pool().len(), 1);

    // The earlier package runs; the later one is still pending and starts on the next call.
    host.invoke("demo.a.echo", request(""))
        .expect("first invoke");
    assert!(host.has_pending_runners());
    assert!(matches!(
        host.invoke("demo.c.echo", request("")),
        Err(FfiHostError::RunnerNotStarted { .. })
    ));
    let keys = host
        .start_runners(&mut registry, &factory)
        .expect("remaining package starts");
    assert_eq!(keys.len(), 1);
    assert!(!host.has_pending_runners());
    assert_eq!(
        host.plan_for_node("demo.c.echo").unwrap().plugin.id,
        "demo.c"
    );
    host.invoke("demo.c.echo", request(""))
        .expect("last invoke");
}

#[test]
fn shutdown_and_drop_stop_workers() {
    let factory = EchoFactory::default();
    let mut registry = CapabilityRegistry::default();
    let telemetry = FfiHostTelemetry::new();
    let mut host = FfiHost::builder()
        .telemetry(telemetry.clone())
        .install_package(
            &mut registry,
            &python_package("demo.a", "demo.a.echo", "python"),
            &factory,
        )
        .expect("host installs");
    host.add_package(
        &mut registry,
        &python_package("demo.b", "demo.b.echo", "python3"),
        &factory,
    )
    .expect("second package");
    let key = host.runner_key("demo.a.echo").unwrap().as_str().to_owned();

    host.shutdown().expect("clean shutdown");
    assert_eq!(factory.counters.shutdowns.load(Ordering::SeqCst), 2);
    assert_eq!(telemetry.snapshot().backends[&key].runner_shutdowns, 1);

    let dropped = EchoFactory::default();
    let host = FfiHost::install_package(
        &mut CapabilityRegistry::default(),
        &python_package("demo.c", "demo.c.echo", "python"),
        &dropped,
    )
    .expect("host installs");
    drop(host);
    assert_eq!(dropped.counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn persistent_worker_factory_builds_without_spawning() {
    let host = FfiHost::builder()
        .pool_options(RunnerPoolOptions {
            idle_timeout: None,
            limits: RunnerLimits {
                stderr_capture_bytes: 1024,
                ..Default::default()
            },
        })
        .build();
    let factory = host.persistent_worker_factory();
    assert_eq!(factory.limits().stderr_capture_bytes, 1024);

    let runner = factory
        .build_runner("demo.echo", &worker_backend("definitely-not-a-worker"))
        .expect("runner builds lazily");
    assert_eq!(runner.health(), RunnerHealth::Starting);
    assert!(
        factory
            .build_runner("demo.native", &rust_in_process())
            .is_err()
    );
}
