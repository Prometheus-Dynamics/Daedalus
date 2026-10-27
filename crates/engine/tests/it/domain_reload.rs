//! `ExecutionDomain::reload_graph`: a member reloads in place with its links intact, and a
//! reload missing a linked host port is refused before anything changes.
#![cfg(feature = "plugins")]

use daedalus_data::model::Value;
use daedalus_engine::{Engine, ExecutionDomain, HostGraph, LinkMode};
use daedalus_runtime::host_bridge::HostBridgeManager;
use daedalus_transport::Payload;

use crate::host_graph_reload::{Nodes, chain, next_for, setup};
use crate::support::INT_KEY;

fn compile(
    engine: &Engine,
    plugins: &daedalus_runtime::plugins::PluginRegistry,
    ids: &[&str],
) -> HostGraph<Nodes> {
    engine
        .compile_host_graph_plugin_registry(
            plugins,
            chain(ids),
            Nodes,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap()
}

/// `a` (a counter) forwards its host output into `b` (a relay) on every tick.
fn linked_domain(
    engine: &Engine,
    plugins: &daedalus_runtime::plugins::PluginRegistry,
) -> ExecutionDomain<Nodes> {
    let mut domain = ExecutionDomain::new();
    domain
        .add_graph("a", compile(engine, plugins, &["counter"]))
        .unwrap();
    domain
        .add_graph("b", compile(engine, plugins, &["relay"]))
        .unwrap();
    domain.link("a", "out", "b", "in", LinkMode::All).unwrap();
    domain
}

/// Feed `a` one value, tick the domain, and return what `b` emitted.
fn step(domain: &mut ExecutionDomain<Nodes>) -> Vec<i64> {
    domain
        .graph_mut("a")
        .unwrap()
        .push_as("in", INT_KEY, Value::Int(0));
    domain.tick();
    std::iter::from_fn(|| domain.take_payload("b", "out"))
        .map(|payload: Payload| match payload.get_ref::<Value>() {
            Some(Value::Int(n)) => *n,
            other => panic!("unexpected output {other:?}"),
        })
        .collect()
}

#[test]
fn reload_in_place_keeps_links_and_upstream_state() {
    let (engine, plugins) = setup();
    let mut domain = linked_domain(&engine, &plugins);
    assert_eq!(step(&mut domain), vec![1]);
    assert_eq!(step(&mut domain), vec![2]);

    // `a` gains a relay in front of its counter; the counter keeps its state and the link to
    // `b` keeps forwarding.
    let next = next_for(
        &engine,
        &plugins,
        chain(&["relay", "counter"]),
        domain.graph("a").unwrap(),
    );
    let report = domain.reload_graph("a", next).unwrap();
    assert!(
        report.nodes_kept.iter().any(|n| n == "counter"),
        "{report:?}"
    );
    assert_eq!(step(&mut domain), vec![3]);
}

#[test]
fn reload_missing_a_linked_port_is_refused_without_change() {
    let (engine, plugins) = setup();
    let mut domain = linked_domain(&engine, &plugins);
    assert_eq!(step(&mut domain), vec![1]);

    // `b` loses the host input the link feeds: the reload must be refused.
    let mut without_input = chain(&["relay"]);
    without_input.edges.remove(0);
    let next = engine
        .compile_host_graph_plugin_registry(
            &plugins,
            without_input,
            Nodes,
            domain.graph("b").unwrap().bridge_manager().clone(),
            "host",
        )
        .unwrap();
    let error = domain.reload_graph("b", next).expect_err("refused");
    assert!(
        error.to_string().contains("lacks host port 'in'"),
        "{error}"
    );

    // Nothing changed: the link still forwards, with `a`'s state intact.
    assert_eq!(step(&mut domain), vec![2]);
}
