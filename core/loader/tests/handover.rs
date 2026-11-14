//! A plugin node hands a megabyte to the host through a graph, without a copy. The host takes the
//! value, holds it across a graph reload and a plugin reload, and the plugin's release runs once, on
//! the last drop, and the library closes after it.

mod support;

use std::sync::Arc;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{Loader, PluginBackend, bytes_copied, bytes_released};
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::Registry;
use daedalus_value::Value;
use std::sync::{Mutex, MutexGuard};
use support::copy_fixture;

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

/// The tests share the process-wide counts of released records, so they run one at a time.
fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

const SIZE: usize = 1 << 20;

fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

/// `host.x -> fixture.handover -> host.out`: the plugin's node makes the value the host takes.
fn graph() -> GraphBody {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge.label = Some("io".to_string());
    bridge.port_types.insert("x".to_string(), "u32".to_string());
    bridge
        .port_types
        .insert("out".to_string(), "bytes".to_string());
    let mut node = NodeDoc::new("fixture.handover");
    node.label = Some("make".to_string());
    GraphBody {
        nodes: vec![bridge, node],
        edges: vec![edge(0, "x", 1, "in"), edge(1, "out", 0, "out")],
        groups: Vec::new(),
    }
}

/// The graph planned on `plugin`'s backend, with the plugin's nodes registered.
fn planned(plugin: &Arc<daedalus_loader::Plugin>) -> Planned {
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("the plugin's nodes join");
    let backend = PluginBackend::new(Arc::clone(plugin), 0);
    plan(&graph(), &registry, &[&backend], &Config::default()).expect("the graph plans")
}

fn load(planned: Planned) -> LoadedHost {
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

/// Feeds one value, steps once, and takes what came out.
fn tick(host: &mut LoadedHost) -> Value {
    host.host()
        .feed_inline("x", &1u32.to_le_bytes())
        .expect("a feed");
    host.step().expect("a tick");
    host.host()
        .take_value("out")
        .expect("a take")
        .expect("a value")
}

fn assert_megabyte(value: &Value) {
    let bytes = value.bytes().expect("the value holds bytes");
    assert_eq!(bytes.len(), SIZE);
    assert!(bytes.iter().all(|byte| *byte == 7));
}

#[test]
fn the_host_takes_a_handed_value_without_a_copy() {
    let _serial = serial();
    let mut loader = Loader::new();
    let plugin = Arc::new(loader.open(copy_fixture("handover-take")).expect("opens"));
    let mut host = load(planned(&plugin));
    let copied = bytes_copied();
    let value = tick(&mut host);
    assert_megabyte(&value);
    assert_eq!(
        bytes_copied(),
        copied,
        "the value reaches the host with no copy"
    );
}

#[test]
fn a_held_value_survives_a_graph_reload_and_is_released_once_on_its_last_drop() {
    let _serial = serial();
    let mut loader = Loader::new();
    let plugin = Arc::new(loader.open(copy_fixture("handover-reload")).expect("opens"));
    let mut host = load(planned(&plugin));
    let held = tick(&mut host);
    let released = bytes_released();

    host.reload(planned(&plugin)).expect("a reload");
    assert_megabyte(&held);
    assert_eq!(
        bytes_released(),
        released,
        "a reload does not release a held value"
    );

    drop(held);
    assert_eq!(
        bytes_released(),
        released + 1,
        "the last drop releases the bytes once"
    );
}

#[test]
fn a_plugin_reload_to_a_new_generation_keeps_a_held_value_readable() {
    let _serial = serial();
    let mut loader = Loader::new();
    let first = Arc::new(loader.open(copy_fixture("handover-gen-1")).expect("opens"));
    let probe = first.generation_probe();
    let mut host = load(planned(&first));
    let held = tick(&mut host);
    let released = bytes_released();
    drop(first);

    let second = Arc::new(
        loader
            .open(copy_fixture("handover-gen-2"))
            .expect("reopens"),
    );
    assert_eq!(second.generation().number(), 2);
    host.reload(planned(&second)).expect("a reload");
    assert_megabyte(&held);
    assert!(
        probe.upgrade().is_some(),
        "the held value keeps the old generation open"
    );
    assert_eq!(
        bytes_released(),
        released,
        "nothing is released while the value is held"
    );

    drop(held);
    assert_eq!(bytes_released(), released + 1);
    assert!(
        probe.upgrade().is_none(),
        "the old generation closes with its last value"
    );
}

#[test]
fn a_held_value_outlives_the_graph_that_made_it() {
    let _serial = serial();
    let mut loader = Loader::new();
    let plugin = Arc::new(loader.open(copy_fixture("handover-drop")).expect("opens"));
    let probe = plugin.generation_probe();
    let mut host = load(planned(&plugin));
    let held = tick(&mut host);
    let released = bytes_released();
    drop(host);
    drop(plugin);
    assert_megabyte(&held);
    assert!(
        probe.upgrade().is_some(),
        "the value keeps the library open after the graph is gone"
    );
    assert_eq!(bytes_released(), released);

    drop(held);
    assert_eq!(bytes_released(), released + 1);
    assert!(
        probe.upgrade().is_none(),
        "the library closes after the last value"
    );
}

#[test]
fn a_steady_state_take_of_a_handed_megabyte_allocates_nothing() {
    let _serial = serial();
    let mut loader = Loader::new();
    let plugin = Arc::new(loader.open(copy_fixture("handover-allocs")).expect("opens"));
    let mut host = load(planned(&plugin));
    // The first tick may fill the caches the host and the loader keep.
    drop(tick(&mut host));
    let copied = bytes_copied();
    let (value, allocations) = counted(|| tick(&mut host));
    assert_megabyte(&value);
    assert_eq!(bytes_copied(), copied, "no bytes are copied");
    assert!(
        allocations == 0,
        "a steady-state tick and take of a megabyte allocate nothing: {allocations}"
    );
    drop(value);
}
