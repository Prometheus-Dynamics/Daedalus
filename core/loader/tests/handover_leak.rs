//! Reloading a graph whose plugin node hands out bytes does not grow the heap, the promoted
//! declarations, or the plugin tables held, and every hand-over is released as it is dropped.

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{Loader, PluginBackend, bytes_released, plugin_tables_held};
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{Registry, promoted_declarations};
use support::copy_fixture;

/// Counts the bytes this thread holds live: allocations add, frees subtract.
struct Tracking;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
}

fn live_bytes() -> isize {
    LIVE.with(Cell::get)
}

fn add(delta: isize) {
    let _ = LIVE.try_with(|live| live.set(live.get() + delta));
}

// SAFETY: forwards every call to `System` unchanged, and only adds to a thread-local counter.
unsafe impl GlobalAlloc for Tracking {
    // SAFETY: the caller's contract is passed straight through.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        add(layout.size() as isize);
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.alloc(layout) }
    }

    // SAFETY: the caller's contract is passed straight through.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        add(-(layout.size() as isize));
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.dealloc(ptr, layout) }
    }

    // SAFETY: the caller's contract is passed straight through.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        add(new_size as isize - layout.size() as isize);
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Tracking = Tracking;

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

fn planned(plugin: &Arc<daedalus_loader::Plugin>) -> Planned {
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("the plugin's nodes join");
    let backend = PluginBackend::new(Arc::clone(plugin), 0);
    plan(&graph(), &registry, &[&backend], &Config::default()).expect("the graph plans")
}

/// One reload round: load the graph, take a value, drop it and the host, and load again.
fn round(loader: &mut Loader) {
    let plugin = Arc::new(loader.open(copy_fixture("handover-leak")).expect("opens"));
    let mut host = LoadedHost::load(planned(&plugin), LoadOptions::default()).expect("loads");
    host.host()
        .feed_inline("x", &1u32.to_le_bytes())
        .expect("a feed");
    host.step().expect("a tick");
    let value = host.host().take_value("out").expect("a take");
    drop(value);
    host.reload(planned(&plugin)).expect("a reload");
    host.host()
        .feed_inline("x", &2u32.to_le_bytes())
        .expect("a feed");
    host.step().expect("a tick");
    drop(host.host().take_value("out").expect("a take"));
}

#[test]
fn reloading_a_graph_with_handed_bytes_does_not_grow() {
    let _serial = serial();
    let mut loader = Loader::new();
    for _ in 0..3 {
        round(&mut loader);
    }
    let live = live_bytes();
    let promoted = promoted_declarations();
    let tables = plugin_tables_held();
    let released = bytes_released();

    for _ in 0..20 {
        round(&mut loader);
    }

    assert_eq!(
        promoted_declarations(),
        promoted,
        "no declaration is promoted again"
    );
    assert_eq!(
        plugin_tables_held(),
        tables,
        "every plugin entry is released"
    );
    assert_eq!(
        bytes_released(),
        released + 20 * 2,
        "each hand-over is released once, as its value is dropped"
    );
    assert_eq!(live_bytes(), live, "reloading keeps no memory live");
}

/// The tests share the process-wide counts, so they run one at a time.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
