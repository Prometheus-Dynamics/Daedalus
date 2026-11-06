//! Plugin-local objects on the stable path: `examples/plugins/stable_abi`'s `local_*` nodes make
//! a `Local<Tracker>` once, keep it in node state and pass it by handle along two edges to other
//! nodes of the plugin. The object keeps one address (the handle's `data()`) on every edge and
//! every tick, statically and through the stable path; the host never reads it. Across a reload,
//! an object of the old build is refused by the new one, and keeps the old build loaded exactly
//! as long as the host holds it.
#![cfg(not(target_env = "musl"))]

#[path = "support/cdylib.rs"]
mod cdylib;

use std::path::PathBuf;
use std::sync::OnceLock;

use daedalus::PluginLibrary;
use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::{HostGraphReloadExt, InstallPath};
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::transport::{Local, LocalInterface, Payload};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use daedalus_plugins_stable_abi::locals::{TRACKER_KEY, Tracker};

fn library() -> &'static PluginLibrary {
    static LIBRARY: OnceLock<PluginLibrary> = OnceLock::new();
    // Safety: a trusted test plugin.
    LIBRARY.get_or_init(|| unsafe { PluginLibrary::load(cdylib::stable_abi_cdylib()) }.unwrap())
}

fn graph(registry: &PluginRegistry) -> HostGraph<HandlerRegistry> {
    let builder = registry
        .graph_builder()
        .unwrap()
        .input_as("id", TypeExpr::Scalar(ValueType::Int))
        .input_as("obj_in", TypeExpr::opaque(TRACKER_KEY))
        .node_id("stable_abi:local_make", "make")
        .node_id("stable_abi:local_touch", "touch")
        .node_id("stable_abi:local_read", "read")
        .node_id("stable_abi:local_read", "reader")
        .connect("id", "make.id")
        .connect("make.obj", "touch.obj")
        .connect("make.obj", "made")
        .connect("touch.obj", "read.obj")
        .connect("touch.obj", "touched")
        .connect("touch.ptr", "touch_ptr")
        .connect("read.id", "id_out")
        .connect("read.hits", "hits")
        .connect("read.ptr", "read_ptr")
        .connect("obj_in", "reader.obj")
        .connect("reader.hits", "reader_hits");
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

/// The object's address as the host sees a payload: a handle's `data()` (stable path) or the
/// `Local`'s own pointer (static install, where it is a Rust payload).
fn address(payload: &Payload) -> usize {
    if let Some(handle) = payload.foreign_handle() {
        assert!(
            handle.view::<LocalInterface>().is_ok(),
            "a plugin-local handle"
        );
        return handle.data() as usize;
    }
    payload
        .get_ref::<Local<Tracker>>()
        .expect("a Local<Tracker>")
        .as_ptr() as usize
}

/// Three ticks: the object's address on every edge and tick is one address, and its hits count
/// up (it is the same object, changed in place).
fn one_address_on_every_edge_and_tick(registry: &PluginRegistry) -> usize {
    let mut host = graph(registry);
    let mut seen = None;
    for tick in 1..=3_i64 {
        host.push("id", 42_i64);
        host.tick().unwrap();
        let made = address(&host.take_payload("made").expect("made"));
        let touched = address(&host.take_payload("touched").expect("touched"));
        let touch_ptr = host.take::<i64>("touch_ptr").expect("touch_ptr") as usize;
        let read_ptr = host.take::<i64>("read_ptr").expect("read_ptr") as usize;
        assert_eq!(host.take::<i64>("id_out"), Some(42));
        assert_eq!(
            host.take::<i64>("hits"),
            Some(tick),
            "the same object, in place"
        );
        assert_eq!(
            [made, touched, touch_ptr, read_ptr],
            [made; 4],
            "one address on every edge, tick {tick}"
        );
        assert_eq!(*seen.get_or_insert(made), made, "one address on every tick");
    }
    seen.unwrap()
}

#[test]
fn a_local_object_keeps_its_address_across_edges_and_ticks() {
    let mut statics = PluginRegistry::new();
    statics.install_plugin(&StableAbiPlugin::new()).unwrap();
    one_address_on_every_edge_and_tick(&statics);

    let mut stables = PluginRegistry::new();
    assert_eq!(
        library().install_into(&mut stables).unwrap(),
        InstallPath::Stable
    );
    one_address_on_every_edge_and_tick(&stables);
}

#[test]
fn a_reload_refuses_old_objects_and_closes_the_old_build_when_they_are_gone() {
    let dir = std::env::temp_dir().join(format!("daedalus-local-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path: PathBuf = dir.join(format!("libfixture{}", std::env::consts::DLL_SUFFIX));
    std::fs::copy(cdylib::stable_abi_cdylib(), &path).unwrap();

    // Safety: a trusted test plugin.
    let mut library = unsafe { PluginLibrary::load_reloadable(&path) }.unwrap();
    let mut registry = PluginRegistry::new();
    library
        .install_into_as(&mut registry, InstallPath::Stable)
        .unwrap();
    let mut host = graph(&registry);

    host.push("id", 1_i64);
    host.tick().unwrap();
    let old = host.take_payload("touched").expect("touched");
    let old_address = address(&old);

    // Its own build reads it back from the host.
    host.push_payload("obj_in", old.clone());
    host.tick().unwrap();
    assert_eq!(host.take::<i64>("reader_hits"), Some(1));

    // Safety: as above (the same file, opened again as a new build).
    assert_eq!(unsafe { host.reload_plugin(&mut library) }.unwrap(), 2);
    host.push_payload("obj_in", old.clone());
    let err = host.tick().unwrap_err().to_string();
    assert!(err.contains("another build"), "{err}");

    // The new build makes its own object; every node instance has moved on.
    host.push("id", 2_i64);
    host.tick().unwrap();
    let new = host.take_payload("touched").expect("touched");
    assert_ne!(address(&new), 0);
    assert_eq!(
        address(&old),
        old_address,
        "still the same object while held"
    );

    // Only the old object keeps the old build loaded, and only the payloads holding it.
    assert_eq!(library.live_generations(), vec![2, 1]);
    drop(old);
    assert_eq!(library.live_generations(), vec![2]);

    drop((host, registry, new));
    let _ = std::fs::remove_dir_all(&dir);
}
