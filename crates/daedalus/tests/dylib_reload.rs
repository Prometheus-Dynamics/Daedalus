//! Hot-reloads `examples/plugins/stable_abi` installed through the stable path: the fixture is
//! rebuilt between loads (`reload-v2` changes what its `generation` node returns,
//! `reload-incompatible` adds a port), and a running graph switches builds between two ticks.
//! Node state resets across the reload, and the old build stays loaded exactly as long as a
//! buffer it produced is held.
// musl executables are static and cannot dlopen (see `dylib::library`): no loader there.
#![cfg(not(target_env = "musl"))]

#[path = "support/cdylib.rs"]
mod cdylib;

use std::path::{Path, PathBuf};

use daedalus::data::model::{StructFieldValue, TypeExpr, Value, ValueType};
use daedalus::dylib::HostGraphReloadExt;
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::PluginRegistry;
use daedalus::transport::TypeKey;
use daedalus::{InstallPath, PluginLibrary, PluginLibraryError, SharedBytes};
use daedalus_plugins_stable_abi::locals::TRACKER_KEY;

/// Build the fixture with `features` and move it to `dest` the way a build should replace a
/// loaded plugin: write a new file, then rename it over the old path.
fn build_fixture(features: &str, dest: &Path) {
    let built = cdylib::build_cdylib(
        "daedalus-plugins-stable-abi",
        "daedalus_plugins_stable_abi",
        features,
    );
    let staged = dest.with_extension("new");
    std::fs::copy(&built, &staged).unwrap();
    std::fs::rename(&staged, dest).unwrap();
}

fn graph(registry: &PluginRegistry) -> HostGraph<HandlerRegistry> {
    let scalar = |ty| TypeExpr::Scalar(ty);
    let builder = registry
        .graph_builder()
        .unwrap()
        .input_as("tick", scalar(ValueType::Int))
        .input_as("step", scalar(ValueType::Int))
        .input_as("len", scalar(ValueType::Int))
        .input_as("value", scalar(ValueType::Int))
        .node_id("stable_abi:generation", "generation")
        .node_id("stable_abi:accumulate", "accumulate")
        .node_id("stable_abi:shared_fill", "shared_fill")
        .connect("tick", "generation.tick")
        .connect("generation.out", "out")
        .connect("step", "accumulate.step")
        .connect("accumulate.total", "total")
        .connect("len", "shared_fill.len")
        .connect("value", "shared_fill.value")
        .connect("shared_fill.bytes", "bytes");
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

/// One tick: (`generation` of `tick`, running total after `step`, a 3-byte buffer of `fill`).
fn tick(
    host: &mut HostGraph<HandlerRegistry>,
    tick: i64,
    step: i64,
    fill: i64,
) -> (i64, i64, SharedBytes) {
    host.push("tick", tick);
    host.push("step", step);
    host.push("len", 3_i64);
    host.push("value", fill);
    host.tick().unwrap();
    (
        host.take::<i64>("out").expect("out"),
        host.take::<i64>("total").expect("total"),
        host.take::<SharedBytes>("bytes").expect("bytes"),
    )
}

#[test]
fn a_rebuilt_plugin_reloads_between_ticks_with_fresh_state() {
    let dir = std::env::temp_dir().join(format!("daedalus-reload-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path: PathBuf = dir.join(format!("libfixture{}", std::env::consts::DLL_SUFFIX));
    build_fixture("dylib", &path);

    // Safety: a trusted test plugin.
    let mut library = unsafe { PluginLibrary::load_reloadable(&path) }.unwrap();
    assert_eq!(library.generation(), 1);
    let mut registry = PluginRegistry::new();
    library
        .install_into_as(&mut registry, InstallPath::Stable)
        .unwrap();
    let mut host = graph(&registry);

    let (out, total, _) = tick(&mut host, 5, 2, 1);
    assert_eq!((out, total), (1005, 2));
    let (out, total, old_buffer) = tick(&mut host, 6, 3, 1);
    assert_eq!((out, total), (1006, 5), "node state accumulates");

    // A tracker of build 1, made through its codec.
    let tracker = |id: i64| {
        let field = |name: &str, value: i64| StructFieldValue {
            name: name.into(),
            value: Value::Int(value),
        };
        Value::Struct(vec![field("id", id), field("hits", 0)])
    };
    let key = TypeKey::new(TRACKER_KEY);
    let codec = registry
        .payload_codec(&key)
        .expect("the tracker's codec")
        .clone();
    let old_tracker = (codec.from_value.as_ref().unwrap())(key.clone(), &tracker(11)).unwrap();

    build_fixture("dylib,reload-v2", &path);
    // Safety: as above.
    assert_eq!(unsafe { host.reload_plugin(&mut library) }.unwrap(), 2);
    assert_eq!(library.generation(), 2);

    let (out, total, new_buffer) = tick(&mut host, 7, 4, 9);
    assert_eq!(out, 2007, "the next tick runs the new build");
    assert_eq!(total, 4, "node state reset across the reload");
    assert_eq!((&*new_buffer, &*old_buffer), (&[9; 3][..], &[1; 3][..]));

    // The old build's tracker converts on the build that made it (the new build refuses another
    // build's object); a new one on the new build.
    let to_value = codec.to_value.as_ref().unwrap();
    assert_eq!(to_value(&old_tracker).unwrap(), tracker(11));
    let new_tracker = (codec.from_value.as_ref().unwrap())(key.clone(), &tracker(12)).unwrap();
    assert_eq!(to_value(&new_tracker).unwrap(), tracker(12));

    // Every node instance moved on; only the buffer and the tracker the old build produced keep
    // it loaded.
    assert_eq!(library.live_generations(), vec![2, 1]);
    drop(old_buffer);
    assert_eq!(library.live_generations(), vec![2, 1]);
    drop(old_tracker);
    assert_eq!(library.live_generations(), vec![2]);
    drop((new_tracker, codec));

    // A build with other ports cannot replace the installed one; the current build keeps going.
    build_fixture("dylib,reload-incompatible", &path);
    // Safety: as above.
    let err = unsafe { host.reload_plugin(&mut library) }.unwrap_err();
    assert!(
        matches!(&err, PluginLibraryError::ReloadIncompatible { changes, .. }
            if changes.iter().any(|change| change.contains("stable_abi:generation"))),
        "{err}"
    );
    assert_eq!(library.generation(), 2);
    let (out, total, _) = tick(&mut host, 8, 1, 2);
    assert_eq!((out, total), (2008, 5));

    drop(host);
    drop(registry);
    drop(new_buffer);
    let _ = std::fs::remove_dir_all(&dir);
}
