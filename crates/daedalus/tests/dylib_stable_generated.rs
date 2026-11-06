//! Nodes registered at runtime (a `NodeDecl` and a `handlers.try_on` handler, no `#[node]`),
//! with each port's stable codec declared through `declare_stable_port`: `stable_abi:gen_boxes`
//! takes a `Vec<BoxPod>` (a `StablePod` view) and returns its first box and count, and
//! `stable_abi:gen_tracker` reads a `Local<Tracker>` by handle and passes it on. They run the
//! same statically and through the stable path, where without the declarations their handlers
//! would see a `PodValue` and a bare handle instead of their Rust types.
#![cfg(not(target_env = "musl"))]

#[path = "support/cdylib.rs"]
mod cdylib;

use daedalus::PluginLibrary;
use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::InstallPath;
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use daedalus_plugins_stable_abi::generated::BOX_KEY;
use daedalus_plugins_stable_abi::pods::{BOXES_KEY, BoxPod};

fn graph(registry: &PluginRegistry) -> HostGraph<HandlerRegistry> {
    let builder = registry
        .graph_builder()
        .unwrap()
        .input_as("n", TypeExpr::Scalar(ValueType::Int))
        .input_as("id", TypeExpr::Scalar(ValueType::Int))
        .node_id("stable_abi:pod_make", "make")
        .node_id("stable_abi:gen_boxes", "boxes")
        .node_id("stable_abi:local_make", "tracker")
        .node_id("stable_abi:gen_tracker", "gen")
        .node_id("stable_abi:local_read", "read")
        .connect("n", "make.n")
        .connect("make.boxes", "boxes.boxes")
        .connect("boxes.first", "first")
        .connect("boxes.count", "count")
        .connect("id", "tracker.id")
        .connect("tracker.obj", "gen.obj")
        .connect("gen.id", "gen_id")
        .connect("gen.obj", "read.obj")
        .connect("read.hits", "hits");
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

fn runs(registry: &PluginRegistry) {
    let mut host = graph(registry);
    host.push("n", 3_i64);
    host.push("id", 9_i64);
    host.tick().unwrap();
    assert_eq!(host.take::<i64>("count"), Some(3));
    let first = host.take::<BoxPod>("first").expect("the first box");
    assert_eq!((first.id, first.corners[1]), (0, [1, 0]));
    assert_eq!(host.take::<i64>("gen_id"), Some(9));
    assert_eq!(
        host.take::<i64>("hits"),
        Some(0),
        "the same tracker, read on"
    );
}

#[test]
fn generated_nodes_with_declared_codecs_run_statically_and_through_the_stable_path() {
    let mut statics = PluginRegistry::new();
    statics.install_plugin(&StableAbiPlugin::new()).unwrap();
    runs(&statics);

    // Safety: a trusted test plugin.
    let library = unsafe { PluginLibrary::load(cdylib::stable_abi_cdylib()) }.unwrap();
    let mut stables = PluginRegistry::new();
    stables
        .register_stable_pod::<Vec<BoxPod>>(BOXES_KEY)
        .unwrap();
    stables.register_stable_pod::<BoxPod>(BOX_KEY).unwrap();
    assert_eq!(
        library.install_into(&mut stables).unwrap(),
        InstallPath::Stable
    );
    runs(&stables);
}
