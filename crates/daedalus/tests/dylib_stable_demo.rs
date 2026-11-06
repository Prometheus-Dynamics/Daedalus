//! The `daedalus-nodes` demo bundle through the stable path: `demo:frame_src` -> `demo:decode`
//! -> `demo:sink`, and a second source and decoder delivering detections to the host. Its `Frame` crosses
//! as a `Value` (`ToValue` out, serde in) and its `Vec<Detection>` as a `StablePod` view, which
//! a host that registered the type reads back as `Vec<Detection>`; statically the same graph
//! gives the same detections.
#![cfg(not(target_env = "musl"))]

#[path = "support/cdylib.rs"]
mod cdylib;

use daedalus::PluginLibrary;
use daedalus::dylib::InstallPath;
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus_nodes::bundle_demo::{DemoPlugin, Detection};

fn graph(registry: &PluginRegistry) -> HostGraph<HandlerRegistry> {
    let builder = registry
        .graph_builder()
        .unwrap()
        .node_id("demo:frame_src", "src")
        .node_id("demo:decode", "decode")
        .node_id("demo:sink", "sink")
        .connect("src.frame", "decode.frame")
        .connect("decode.detections", "sink.detections")
        // A second chain to the host (the nodes take their inputs by value, so no fan-out).
        .node_id("demo:frame_src", "src2")
        .node_id("demo:decode", "decode2")
        .connect("src2.frame", "decode2.frame")
        .connect("decode2.detections", "detections");
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

/// `b"frame"` is 5 bytes, so one detection of id 5, every tick.
fn runs(registry: &PluginRegistry) {
    let mut host = graph(registry);
    for _ in 0..3 {
        host.tick().unwrap();
        let detections = host
            .take::<Vec<Detection>>("detections")
            .expect("the detections");
        assert_eq!(detections, vec![Detection { id: 5 }]);
    }
}

#[test]
fn the_demo_graph_runs_statically_and_through_the_stable_path() {
    let mut statics = PluginRegistry::new();
    statics.install_plugin(&DemoPlugin::new()).unwrap();
    runs(&statics);

    // Safety: a trusted test plugin.
    let library = unsafe { PluginLibrary::load(cdylib::demo_stable_cdylib()) }.unwrap();
    let mut stables = PluginRegistry::new();
    let key = library
        .schema()
        .nodes
        .iter()
        .flat_map(|node| &node.outputs)
        .find(|port| port.name == "detections")
        .and_then(|port| port.type_key.clone())
        .expect("the detections key");
    stables.register_stable_pod::<Vec<Detection>>(key).unwrap();
    assert_eq!(
        library.install_into(&mut stables).unwrap(),
        InstallPath::Stable
    );
    runs(&stables);
}
