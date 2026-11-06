//! Plain-old-data values on the stable path: a `#[derive(StablePod)]` struct and a `Vec` of them
//! cross `examples/plugins/stable_abi` (installed through the stable path) as typed byte views,
//! and come back as the host's own type when it registered one, or as a `PodValue` when it did
//! not. A host type with another layout under the same name is a typed error on both sides.
#![cfg(not(target_env = "musl"))]

#[path = "support/cdylib.rs"]
mod cdylib;

use std::sync::OnceLock;

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::InstallPath;
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::transport::{PodValue, StablePod as _};
use daedalus::{PluginLibrary, StablePod};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use daedalus_plugins_stable_abi::pods::{BOXES_KEY, BoxPod};

const BOX_KEY: &str = "stable_abi:box";

/// The host's own definition of the plugin's `BoxPod`, with another field type: same name, other
/// layout.
#[derive(Clone, Copy, Debug, Default, PartialEq, StablePod)]
#[repr(C)]
#[stable_pod(name = "BoxPod")]
struct OtherBox {
    id: u32,
    score: f32,
    corners: [[f32; 2]; 4],
}

fn library() -> &'static PluginLibrary {
    static LIBRARY: OnceLock<PluginLibrary> = OnceLock::new();
    // Safety: a trusted test plugin.
    LIBRARY.get_or_init(|| unsafe { PluginLibrary::load(cdylib::stable_abi_cdylib()) }.unwrap())
}

fn stable_registry(register: impl FnOnce(&mut PluginRegistry)) -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    register(&mut registry);
    assert_eq!(
        library().install_into(&mut registry).unwrap(),
        InstallPath::Stable
    );
    registry
}

fn static_registry() -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    registry
}

fn typed(registry: &mut PluginRegistry) {
    registry.register_stable_pod::<BoxPod>(BOX_KEY).unwrap();
    registry
        .register_stable_pod::<Vec<BoxPod>>(BOXES_KEY)
        .unwrap();
}

fn graph(registry: &PluginRegistry) -> HostGraph<HandlerRegistry> {
    let builder = registry
        .graph_builder()
        .unwrap()
        .input_as("b", TypeExpr::opaque(BOX_KEY))
        .input_as("k", TypeExpr::Scalar(ValueType::I32))
        .input_as("n", TypeExpr::Scalar(ValueType::Int))
        .input_as("dx", TypeExpr::Scalar(ValueType::I32))
        .node_id("stable_abi:pod_scale", "pod_scale")
        .node_id("stable_abi:pod_make", "pod_make")
        .node_id("stable_abi:pod_shift", "pod_shift")
        .node_id("stable_abi:pod_count", "pod_count")
        .connect("b", "pod_scale.b")
        .connect("k", "pod_scale.k")
        .connect("pod_scale.out", "scaled")
        .connect("n", "pod_make.n")
        .connect("pod_make.boxes", "pod_shift.boxes")
        .connect("dx", "pod_shift.dx")
        .connect("pod_shift.out", "shifted")
        .connect("pod_shift.out", "pod_count.boxes")
        .connect("pod_count.count", "count")
        .connect("pod_count.sum", "sum");
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

const SAMPLE: BoxPod = BoxPod {
    id: 7,
    score: 1.25,
    corners: [[1, 2], [3, 4], [5, 6], [7, 8]],
};

/// One tick: the scaled box, the shifted boxes (as the host reads them) and the count and sum.
fn tick<T>(
    host: &mut HostGraph<HandlerRegistry>,
    b: impl Send + Sync + 'static,
    read: impl Fn(&daedalus::transport::Payload) -> T,
) -> (T, T, i64, i64) {
    host.push_as("b", BOX_KEY, b);
    host.push("k", 3_i32);
    host.push("n", 5_i64);
    host.push("dx", 10_i32);
    host.tick().unwrap();
    let scaled = read(&host.take_payload("scaled").expect("scaled"));
    let shifted = read(&host.take_payload("shifted").expect("shifted"));
    let count = host.take::<i64>("count").expect("count");
    let sum = host.take::<i64>("sum").expect("sum");
    (scaled, shifted, count, sum)
}

#[test]
fn a_struct_and_a_vec_of_structs_cross_typed_or_as_pod_values() {
    // Statically installed: the plugin's own types.
    let mut host = graph(&static_registry());
    let (scaled, _, count, sum) = tick(&mut host, SAMPLE, |p| p.clone());
    let scaled = *scaled.get_ref::<BoxPod>().unwrap();
    let expected_shifted = {
        let mut shifted = graph(&static_registry());
        let (_, shifted, ..) = tick(&mut shifted, SAMPLE, |p| p.clone());
        shifted.get_ref::<Vec<BoxPod>>().unwrap().clone()
    };
    assert_eq!(scaled.id, 8);
    assert_eq!(scaled.corners[3], [21, 24]);
    assert_eq!(expected_shifted.len(), 5);

    // Stable, host registered the types: they come back as the host's `BoxPod`, twice (the
    // second tick reuses the decoded payloads' allocations).
    let mut host = graph(&stable_registry(typed));
    for _ in 0..2 {
        let (stable_scaled, shifted, stable_count, stable_sum) =
            tick(&mut host, SAMPLE, |p| p.clone());
        assert_eq!(stable_scaled.get_ref::<BoxPod>(), Some(&scaled));
        assert_eq!(shifted.get_ref::<Vec<BoxPod>>(), Some(&expected_shifted));
        assert_eq!((stable_count, stable_sum), (count, sum));
    }

    // Stable, host without the types: it feeds and gets `PodValue`s, read back by layout; the
    // shifted boxes reach `pod_count` as a `PodValue` and are checked by the plugin.
    let mut host = graph(&stable_registry(|_| {}));
    let (stable_scaled, shifted, stable_count, stable_sum) =
        tick(&mut host, PodValue::from_value(&SAMPLE), |p| {
            p.get_ref::<PodValue>().expect("a PodValue").clone()
        });
    assert_eq!(stable_scaled.get::<BoxPod>(), Ok(&scaled));
    assert!(!stable_scaled.is_slice());
    assert_eq!(shifted.as_slice::<BoxPod>().unwrap(), &expected_shifted[..]);
    assert_eq!(shifted.name(), "BoxPod");
    assert_eq!(shifted.layout_hash(), BoxPod::LAYOUT.hash);
    assert_eq!((stable_count, stable_sum), (count, sum));
    assert!(shifted.as_slice::<OtherBox>().is_err());
}

#[test]
fn another_layout_under_the_same_name_is_a_typed_error() {
    assert_ne!(OtherBox::LAYOUT.hash, BoxPod::LAYOUT.hash);
    assert_eq!(OtherBox::LAYOUT.name, BoxPod::LAYOUT.name);

    // The plugin refuses the host's `OtherBox` as its `BoxPod`.
    let mut host = graph(&stable_registry(|registry| {
        registry.register_stable_pod::<OtherBox>(BOX_KEY).unwrap();
    }));
    host.push("b", OtherBox::default());
    host.push("k", 1_i32);
    let err = host.tick().unwrap_err().to_string();
    assert!(
        err.contains("layout mismatch") && err.contains("pod_scale"),
        "{err}"
    );

    // The host refuses the plugin's `Vec<BoxPod>` as its `Vec<OtherBox>`.
    let mut host = graph(&stable_registry(|registry| {
        registry
            .register_stable_pod::<Vec<OtherBox>>(BOXES_KEY)
            .unwrap();
    }));
    host.push("n", 2_i64);
    let err = host.tick().unwrap_err().to_string();
    assert!(
        err.contains("layout mismatch") && err.contains("pod_make"),
        "{err}"
    );
}
