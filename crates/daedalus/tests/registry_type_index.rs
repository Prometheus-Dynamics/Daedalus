//! Generic pushes (`push::<T>`, `bind_input`, `NodeIo::push_to`, `input_typed`) resolve through
//! the registry a graph came from, never through process-global state, so install order does
//! not matter; payloads whose key the registry maps to another Rust type are refused at the
//! host bridge; and type registrations are strict.

use std::sync::Arc;

use daedalus::{
    data::{model::TypeExpr, named_types::HostExportPolicy},
    engine::{Engine, EngineConfig, EngineError, HostGraph},
    macros::{node, plugin},
    runtime::{
        NodeError,
        handler_registry::HandlerRegistry,
        io::NodeIo,
        plugins::{PluginError, PluginRegistry, RegistryPluginExt},
    },
    transport::{FeedOutcome, Payload, TypeKey, TypeKeyError},
};

/// Stands in for a library whose optional `daedalus` feature owns its key.
mod lib {
    #[daedalus::type_key("test:keyindex:frame")]
    pub struct Frame {
        pub width: u32,
    }

    #[daedalus::plugin(id = "test.keyindex.owner", types(Frame))]
    pub struct OwnerPlugin;
}

const FRAME_KEY: &str = "test:keyindex:frame";

#[node(id = "frame_width", inputs("frame"), outputs("width"))]
fn frame_width(frame: &lib::Frame) -> Result<i64, NodeError> {
    Ok(i64::from(frame.width))
}

#[plugin(id = "test.keyindex.consumer", nodes(frame_width))]
struct ConsumerPlugin;

#[node(id = "double", inputs("x"), outputs("y"))]
fn double(x: i64) -> Result<i64, NodeError> {
    Ok(x * 2)
}

#[plugin(id = "test.keyindex.math", nodes(double))]
struct MathPlugin;

/// A type no registry knows.
struct Unregistered;

/// `host.frame -> frame_width -> host.width`, the consumer installed before the type's owner.
fn frame_graph() -> Result<HostGraph<HandlerRegistry>, EngineError> {
    let mut registry = PluginRegistry::new();
    let consumer = ConsumerPlugin::new();
    registry.install_plugin(&consumer).unwrap();
    registry.install_plugin(&lib::OwnerPlugin::new()).unwrap();
    let node = consumer.frame_width.alias("width_of");
    let graph = registry
        .graph_builder()
        .unwrap()
        .input_typed::<lib::Frame>("frame")
        .and_then(|b| b.try_node(&node))
        .and_then(|b| b.try_connect("frame", &node.inputs.frame))
        .and_then(|b| b.try_connect(&node.outputs.width, "width"))
        .unwrap()
        .build();
    Engine::new(EngineConfig::default())?.compile_registry(&registry, graph)
}

#[test]
fn typed_pushes_resolve_through_the_registry_whatever_the_install_order() {
    let mut host = frame_graph().expect("compile");
    assert_eq!(
        host.host_inputs()[0].type_expr,
        Some(TypeExpr::opaque(FRAME_KEY))
    );
    assert_eq!(
        host.type_index().key_of::<lib::Frame>(),
        Ok(TypeKey::new(FRAME_KEY))
    );

    let outcome = host.push("frame", lib::Frame { width: 7 });
    assert!(
        matches!(outcome, FeedOutcome::Accepted { .. }),
        "{outcome:?}"
    );
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("width"), Some(7));

    let input = host.bind_input::<lib::Frame>("frame").expect("bind");
    input.push(lib::Frame { width: 9 });
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("width"), Some(9));
}

#[test]
fn unknown_types_fail_with_the_fixes_even_if_another_registry_knows_them() {
    // Another registry in this process knows `Frame`; this graph's registry does not.
    PluginRegistry::new()
        .install_plugin(&lib::OwnerPlugin::new())
        .unwrap();
    let mut registry = PluginRegistry::new();
    let math = MathPlugin::new();
    registry.install_plugin(&math).unwrap();
    let node = math.double.alias("double");
    let graph = registry
        .graph_builder()
        .unwrap()
        .try_node(&node)
        .and_then(|b| b.try_connect("x", &node.inputs.x))
        .and_then(|b| b.try_connect(&node.outputs.y, "y"))
        .unwrap()
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(&registry, graph)
        .unwrap();

    let FeedOutcome::Rejected(err) = host.push("x", lib::Frame { width: 1 }) else {
        panic!("unknown type must be rejected");
    };
    let TypeKeyError::Unkeyed { rust_type } = *err else {
        panic!("unexpected error: {err}");
    };
    assert!(rust_type.ends_with("lib::Frame"), "{rust_type}");
    let err = host.bind_input::<Unregistered>("x").err().expect("unknown");
    let message = err.to_string();
    for fix in [
        "install the plugin",
        "foreign_types(",
        "type_key",
        "push_as",
    ] {
        assert!(message.contains(fix), "{message}");
    }
    assert!(matches!(
        registry
            .graph_builder()
            .unwrap()
            .input_typed::<Unregistered>("x"),
        Err(daedalus::runtime::graph_builder::GraphBuildError::TypeKey(
            TypeKeyError::Unkeyed { .. }
        ))
    ));

    // Builtins need no registration.
    assert_eq!(host.run_once::<_, i64>(("x", 21_i64), "y").unwrap(), [42]);
}

#[test]
fn node_io_pushes_resolve_through_the_executor_type_index() {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&lib::OwnerPlugin::new()).unwrap();
    let mut io = NodeIo::empty().with_type_index(Some(registry.type_index()));
    io.push_to("out", lib::Frame { width: 1 }).unwrap();
    assert_eq!(io.outputs()[0].1.inner.type_key().as_str(), FRAME_KEY);
    assert!(io.push_to("out", Unregistered).is_err());
    // Without an index only builtins resolve.
    assert!(
        NodeIo::empty()
            .push_to("out", lib::Frame { width: 1 })
            .is_err()
    );
}

#[test]
fn payloads_holding_another_rust_type_than_their_key_are_rejected_at_the_host_bridge() {
    let host = frame_graph().expect("compile");
    let outcome = host.push_payload("frame", Payload::owned(FRAME_KEY, 5_u32));
    let FeedOutcome::Rejected(err) = outcome else {
        panic!("mismatched payload must be rejected: {outcome:?}");
    };
    let message = err.to_string();
    assert!(
        message
            .starts_with("payload for `test:keyindex:frame` holds `u32` but this graph expects `"),
        "{message}"
    );
    assert!(
        message.contains("lib::Frame` (built separately?)"),
        "{message}"
    );

    // Bytes and unknown keys pass the check.
    let bytes = Payload::bytes_with_type_key(FRAME_KEY, Arc::from(&[1_u8][..]));
    assert!(!matches!(
        host.push_payload("frame", bytes),
        FeedOutcome::Rejected(_)
    ));
    let unknown = Payload::owned("test:keyindex:other", 5_u32);
    assert!(!matches!(
        host.push_payload("frame", unknown),
        FeedOutcome::Rejected(_)
    ));
}

mod one {
    pub struct Shared;
}
mod two {
    pub struct Shared;
}

#[plugin(
    id = "test.keyindex.one",
    foreign_types(one::Shared = "test:keyindex:shared")
)]
struct OnePlugin;

#[plugin(
    id = "test.keyindex.two",
    foreign_types(two::Shared = "test:keyindex:shared")
)]
struct TwoPlugin;

#[test]
fn two_plugins_using_one_key_for_different_rust_types_fail_install() {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&OnePlugin::new()).unwrap();
    let err = registry.install_plugin(&TwoPlugin::new()).unwrap_err();
    let PluginError::BoundaryTypeConflict(ref conflict) = err else {
        panic!("unexpected error: {err}");
    };
    assert_eq!(conflict.key.as_str(), "test:keyindex:shared");
    assert!(conflict.registered.type_name.ends_with("one::Shared"));
    assert!(conflict.new.type_name.ends_with("two::Shared"));
    let message = err.to_string();
    assert!(
        message.contains("one::Shared") && message.contains("two::Shared"),
        "{message}"
    );
}

#[test]
fn registering_a_key_again_is_a_no_op_only_when_identical() {
    struct Mapped;
    let mut registry = PluginRegistry::new();
    registry
        .register_foreign_type::<Mapped>("test:keyindex:mapped")
        .unwrap();
    registry
        .register_foreign_type::<Mapped>("test:keyindex:mapped")
        .unwrap();
    let err = registry
        .register_foreign_type::<Mapped>("test:keyindex:remapped")
        .unwrap_err();
    assert!(
        matches!(err, PluginError::TypeKeyedTwice { ref existing, ref new, .. }
            if existing.as_str() == "test:keyindex:mapped" && new.as_str() == "test:keyindex:remapped"),
        "{err}"
    );

    registry.install_plugin(&lib::OwnerPlugin::new()).unwrap();
    registry.install_plugin(&lib::OwnerPlugin::new()).unwrap();

    let int = TypeExpr::Scalar(daedalus::data::model::ValueType::Int);
    let named = "test:keyindex:named";
    registry
        .register_named_type(named, int.clone(), HostExportPolicy::Value)
        .unwrap();
    registry
        .register_named_type(named, int, HostExportPolicy::Value)
        .unwrap();
    let err = registry
        .register_named_type(named, TypeExpr::opaque("other"), HostExportPolicy::Value)
        .unwrap_err();
    assert!(
        matches!(err, PluginError::TypeDeclarationConflict { ref key, .. } if key.as_str() == named),
        "{err}"
    );
}

#[node(id = "make_rgb", inputs("width"), outputs("image"))]
fn make_rgb(width: u32) -> Result<image::RgbImage, NodeError> {
    Ok(image::RgbImage::new(width, 1))
}

#[plugin(
    id = "test.keyindex.rgb",
    foreign_types(image::RgbImage = "test:keyindex:rgb"),
    nodes(make_rgb)
)]
struct RgbPlugin;

#[plugin(
    id = "test.keyindex.rgb",
    foreign_types(image::RgbImage = "test:keyindex:rgb:other"),
    nodes(make_rgb)
)]
struct OtherRgbPlugin;

/// The key `make_rgb` pushes its output under, in a registry with only `plugin` installed.
fn pushed_rgb_key(plugin: &dyn daedalus::runtime::plugins::Plugin) -> String {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(plugin).unwrap();
    let node = daedalus::NodeHandle::new("test.keyindex.rgb:make_rgb").alias("make");
    let graph = registry
        .graph_builder()
        .unwrap()
        .try_node(&node)
        .and_then(|b| b.try_connect("width", &node.input("width")))
        .and_then(|b| b.try_connect(&node.output("image"), "image"))
        .unwrap()
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(&registry, graph)
        .unwrap();
    assert!(matches!(
        host.push("width", 3_u32),
        FeedOutcome::Accepted { .. }
    ));
    host.tick().expect("tick");
    let payload = host.take_payload("image").expect("output");
    payload.type_key().as_str().to_string()
}

#[test]
fn macro_outputs_of_mapped_foreign_types_use_the_installing_registry_key() {
    // Each registry resolves the mapping it installed; neither leaks into the other.
    assert_eq!(pushed_rgb_key(&RgbPlugin::new()), "test:keyindex:rgb");
    assert_eq!(
        pushed_rgb_key(&OtherRgbPlugin::new()),
        "test:keyindex:rgb:other"
    );
}
