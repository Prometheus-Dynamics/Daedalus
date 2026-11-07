//! Codecs on the stable path: `examples/plugins/stable_abi` registers a codec for its
//! `Local<Tracker>` (`{ id, hits }` as a struct `Value`, and 16 bytes). A tracker reaches a host
//! `Value` port through the adapter the planner inserts, a `Value` from the host comes back as a
//! tracker the plugin's nodes read, the host bridge inspects a tracker as its `Value`, and the
//! byte conversions round-trip; statically and through the stable path, where the host has no
//! `Tracker` type and every conversion runs in the plugin through its C codec table.
#![cfg(not(target_env = "musl"))]

#[path = "support/cdylib.rs"]
mod cdylib;

use std::sync::OnceLock;

use daedalus::PluginLibrary;
use daedalus::data::model::{StructFieldValue, TypeExpr, Value, ValueType};
use daedalus::dylib::InstallPath;
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::host_bridge::PayloadInspection;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::transport::{FeedOutcome, Payload, TypeKey, TypeKeyError};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use daedalus_plugins_stable_abi::locals::{TRACKER_KEY, tracker_schema};

fn library() -> &'static PluginLibrary {
    static LIBRARY: OnceLock<PluginLibrary> = OnceLock::new();
    // Safety: a trusted test plugin.
    LIBRARY.get_or_init(|| unsafe { PluginLibrary::load(cdylib::stable_abi_cdylib()) }.unwrap())
}

fn tracker(id: i64, hits: i64) -> Value {
    let field = |name: &str, value: i64| StructFieldValue {
        name: name.into(),
        value: Value::Int(value),
    };
    Value::Struct(vec![field("id", id), field("hits", hits)])
}

fn graph(registry: &PluginRegistry) -> HostGraph<HandlerRegistry> {
    let builder = registry
        .graph_builder()
        .unwrap()
        .input_as("id", TypeExpr::Scalar(ValueType::Int))
        .input_as("value_in", tracker_schema())
        .output_as("value", tracker_schema())
        .node_id("stable_abi:local_make", "make")
        .node_id("stable_abi:local_read", "reader")
        .connect("id", "make.id")
        .connect("make.obj", "value")
        .connect("make.obj", "made")
        .connect("value_in", "reader.obj")
        .connect("reader.id", "id_back")
        .connect("reader.hits", "hits_back");
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

fn value_of(payload: &Payload) -> &Value {
    payload.get_ref::<Value>().unwrap_or_else(|| {
        panic!(
            "a `Value` payload, found {:?} {:?}",
            payload.type_key(),
            payload.storage_rust_type_name()
        )
    })
}

fn trackers_cross_to_values_and_back(registry: &PluginRegistry) {
    let mut host = graph(registry);
    let plan = format!("{:?}", host.runtime_plan().edge_transports);
    assert!(
        plan.contains("daedalus.codec:stable_abi:tracker->value"),
        "the planner inserts the codec: {plan}"
    );
    assert!(plan.contains("daedalus.codec:value->stable_abi:tracker"));

    // A tracker to the host's `Value` port.
    host.push("id", 42_i64);
    host.tick().unwrap();
    let value = host.take_payload("value").expect("value");
    assert_eq!(value_of(&value), &tracker(42, 0));

    // The host bridge inspects the opaque object as its `Value`.
    let made = host.take_payload("made").expect("made");
    match host.inspect_payload(&made) {
        PayloadInspection::Value { value, .. } => assert_eq!(value, tracker(42, 0)),
        other => panic!("expected a value, got {other:?}"),
    }

    // A `Value` from the host back to a tracker the plugin's node reads.
    host.push_payload("value_in", Payload::owned("value_in", tracker(7, 3)));
    host.tick().unwrap();
    assert_eq!(host.take::<i64>("id_back"), Some(7));
    assert_eq!(host.take::<i64>("hits_back"), Some(3));

    // Bytes, through the registry's codec of the key.
    let codec = registry
        .payload_codec(&TypeKey::new(TRACKER_KEY))
        .expect("the tracker's codec");
    let bytes = (codec.to_bytes.as_ref().unwrap())(&made).unwrap();
    assert_eq!(bytes, [42_i64.to_le_bytes(), 0_u64.to_le_bytes()].concat());
    let back = (codec.from_bytes.as_ref().unwrap())(TypeKey::new(TRACKER_KEY), &bytes).unwrap();
    assert_eq!(
        (codec.to_value.as_ref().unwrap())(&back).unwrap(),
        tracker(42, 0)
    );
    let err = (codec.from_bytes.as_ref().unwrap())(TypeKey::new(TRACKER_KEY), &[1, 2, 3]);
    assert!(err.unwrap_err().contains("16 bytes"));
}

#[test]
fn a_plugin_type_crosses_to_a_host_value_port_and_back() {
    let mut statics = PluginRegistry::new();
    statics.install_plugin(&StableAbiPlugin::new()).unwrap();
    trackers_cross_to_values_and_back(&statics);

    let mut stables = PluginRegistry::new();
    assert_eq!(
        library().install_into(&mut stables).unwrap(),
        InstallPath::Stable
    );
    trackers_cross_to_values_and_back(&stables);
}

/// A host input keyed by the tracker's own key: `push_value` builds the tracker from a `Value`
/// through the key's codec (`from_value`), so the host never names the Rust type.
fn value_becomes_a_tracker_through_its_codec(registry: &PluginRegistry) {
    let builder = registry
        .graph_builder()
        .unwrap()
        .input_as("tracker_in", TypeExpr::opaque(TRACKER_KEY))
        .output_as("hits_back", TypeExpr::Scalar(ValueType::Int))
        .node_id("stable_abi:local_read", "reader")
        .connect("tracker_in", "reader.obj")
        .connect("reader.hits", "hits_back");
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap();

    let outcome = host.push_value("tracker_in", tracker(7, 3));
    assert!(
        matches!(outcome, FeedOutcome::Accepted { .. }),
        "{outcome:?}"
    );
    host.tick().unwrap();
    assert_eq!(host.take::<i64>("hits_back"), Some(3));

    let error = match host.push_value("tracker_in", Value::Int(1)) {
        FeedOutcome::Rejected(error) => *error,
        other => panic!("expected a rejection, got {other:?}"),
    };
    assert!(
        matches!(error, TypeKeyError::ValueRejected { ref port, .. } if port == "tracker_in"),
        "{error}"
    );
}

#[test]
fn a_value_is_converted_to_a_plugin_type_by_its_codec() {
    let mut statics = PluginRegistry::new();
    statics.install_plugin(&StableAbiPlugin::new()).unwrap();
    value_becomes_a_tracker_through_its_codec(&statics);

    let mut stables = PluginRegistry::new();
    assert_eq!(
        library().install_into(&mut stables).unwrap(),
        InstallPath::Stable
    );
    value_becomes_a_tracker_through_its_codec(&stables);
}
