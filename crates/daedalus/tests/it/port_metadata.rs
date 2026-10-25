//! `label`, `unit`, `group` and `advanced` on a port (a `NodeConfig` field's `#[port(...)]`, or a
//! node's `port(...)` input) are carried as `inputs.<port>.<key>` metadata: on the config, and on
//! the node declaration that the registry and catalog export. Ports without them carry none.

use daedalus::{
    data::model::Value,
    macros::{NodeConfig, node, plugin},
    runtime::{NodeError, config::NodeConfig as _, plugins::PluginRegistry},
};

#[derive(Clone, Debug, NodeConfig)]
struct TunedCfg {
    #[port(
        default = 4,
        min = 1,
        max = 64,
        label = "Window size",
        unit = "px",
        group = "Search"
    )]
    window: i64,
    #[port(
        default = 0.5,
        label = "Threshold",
        unit = "m",
        group = "Refine",
        advanced = true
    )]
    threshold: f64,
    #[port(default = true, advanced)]
    flag: bool,
    #[port(default = 2, advanced = false)]
    plain: i64,
}

/// `gain` is an expert knob; `value` has no display metadata.
#[node(
    id = "test.port_metadata.tune",
    inputs(
        "value",
        port(name = "gain", default = 2.0, label = "Gain", unit = "x", group = "Output", advanced = true),
        config = TunedCfg
    ),
    outputs("out")
)]
fn tune(value: f64, gain: f64, cfg: TunedCfg) -> Result<f64, NodeError> {
    Ok(value * gain + cfg.window as f64 + cfg.threshold + f64::from(cfg.flag as u8))
}

#[plugin(id = "test.port_metadata", nodes(tune))]
struct PortMetadataPlugin;

#[test]
fn config_ports_carry_their_display_metadata() {
    let meta = TunedCfg::metadata();
    let text = |key: &str| {
        meta.get(key).and_then(|v| match v {
            Value::String(s) => Some(s.to_string()),
            _ => None,
        })
    };
    assert_eq!(text("inputs.window.label").as_deref(), Some("Window size"));
    assert_eq!(text("inputs.window.unit").as_deref(), Some("px"));
    assert_eq!(text("inputs.window.group").as_deref(), Some("Search"));
    assert_eq!(text("inputs.threshold.group").as_deref(), Some("Refine"));
    assert!(matches!(
        meta.get("inputs.threshold.advanced"),
        Some(Value::Bool(true))
    ));
    // A bare `advanced` is true; `advanced = false` is an explicit false.
    assert!(matches!(
        meta.get("inputs.flag.advanced"),
        Some(Value::Bool(true))
    ));
    assert!(matches!(
        meta.get("inputs.plain.advanced"),
        Some(Value::Bool(false))
    ));
    // Ports without the keys carry none, and `description` still works next to them.
    assert!(!meta.contains_key("inputs.plain.label"));
    assert!(!meta.contains_key("inputs.window.advanced"));
}

#[test]
fn node_ports_carry_their_display_metadata_into_the_registry() {
    let mut registry = PluginRegistry::new();
    registry
        .install(&PortMetadataPlugin::new())
        .expect("install");
    let decl = registry
        .transport_capabilities
        .nodes()
        .values()
        .find(|decl| decl.id.0 == "test.port_metadata:test.port_metadata.tune")
        .expect("tune declared");
    // Metadata values are stored as tagged JSON (`{"type": ..., "value": ...}`).
    let json = |text: &str| format!("{{\"type\":\"String\",\"value\":\"{text}\"}}");
    assert_eq!(
        decl.metadata_json.get("inputs.gain.label"),
        Some(&json("Gain"))
    );
    assert_eq!(decl.metadata_json.get("inputs.gain.unit"), Some(&json("x")));
    assert_eq!(
        decl.metadata_json.get("inputs.gain.group"),
        Some(&json("Output"))
    );
    assert_eq!(
        decl.metadata_json.get("inputs.gain.advanced"),
        Some(&"{\"type\":\"Bool\",\"value\":true}".to_string())
    );
    // The config's ports are declared with the node too.
    assert_eq!(
        decl.metadata_json.get("inputs.window.label"),
        Some(&json("Window size"))
    );
    assert_eq!(decl.metadata_json.get("inputs.window.advanced"), None);
    assert!(!decl.metadata_json.contains_key("inputs.value.label"));
}
