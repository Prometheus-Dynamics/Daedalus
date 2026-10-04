//! Enum (and other derived) config fields and node inputs bind constants without manual
//! `register_enum`/`register_const_coercer` calls: the node macros register a const coercer for
//! every port type at plugin install.

use std::borrow::Cow;

use daedalus::{
    DaedalusToValue, DaedalusTypeExpr,
    data::model::{EnumValue, Value},
    engine::{Engine, EngineConfig, HostGraph},
    macros::{NodeConfig, node, plugin},
    runtime::{
        NodeError,
        handler_registry::HandlerRegistry,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

#[derive(
    Clone, Copy, Debug, Default, PartialEq, serde::Deserialize, DaedalusTypeExpr, DaedalusToValue,
)]
#[serde(rename_all = "snake_case")]
#[daedalus(type_key = "test:enumcfg:border")]
enum Border {
    #[default]
    Reflect,
    Constant,
    Wrap,
}

/// Derives only the schema: no serde, coerced through `DaedalusTypeExpr::from_value`.
#[derive(Clone, Copy, Debug, PartialEq, DaedalusTypeExpr)]
#[daedalus(type_key = "test:enumcfg:interp")]
enum Interp {
    Nearest,
    Linear,
}

/// A struct const, coerced through serde.
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
struct Size {
    w: i64,
    h: i64,
}

#[derive(Clone, Debug, NodeConfig)]
struct BlurCfg {
    #[port(default = "constant")]
    border: Border,
    #[port(default = "linear")]
    interp: Interp,
    #[port(default = 3)]
    radius: i32,
}

#[node(id = "blur", inputs("x", config = BlurCfg), outputs("out"))]
fn blur(x: i64, cfg: BlurCfg) -> Result<String, NodeError> {
    Ok(format!(
        "{x}:{:?}:{:?}:{}",
        cfg.border, cfg.interp, cfg.radius
    ))
}

#[node(
    id = "pick",
    inputs("x", port(name = "mode", default = "wrap"), "size"),
    outputs("out")
)]
fn pick(x: i64, mode: Border, size: &Size) -> Result<String, NodeError> {
    Ok(format!("{x}:{mode:?}:{}x{}", size.w, size.h))
}

/// Passes `x` on so `blur` is fed by an edge (fanning a host input out is a separate issue).
#[node(id = "border_source", inputs("x"), outputs("border", "x"))]
fn border_source(x: i64) -> Result<(Border, i64), NodeError> {
    Ok((if x > 0 { Border::Wrap } else { Border::Reflect }, x))
}

#[plugin(id = "test.enumcfg", nodes(blur, pick, border_source))]
struct EnumCfgPlugin;

fn blur_graph(consts: &[(&str, Value)]) -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    let plugin = EnumCfgPlugin::new();
    registry.install_plugin(&plugin).expect("install");
    let node = plugin.blur.alias("blur");
    let mut builder = registry
        .graph_builder()
        .unwrap()
        .try_node(&node)
        .and_then(|b| b.try_connect("x", &node.inputs.x))
        .and_then(|b| b.try_connect(&node.outputs.out, "out"))
        .unwrap();
    for (port, value) in consts {
        builder = builder.const_input_by_id("blur", *port, Some(value.clone()));
    }
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(&registry, builder.build())
        .expect("compile")
}

fn run(host: &mut HostGraph<HandlerRegistry>) -> String {
    host.run_once::<_, String>(("x", 1_i64), "out")
        .expect("run")
        .pop()
        .expect("one output")
}

#[test]
fn enum_config_fields_use_their_port_defaults() {
    assert_eq!(run(&mut blur_graph(&[])), "1:Constant:Linear:3");
}

#[test]
fn enum_config_fields_take_graph_constants_by_name_index_or_enum_value() {
    let enum_value = |name: &str| {
        Value::Enum(EnumValue {
            name: name.into(),
            value: None,
        })
    };
    for (border, interp, expected) in [
        (
            Value::String(Cow::from("wrap")),
            Value::Int(0),
            "Wrap:Nearest",
        ),
        (
            Value::String(Cow::from("Reflect")),
            enum_value("linear"),
            "Reflect:Linear",
        ),
        (
            enum_value("wrap"),
            Value::String(Cow::from("Nearest")),
            "Wrap:Nearest",
        ),
        (Value::Int(1), Value::Int(1), "Constant:Linear"),
    ] {
        let mut host = blur_graph(&[("border", border), ("interp", interp)]);
        assert_eq!(run(&mut host), format!("1:{expected}:3"));
    }
}

#[test]
fn enum_config_fields_take_values_from_graph_edges() {
    let mut registry = PluginRegistry::new();
    let plugin = EnumCfgPlugin::new();
    registry.install_plugin(&plugin).expect("install");
    let source = plugin.border_source.alias("source");
    let node = plugin.blur.alias("blur");
    let graph = registry
        .graph_builder()
        .unwrap()
        .try_node(&source)
        .and_then(|b| b.try_node(&node))
        .and_then(|b| b.try_connect("x", &source.inputs.x))
        .and_then(|b| b.try_connect(&source.outputs.x, &node.inputs.x))
        .and_then(|b| b.try_connect(&source.outputs.border, &node.spec.input("border")))
        .and_then(|b| b.try_connect(&node.outputs.out, "out"))
        .unwrap()
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(&registry, graph)
        .expect("compile");
    assert_eq!(run(&mut host), "1:Wrap:Linear:3");
}

#[test]
fn plain_enum_and_struct_inputs_take_constants() {
    let mut registry = PluginRegistry::new();
    let plugin = EnumCfgPlugin::new();
    registry.install_plugin(&plugin).expect("install");
    let node = plugin.pick.alias("pick");
    let size = Value::Struct(vec![
        daedalus::data::model::StructFieldValue {
            name: "w".into(),
            value: Value::Int(4),
        },
        daedalus::data::model::StructFieldValue {
            name: "h".into(),
            value: Value::Int(2),
        },
    ]);
    let graph = |mode: Option<Value>| {
        let mut builder = registry
            .graph_builder()
            .unwrap()
            .try_node(&node)
            .and_then(|b| b.try_connect("x", &node.inputs.x))
            .and_then(|b| b.try_connect(&node.outputs.out, "out"))
            .unwrap()
            .const_input(&node.inputs.size, Some(size.clone()));
        if mode.is_some() {
            builder = builder.const_input(&node.inputs.mode, mode);
        }
        Engine::new(EngineConfig::default())
            .unwrap()
            .compile_registry(&registry, builder.build())
            .expect("compile")
    };
    assert_eq!(run(&mut graph(None)), "1:Wrap:4x2");
    assert_eq!(
        run(&mut graph(Some(Value::String(Cow::from("constant"))))),
        "1:Constant:4x2"
    );
}

#[test]
fn explicit_coercers_win_over_generated_ones() {
    let mut registry = PluginRegistry::new();
    registry.register_const_coercer::<Border, _>(|_| Some(Border::Reflect));
    let plugin = EnumCfgPlugin::new();
    registry.install_plugin(&plugin).expect("install");
    let node = plugin.blur.alias("blur");
    let graph = registry
        .graph_builder()
        .unwrap()
        .try_node(&node)
        .and_then(|b| b.try_connect("x", &node.inputs.x))
        .and_then(|b| b.try_connect(&node.outputs.out, "out"))
        .unwrap()
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(&registry, graph)
        .expect("compile");
    assert_eq!(run(&mut host), "1:Reflect:Linear:3");
}
