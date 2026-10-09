//! Declared host port types survive embedding: an inner graph whose typed host input `x: i32`
//! fans into an `i32` node and, through the planner's widening adapter, an `i64` node keeps
//! `i32` as the outer host port's type when the outer graph leaves that port undeclared, both for
//! graph-backed nodes (the planner's embedded-graph expansion) and `GraphBuilder::nest`.

use daedalus::{
    data::model::Value,
    engine::{Engine, EngineConfig, HostGraph},
    graph_builder::{NestedGraph, graph_to_json},
    macros::{node, plugin},
    planner::Graph,
    registry::capability::{NodeDecl, PortDecl},
    runtime::{
        EMBEDDED_GRAPH_KEY, EMBEDDED_HOST_KEY, NodeError, handler_registry::HandlerRegistry,
        plugins::PluginRegistry,
    },
};

#[node(id = "embed.inc", inputs("x"), outputs("out"))]
fn inc(x: i32) -> Result<i32, NodeError> {
    Ok(x + 1)
}

#[node(id = "embed.double", inputs("x"), outputs("out"))]
fn double(x: i64) -> Result<i64, NodeError> {
    Ok(x * 2)
}

#[plugin(id = "embed", nodes(inc, double))]
struct EmbedPlugin;

fn installed() -> (PluginRegistry, EmbedPlugin) {
    let mut registry = PluginRegistry::new();
    let plugin = EmbedPlugin::new();
    registry.install(&plugin).expect("install");
    (registry, plugin)
}

/// host `x: i32` → inc.x and → double.x (`i32 -> i64` adapter); outputs `inc`, `doubled`.
fn inner_graph(registry: &PluginRegistry, plugin: &EmbedPlugin) -> Graph {
    let (inc, double) = (
        plugin.inc.clone().alias("inc"),
        plugin.double.clone().alias("double"),
    );
    registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i32>("x")
        .and_then(|b| b.try_node(&inc))
        .and_then(|b| b.try_node(&double))
        .and_then(|b| b.try_connect("x", &inc.inputs.x))
        .and_then(|b| b.try_connect("x", &double.inputs.x))
        .and_then(|b| b.try_connect(&inc.outputs.out, "inc"))
        .and_then(|b| b.try_connect(&double.outputs.out, "doubled"))
        .expect("wire inner graph")
        .build()
}

fn compile(registry: &PluginRegistry, graph: Graph) -> HostGraph<HandlerRegistry> {
    Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(registry, graph)
        .expect("compile")
}

fn assert_runs(mut host: HostGraph<HandlerRegistry>) {
    host.push("x", 20_i32);
    host.tick().expect("tick");
    assert_eq!(host.take::<i32>("inc"), Some(21));
    assert_eq!(host.take::<i64>("doubled"), Some(40));
}

#[test]
fn graph_backed_node_carries_its_typed_host_input() {
    let (mut registry, plugin) = installed();
    let json = graph_to_json(&inner_graph(&registry, &plugin)).expect("inner json");
    let port = |port: &PortDecl, name: &str| PortDecl {
        name: name.into(),
        ..port.clone()
    };
    let (inc, double) = (
        IncNode::node_decl().unwrap(),
        DoubleNode::node_decl().unwrap(),
    );
    let decl = NodeDecl::new("embed.widen")
        .input(port(&inc.inputs[0], "x"))
        .output(port(&inc.outputs[0], "inc"))
        .output(port(&double.outputs[0], "doubled"))
        .metadata(EMBEDDED_GRAPH_KEY, Value::String(json.into()))
        .metadata(EMBEDDED_HOST_KEY, Value::String("host".into()));
    registry
        .register_node_decl(decl)
        .expect("register graph node");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .try_node_from_id("embed.widen", "widen")
        .and_then(|b| b.try_connect("x", ("widen", "x")))
        .and_then(|b| b.try_connect(("widen", "inc"), "inc"))
        .and_then(|b| b.try_connect(("widen", "doubled"), "doubled"))
        .expect("wire outer graph")
        .build();
    assert_runs(compile(&registry, graph));
}

#[test]
fn nested_graph_carries_its_typed_host_input() {
    let (registry, plugin) = installed();
    let nested = NestedGraph::first_host(inner_graph(&registry, &plugin)).expect("nested");
    let (builder, handle) = registry
        .graph_builder()
        .expect("builder")
        .try_nest(&nested, "widen")
        .expect("nest");
    let graph = builder
        .try_connect_to_nested("x", &handle, "x")
        .and_then(|b| b.try_connect_from_nested(&handle, "inc", "inc"))
        .and_then(|b| b.try_connect_from_nested(&handle, "doubled", "doubled"))
        .expect("wire outer graph")
        .build();
    assert_runs(compile(&registry, graph));
}
