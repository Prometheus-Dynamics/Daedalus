//! Every direct host path (`run_direct_once`, lanes, `tick_direct_*`) delivers const inputs,
//! config fields and port defaults exactly like a regular tick (`run_once`), for single-node
//! routes and routes through several nodes.

use daedalus::{
    data::model::Value,
    engine::{Engine, EngineConfig, HostGraph},
    macros::{NodeConfig, node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
    transport::Payload,
};

#[derive(Clone, Debug, NodeConfig)]
struct ScaleCfg {
    #[port(default = 10)]
    factor: i64,
    #[port(default = 1)]
    offset: i64,
}

/// `weight` comes from a const input, `bias` from its port default, `cfg` from config fields.
#[node(
    id = "test.direct.weighted",
    inputs("value", "weight", port(name = "bias", default = 5), config = ScaleCfg),
    outputs("out")
)]
fn weighted(value: i64, weight: i64, bias: i64, cfg: ScaleCfg) -> Result<i64, NodeError> {
    Ok((value * weight + bias) * cfg.factor + cfg.offset)
}

#[node(id = "test.direct.add", inputs("value", "amount"), outputs("out"))]
fn add(value: &i64, amount: &i64) -> Result<i64, NodeError> {
    Ok(value + amount)
}

#[plugin(id = "test.direct_consts", nodes(weighted, add))]
struct DirectConstsPlugin;

/// `in -> weighted -> out`, or `in -> add -> weighted -> out` when `chained`, with
/// `weighted.weight = 3`, `weighted.factor = 2` and `add.amount = 100` as constants.
fn graph(chained: bool) -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    let plugin = DirectConstsPlugin::new();
    registry.install(&plugin).expect("install plugin");
    let weighted = plugin.weighted.alias("weighted");
    let add = plugin.add.alias("add");
    let mut builder = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&weighted)
        .expect("weighted node")
        .try_connect(&weighted.outputs.out, "out")
        .expect("output edge")
        .const_input(&weighted.inputs.weight, Some(Value::Int(3)))
        .const_input_by_id("weighted", "factor", Some(Value::Int(2)));
    builder = if chained {
        builder
            .try_node(&add)
            .and_then(|b| b.try_connect("in", &add.inputs.value))
            .and_then(|b| b.try_connect(&add.outputs.out, &weighted.inputs.value))
            .expect("chain")
            .const_input(&add.inputs.amount, Some(Value::Int(100)))
    } else {
        builder
            .try_connect("in", &weighted.inputs.value)
            .expect("input edge")
    };
    Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, builder.build())
        .expect("compile graph")
}

/// What every entry point must produce for input `value`.
fn expected(chained: bool, value: i64) -> i64 {
    let value = if chained { value + 100 } else { value };
    (value * 3 + 5) * 2 + 1
}

fn int(payload: Option<Payload>) -> i64 {
    *payload
        .expect("an output payload")
        .get_ref::<i64>()
        .expect("an i64 output")
}

#[test]
fn direct_paths_deliver_consts_defaults_and_config_like_run_once() {
    for chained in [false, true] {
        let mut host = graph(chained);
        let want = |value| expected(chained, value);
        assert_eq!(
            host.run_once::<_, i64>(("in", 1_i64), "out")
                .expect("run_once"),
            [want(1)],
            "run_once (chained: {chained})"
        );
        assert_eq!(
            host.run_direct_once::<_, i64>("in", "out", 2_i64)
                .expect("run_direct_once"),
            Some(want(2)),
            "run_direct_once (chained: {chained})"
        );

        let lane = host.bind_lane::<i64>("in", "out").expect("lane");
        assert_eq!(int(host.run_lane(&lane, 3).expect("run_lane")), want(3));
        assert_eq!(
            host.run_lane_owned::<_, i64>(&lane, 4)
                .expect("run_lane_owned"),
            Some(want(4))
        );

        let key = host.type_index().key_of::<i64>().expect("i64 key");
        let payload = |value: i64| Payload::owned(key.clone(), value);
        let (_, out) = host
            .tick_direct_payload("in", payload(5), "out")
            .expect("tick_direct_payload")
            .expect("a direct route");
        assert_eq!(int(out), want(5));

        let route = host.direct_host_route("in", "out").expect("route");
        assert_eq!(
            route.is_single_node(),
            !chained,
            "single-node fast path only without the chain"
        );
        let (_, out) = host
            .tick_direct_route(&route, payload(6))
            .expect("tick_direct_route");
        assert_eq!(int(out), want(6));
        let out = host
            .tick_direct_route_payload(&route, payload(7))
            .expect("tick_direct_route_payload");
        assert_eq!(int(out), want(7));
    }
}
