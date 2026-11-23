//! `patch_const` on a `#[node]`: the next tick reads the new value, the node's state stays, a
//! value that does not fit is refused when it is offered, and a reload keeps what was patched.

use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_derive::{FromLiteral, NodeConfig, Type};
use daedalus_graph::{
    EdgeDoc, EdgePolicyDoc, GraphBody, Lit, LitField, NodeDoc, NodeKindDoc, PortRefDoc,
};
use daedalus_host::{LoadOptions, LoadedHost, PatchError, PatchOutcome};
use daedalus_macros::node;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{NodeRecord, Registry};

#[derive(Default)]
struct Total(i64);

/// A running total, scaled by a constant.
#[node(id = "p.total", inputs(x, scale), outputs(y), state(Total))]
fn total(x: i64, scale: i64, state: &mut Total) -> i64 {
    state.0 += x * scale;
    state.0
}

#[derive(Clone, Type, NodeConfig)]
#[type_key("p:offset")]
struct Offset {
    #[port(default = 1)]
    by: i64,
    #[port(default = 1)]
    times: i64,
}

impl Default for Offset {
    fn default() -> Offset {
        Offset { by: 1, times: 1 }
    }
}

/// `x` moved by its config.
#[node(id = "p.offset", inputs(x, config = Offset), outputs(y))]
fn offset(x: i64, config: &Offset) -> i64 {
    (x + config.by) * config.times
}

#[node(id = "p.negate", inputs(x), outputs(y))]
fn negate(x: i64) -> i64 {
    -x
}

const NODES: [&NodeRecord; 3] = [&TOTAL_NODE, &OFFSET_NODE, &NEGATE_NODE];

fn labelled(id: &str, label: &str) -> NodeDoc {
    let mut doc = NodeDoc::new(id);
    doc.label = Some(label.to_string());
    doc
}

fn edge(from: u32, to: u32) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: if from == 0 { "x" } else { "y" }.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: if to == 0 { "y" } else { "x" }.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

fn offset_config(by: i64, times: i64) -> Lit {
    let field = |name: &str, value| LitField {
        name: name.to_string(),
        value: Lit::Int(value),
    };
    Lit::Struct(vec![field("by", by), field("times", times)])
}

/// `host.x -> offset -> (negate ->) total -> host.y`, the total scaled by 5 and the offset by 1.
fn planned(merge: bool, negate: bool) -> Planned {
    let mut bridge = labelled("host", "io");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "y"] {
        bridge
            .port_types
            .insert(port.to_string(), "i64".to_string());
    }
    let mut offset = labelled("p.offset", "offset");
    offset
        .consts
        .insert("config".to_string(), offset_config(1, 1));
    let mut total = labelled("p.total", "total");
    total.consts.insert("scale".to_string(), Lit::Int(5));
    let (nodes, edges) = if negate {
        let negate = labelled("p.negate", "negate");
        let edges = vec![edge(0, 1), edge(1, 2), edge(2, 3), edge(3, 0)];
        (vec![bridge, offset, negate, total], edges)
    } else {
        let edges = vec![edge(0, 1), edge(1, 2), edge(2, 0)];
        (vec![bridge, offset, total], edges)
    };
    let graph = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    let config = Config {
        merge,
        ..Config::default()
    };
    plan(&graph, &registry, &[&CpuBackend], &config).unwrap()
}

fn loaded(merge: bool) -> LoadedHost {
    LoadedHost::load(planned(merge, false), LoadOptions::default()).unwrap()
}

fn through(host: &mut LoadedHost, x: i64) -> i64 {
    host.host().feed_typed("x", Arc::new(x)).unwrap();
    host.step().unwrap();
    host.host().take_copy::<i64>("y").unwrap().unwrap()
}

fn patched_input(merge: bool) {
    let mut host = loaded(merge);
    // (1 + 1) * 5, then 10 + (2 + 1) * 5.
    assert_eq!([1, 2].map(|x| through(&mut host, x)), [10, 25]);
    let outcome = host.patch_const("total", "scale", Lit::Int(7)).unwrap();
    assert_eq!(outcome, PatchOutcome::Changed { version: 1 });
    // The next tick multiplies by 7, and the total goes on from 25.
    assert_eq!(through(&mut host, 1), 25 + 2 * 7);
    assert_eq!(through(&mut host, 1), 39 + 2 * 7);
}

#[test]
fn a_patched_input_constant_is_read_on_the_next_tick_and_the_state_stays() {
    patched_input(false);
}

#[cfg(feature = "merge")]
#[test]
fn a_patched_constant_reaches_a_node_inside_a_merged_step() {
    patched_input(true);
}

#[test]
fn a_patched_config_is_read_on_the_next_tick() {
    let mut host = loaded(false);
    assert_eq!(through(&mut host, 1), 10);
    host.patch_const("offset", "config", offset_config(3, 2))
        .unwrap();
    // ((1 + 3) * 2) * 5 on top of 10.
    assert_eq!(through(&mut host, 1), 50);
}

#[derive(Type, FromLiteral, Clone)]
#[type_key("p:window")]
struct Window {
    low: i64,
    high: i64,
}

/// `x` kept inside a window, which is a struct given as a constant.
#[node(id = "p.clamp", inputs(x, window), outputs(y))]
fn clamp(x: i64, window: &Window) -> i64 {
    x.clamp(window.low, window.high)
}

fn window(low: i64, high: i64) -> Lit {
    let field = |name: &str, value| LitField {
        name: name.to_string(),
        value: Lit::Int(value),
    };
    Lit::Struct(vec![field("low", low), field("high", high)])
}

#[test]
fn the_bytes_a_decode_keeps_are_counted() {
    let mut bridge = labelled("host", "io");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "y"] {
        bridge
            .port_types
            .insert(port.to_string(), "i64".to_string());
    }
    let mut clamp = labelled("p.clamp", "clamp");
    clamp.consts.insert("window".to_string(), window(0, 5));
    let graph = GraphBody {
        nodes: vec![bridge, clamp],
        edges: vec![edge(0, 1), edge(1, 0)],
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&[&CLAMP_NODE]).unwrap();
    let planned = plan(&graph, &registry, &[&CpuBackend], &Config::default()).unwrap();
    let mut host = LoadedHost::load(planned, LoadOptions::default()).unwrap();
    assert_eq!(through(&mut host, 9), 5);

    // A struct given as a constant keeps 48 bytes a field and the field names, `low` and
    // `high`, each time it is decoded: the figures the host's documentation gives. Other tests
    // decode too, so the count may grow by more.
    #[cfg(target_pointer_width = "64")]
    {
        assert_eq!(size_of::<daedalus_types::Literal>(), 32);
        assert_eq!(size_of::<(&str, daedalus_types::Literal)>(), 48);
    }
    let field = size_of::<(&str, daedalus_types::Literal)>();
    let before = daedalus_cpu::literal_bytes_kept();
    host.patch_const("clamp", "window", window(0, 7)).unwrap();
    assert!(daedalus_cpu::literal_bytes_kept() >= before + 2 * field + 7);
    assert_eq!(through(&mut host, 9), 7);
}

#[test]
fn a_value_that_does_not_fit_is_refused_at_the_patch_and_the_old_one_stays() {
    let mut host = loaded(false);
    let error = host
        .patch_const("total", "scale", Lit::Str("seven".to_string()))
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "const `total.scale` was refused: the constant for port `scale` doesn't fit"
    );
    let error = host
        .patch_const(
            "offset",
            "config",
            Lit::Struct(vec![LitField {
                name: "by".to_string(),
                value: Lit::Str("far".to_string()),
            }]),
        )
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "const `offset.config` was refused: the config doesn't fit its type"
    );
    assert!(matches!(error, PatchError::Refused { .. }));
    // Neither moved: the table and the steps hold what the plan gave.
    assert_eq!(
        *host.consts().get("total", "scale").unwrap().value,
        Lit::Int(5)
    );
    assert_eq!(through(&mut host, 1), 10);
}

#[test]
fn a_reload_keeps_a_patched_constant_whether_its_step_is_kept_or_made_again() {
    let mut host = loaded(false);
    host.patch_const("total", "scale", Lit::Int(7)).unwrap();
    assert_eq!(through(&mut host, 1), 14);

    // The same graph: the total's step is kept, with the 7 it holds.
    let report = host.reload(planned(false, false)).unwrap();
    assert_eq!(report.fresh.total(), 0);
    assert_eq!(through(&mut host, 1), 28);

    // A node is put before the total, so the total is at another place in the graph and its
    // step is made again from the plan, which says 5. The patched 7 is offered to the new step.
    let report = host.reload(planned(false, true)).unwrap();
    assert!(report.fresh.names().iter().any(|name| name == "total"));
    assert_eq!(
        *host.consts().get("total", "scale").unwrap().value,
        Lit::Int(7)
    );
    assert_eq!(through(&mut host, 1), -14);
}
