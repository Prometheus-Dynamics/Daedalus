//! A sensor graph the generated-code tests share.
//!
//! ```text
//! host.sample (latest) -> scale -> lowpass -> threshold -> rising -> host.rises (bounded 8)
//!                                     |            \-----> blink  -> host.led
//!                                     \-> host.level (latest)
//! ```

#![allow(dead_code)]

use daedalus_cpu::source::SourceBackend;
use daedalus_cpu::{Ctx, FanIn, Fault};
use daedalus_graph::{
    EdgeDoc, EdgePolicyDoc, GraphBody, Lit, LitField, NodeDoc, NodeKindDoc, PortRefDoc,
};
use daedalus_macros::node;
use daedalus_planner::{Config, Form, PlanError, plan};
use daedalus_registry::Registry;

#[node(id = "blink.scale", inputs(raw, per_count), outputs(volts))]
pub fn scale(raw: u16, per_count: f32) -> f32 {
    f32::from(raw) * per_count
}

#[derive(Default)]
pub struct Lowpass {
    pub y: f32,
    pub primed: bool,
}

#[node(id = "blink.lowpass", inputs(x, alpha), outputs(y), state(Lowpass))]
pub fn lowpass(x: f32, alpha: f32, state: &mut Lowpass) -> f32 {
    if !state.primed {
        state.y = x;
        state.primed = true;
    }
    state.y += alpha * (x - state.y);
    state.y
}

#[node(
    id = "blink.threshold",
    inputs(level, on, off),
    outputs(active),
    state(bool)
)]
pub fn threshold(level: &f32, on: f32, off: f32, state: &mut bool) -> bool {
    if *level >= on {
        *state = true;
    } else if *level <= off {
        *state = false;
    }
    *state
}

#[derive(Default)]
pub struct Edges {
    pub last: bool,
    pub count: u32,
}

#[node(id = "blink.rising", inputs(active), outputs(count), state(Edges))]
pub fn rising(active: bool, state: &mut Edges) -> Option<u32> {
    let rose = active && !state.last;
    state.last = active;
    rose.then(|| {
        state.count += 1;
        state.count
    })
}

#[node(id = "blink.blink", inputs(active, period), outputs(led))]
pub fn blink(ctx: &Ctx, active: bool, period: u64) -> bool {
    active && (ctx.now_ns / period.max(1)).is_multiple_of(2)
}

pub const ALPHA: f32 = 0.25;
pub const PER_COUNT: f32 = 0.001;
pub const ON: f32 = 2.0;
pub const OFF: f32 = 1.0;
pub const PERIOD: u64 = 4;

fn wire(from: u32, port: &str, to: u32, input: &str, policy: EdgePolicyDoc) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: input.to_string(),
        },
        policy,
    }
}

fn bridge(ports: &[(&str, &str)]) -> NodeDoc {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    for (port, ty) in ports {
        bridge.port_types.insert(port.to_string(), ty.to_string());
    }
    bridge
}

fn with(id: &str, consts: &[(&str, Lit)]) -> NodeDoc {
    let mut node = NodeDoc::new(id);
    for (port, value) in consts {
        node.consts.insert(port.to_string(), value.clone());
    }
    node
}

pub fn graph() -> GraphBody {
    let bounded = EdgePolicyDoc::Bounded {
        capacity: 8,
        overflow: "drop_oldest".to_string(),
    };
    let fifo = EdgePolicyDoc::default;
    GraphBody {
        nodes: vec![
            bridge(&[
                ("sample", "u16"),
                ("level", "f32"),
                ("led", "bool"),
                ("rises", "u32"),
            ]),
            with(
                "blink.scale",
                &[("per_count", Lit::Float(f64::from(PER_COUNT)))],
            ),
            with("blink.lowpass", &[("alpha", Lit::Float(f64::from(ALPHA)))]),
            with(
                "blink.threshold",
                &[
                    ("on", Lit::Float(f64::from(ON))),
                    ("off", Lit::Float(f64::from(OFF))),
                ],
            ),
            with("blink.rising", &[]),
            with("blink.blink", &[("period", Lit::Int(PERIOD as i64))]),
        ],
        edges: vec![
            wire(0, "sample", 1, "raw", EdgePolicyDoc::LatestOnly),
            wire(1, "volts", 2, "x", fifo()),
            wire(2, "y", 3, "level", fifo()),
            wire(2, "y", 0, "level", EdgePolicyDoc::LatestOnly),
            wire(3, "active", 4, "active", fifo()),
            wire(3, "active", 5, "active", fifo()),
            wire(4, "count", 0, "rises", bounded),
            wire(5, "led", 0, "led", fifo()),
        ],
        groups: Vec::new(),
    }
}

fn labeled(mut node: NodeDoc, label: &str) -> NodeDoc {
    node.label = Some(label.to_string());
    node
}

fn field(name: &str, value: Lit) -> LitField {
    LitField {
        name: name.to_string(),
        value,
    }
}

/// [`graph`] with four of its constants marked as parameters.
pub fn tunable() -> GraphBody {
    let mut graph = graph();
    let mark = |node: &mut NodeDoc, value: Lit| {
        node.metadata.insert(PARAMS_KEY.to_string(), value);
    };
    graph.nodes[2] = labeled(graph.nodes[2].clone(), "lowpass");
    graph.nodes[3] = labeled(graph.nodes[3].clone(), "threshold");
    graph.nodes[5] = labeled(graph.nodes[5].clone(), "blink");
    let range = |low: f64, high: f64| Lit::List(vec![Lit::Float(low), Lit::Float(high)]);
    mark(
        &mut graph.nodes[2],
        Lit::Struct(vec![field("alpha", range(0.0, 1.0))]),
    );
    mark(
        &mut graph.nodes[3],
        Lit::Struct(vec![field("on", range(0.0, 5.0)), field("off", Lit::Unit)]),
    );
    mark(
        &mut graph.nodes[5],
        Lit::List(vec![Lit::Str("period".to_string())]),
    );
    graph
}

pub const PARAMS_KEY: &str = "daedalus.chip.params";

/// A graph whose two host inputs become the same function name.
pub fn clashing() -> GraphBody {
    let mut graph = graph();
    graph.nodes[0] = bridge(&[
        ("a-b", "u16"),
        ("a_b", "f32"),
        ("level", "f32"),
        ("led", "bool"),
        ("rises", "u32"),
    ]);
    graph.nodes[1] = with("blink.scale", &[]);
    graph.edges[0] = wire(0, "a-b", 1, "raw", EdgePolicyDoc::LatestOnly);
    graph
        .edges
        .push(wire(0, "a_b", 1, "per_count", EdgePolicyDoc::LatestOnly));
    graph
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Reading {
    pub v: u32,
}

impl daedalus_types::Typed for Reading {
    const KEY: daedalus_types::TypeKey = daedalus_types::TypeKey::new("t:reading");
    const EXPR: daedalus_types::TypeExpr = <u32 as daedalus_types::Typed>::EXPR;
}

#[node(id = "mix.wrap", inputs(a), outputs(out))]
pub fn wrap(a: u32) -> Reading {
    Reading { v: a }
}

#[node(id = "mix.bump", inputs(r), outputs(out))]
pub fn bump(r: &mut u32) -> Reading {
    *r += 1;
    Reading { v: *r }
}

#[node(id = "mix.sum", inputs(items), outputs(out), fire = "any")]
pub fn sum(items: FanIn<'_, Reading>) -> u32 {
    items.iter().map(|item| item.v).sum()
}

#[node(id = "mix.check", inputs(a, limit), outputs(out))]
pub fn check(a: u32, limit: u32) -> Result<u32, Fault> {
    if a > limit {
        return Err(Fault {
            code: a,
            reason: "too big",
        });
    }
    Ok(a)
}

#[node(id = "mix.split", inputs(a), outputs(low, high))]
pub fn split(a: u32) -> (u32, u32) {
    (a, a + 1)
}

#[node(id = "mix.fallback", inputs(a, by), outputs(out))]
pub fn fallback(a: Option<&u32>, by: u32) -> Reading {
    Reading {
        v: a.copied().unwrap_or(by),
    }
}

#[node(id = "mix.text", inputs(a), outputs(out))]
pub fn text(a: u32) -> String {
    a.to_string()
}

#[node(id = "mix.len", inputs(s), outputs(out))]
pub fn length(s: String) -> u32 {
    s.len() as u32
}

/// A value that can't live in a fixed queue.
pub fn heap_port() -> GraphBody {
    GraphBody {
        nodes: vec![
            bridge(&[("a", "u32"), ("low", "u32")]),
            with("mix.text", &[]),
            with("mix.len", &[]),
        ],
        edges: vec![
            wire(0, "a", 1, "a", EdgePolicyDoc::default()),
            wire(1, "out", 2, "s", EdgePolicyDoc::default()),
            wire(2, "out", 0, "low", EdgePolicyDoc::default()),
        ],
        groups: Vec::new(),
    }
}

/// Two graphs with nothing between them, which can't share one module.
pub fn disconnected() -> GraphBody {
    GraphBody {
        nodes: vec![
            bridge(&[("a", "u32"), ("b", "u32"), ("x", "u32"), ("y", "u32")]),
            with("mix.split", &[]),
            with("mix.split", &[]),
        ],
        edges: vec![
            wire(0, "a", 1, "a", EdgePolicyDoc::default()),
            wire(1, "low", 0, "x", EdgePolicyDoc::default()),
            wire(0, "b", 2, "a", EdgePolicyDoc::default()),
            wire(2, "low", 0, "y", EdgePolicyDoc::default()),
        ],
        groups: Vec::new(),
    }
}

/// Fan-in, a fallible body, `&mut` inputs, optional inputs, tuples and a type of the crate's own.
pub fn mixed() -> GraphBody {
    let fifo = EdgePolicyDoc::default;
    let latest = || EdgePolicyDoc::LatestOnly;
    GraphBody {
        nodes: vec![
            bridge(&[
                ("a", "u32"),
                ("b", "u32"),
                ("maybe", "u32"),
                ("low", "u32"),
                ("high", "u32"),
            ]),
            with("mix.wrap", &[]),
            with("mix.bump", &[]),
            with("mix.sum", &[]),
            with("mix.check", &[("limit", Lit::Int(50))]),
            with("mix.split", &[]),
            with("mix.fallback", &[("by", Lit::Int(9))]),
            with("mix.wrap", &[]),
        ],
        edges: vec![
            wire(0, "a", 1, "a", fifo()),
            wire(1, "out", 3, "items0", fifo()),
            wire(0, "b", 2, "r", fifo()),
            wire(0, "a", 7, "a", fifo()),
            wire(7, "out", 3, "items2", fifo()),
            wire(2, "out", 3, "items1", fifo()),
            wire(3, "out", 4, "a", fifo()),
            wire(4, "out", 5, "a", fifo()),
            wire(5, "low", 0, "low", fifo()),
            wire(5, "high", 0, "high", fifo()),
            wire(0, "maybe", 6, "a", latest()),
            wire(6, "out", 3, "items3", latest()),
        ],
        groups: Vec::new(),
    }
}

pub fn generate(graph: &GraphBody) -> Result<String, PlanError> {
    generate_with(graph, Config::default().fifo_capacity)
}

pub fn generate_with(graph: &GraphBody, fifo_capacity: u32) -> Result<String, PlanError> {
    generate_full(graph, fifo_capacity, false)
}

/// With every marked parameter fixed.
pub fn generate_frozen(graph: &GraphBody) -> Result<String, PlanError> {
    generate_full(graph, Config::default().fifo_capacity, true)
}

fn generate_full(
    graph: &GraphBody,
    fifo_capacity: u32,
    freeze_params: bool,
) -> Result<String, PlanError> {
    let mut registry = Registry::new();
    registry
        .register_all(&[
            &SCALE_NODE,
            &LOWPASS_NODE,
            &THRESHOLD_NODE,
            &RISING_NODE,
            &BLINK_NODE,
            &WRAP_NODE,
            &BUMP_NODE,
            &SUM_NODE,
            &CHECK_NODE,
            &SPLIT_NODE,
            &FALLBACK_NODE,
            &TEXT_NODE,
            &LENGTH_NODE,
        ])
        .unwrap();
    let backend = SourceBackend {
        name: "Graph",
        root: Some("crate"),
    };
    let config = Config {
        form: Form::Source,
        fifo_capacity,
        freeze_params,
        ..Config::default()
    };
    plan(graph, &registry, &[&backend], &config)?.source()
}
