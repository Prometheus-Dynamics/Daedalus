//! A group's edge policy on a stage its input feeds, at run time. The stage fires only when a
//! rarer input comes, so a FIFO into it fills and overflows; the group's `LatestOnly` keeps it at
//! the newest value, whatever policy the caller's own edge into the group has.

use std::sync::Arc;

use daedalus_graph::{EdgePolicy, EdgePolicyDoc, GraphBody, GroupDef, NodeKindDoc, StagePort};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_planner::{Config, plan};
use daedalus_planner_tests::{decl, edge, node, u32_ty};
use daedalus_registry::Registry;

use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, Transfer,
};

use super::c21_host_policy::Test;

/// [`Test`] under another id, over the same memory: a node of each is a region of its own, so the
/// edge between them is a slot with the queue its policy makes.
struct Other;

impl Backend for Other {
    fn id(&self) -> &str {
        "u"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        Test.memory()
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        Test.can_take(offer)
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        Test.compile(region, cx)
    }
}

const fn stage(stage: &'static str, port: &'static str) -> StagePort {
    StagePort { stage, port }
}

/// `x` feeds the stage `rare`, which also waits for `gate`: it runs only when `gate` comes.
const RARE: GroupDef = GroupDef {
    stages: &[("rare", "t.pair")],
    inputs: &[
        ("x", &[stage("rare", "in")]),
        ("gate", &[stage("rare", "cal")]),
    ],
    outputs: &[("out", stage("rare", "out"))],
    edge_policies: &[(stage("rare", "in"), EdgePolicy::LatestOnly)],
    ..GroupDef::new("t.rare")
};

/// The same group with no edge policy.
static PLAIN: GroupDef = GroupDef {
    id: "t.plain",
    edge_policies: &[],
    ..RARE
};

static LATEST: GroupDef = RARE;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.add")
        .input("in", u32_ty())
        .output("out", u32_ty())
        .body("u")
        .add(&mut registry);
    decl("t.pair")
        .input("in", u32_ty())
        .input("cal", u32_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    for group in [&LATEST, &PLAIN] {
        decl(group.id)
            .input("x", u32_ty())
            .input("gate", u32_ty())
            .output("out", u32_ty())
            .group(group)
            .body("t")
            .add(&mut registry);
    }
    registry
}

/// The host's `x` through `src` (on the other backend) into the group's `x` over an edge with
/// `policy`, the host's `gate` into the group's `gate`, and the group's `out` to the host's `y`.
fn caller(group: &str, policy: EdgePolicyDoc) -> GraphBody {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    for port in ["x", "gate", "y"] {
        host.port_types.insert(port.to_string(), "u32".to_string());
    }
    let mut into_group = edge(1, "out", 2, "x");
    into_group.policy = policy;
    GraphBody {
        nodes: vec![host, node("t.add"), node(group)],
        edges: vec![
            edge(0, "x", 1, "in"),
            into_group,
            edge(0, "gate", 2, "gate"),
            edge(2, "out", 0, "y"),
        ],
        groups: Vec::new(),
    }
}

/// Feeds `x` every tick for more ticks than a FIFO holds and `gate` once near the end. The first
/// failed tick, if any, and what `y` gave.
fn drive(group: &str, policy: EdgePolicyDoc) -> (Option<String>, Option<u32>) {
    let planned = plan(
        &caller(group, policy),
        &registry(),
        &[&Test, &Other],
        &Config::default(),
    )
    .unwrap();
    let options = LoadOptions::default();
    let ticks = options.fifo_capacity + 16;
    let mut host = LoadedHost::load(planned, options).unwrap();
    for tick in 0..ticks {
        if let Err(error) = host.host().feed_typed("x", Arc::new(tick)) {
            return (Some(format!("tick {tick}: {error:?}")), None);
        }
        if tick == ticks - 1 {
            host.host().feed_typed("gate", Arc::new(1000_u32)).unwrap();
        }
        if let Err(error) = host.step() {
            return (Some(format!("tick {tick}: {error:?}")), None);
        }
    }
    let y = host
        .host()
        .take_value("y")
        .unwrap()
        .and_then(|value| value.typed::<u32>().copied());
    (None, y)
}

#[test]
fn an_input_fed_conditional_stage_with_latest_only_runs_without_overflow_for_any_caller() {
    let last = LoadOptions::default().fifo_capacity + 15;
    for caller in [EdgePolicyDoc::Fifo, EdgePolicyDoc::LatestOnly] {
        let (failed, y) = drive("t.rare", caller.clone());
        assert_eq!(failed, None, "{caller:?}");
        // The stage ran once, on the newest `x` and the gate.
        assert_eq!(y, Some(last + 1000), "{caller:?}");
    }
}

#[test]
fn the_same_stage_with_no_policy_overflows_behind_a_fifo_caller() {
    let (failed, _) = drive("t.plain", EdgePolicyDoc::Fifo);
    let failed = failed.expect("a FIFO into a stage that rarely runs must overflow");
    let at = LoadOptions::default().fifo_capacity;
    assert!(failed.starts_with(&format!("tick {at}:")), "{failed}");
}
