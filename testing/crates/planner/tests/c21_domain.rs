//! Several graphs on one thread: link order, one upstream run per tick, reload in place, and the
//! refusals.

use std::sync::{Arc, Mutex};

use daedalus_abi::{OwnedStep, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    StepUnit, Transfer, UnitInfo,
};
use daedalus_graph::{GraphBody, NodeDoc, NodeKindDoc};
use daedalus_host::domain::{Domain, DomainError};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_planner::{Config, Planned, plan};
use daedalus_planner_tests::{decl, edge, node, u8_ty, u32_ty};
use daedalus_registry::{Registry, TypeRef};
use daedalus_runner::{put_typed, typed_view};
use daedalus_types::{NodeId, SpaceIndex, TypeIndex};

/// Adds one, and logs where the input lived and that it ran.
struct AddOne {
    log: Log,
}

type Log = Arc<Mutex<Vec<usize>>>;

impl OwnedStep for AddOne {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let cell = *slots.cell(0).unwrap();
        assert_eq!(cell.state, SlotState::Full as u32);
        let input: &u32 = typed_view(&cell).unwrap();
        self.log
            .lock()
            .unwrap()
            .push(std::ptr::from_ref(input) as usize);
        let value = *input + 1;
        let last = slots.len() - 1;
        put_typed(
            slots.cell(last).unwrap(),
            TypeIndex(0),
            SpaceIndex(0),
            Arc::new(value),
        );
        StepEnd::Done
    }
}

struct Test {
    log: Log,
}

impl Backend for Test {
    fn id(&self) -> &str {
        "t"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: "t.host".into(),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        offer
            .nodes
            .iter()
            .map(|n| NodeRef {
                node: n.node,
                body: n.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let info = UnitInfo {
            covers: region.nodes.to_vec(),
            reads: region.inputs.to_vec(),
            writes: region.outputs.to_vec(),
            absorbed: Vec::new(),
            may_pend: false,
        };
        let step = Step::boxed(AddOne {
            log: Arc::clone(&self.log),
        });
        Ok(Compiled::Steps(vec![StepUnit { step, info }]))
    }
}

fn add_node(registry: &mut Registry, id: &str, ty: TypeRef) {
    decl(id)
        .input("in", ty.clone())
        .output("out", ty)
        .body("t")
        .add(registry);
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    add_node(&mut registry, "t.add", u32_ty());
    add_node(&mut registry, "t.bytes", u8_ty());
    registry
}

fn bridge(input: &str, output: &str, ty: &str) -> NodeDoc {
    let mut doc = node("host");
    doc.kind = NodeKindDoc::HostBridge;
    doc.port_types.insert(input.to_string(), ty.to_string());
    doc.port_types.insert(output.to_string(), ty.to_string());
    doc
}

fn graph(body: &str, input: &str, output: &str, ty: &str) -> GraphBody {
    let mut inner = node(body);
    inner.label = Some("n".to_string());
    GraphBody {
        nodes: vec![bridge(input, output, ty), inner],
        edges: vec![edge(0, input, 1, "in"), edge(1, "out", 0, output)],
        groups: Vec::new(),
    }
}

fn planned(graph: &GraphBody, log: &Log) -> Planned {
    let backend = Test {
        log: Arc::clone(log),
    };
    plan(graph, &registry(), &[&backend], &Config::default()).unwrap()
}

fn host(log: &Log) -> LoadedHost {
    host_of(&graph("t.add", "x", "y", "u32"), log)
}

fn host_of(graph: &GraphBody, log: &Log) -> LoadedHost {
    LoadedHost::load(planned(graph, log), LoadOptions::default()).unwrap()
}

fn number(value: u32) -> Arc<u32> {
    Arc::new(value)
}

fn out(domain: &mut Domain, graph: &str) -> Option<u32> {
    let value = domain.take(graph, "y").unwrap()?;
    value.typed::<u32>().copied()
}

/// `up` feeding `a` and `b`, added downstream first.
fn fan_out(log: &Log) -> Domain {
    let mut domain = Domain::new();
    domain.add("b", host(log)).unwrap();
    domain.add("a", host(log)).unwrap();
    domain.add("up", host(log)).unwrap();
    domain.link("up", "y", "a", "x").unwrap();
    domain.link("up", "y", "b", "x").unwrap();
    domain
}

#[test]
fn graphs_tick_upstream_first_with_ties_in_the_order_added() {
    let domain = fan_out(&Log::default());
    assert_eq!(domain.tick_order().collect::<Vec<_>>(), ["up", "b", "a"]);
}

#[test]
fn a_shared_upstream_runs_once_per_tick_and_readers_share_its_value() {
    let log = Log::default();
    let mut domain = fan_out(&log);
    for round in 0..4 {
        domain.push_typed("up", "x", number(round)).unwrap();
        let tick = domain.tick();
        assert_eq!((tick.ran, tick.idle, tick.failed), (3, 0, 0));
        assert_eq!(out(&mut domain, "a"), Some(round + 2));
        assert_eq!(out(&mut domain, "b"), Some(round + 2));
        let calls = log.lock().unwrap();
        let last = &calls[calls.len() - 2..];
        assert_eq!(last[0], last[1], "both readers saw one allocation");
    }
    let stats = domain.stats();
    assert_eq!(stats.ticks, 4);
    assert_eq!(stats.avoided_runs, 4);
    let up = stats.graphs.iter().find(|row| row.name == "up").unwrap();
    assert_eq!((up.runs, up.consumers, up.avoided_runs), (4, 2, 4));
}

#[test]
fn a_graph_with_nothing_pending_does_not_tick() {
    let mut domain = fan_out(&Log::default());
    let tick = domain.tick();
    assert_eq!((tick.ran, tick.idle), (0, 3));
}

#[test]
fn the_overhead_window_reaches_domain_stats() {
    let log = Log::default();
    let mut domain = Domain::new();
    let options = LoadOptions {
        overhead_window: 8,
        ..LoadOptions::default()
    };
    let probed =
        LoadedHost::load(planned(&graph("t.add", "x", "y", "u32"), &log), options).unwrap();
    domain.add("up", probed).unwrap();
    domain.push_typed("up", "x", number(1)).unwrap();
    domain.tick();
    let stats = domain.stats();
    assert_eq!(stats.graphs[0].overhead.unwrap().ticks, 1);
}

#[test]
fn reload_in_place_keeps_links_and_upstream_state() {
    let log = Log::default();
    let mut domain = fan_out(&log);
    let up = domain.graph("up").unwrap();
    up.state().set(NodeId::new("n"), 0, 9_u32);
    domain
        .reload("up", planned(&graph("t.add", "x", "y", "u32"), &log))
        .unwrap();
    let up = domain.graph("up").unwrap();
    assert_eq!(up.state().get::<u32>(NodeId::new("n"), 0), Some(&9));
    domain.push_typed("up", "x", number(1)).unwrap();
    domain.tick();
    assert_eq!(out(&mut domain, "a"), Some(3));
}

#[test]
fn reload_missing_a_linked_port_is_refused_without_change() {
    let log = Log::default();
    let mut domain = fan_out(&log);
    let error = domain
        .reload("up", planned(&graph("t.add", "x", "gone", "u32"), &log))
        .unwrap_err();
    assert!(
        matches!(error, DomainError::ReloadBreaksLink { .. }),
        "{error}"
    );
    domain.push_typed("up", "x", number(1)).unwrap();
    domain.tick();
    assert_eq!(out(&mut domain, "a"), Some(3));
}

#[test]
fn links_are_checked() {
    let log = Log::default();
    let mut domain = fan_out(&log);
    domain
        .add("other", host_of(&graph("t.bytes", "x", "y", "u8"), &log))
        .unwrap();
    let cases = [
        (("up", "x"), ("a", "x")),
        (("up", "y"), ("a", "y")),
        (("up", "y"), ("a", "nope")),
        (("nope", "y"), ("a", "x")),
        (("up", "y"), ("a", "x")),
        (("a", "y"), ("up", "x")),
        (("up", "y"), ("other", "x")),
    ];
    let expected = [
        "WrongDirection",
        "WrongDirection",
        "NoPort",
        "NoGraph",
        "AlreadyLinked",
        "Cycle",
        "TypeMismatch",
    ];
    for (((from, out), (to, input)), name) in cases.into_iter().zip(expected) {
        let error = domain.link(from, out, to, input).unwrap_err();
        assert!(format!("{error:?}").starts_with(name), "{error:?}");
    }
    assert_eq!(domain.explain().links.len(), 2);
}

#[test]
fn explain_renders_graphs_and_links() {
    let domain = fan_out(&Log::default());
    let explanation = domain.explain();
    assert_eq!(explanation.graph("up").unwrap().consumers, ["a", "b"]);
    assert_eq!(explanation.graph("a").unwrap().upstreams, ["up"]);
    let text = explanation.to_string();
    assert!(text.contains("link up.y -> a.x"), "{text}");
    assert!(text.contains("graph up -> a, b"), "{text}");
}

#[test]
fn a_graph_comes_and_goes_between_ticks() {
    let log = Log::default();
    let mut domain = fan_out(&log);
    let removed = domain.remove("a").unwrap();
    assert_eq!(domain.tick_order().collect::<Vec<_>>(), ["up", "b"]);
    domain.add("a", removed).unwrap();
    domain.link("up", "y", "a", "x").unwrap();
    domain.push_typed("up", "x", number(1)).unwrap();
    assert_eq!(domain.tick().ran, 3);
    assert_eq!(out(&mut domain, "a"), Some(3));
}
