//! Looking at a loaded graph's ports, patching a const between ticks, and switching nodes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    StepUnit, Transfer, UnitInfo,
};
use daedalus_graph::{GraphBody, Lit, NodeDoc, NodeKindDoc};
use daedalus_host::{
    ConstTable, ControlError, Dir, LoadOptions, LoadedHost, PatchError, PatchOutcome,
};
use daedalus_planner::{Config, plan};
use daedalus_planner_tests::{decl, edge, node, u32_ty};
use daedalus_registry::Registry;
use daedalus_runner::put_typed;
use daedalus_types::{NodeId, SpaceIndex, TypeIndex};
use daedalus_value::{Owner, Value};

static REFUSED: AtomicU32 = AtomicU32::new(0);

/// Writes its const to its output. Decodes only when the version moves.
struct Gain {
    consts: ConstTable,
    node: &'static str,
    decodes: Arc<AtomicU32>,
    seen: Option<u32>,
    value: i64,
}

impl OwnedStep for Gain {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let attempt = self.consts.patch(self.node, "amount", Lit::Int(99));
        if attempt == Err(PatchError::TickRunning) {
            REFUSED.fetch_add(1, Ordering::Relaxed);
        }
        let current = self.consts.get(self.node, "amount").unwrap();
        if self.seen != Some(current.version) {
            self.decodes.fetch_add(1, Ordering::Relaxed);
            self.seen = Some(current.version);
            let Lit::Int(value) = *current.value else {
                return StepEnd::Fault;
            };
            self.value = value;
        }
        let last = slots.len() - 1;
        let cell = slots.cell(last).unwrap();
        put_typed(
            cell,
            TypeIndex(0),
            SpaceIndex(0),
            Arc::new(self.value as u32),
        );
        StepEnd::Done
    }
}

struct Test {
    consts: ConstTable,
    decodes: Arc<AtomicU32>,
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
        let step = Step::boxed(Gain {
            consts: self.consts.clone(),
            node: "gain",
            decodes: Arc::clone(&self.decodes),
            seen: None,
            value: 0,
        });
        Ok(Compiled::Steps(vec![StepUnit { step, info }]))
    }
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.gain")
        .input("in", u32_ty())
        .optional("amount", daedalus_planner_tests::f64_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    registry
}

fn bridge() -> NodeDoc {
    let mut doc = node("host");
    doc.kind = NodeKindDoc::HostBridge;
    doc.port_types.insert("v".to_string(), "u32".to_string());
    doc.port_types
        .insert("result".to_string(), "u32".to_string());
    doc
}

fn graph() -> GraphBody {
    let mut gain = node("t.gain");
    gain.label = Some("gain".to_string());
    gain.consts.insert("amount".to_string(), Lit::Int(2));
    GraphBody {
        nodes: vec![bridge(), gain],
        edges: vec![edge(0, "v", 1, "in"), edge(1, "out", 0, "result")],
        groups: Vec::new(),
    }
}

fn loaded() -> (LoadedHost, Arc<AtomicU32>) {
    let consts = ConstTable::new();
    let decodes = Arc::new(AtomicU32::new(0));
    let backend = Test {
        consts: consts.clone(),
        decodes: Arc::clone(&decodes),
    };
    let planned = plan(&graph(), &registry(), &[&backend], &Config::default()).unwrap();
    let host = LoadedHost::load_with_consts(planned, LoadOptions::default(), consts).unwrap();
    (host, decodes)
}

fn input() -> Value {
    Value {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
        owner: Owner::Static(b"x"),
    }
}

fn run(host: &mut LoadedHost) -> u32 {
    host.host().feed("v", input()).unwrap();
    host.step().unwrap();
    let value = host.host().take_value("result").unwrap().unwrap();
    *value.typed::<u32>().unwrap()
}

#[test]
fn ports_report_name_type_direction_policy_and_held_state() {
    let (mut host, _) = loaded();
    let ports: Vec<_> = host.host().port_infos().collect();
    assert_eq!(ports.len(), 2);
    assert_eq!(ports[0].name, "result");
    assert_eq!(ports[0].dir, Dir::Output);
    assert_eq!(ports[1].name, "v");
    assert_eq!(ports[1].dir, Dir::Input);
    assert_eq!(ports[1].type_key, ports[0].type_key);
    assert!(!ports[1].held);
    assert_eq!(ports[1].queued, 0);
    host.host().feed("v", input()).unwrap();
    assert_eq!(host.host().port_infos().nth(1).unwrap().queued, 1);
}

#[test]
fn a_patch_changes_the_next_tick_decodes_once_and_leaves_state_alone() {
    let (mut host, decodes) = loaded();
    host.state().set(NodeId::new("gain"), 0, 11_u32);
    assert_eq!(run(&mut host), 2);
    assert_eq!(run(&mut host), 2);
    assert_eq!(decodes.load(Ordering::Relaxed), 1);

    let outcome = host.patch_const("gain", "amount", Lit::Int(5)).unwrap();
    assert_eq!(outcome, PatchOutcome::Changed { version: 1 });
    assert_eq!(run(&mut host), 5);
    assert_eq!(decodes.load(Ordering::Relaxed), 2);
    assert_eq!(host.state().get::<u32>(NodeId::new("gain"), 0), Some(&11));
}

#[test]
fn a_patch_is_refused_while_a_tick_runs() {
    let (mut host, _) = loaded();
    run(&mut host);
    assert!(REFUSED.load(Ordering::Relaxed) > 0);
    assert_eq!(
        *host.consts().get("gain", "amount").unwrap().value,
        Lit::Int(2)
    );
}

#[test]
fn an_equal_patch_moves_nothing_and_a_missing_const_is_refused() {
    let (mut host, _) = loaded();
    let same = host.patch_const("gain", "amount", Lit::Int(2)).unwrap();
    assert_eq!(same, PatchOutcome::Unchanged);
    let error = host.patch_const("gain", "other", Lit::Int(1)).unwrap_err();
    assert!(matches!(error, PatchError::NoSuchConst { .. }));
}

#[test]
fn a_reader_keeps_what_it_took_when_a_patch_lands() {
    let (mut host, _) = loaded();
    let before = host.consts().get("gain", "amount").unwrap();
    host.patch_const("gain", "amount", Lit::Int(9)).unwrap();
    assert_eq!(*before.value, Lit::Int(2));
    assert_eq!(
        *host.consts().get("gain", "amount").unwrap().value,
        Lit::Int(9)
    );
}

#[test]
fn a_node_switched_off_is_not_called_until_switched_on() {
    let (mut host, _) = loaded();
    assert_eq!(host.node_is_on("gain"), Some(true));
    host.set_node_on("gain", false).unwrap();
    assert_eq!(host.node_is_on("gain"), Some(false));
    host.host().feed("v", input()).unwrap();
    host.step().unwrap();
    assert!(host.host().take_value("result").unwrap().is_none());

    host.set_node_on("gain", true).unwrap();
    assert_eq!(run(&mut host), 2);
    let error = host.set_node_on("missing", false).unwrap_err();
    assert!(matches!(error, ControlError::NoSuchNode { .. }));
}
