//! A body here is one node on a small backend whose steps do real work on their frame: the full
//! cell is the input and the others are outputs.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use daedalus_abi::testing::run_once;
use daedalus_abi::{NO_SLOT, OwnedStep, Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    StepUnit, Transfer, UnitInfo,
};
use daedalus_graph::{ControlDoc, ControlOp, EdgeDoc, GraphBody, NodeDoc, NodeKindDoc};
use daedalus_planner::control::{BODY, ELSE, THEN};
use daedalus_planner::stitch::Item;
use daedalus_planner::{Config, PlanError, Planned, plan};
use daedalus_planner_tests::{decl, edge, node, u32_ty};
use daedalus_registry::{Registry, TypeRef};
use daedalus_runner::{SlotList, put_typed, typed_raw, typed_view};
use daedalus_types::{Scalar, SpaceIndex, TypeExpr, TypeIndex};

fn bool_ty() -> TypeRef {
    TypeRef::Expr(TypeExpr::Scalar(Scalar::Bool))
}

static COUNTS: Mutex<BTreeMap<String, u32>> = Mutex::new(BTreeMap::new());

fn count(id: &str) -> u32 {
    COUNTS.lock().unwrap().get(id).copied().unwrap_or(0)
}

fn write_all<T: Send + Sync + 'static + Copy>(slots: &mut Slots, value: T) {
    for at in 0..slots.len() {
        let cell = slots.cell(at).unwrap();
        if cell.state != daedalus_abi::SlotState::Full as u32 {
            put_typed(cell, TypeIndex(0), SpaceIndex(0), Arc::new(value));
        }
    }
}

fn first_full(slots: &mut Slots) -> Option<Slot> {
    (0..slots.len())
        .map(|at| *slots.cell(at).unwrap())
        .find(|cell| cell.state == daedalus_abi::SlotState::Full as u32)
}

struct Body {
    id: String,
    /// `true` once this many rounds have run, for `until` bodies. `0` writes a `u32` instead.
    done_after: u32,
    rounds: AtomicU32,
}

impl OwnedStep for Body {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        *COUNTS.lock().unwrap().entry(self.id.clone()).or_default() += 1;
        if self.done_after > 0 {
            let round = self.rounds.fetch_add(1, Ordering::Relaxed) + 1;
            write_all(slots, round >= self.done_after);
            return StepEnd::Done;
        }
        let input = first_full(slots).and_then(|cell| typed_view::<u32>(&cell).copied());
        write_all(slots, input.unwrap_or(7) * 10);
        StepEnd::Done
    }
}

struct Test;

fn done_after(id: &str) -> u32 {
    id.strip_prefix("t.done")
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
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

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let id = cx.bodies[0].text.to_string();
        let step = Step::boxed(Body {
            done_after: done_after(&id),
            id,
            rounds: AtomicU32::new(0),
        });
        let info = UnitInfo {
            covers: region.nodes.to_vec(),
            reads: region.inputs.to_vec(),
            writes: region.outputs.to_vec(),
            absorbed: Vec::new(),
            may_pend: false,
        };
        Ok(Compiled::Steps(vec![StepUnit { step, info }]))
    }
}

fn add(r: &mut Registry, id: &str, inputs: &[(&str, TypeRef)], outputs: &[(&str, TypeRef)]) {
    let mut d = decl(id);
    for (name, ty) in inputs {
        d = d.input(name, ty.clone());
    }
    for (name, ty) in outputs {
        d = d.output(name, ty.clone());
    }
    d.body("t").add(r);
}

fn registry() -> Registry {
    let mut r = Registry::new();
    for id in [
        "t.then_a", "t.else_a", "t.then_b", "t.else_b", "t.each_a", "t.each_b", "t.each_c",
    ] {
        add(&mut r, id, &[("in", u32_ty())], &[("out", u32_ty())]);
    }
    add(&mut r, "t.list", &[], &[("out", list_ty())]);
    add(&mut r, "t.list_sink", &[("in", list_ty())], &[]);
    add(
        &mut r,
        "t.bool_out",
        &[("in", u32_ty())],
        &[("out", bool_ty())],
    );
    add(
        &mut r,
        "t.done3",
        &[("in", bool_ty())],
        &[("state", bool_ty()), ("done", bool_ty())],
    );
    add(
        &mut r,
        "t.done9",
        &[("in", bool_ty())],
        &[("state", bool_ty()), ("done", bool_ty())],
    );
    r
}

fn bridge(ports: &[(&str, &str)]) -> NodeDoc {
    let mut doc = node("ports");
    doc.kind = NodeKindDoc::HostBridge;
    for (name, ty) in ports {
        doc.port_types.insert(name.to_string(), ty.to_string());
    }
    doc
}

/// `ports.v -> id.in`, `id.out -> ports.out`.
fn mapping(id: &str, input: &str, output: &str, out_ty: &str) -> GraphBody {
    GraphBody {
        nodes: vec![bridge(&[(input, "u32"), (output, out_ty)]), node(id)],
        edges: vec![edge(0, input, 1, "in"), edge(1, "out", 0, output)],
        groups: Vec::new(),
    }
}

fn control(op: ControlOp, max: Option<u32>, bodies: Vec<(&str, GraphBody)>) -> NodeDoc {
    let mut doc = node("flow");
    doc.kind = NodeKindDoc::Control;
    let bodies = bodies
        .into_iter()
        .map(|(name, body)| (name.to_string(), body))
        .collect();
    doc.control = Some(ControlDoc { op, max, bodies });
    doc
}

fn outer(flow: NodeDoc, ports: &[(&str, &str)], edges: Vec<EdgeDoc>) -> GraphBody {
    GraphBody {
        nodes: vec![bridge(ports), flow],
        edges,
        groups: Vec::new(),
    }
}

fn planned(graph: &GraphBody) -> Result<Planned, PlanError> {
    plan(graph, &registry(), &[&Test], &Config::default())
}

fn cells_for(planned: &Planned) -> Vec<Slot> {
    vec![Slot::EMPTY; planned.stitched.slots.len().max(1)]
}

fn input_slot(planned: &Planned, port: &str) -> usize {
    planned
        .stitched
        .host_inputs
        .iter()
        .find(|p| p.port == port)
        .unwrap()
        .slot as usize
}

fn output_slot(planned: &Planned, port: &str) -> usize {
    planned
        .stitched
        .host_outputs
        .iter()
        .find(|p| p.port == port)
        .unwrap()
        .slot as usize
}

/// Runs the plan's control unit on a frame built from `cells`, as the runner would.
fn tick(planned: &mut Planned, cells: &mut [Slot]) -> (StepEnd, String, Vec<u32>) {
    let (reads, writes) = planned
        .stitched
        .layout
        .iter()
        .find_map(|item| match item {
            Item::Run {
                covers,
                reads,
                writes,
                ..
            } if covers == &[1] => Some((reads.clone(), writes.clone())),
            _ => None,
        })
        .unwrap();
    let mut frame: Vec<Slot> = reads
        .iter()
        .map(|&slot| {
            if slot == NO_SLOT {
                Slot::EMPTY
            } else {
                cells[slot as usize]
            }
        })
        .chain(writes.iter().map(|_| Slot::EMPTY))
        .collect();
    let mut slots = Slots::of(&mut frame);
    let mut cx = StepCx::new();
    let region = planned
        .regions
        .regions
        .iter_mut()
        .find(|r| r.formed.nodes == [1])
        .unwrap();
    let Compiled::Steps(units) = &mut region.compiled else {
        panic!("control compiles to steps");
    };
    let end = run_once(&mut units[0].step, &mut slots, &mut cx);
    for (at, &slot) in writes.iter().enumerate() {
        if slot != NO_SLOT {
            cells[slot as usize] = frame[reads.len() + at];
        }
    }
    let covers = units[0].info.covers.iter().map(|n| n.0).collect();
    (end, cx.reason().to_string(), covers)
}

fn feed<T: Send + Sync + 'static>(cells: &mut [Slot], slot: usize, value: T) {
    put_typed(
        &mut cells[slot],
        TypeIndex(0),
        SpaceIndex(0),
        Arc::new(value),
    );
}

fn branches(then: &str, other: &str, other_ty: &str) -> NodeDoc {
    control(
        ControlOp::If,
        None,
        vec![
            (THEN, mapping(then, "v", "out", "u32")),
            (ELSE, mapping(other, "v", "out", other_ty)),
        ],
    )
}

fn if_graph(then: &str, other: &str, other_ty: &str) -> GraphBody {
    outer(
        branches(then, other, other_ty),
        &[("cond", "bool"), ("v", "u32"), ("out", "u32")],
        vec![
            edge(0, "cond", 1, "cond"),
            edge(0, "v", 1, "v"),
            edge(1, "out", 0, "out"),
        ],
    )
}

#[test]
fn if_runs_only_the_taken_branch() {
    let mut plan = planned(&if_graph("t.then_a", "t.else_a", "u32")).unwrap();
    let mut cells = cells_for(&plan);
    feed(&mut cells, input_slot(&plan, "cond"), true);
    feed(&mut cells, input_slot(&plan, "v"), 4_u32);
    let (end, reason, _) = tick(&mut plan, &mut cells);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    assert_eq!((count("t.then_a"), count("t.else_a")), (1, 0));
    let out = output_slot(&plan, "out");
    assert_eq!(typed_view::<u32>(&cells[out]), Some(&40));
}

#[test]
fn if_branch_output_mismatch_fails() {
    let mut other = mapping("t.bool_out", "v", "out", "bool");
    other.nodes[0]
        .port_types
        .insert("out".to_string(), "bool".to_string());
    let flow = control(
        ControlOp::If,
        None,
        vec![
            (THEN, mapping("t.then_b", "v", "out", "u32")),
            (ELSE, other),
        ],
    );
    let graph = outer(
        flow,
        &[("cond", "bool"), ("v", "u32"), ("out", "u32")],
        vec![
            edge(0, "cond", 1, "cond"),
            edge(0, "v", 1, "v"),
            edge(1, "out", 0, "out"),
        ],
    );
    let error = planned(&graph).err().unwrap();
    assert!(error.to_string().contains("`then` has"), "{error}");
}

static ELEMENT: TypeExpr = TypeExpr::Scalar(Scalar::U32);

fn list_ty() -> TypeRef {
    TypeRef::Expr(TypeExpr::List(&ELEMENT))
}

fn for_each_graph(body: &str) -> GraphBody {
    let flow = control(
        ControlOp::ForEach,
        None,
        vec![(BODY, mapping(body, "item", "out", "u32"))],
    );
    GraphBody {
        nodes: vec![node("t.list"), flow, node("t.list_sink")],
        edges: vec![edge(0, "out", 1, "list"), edge(1, "out", 2, "in")],
        groups: Vec::new(),
    }
}

fn cells_of(planned: &Planned) -> (usize, usize) {
    planned
        .stitched
        .layout
        .iter()
        .find_map(|item| match item {
            Item::Run {
                covers,
                reads,
                writes,
                ..
            } if covers == &[1] => Some((reads[0] as usize, writes[0] as usize)),
            _ => None,
        })
        .unwrap()
}

fn list_of(items: &[u32]) -> SlotList {
    let mut list = SlotList::with_capacity(items.len());
    for item in items {
        list.push(typed_raw(TypeIndex(0), SpaceIndex(0), Arc::new(*item)));
    }
    list
}

#[test]
fn for_each_preserves_order() {
    let mut plan = planned(&for_each_graph("t.each_a")).unwrap();
    let mut cells = cells_for(&plan);
    let (read, write) = cells_of(&plan);
    feed(&mut cells, read, list_of(&[1, 2, 3]));
    let (end, reason, _) = tick(&mut plan, &mut cells);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    let out = typed_view::<SlotList>(&cells[write]).unwrap();
    let values: Vec<u32> = (0..out.len())
        .map(|at| *typed_view::<u32>(out.get(at).unwrap()).unwrap())
        .collect();
    assert_eq!(values, [10, 20, 30]);
    assert_eq!(count("t.each_a"), 3);
}

#[test]
fn a_body_only_borrows_its_elements() {
    let element = Arc::new(5_u32);
    let mut list = SlotList::with_capacity(1);
    list.push(typed_raw(TypeIndex(0), SpaceIndex(0), Arc::clone(&element)));
    let mut plan = planned(&for_each_graph("t.each_c")).unwrap();
    let mut cells = cells_for(&plan);
    let (read, _) = cells_of(&plan);
    feed(&mut cells, read, list);
    tick(&mut plan, &mut cells);
    assert_eq!(
        Arc::strong_count(&element),
        2,
        "the list still holds its element"
    );
    daedalus_runner::drop_owned(cells[read]);
    assert_eq!(Arc::strong_count(&element), 1);
}

#[test]
fn for_each_empty_list_runs_nothing() {
    let mut plan = planned(&for_each_graph("t.each_b")).unwrap();
    let mut cells = cells_for(&plan);
    let (read, write) = cells_of(&plan);
    feed(&mut cells, read, list_of(&[]));
    let (end, ..) = tick(&mut plan, &mut cells);
    assert!(matches!(end, StepEnd::Done));
    assert_eq!(count("t.each_b"), 0);
    assert!(typed_view::<SlotList>(&cells[write]).unwrap().is_empty());
}

fn until_graph(id: &str, max: Option<u32>) -> GraphBody {
    let body = GraphBody {
        nodes: vec![bridge(&[("state", "bool"), ("done", "bool")]), node(id)],
        edges: vec![
            edge(0, "state", 1, "in"),
            edge(1, "state", 0, "state"),
            edge(1, "done", 0, "done"),
        ],
        groups: Vec::new(),
    };
    outer(
        control(ControlOp::Until, max, vec![(BODY, body)]),
        &[("init", "bool"), ("state", "bool")],
        vec![edge(0, "init", 1, "init"), edge(1, "state", 0, "state")],
    )
}

#[test]
fn until_stops_on_done() {
    let mut plan = planned(&until_graph("t.done3", Some(10))).unwrap();
    let mut cells = cells_for(&plan);
    feed(&mut cells, input_slot(&plan, "init"), false);
    let (end, reason, _) = tick(&mut plan, &mut cells);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    assert_eq!(count("t.done3"), 3);
    assert_eq!(
        typed_view::<bool>(&cells[output_slot(&plan, "state")]),
        Some(&true)
    );
}

#[test]
fn until_reaching_max_faults_with_covers() {
    let mut plan = planned(&until_graph("t.done9", Some(4))).unwrap();
    let mut cells = cells_for(&plan);
    feed(&mut cells, input_slot(&plan, "init"), false);
    let (end, reason, covers) = tick(&mut plan, &mut cells);
    assert!(matches!(end, StepEnd::Fault));
    assert!(reason.contains("limit of 4"), "{reason}");
    assert_eq!(covers, [1]);
    assert_eq!(count("t.done9"), 4);
}

#[test]
fn until_needs_a_max_in_range() {
    let error = planned(&until_graph("t.done3", None)).err().unwrap();
    assert!(error.to_string().contains("max"), "{error}");
    let error = planned(&until_graph("t.done3", Some(70_000)))
        .err()
        .unwrap();
    assert!(error.to_string().contains("max"), "{error}");
}

#[test]
fn body_edge_to_outside_is_error() {
    let mut body = mapping("t.then_b", "item", "out", "u32");
    body.edges.push(edge(1, "out", 9, "elsewhere"));
    let graph = GraphBody {
        nodes: vec![
            node("t.list"),
            control(ControlOp::ForEach, None, vec![(BODY, body)]),
        ],
        edges: vec![edge(0, "out", 1, "list")],
        groups: Vec::new(),
    };
    let error = planned(&graph).err().unwrap().to_string();
    assert!(
        error.contains("`body`") && error.contains("`elsewhere`"),
        "{error}"
    );
}

#[test]
fn nested_steps_cover_body_nodes() {
    let plan = planned(&if_graph("t.then_b", "t.else_b", "u32")).unwrap();
    assert_eq!(plan.nested.len(), 2);
    for body in &plan.nested {
        assert_eq!(body.node, 1);
        assert_eq!(body.covers, [vec![1]]);
    }
}
