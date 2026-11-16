//! A made plan: written, read, bound and loaded on a host, against the same plan loaded as planned.
//! Both hosts get the same script of feeds, ticks and takes, and must give the same values and
//! reports.

use std::borrow::Cow;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    Transfer, TransferCost,
};
use daedalus_fake_npu::{BACKEND_ID as NPU_ID, DOWNLOAD, FakeNpu};
use daedalus_graph::{ControlDoc, ControlOp, EdgePolicyDoc, GraphBody, Lit, NodeDoc, NodeKindDoc};
use daedalus_host::{Dir, LoadOptions, LoadedHost};
use daedalus_made::Item;
use daedalus_made::{BindError, Bound, MadeError, bind, decode, encode};
use daedalus_planner::PlanError;
use daedalus_planner::control::{ELSE, THEN};
use daedalus_planner::{Config, Form, Planned, plan};
use daedalus_planner_tests::{decl, edge, node, u8_ty, u32_ty};
use daedalus_registry::Registry;
use daedalus_runner::{INLINE_BYTES, put_inline};
use daedalus_sim::{BACKEND_ID, SimBackend, SimConfig, SimForm};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::{Owner, Value};

fn registry() -> Registry {
    let mut registry = Registry::new();
    for id in ["t.a", "t.b", "t.c"] {
        decl(id)
            .optional("in", u8_ty())
            .output("out", u8_ty())
            .body(BACKEND_ID)
            .add(&mut registry);
    }
    registry
}

fn backend() -> SimBackend {
    SimBackend::new(SimConfig {
        form: SimForm::Steps,
        ..SimConfig::default()
    })
}

fn config() -> Config {
    Config {
        form: Form::Steps,
        ..Config::default()
    }
}

fn labelled(id: &str, label: &str) -> NodeDoc {
    let mut doc = node(id);
    doc.label = Some(label.to_string());
    doc
}

fn bridge(label: &str) -> NodeDoc {
    let mut host = labelled("host", label);
    host.kind = NodeKindDoc::HostBridge;
    host.port_types.insert("out".to_string(), "u8".to_string());
    host
}

fn chain(nodes: Vec<NodeDoc>) -> GraphBody {
    let edges = (1..nodes.len() as u32)
        .map(|i| edge(i - 1, "out", i, "in"))
        .collect();
    GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    }
}

/// A bridge feeding two nodes, which is a value read by two readers.
fn fan_out() -> GraphBody {
    GraphBody {
        nodes: vec![bridge("left"), labelled("t.a", "a"), labelled("t.b", "b")],
        edges: vec![edge(0, "out", 1, "in"), edge(0, "out", 2, "in")],
        groups: Vec::new(),
    }
}

/// A chain whose middle edge keeps a bounded queue.
fn bounded() -> GraphBody {
    let mut graph = chain(vec![
        bridge("left"),
        labelled("t.a", "a"),
        labelled("t.b", "b"),
    ]);
    graph.edges[1].policy = EdgePolicyDoc::Bounded {
        capacity: 2,
        overflow: "drop_oldest".to_string(),
    };
    graph
}

/// A node whose recovery is written in its metadata, which a host reads when it loads the plan.
fn recovering() -> GraphBody {
    let mut skip = labelled("t.a", "a");
    skip.metadata.insert(
        "daedalus.node.recovery".to_string(),
        Lit::Str("skip".to_string()),
    );
    chain(vec![bridge("left"), skip])
}

fn corpus() -> Vec<(&'static str, GraphBody)> {
    vec![
        (
            "chain",
            chain(vec![
                bridge("left"),
                labelled("t.a", "a"),
                labelled("t.b", "b"),
            ]),
        ),
        ("fan_out", fan_out()),
        ("bounded", bounded()),
        ("recovering", recovering()),
    ]
}

fn planned(graph: &GraphBody) -> Planned {
    let backend = backend();
    plan(graph, &registry(), &[&backend], &config()).unwrap()
}

fn value() -> Value {
    Value {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
        owner: Owner::Static(b"x"),
    }
}

/// Feeds each input, then runs `ticks` ticks, taking every output after each one. The text it
/// returns is what the two hosts must agree on.
fn script(host: &mut LoadedHost, ticks: usize) -> Vec<String> {
    script_with(host, ticks, value)
}

fn script_with(host: &mut LoadedHost, ticks: usize, make: fn() -> Value) -> Vec<String> {
    let inputs: Vec<String> = host
        .host()
        .port_infos()
        .filter(|info| info.dir == Dir::Input)
        .map(|info| info.name.to_string())
        .collect();
    let outputs: Vec<String> = host
        .host()
        .port_infos()
        .filter(|info| info.dir == Dir::Output)
        .map(|info| info.name.to_string())
        .collect();
    let mut seen = Vec::new();
    for _ in 0..ticks {
        for name in &inputs {
            let fed = host.host().feed(name, make());
            seen.push(format!("feed {name}: {fed:?}"));
        }
        let report = host.host().step();
        seen.push(format!("step: {:?}", report.map(|r| format!("{r:?}"))));
        for name in &outputs {
            let taken = host.host().take_value(name);
            seen.push(format!(
                "take {name}: {:?}",
                taken.map(|v| v.map(|v| (v.ty.0, v.space.0)))
            ));
        }
    }
    seen
}

#[test]
fn every_corpus_plan_encodes_and_decodes_to_itself() {
    for (name, graph) in corpus() {
        let made = planned(&graph)
            .made()
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let bytes = encode(&made).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back, made, "{name}");
        assert_eq!(
            encode(&back).unwrap(),
            bytes,
            "{name}: the bytes are the same"
        );
    }
}

#[test]
fn a_made_plan_binds_to_the_facts_of_the_planned_one() {
    for (name, graph) in corpus() {
        let made = planned(&graph).made().unwrap();
        let backend = backend();
        let bound = bind(&made, &registry(), &[&backend]).unwrap();
        let from_plan = Bound::from(planned(&graph));
        assert_eq!(bound.facts, from_plan.facts, "{name}");
        assert_eq!(bound.facts, made.facts, "{name}");
    }
}

#[test]
fn a_made_plan_runs_the_same_as_the_planned_one() {
    for (name, graph) in corpus() {
        let mut planned_host = LoadedHost::load(planned(&graph), LoadOptions::default()).unwrap();
        let expected = script(&mut planned_host, 3);

        let made = planned(&graph).made().unwrap();
        let bytes = encode(&made).unwrap();
        let backend = backend();
        let mut made_host = LoadedHost::load_made(
            &bytes,
            &registry(),
            &[&backend as &dyn Backend],
            LoadOptions::default(),
        )
        .unwrap();
        assert!(
            expected.iter().any(|line| line.starts_with("step: ")),
            "{name}"
        );
        assert_eq!(script(&mut made_host, 3), expected, "{name}");
    }
}

#[test]
fn a_reload_from_one_made_plan_to_another_reports_as_the_planned_one_does() {
    let before = chain(vec![
        bridge("left"),
        labelled("t.a", "a"),
        labelled("t.b", "b"),
    ]);
    let after = chain(vec![bridge("left"), labelled("t.a", "a")]);

    let mut planned_host = LoadedHost::load(planned(&before), LoadOptions::default()).unwrap();
    let planned_report = planned_host.reload(planned(&after)).unwrap();

    let backend = backend();
    let before_bytes = encode(&planned(&before).made().unwrap()).unwrap();
    let mut made_host = LoadedHost::load_made(
        &before_bytes,
        &registry(),
        &[&backend as &dyn Backend],
        LoadOptions::default(),
    )
    .unwrap();
    let after_bytes = encode(&planned(&after).made().unwrap()).unwrap();
    let after_made = decode(&after_bytes).unwrap();
    let bound = bind(&after_made, &registry(), &[&backend as &dyn Backend]).unwrap();
    let made_report = made_host.reload(bound).unwrap();
    assert_eq!(format!("{made_report:?}"), format!("{planned_report:?}"));
}

#[test]
fn bytes_cut_short_or_changed_are_refused() {
    let bytes = encode(&planned(&bounded()).made().unwrap()).unwrap();
    for cut in [0, 7, 17, 18, bytes.len() / 2, bytes.len() - 1] {
        assert!(decode(&bytes[..cut]).is_err(), "cut at {cut}");
    }
    for at in (18..bytes.len()).step_by(bytes.len() / 13 + 1) {
        let mut flipped = bytes.clone();
        flipped[at] ^= 0x10;
        assert!(decode(&flipped).is_err(), "flip at {at}");
    }
    let mut other = bytes.clone();
    other[0] = b'X';
    assert!(matches!(decode(&other), Err(MadeError::NotAPlan)));
}

#[test]
fn a_plan_needing_a_backend_the_host_lacks_is_refused() {
    let made = planned(&chain(vec![bridge("left"), labelled("t.a", "a")]))
        .made()
        .unwrap();
    let other = SimBackend::new(SimConfig::default());
    // The same id, so the same backend: it binds.
    assert!(bind(&made, &registry(), &[&other as &dyn Backend]).is_ok());
    let none: [&dyn Backend; 0] = [];
    assert!(matches!(
        bind(&made, &registry(), &none),
        Err(BindError::NoBackend { .. })
    ));
}

/// A bridge with the named ports, of the given types.
fn ported(label: &str, ports: &[(&str, &str)]) -> NodeDoc {
    let mut host = labelled("host", label);
    host.kind = NodeKindDoc::HostBridge;
    for (name, ty) in ports {
        host.port_types.insert(name.to_string(), ty.to_string());
    }
    host
}

/// A chain's ports and nodes at u8, with the control node's two bodies.
fn control_graph() -> GraphBody {
    let mut flow = node("flow");
    flow.kind = NodeKindDoc::Control;
    let mapping = |id: &str| GraphBody {
        nodes: vec![ported("body", &[("v", "u8"), ("out", "u8")]), node(id)],
        edges: vec![edge(0, "v", 1, "in"), edge(1, "out", 0, "out")],
        groups: Vec::new(),
    };
    flow.control = Some(ControlDoc {
        op: ControlOp::If,
        max: None,
        bodies: [(THEN, mapping("t.a")), (ELSE, mapping("t.b"))]
            .into_iter()
            .map(|(name, body)| (name.to_string(), body))
            .collect(),
    });
    GraphBody {
        nodes: vec![
            ported("ports", &[("cond", "bool"), ("v", "u8"), ("out", "u8")]),
            flow,
        ],
        edges: vec![
            edge(0, "cond", 1, "cond"),
            edge(0, "v", 1, "v"),
            edge(1, "out", 0, "out"),
        ],
        groups: Vec::new(),
    }
}

#[test]
fn a_control_node_plans_but_has_no_made_form() {
    let backend = backend();
    let plan = plan(&control_graph(), &registry(), &[&backend], &config()).unwrap();
    let error = plan.made().unwrap_err();
    assert!(
        matches!(&error, PlanError::MadeUnavailable { node, .. } if node == "flow"),
        "{error}"
    );
}

#[test]
fn a_plan_does_not_bind_to_a_backend_that_declares_its_moves_otherwise() {
    let made = planned(&chain(vec![bridge("left"), labelled("t.a", "a")]))
        .made()
        .unwrap();
    assert!(
        made.facts
            .stitched
            .transfers
            .iter()
            .any(|used| used.id == "sim.to_device"),
        "the plan moves a value, so the declaration covers the move"
    );
    // The same backend without its moves: the move is gone, and the declaration says so.
    let changed = SimBackend::new(SimConfig {
        no_transfers: true,
        ..SimConfig::default()
    });
    let error = bind(&made, &registry(), &[&changed as &dyn Backend]).unwrap_err();
    assert!(matches!(error, BindError::Declaration { .. }), "{error}");
}

/// Two backends: sim runs the `t.*` nodes, and the fake NPU runs `npu.scale`, with its own memory.
fn npu_registry() -> Registry {
    let mut registry = Registry::new();
    for id in ["t.a", "t.b"] {
        decl(id)
            .optional("in", u32_ty())
            .output("out", u32_ty())
            .body(BACKEND_ID)
            .add(&mut registry);
    }
    decl("npu.scale")
        .optional("in", u32_ty())
        .output("out", u32_ty())
        .body_with_text(NPU_ID, "scale2")
        .add(&mut registry);
    registry
}

/// `left -> npu.scale -> t.b -> right`: the bridge's value goes into the NPU's memory and out again.
fn moving_graph() -> GraphBody {
    GraphBody {
        nodes: vec![
            ported("left", &[("out", "u32")]),
            node("npu.scale"),
            node("t.b"),
            ported("right", &[("in", "u32")]),
        ],
        edges: vec![
            edge(0, "out", 1, "in"),
            edge(1, "out", 2, "in"),
            edge(2, "out", 3, "in"),
        ],
        groups: Vec::new(),
    }
}

/// A `u32` the NPU's ops read as four bytes.
fn word() -> Value {
    Value {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
        owner: Owner::Static(&[7, 0, 0, 0]),
    }
}

fn plan_npu(sim: &SimBackend, link: &Link, npu: &FakeNpu) -> Planned {
    plan(
        &moving_graph(),
        &npu_registry(),
        &[sim, link, npu],
        &config(),
    )
    .unwrap()
}

#[test]
fn a_move_between_backends_gives_the_same_values_made_or_planned() {
    let sim = backend();
    let npu = FakeNpu::new();
    let link = Link;
    let planned = plan_npu(&sim, &link, &npu);
    let moves: Vec<&str> = planned
        .stitched
        .layout
        .iter()
        .filter_map(|item| match item {
            Item::Move { transfer, .. } => {
                Some(planned.stitched.transfers[*transfer as usize].id.as_str())
            }
            _ => None,
        })
        .collect();
    assert!(
        moves.contains(&DOWNLOAD) && moves.iter().any(|id| id.starts_with("link.")),
        "a value leaves the NPU through its own move, then sim's: {moves:?}"
    );

    // Each run's NPU work, counted from the device's own counters: three ticks, one op a tick.
    let counted = |run: &mut dyn FnMut() -> Vec<String>| {
        let before = daedalus_fake_npu::counters();
        let lines = run();
        let after = daedalus_fake_npu::counters();
        assert_eq!(after.ops - before.ops, 3, "ops");
        assert!(after.downloads > before.downloads, "the NPU's download ran");
        lines
    };
    let mut planned_host =
        LoadedHost::load(plan_npu(&sim, &link, &npu), LoadOptions::default()).unwrap();
    let expected = counted(&mut || script_with(&mut planned_host, 3, word));

    let made = plan_npu(&sim, &link, &npu).made().unwrap();
    let bytes = encode(&made).unwrap();
    let backends: [&dyn Backend; 3] = [&sim, &link, &npu];
    let decoded = decode(&bytes).unwrap();
    assert_eq!(decoded, made);
    bind(&decoded, &npu_registry(), &backends).unwrap();
    let mut made_host =
        LoadedHost::load_made(&bytes, &npu_registry(), &backends, LoadOptions::default()).unwrap();
    assert_eq!(
        counted(&mut || script_with(&mut made_host, 3, word)),
        expected
    );
}

/// The host memory the fake NPU moves values through. It runs nothing. It links sim's host and
/// device memory to that memory, so a value can go from sim to the NPU and back.
struct Link;

const HOST_MEMORY: &str = "cpu.host";

/// Copies plain data from slot 0 to slot 1, as the fake NPU's own moves do.
struct Copy {
    held: [u8; INLINE_BYTES],
}

impl OwnedStep for Copy {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let Some(bytes) = slots.cell(0).and_then(|cell| cell.bytes()) else {
            return StepEnd::Skipped;
        };
        let len = bytes.len();
        self.held[..len].copy_from_slice(bytes);
        let Some(cell) = slots.cell(1) else {
            return StepEnd::Fault;
        };
        match put_inline(cell, TypeIndex(0), SpaceIndex(0), &self.held[..len]) {
            Some(_) => StepEnd::Done,
            None => StepEnd::Fault,
        }
    }
}

fn moved(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(Copy {
        held: [0; INLINE_BYTES],
    }))
}

impl Backend for Link {
    fn id(&self) -> &str {
        "link"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed(HOST_MEMORY),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        let transfer = |id: &'static str, from: &'static str, to: &'static str| Transfer {
            id,
            key: None,
            from: Cow::Borrowed(from),
            to: Cow::Borrowed(to),
            cost: TransferCost::Copy,
            make: moved,
        };
        vec![
            transfer("link.host_up", "sim.host", HOST_MEMORY),
            transfer("link.up", "sim.device", HOST_MEMORY),
            transfer("link.host_down", HOST_MEMORY, "sim.host"),
            transfer("link.down", HOST_MEMORY, "sim.device"),
        ]
    }

    fn can_take(&self, _: &Offer<'_>) -> Vec<NodeRef> {
        Vec::new()
    }

    fn compile(&self, _: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        Err(Refusal::whole_region("the link runs nothing"))
    }
}
