//! A node declared at run time, with a closure for its body, plans and runs on the CPU backend.

use std::sync::{Arc, Mutex};

use daedalus_abi::{OwnedStep, Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::Compiled;
use daedalus_cpu::CpuBackend;
use daedalus_cpu::steps::CpuStepBody;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, PortRefDoc};
use daedalus_planner::stitch::Item;
use daedalus_planner::{Config, plan};
use daedalus_registry::{OwnedBody, OwnedDecl, OwnedPort, Registry, TypeRef};
use daedalus_runner::{UNSTAMPED, put_typed, typed_view};
use daedalus_types::Typed;

/// Writes the text it was made with.
struct Says(String);

impl OwnedStep for Says {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let Some(out) = slots.cell(0) else {
            cx.fault(None, "no output cell");
            return StepEnd::Fault;
        };
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, Arc::new(self.0.clone()));
        StepEnd::Done
    }
}

/// Repeats its input as many times as it was made to.
struct Repeats(usize);

impl OwnedStep for Repeats {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let text = slots
            .cell(0)
            .and_then(|cell| typed_view::<String>(cell).cloned());
        let (Some(text), Some(out)) = (text, slots.cell(1)) else {
            cx.fault(None, "no text to repeat");
            return StepEnd::Fault;
        };
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, Arc::new(text.repeat(self.0)));
        StepEnd::Done
    }
}

/// Keeps what it was given, where the test can see it.
struct Keeps(Arc<Mutex<Vec<String>>>);

impl OwnedStep for Keeps {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        if let Some(text) = slots.cell(0).and_then(|cell| typed_view::<String>(cell)) {
            self.0.lock().unwrap().push(text.clone());
        }
        StepEnd::Done
    }
}

fn text_port(name: &str) -> OwnedPort {
    OwnedPort {
        name: name.to_string(),
        ty: TypeRef::Expr(<String as Typed>::EXPR),
        optional: false,
    }
}

/// A declaration made here and now, whose body is `make`.
fn declare(
    id: &str,
    inputs: &[&str],
    outputs: &[&str],
    make: impl Fn() -> Step + Send + Sync + 'static,
) -> OwnedDecl {
    OwnedDecl {
        id: id.to_string(),
        source: "made at run time".to_string(),
        inputs: inputs.iter().map(|name| text_port(name)).collect(),
        outputs: outputs.iter().map(|name| text_port(name)).collect(),
        body: OwnedBody::code("cpu", id, Arc::new(CpuStepBody::new(move |_| Ok(make())))),
        conversion: None,
    }
}

fn wire(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

#[test]
fn nodes_declared_at_run_time_with_closures_plan_and_run() {
    // What the closures hold is only known now.
    let (word, times) = (String::from("ab"), 3);
    let kept = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&kept);
    let mut registry = Registry::new();
    for decl in [
        declare("rt.say", &[], &["text"], move || {
            Step::boxed(Says(word.clone()))
        }),
        declare("rt.repeat", &["text"], &["text"], move || {
            Step::boxed(Repeats(times))
        }),
        declare("rt.keep", &["text"], &[], move || {
            Step::boxed(Keeps(Arc::clone(&sink)))
        }),
    ] {
        registry.register_owned(decl).expect("registers");
    }
    let graph = GraphBody {
        nodes: ["rt.say", "rt.repeat", "rt.keep"]
            .into_iter()
            .map(NodeDoc::new)
            .collect(),
        edges: vec![wire(0, "text", 1, "text"), wire(1, "text", 2, "text")],
        groups: Vec::new(),
    };
    let mut planned =
        plan(&graph, &registry, &[&CpuBackend], &Config::default()).expect("the graph plans");

    // Run the plan's steps in order, each on its own cells, as the runner would.
    let mut cells = vec![Slot::EMPTY; planned.stitched.slots.len()];
    for item in planned.stitched.layout.clone() {
        let Item::Run {
            covers,
            reads,
            writes,
            ..
        } = item
        else {
            continue;
        };
        let mut local: Vec<Slot> = reads
            .iter()
            .map(|&slot| cells[slot as usize])
            .chain(writes.iter().map(|_| Slot::EMPTY))
            .collect();
        let unit = planned
            .regions
            .regions
            .iter_mut()
            .find_map(|region| match &mut region.compiled {
                Compiled::Steps(units) => units
                    .iter_mut()
                    .find(|unit| unit.info.covers[0].0 == covers[0]),
                _ => None,
            })
            .expect("a step for the node");
        let mut slots = Slots::of(&mut local);
        let end = daedalus_abi::testing::run_once(&mut unit.step, &mut slots, &mut StepCx::new());
        assert!(matches!(end, StepEnd::Done));
        for (at, &slot) in writes.iter().enumerate() {
            if slot != daedalus_abi::NO_SLOT {
                cells[slot as usize] = local[reads.len() + at];
            }
        }
    }
    assert_eq!(*kept.lock().unwrap(), ["ababab"]);
    for cell in cells {
        daedalus_runner::drop_owned(cell);
    }
}

#[test]
fn a_body_with_no_code_is_still_refused_by_the_cpu_backend() {
    let mut registry = Registry::new();
    registry
        .register_owned(OwnedDecl {
            id: "rt.text_only".to_string(),
            source: "made at run time".to_string(),
            inputs: Vec::new(),
            outputs: vec![text_port("text")],
            body: OwnedBody::text("cpu", "rt.text_only"),
            conversion: None,
        })
        .expect("registers");
    let graph = GraphBody {
        nodes: vec![NodeDoc::new("rt.text_only")],
        ..GraphBody::default()
    };
    let refused = plan(&graph, &registry, &[&CpuBackend], &Config::default())
        .map(drop)
        .unwrap_err();
    assert!(
        format!("{refused:?}").contains("isn't a CPU function"),
        "{refused:?}"
    );
}
