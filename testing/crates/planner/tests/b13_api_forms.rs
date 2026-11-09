use daedalus_abi::testing::run_once;
use daedalus_abi::{Slot, Slots, StepCx};
use daedalus_backend::{Backend, Compiled};
use daedalus_graph::GraphBody;
use daedalus_plan::PlanView;
use daedalus_planner::{Config, Form, PlanError, Planned, Planner, plan};
use daedalus_planner_tests::{Named, decl, edge, node, u8_ty};
use daedalus_registry::Registry;
use daedalus_sim::{BACKEND_ID, SimBackend, SimConfig, SimForm, ran};

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.s")
        .optional("in", u8_ty())
        .output("out", u8_ty())
        .body(BACKEND_ID)
        .body("other")
        .add(&mut registry);
    registry
}

fn chain(count: u32) -> GraphBody {
    GraphBody {
        nodes: (0..count).map(|_| node("t.s")).collect(),
        edges: (1..count).map(|i| edge(i - 1, "out", i, "in")).collect(),
        groups: Vec::new(),
    }
}

fn form(sim: SimForm, form: Form) -> Result<Planned, PlanError> {
    let backend = SimBackend::new(SimConfig {
        form: sim,
        merge: true,
        ..SimConfig::default()
    });
    let config = Config {
        form,
        ..Config::default()
    };
    plan(&chain(2), &registry(), &[&backend], &config)
}

#[test]
fn steps_form_runs_on_sim() {
    let mut planned = form(SimForm::Steps, Form::Steps).unwrap();
    let mut cells = vec![Slot::EMPTY; planned.stitched.slots.len().max(1)];
    let mut slots = Slots::of(&mut cells);
    for region in &mut planned.regions.regions {
        let Compiled::Steps(units) = &mut region.compiled else {
            panic!("the steps form holds steps");
        };
        for unit in units {
            let mut cx = StepCx::new();
            run_once(&mut unit.step, &mut slots, &mut cx);
        }
    }
    assert!(ran(0) >= 1 && ran(1) >= 1);
}

#[test]
fn blob_round_trip_keeps_hash() {
    let planned = form(SimForm::Blob, Form::Blob).unwrap();
    let path = std::env::temp_dir().join(format!("b13-{}.plan", std::process::id()));
    planned.write(&path).unwrap();
    let read = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(read, planned.blob().unwrap());
    let view = PlanView::parse(&read).unwrap();
    let again = PlanView::parse(&planned.blob().unwrap()).unwrap().hash();
    assert_eq!(view.hash(), again);
    assert_eq!(view.steps().count(), planned.stitched.plan.steps.len());
}

#[test]
fn source_form_with_steps_only_backend_is_error() {
    let error = form(SimForm::Steps, Form::Source).unwrap_err();
    assert!(
        matches!(&error, PlanError::FormUnavailable { form: Form::Source, backend } if backend == BACKEND_ID),
        "{error:?}"
    );
}

#[test]
fn source_form_text_matches_golden() {
    let planned = form(SimForm::Source, Form::Source).unwrap();
    assert_eq!(planned.source().unwrap(), "// sim region: 0 1\n");
}

#[test]
fn planning_twice_gives_identical_output() {
    let first = form(SimForm::Blob, Form::Blob).unwrap();
    let second = form(SimForm::Blob, Form::Blob).unwrap();
    assert_eq!(first.stitched, second.stitched);
    assert_eq!(first.blob().unwrap(), second.blob().unwrap());
}

#[test]
fn the_config_priority_list_outranks_the_order_given() {
    let (sim, other) = (SimBackend::new(SimConfig::default()), Named("other"));
    let backends: [&dyn Backend; 2] = [&sim, &other];
    let config = Config {
        priority: vec!["other".to_string()],
        form: Form::Blob,
        ..Config::default()
    };
    let planned = Planner::new(&chain(1), &registry())
        .backends(&backends)
        .config(config)
        .solve()
        .unwrap();
    assert_eq!(planned.regions.assignment.bodies.len(), 1);
    assert_eq!(planned.regions.regions[0].formed.nodes, [0]);
    assert_eq!(
        planned.regions.regions[0].formed.backend.0, 0,
        "other is first once sorted"
    );
}
