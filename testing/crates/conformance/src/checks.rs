//! The checks. Each makes its own backend, so one can't disturb another.

use std::collections::BTreeSet;

use daedalus_abi::counting::counted;
use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{NO_NODE, REASON_CAPACITY, Slot, Slots, StepCx, StepEnd};
use daedalus_backend::{Backend, Compiled, Offer, StepUnit, UnitInfo};
use daedalus_types::NodeIndex;

use crate::Scenario;

pub(crate) type Check<B> = fn(&dyn Fn() -> B, &Scenario) -> Result<(), String>;

pub(crate) fn all<B: Backend>() -> Vec<(&'static str, Check<B>)> {
    vec![
        ("id_is_stable", id_is_stable::<B>),
        ("memory_ids_are_unique", memory_ids_are_unique::<B>),
        (
            "transfers_name_known_spaces",
            transfers_name_known_spaces::<B>,
        ),
        ("can_take_is_pure", can_take_is_pure::<B>),
        (
            "can_take_returns_only_offered_nodes",
            can_take_returns_only_offered::<B>,
        ),
        ("accepted_region_compiles", accepted_region_compiles::<B>),
        (
            "refusal_names_only_region_nodes",
            refusal_names_only_region::<B>,
        ),
        ("covers_stay_inside_the_region", covers_stay_inside::<B>),
        ("steps_do_not_allocate", steps_do_not_allocate::<B>),
        ("faults_stay_inside_the_region", faults_stay_inside::<B>),
        ("running_is_deterministic", running_is_deterministic::<B>),
    ]
}

fn require(holds: bool, failure: impl FnOnce() -> String) -> Result<(), String> {
    if holds { Ok(()) } else { Err(failure()) }
}

fn id_is_stable<B: Backend>(make: &dyn Fn() -> B, _: &Scenario) -> Result<(), String> {
    let backend = make();
    require(!backend.id().is_empty(), || "the id is empty".to_string())?;
    require(backend.id() == backend.id(), || {
        "the id changed between calls".to_string()
    })
}

fn memory_ids_are_unique<B: Backend>(make: &dyn Fn() -> B, _: &Scenario) -> Result<(), String> {
    let memory = make().memory();
    let unique: BTreeSet<_> = memory.iter().map(|space| &space.id).collect();
    require(unique.len() == memory.len(), || {
        "two memory spaces share an id".to_string()
    })?;
    require(memory.iter().all(|space| !space.id.is_empty()), || {
        "an empty space id".to_string()
    })
}

/// A transfer can reach another backend's space, so one end being this backend's is the rule.
fn transfers_name_known_spaces<B: Backend>(
    make: &dyn Fn() -> B,
    scenario: &Scenario,
) -> Result<(), String> {
    let backend = make();
    let memory = backend.memory();
    let own = |id: &str| memory.iter().any(|space| space.id == id);
    for transfer in backend.transfers() {
        require(own(&transfer.from) || own(&transfer.to), || {
            format!(
                "transfer `{}` touches none of this backend's spaces",
                transfer.id
            )
        })?;
        require(transfer.from != transfer.to, || {
            format!("transfer `{}` goes nowhere", transfer.id)
        })?;
        let made = scenario.with_region(&backend, &[], |_, cx| (transfer.make)(cx));
        if let Ok(step) = made {
            release(step);
        }
    }
    Ok(())
}

fn can_take_is_pure<B: Backend>(make: &dyn Fn() -> B, scenario: &Scenario) -> Result<(), String> {
    let backend = make();
    let offered = scenario.offered();
    let before = offered.clone();
    let offer = Offer { nodes: &offered };
    let first = backend.can_take(&offer);
    let second = backend.can_take(&offer);
    require(first == second, || format!("{first:?} and then {second:?}"))?;
    require(offered == before, || "the offer changed".to_string())
}

fn can_take_returns_only_offered<B: Backend>(
    make: &dyn Fn() -> B,
    scenario: &Scenario,
) -> Result<(), String> {
    let offered = scenario.offered();
    for taken in make().can_take(&Offer { nodes: &offered }) {
        require(
            offered
                .iter()
                .any(|node| node.node == taken.node && node.body == taken.body),
            || format!("took {taken:?}, which was not offered"),
        )?;
    }
    Ok(())
}

fn taken_nodes(backend: &dyn Backend, scenario: &Scenario) -> Vec<NodeIndex> {
    scenario
        .taken(backend)
        .iter()
        .map(|node| node.node)
        .collect()
}

fn accepted_region_compiles<B: Backend>(
    make: &dyn Fn() -> B,
    scenario: &Scenario,
) -> Result<(), String> {
    let backend = make();
    let nodes = taken_nodes(&backend, scenario);
    if nodes.is_empty() {
        return Ok(());
    }
    let compiled = scenario
        .with_region(&backend, &nodes, |region, cx| backend.compile(region, cx))
        .map_err(|refusal| format!("refused a region it accepted: {refusal}"))?;
    release_all(compiled);
    Ok(())
}

/// Compiles everything offered, taken or not, because a refusal is what this is looking for.
fn refusal_names_only_region<B: Backend>(
    make: &dyn Fn() -> B,
    scenario: &Scenario,
) -> Result<(), String> {
    let backend = make();
    let nodes = Scenario::all_nodes();
    match scenario.with_region(&backend, &nodes, |region, cx| backend.compile(region, cx)) {
        Ok(compiled) => {
            release_all(compiled);
            Ok(())
        }
        Err(refusal) => {
            let outside: Vec<_> = refusal
                .nodes
                .iter()
                .filter(|n| !nodes.contains(n))
                .collect();
            require(outside.is_empty(), || {
                format!("the refusal names {outside:?}")
            })
        }
    }
}

fn release_all(compiled: Compiled) {
    if let Compiled::Steps(units) = compiled {
        units.into_iter().for_each(|unit| release(unit.step));
    }
}

fn units_of(compiled: &Compiled) -> Vec<&UnitInfo> {
    match compiled {
        Compiled::Steps(units) => units.iter().map(|unit| &unit.info).collect(),
        Compiled::Blob { units, .. } | Compiled::Source { units, .. } => units.iter().collect(),
    }
}

fn covers_stay_inside<B: Backend>(make: &dyn Fn() -> B, scenario: &Scenario) -> Result<(), String> {
    let backend = make();
    let nodes = taken_nodes(&backend, scenario);
    if nodes.is_empty() {
        return Ok(());
    }
    let compiled = scenario
        .with_region(&backend, &nodes, |region, cx| backend.compile(region, cx))
        .map_err(|refusal| refusal.to_string())?;
    let outside = units_of(&compiled)
        .iter()
        .flat_map(|unit| &unit.covers)
        .find(|covered| !nodes.contains(covered))
        .copied();
    release_all(compiled);
    require(outside.is_none(), || {
        format!("a unit covers {outside:?}, outside the region")
    })
}

/// Compiles what the backend took and calls `body` with its steps, if the form is `Steps`.
fn with_steps<B: Backend>(
    make: &dyn Fn() -> B,
    scenario: &Scenario,
    body: impl FnOnce(&mut [StepUnit], &[NodeIndex]) -> Result<(), String>,
) -> Result<(), String> {
    let backend = make();
    let nodes = taken_nodes(&backend, scenario);
    if nodes.is_empty() {
        return Ok(());
    }
    let compiled = scenario
        .with_region(&backend, &nodes, |region, cx| backend.compile(region, cx))
        .map_err(|refusal| refusal.to_string())?;
    let Compiled::Steps(mut units) = compiled else {
        return Ok(());
    };
    let outcome = body(&mut units, &nodes);
    units.into_iter().for_each(|unit| release(unit.step));
    outcome
}

fn run_unit(unit: &mut StepUnit, cx: &mut StepCx) -> StepEnd {
    let mut cells: [Slot; 0] = [];
    let mut slots = Slots::of(&mut cells);
    run_once(&mut unit.step, &mut slots, cx)
}

/// The first run may set things up, so the second one is what is counted.
fn steps_do_not_allocate<B: Backend>(
    make: &dyn Fn() -> B,
    scenario: &Scenario,
) -> Result<(), String> {
    with_steps(make, scenario, |units, _| {
        for (index, unit) in units.iter_mut().enumerate() {
            run_unit(unit, &mut StepCx::new());
            let mut cx = StepCx::new();
            let (_, allocations) = counted(|| run_unit(unit, &mut cx));
            require(allocations == 0, || {
                format!("step {index} allocated {allocations} times")
            })?;
        }
        Ok(())
    })
}

fn faults_stay_inside<B: Backend>(make: &dyn Fn() -> B, scenario: &Scenario) -> Result<(), String> {
    with_steps(make, scenario, |units, nodes| {
        for unit in units.iter_mut() {
            let mut cx = StepCx::new();
            if matches!(run_unit(unit, &mut cx), StepEnd::Fault) {
                let inside = cx.at == NO_NODE || nodes.iter().any(|node| node.0 == cx.at);
                require(inside, || format!("a fault named node {}", cx.at))?;
                require(cx.reason().len() <= REASON_CAPACITY, || {
                    "a long reason".to_string()
                })?;
            }
        }
        Ok(())
    })
}

fn outcome(unit: &mut StepUnit) -> (&'static str, u32, String) {
    let mut cx = StepCx::new();
    let end = match run_unit(unit, &mut cx) {
        StepEnd::Done => "done",
        StepEnd::Pending(_) => "pending",
        StepEnd::Skipped => "skipped",
        StepEnd::Fault => "fault",
    };
    (end, cx.at, cx.reason().to_string())
}

fn running_is_deterministic<B: Backend>(
    make: &dyn Fn() -> B,
    scenario: &Scenario,
) -> Result<(), String> {
    with_steps(make, scenario, |units, _| {
        for (index, unit) in units.iter_mut().enumerate() {
            let (first, second) = (outcome(unit), outcome(unit));
            require(first == second, || {
                format!("step {index}: {first:?} then {second:?}")
            })?;
        }
        Ok(())
    })
}
