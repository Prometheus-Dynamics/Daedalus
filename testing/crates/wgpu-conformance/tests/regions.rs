//! The backend rules every backend passes, and regions of whole shaders, on a real device.

use daedalus_abi::StepEnd;
use daedalus_abi::testing::release;
use daedalus_conformance::{CheckResult, Scenario, check};
use daedalus_wgpu::{Options, WgpuBackend};
use daedalus_wgpu_conformance::{AFFINE, IDLE, In, Net, filled, port, private, run, words};

// The suite installs the counting allocator. Its nodes have no ports, so their body is a
// shader that binds nothing.
const SCENARIO: Scenario = Scenario {
    code: &IDLE,
    text: "",
    needs: &[],
};

fn rule(name: &str) -> CheckResult {
    check(name, || WgpuBackend::open(Options::default()), &SCENARIO).expect("a known check")
}

/// Every backend rule, the one about a warm step allocating nothing among them.
#[test]
fn the_backend_rules_hold() {
    if private().is_none() {
        return;
    }
    for name in [
        "id_is_stable",
        "memory_ids_are_unique",
        "transfers_name_known_spaces",
        "can_take_is_pure",
        "can_take_returns_only_offered_nodes",
        "accepted_region_compiles",
        "refusal_names_only_region_nodes",
        "covers_stay_inside_the_region",
        "steps_do_not_allocate",
        "faults_stay_inside_the_region",
        "running_is_deterministic",
    ] {
        let result = rule(name);
        assert!(result.passed(), "{result}");
    }
}

#[test]
fn a_line_of_shaders_is_one_step_and_gives_what_the_shaders_give() {
    let Some(backend) = private() else { return };
    let net = Net::line(&[&AFFINE, &AFFINE, &AFFINE]);
    let mut unit = net.compile(&backend, &[port(2, 0)]).expect("compiles");
    assert_eq!(unit.info.covers, net.nodes(), "one step for every node");

    let input: Vec<u32> = (0..777).collect();
    let ran = run(
        &backend,
        &mut unit,
        &[In::Buffer(&filled(&backend, &input))],
        1,
    );
    assert!(matches!(ran.end, StepEnd::Pending(_)), "{}", ran.reason);
    let out = ran.outputs[0].as_ref().expect("an output on the device");
    let want: Vec<u32> = input
        .iter()
        .map(|x| ((x * 2 + 1) * 2 + 1) * 2 + 1)
        .collect();
    assert_eq!(words(out), want);

    // The same input gives the same output, run after run.
    for _ in 0..3 {
        let again = run(
            &backend,
            &mut unit,
            &[In::Buffer(&filled(&backend, &input))],
            1,
        );
        assert_eq!(words(again.outputs[0].as_ref().expect("an output")), want);
    }
    release(unit.step);
}

#[test]
fn a_run_with_nothing_to_read_faults_inside_the_region_and_says_why() {
    let Some(backend) = private() else { return };
    let net = Net::line(&[&AFFINE, &AFFINE]);
    let mut unit = net.compile(&backend, &[port(1, 0)]).expect("compiles");
    // The input cell is there and empty.
    let mut cells = [daedalus_abi::Slot::EMPTY; 2];
    let mut cx = daedalus_abi::StepCx::new();
    let end = daedalus_abi::testing::run_once(
        &mut unit.step,
        &mut daedalus_abi::Slots::of(&mut cells),
        &mut cx,
    );
    assert!(matches!(end, StepEnd::Fault));
    assert!(!cx.reason().is_empty());
    assert!(
        cx.at == daedalus_abi::NO_NODE || cx.at < 2,
        "a node of this region"
    );
    // And the step still runs after.
    let ran = run(
        &backend,
        &mut unit,
        &[In::Buffer(&filled(&backend, &[1, 2, 3, 4]))],
        1,
    );
    assert!(matches!(ran.end, StepEnd::Pending(_)), "{}", ran.reason);
    assert_eq!(
        words(ran.outputs[0].as_ref().expect("an output")),
        [7, 11, 15, 19]
    );
    release(unit.step);
}

#[test]
fn a_body_that_is_no_shader_is_refused_by_node() {
    let Some(backend) = private() else { return };
    let mut net = Net::default();
    net.add("conformance.node", &AFFINE, 1, 1);
    net.add("conformance.not_a_shader", &7u32, 1, 1);
    net.inputs.push(port(0, 0));
    net.edges.push((port(0, 0), port(1, 0)));
    let Err(refusal) = net.compile(&backend, &[port(1, 0)]) else {
        panic!("a body that is no shader compiled");
    };
    assert_eq!(
        refusal.nodes,
        [daedalus_types::NodeIndex(1)],
        "only the node at fault"
    );
}
