//! Chains of pixel bodies in a region: one shader for the chain, the same numbers as the bodies
//! run one at a time, and a whole shader in the middle cutting the chain around it.

use std::any::Any;

use daedalus_abi::StepEnd;
use daedalus_abi::testing::release;
use daedalus_wgpu_conformance::{
    AFFINE, FLIP, In, Net, TRIPLE, filled, measure, port, private, run, triple_flip, words,
};

type Code = &'static (dyn Any + Send + Sync);

fn halves() -> Vec<u32> {
    (0..=u32::from(u16::MAX)).collect()
}

#[test]
fn a_chain_is_one_pipeline_and_one_pass_and_gives_what_its_bodies_give() {
    let Some(backend) = private() else { return };
    let chain: [Code; 2] = [&TRIPLE, &FLIP];
    let net = Net::line(&chain);
    let (unit, built) = measure(&backend, || {
        net.compile(&backend, &[port(1, 0)]).expect("compiles")
    });
    let mut unit = unit;
    assert_eq!(built.pipelines, 1, "one shader for the two bodies");

    let input = filled(&backend, &halves());
    let ran = run(&backend, &mut unit, &[In::Buffer(&input)], 1);
    assert!(matches!(ran.end, StepEnd::Pending(_)), "{}", ran.reason);
    assert_eq!((ran.counted.dispatches, ran.counted.submits), (1, 1));
    let merged = words(ran.outputs[0].as_ref().expect("an output"));
    release(unit.step);

    // One at a time, each a region of its own.
    let mut value = input;
    for code in chain {
        let mut alone = Net::line(&[code])
            .compile(&backend, &[port(0, 0)])
            .expect("compiles");
        let ran = run(&backend, &mut alone, &[In::Buffer(&value)], 1);
        value = filled(
            &backend,
            &words(ran.outputs[0].as_ref().expect("an output")),
        );
        release(alone.step);
    }
    assert_eq!(merged, words(&value), "bit for bit");

    // And what the two functions give, worked out here.
    let want: Vec<u32> = halves().into_iter().map(triple_flip).collect();
    assert_eq!(merged, want);

    // The same chain compiled again builds nothing.
    let (again, built) = measure(&backend, || {
        net.compile(&backend, &[port(1, 0)]).expect("compiles")
    });
    assert_eq!(built.pipelines, 0, "found, not built");
    release(again.step);
}

#[test]
fn a_whole_shader_cuts_the_chain_around_it() {
    let Some(backend) = private() else { return };
    // Two pixel bodies, a shader that isn't one, two more: a pass, a pass, a pass.
    let codes: [Code; 5] = [&TRIPLE, &FLIP, &AFFINE, &TRIPLE, &FLIP];
    let net = Net::line(&codes);
    let mut unit = net.compile(&backend, &[port(4, 0)]).expect("compiles");
    assert_eq!(unit.info.covers, net.nodes(), "still one step");
    let input: Vec<u32> = (0..2048).map(|at| at * 31).collect();
    let ran = run(
        &backend,
        &mut unit,
        &[In::Buffer(&filled(&backend, &input))],
        1,
    );
    assert!(matches!(ran.end, StepEnd::Pending(_)), "{}", ran.reason);
    // Three shaders ran, in one pass and one submit.
    assert_eq!(
        (
            ran.counted.dispatches,
            ran.counted.passes,
            ran.counted.submits
        ),
        (3, 1, 1)
    );

    let convert =
        |values: &[u32]| -> Vec<u32> { values.iter().copied().map(triple_flip).collect() };
    let middle: Vec<u32> = convert(&input)
        .iter()
        .map(|x| x.wrapping_mul(2).wrapping_add(1))
        .collect();
    assert_eq!(
        words(ran.outputs[0].as_ref().expect("an output")),
        convert(&middle)
    );
    release(unit.step);
}
