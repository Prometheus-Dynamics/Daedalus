//! Chains of pixel bodies in a region: one shader for the chain, the same numbers as the bodies
//! run one at a time, and a whole shader in the middle cutting the chain around it.

use std::any::Any;

use daedalus_abi::StepEnd;
use daedalus_abi::testing::release;
use daedalus_kernels_sys::reference as r;
use daedalus_wgpu::kernels::{F32_TO_UNORM8, HALF_TO_F32};
use daedalus_wgpu_conformance::{AFFINE, In, Net, filled, measure, port, private, run, words};

type Code = &'static (dyn Any + Send + Sync);

fn halves() -> Vec<u32> {
    (0..=u32::from(u16::MAX)).collect()
}

#[test]
fn a_chain_is_one_pipeline_and_one_pass_and_gives_what_its_bodies_give() {
    let Some(backend) = private() else { return };
    let chain: [Code; 2] = [&HALF_TO_F32, &F32_TO_UNORM8];
    let net = Net::line(&chain);
    let (unit, built) = measure(&backend, || {
        net.compile(&backend, &[port(1, 0)]).expect("compiles")
    });
    let mut unit = unit;
    assert_eq!(built.pipelines, 1, "one shader for the two bodies");

    let input = filled(&backend, &halves());
    let ran = run(&backend, &mut unit, &[In::Buffer(&input)], 1);
    assert!(matches!(ran.end, StepEnd::Pending(_)), "{}", ran.reason);
    assert_eq!((ran.counted.passes, ran.counted.submits), (1, 1));
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

    // And what the CPU kernels give for every binary16 value.
    let bytes: Vec<u8> = (0..=u16::MAX).flat_map(u16::to_le_bytes).collect();
    let want: Vec<u32> = r::f32_to_unorm8(&r::f16_to_f32(&bytes))
        .into_iter()
        .map(u32::from)
        .collect();
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
    let codes: [Code; 5] = [
        &HALF_TO_F32,
        &F32_TO_UNORM8,
        &AFFINE,
        &HALF_TO_F32,
        &F32_TO_UNORM8,
    ];
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
    assert_eq!((ran.counted.passes, ran.counted.submits), (3, 1));

    let convert = |values: &[u32]| -> Vec<u32> {
        let bytes: Vec<u8> = values
            .iter()
            .flat_map(|v| (*v as u16).to_le_bytes())
            .collect();
        r::f32_to_unorm8(&r::f16_to_f32(&bytes))
            .into_iter()
            .map(u32::from)
            .collect()
    };
    let middle: Vec<u32> = convert(&input).iter().map(|x| x * 2 + 1).collect();
    assert_eq!(
        words(ran.outputs[0].as_ref().expect("an output")),
        convert(&middle)
    );
    release(unit.step);
}
