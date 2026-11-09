//! The tunable and stored-plan modes natively. Both match the compiled graph or the node
//! functions, neither allocates per tick, and the runner refuses bad plans and carries on.

mod common;

use common::{Loaded, Out, Reference, sealed, signal};
use daedalus_abi::counting::counted;
use daedalus_chip_blink::loaded::{FANIN_ARENA, Interpreter, LIBRARY, PLAN_A, PLAN_B};
use daedalus_chip_blink::{Graph, tunable};
use daedalus_cpu::blob::{Interpreter as Runner, LoadError, RunError};
use daedalus_cpu::chip::{ParamError, Scalar, Tunable};
use daedalus_cpu::param_host::encode_update;

fn compiled_step(graph: &mut Graph, now: usize, raw: u16) -> Out {
    graph.push_sample(raw).unwrap();
    graph.tick_at(now as u64).unwrap();
    (graph.pop_level(), graph.pop_led(), graph.pop_rises())
}

fn tunable_step(graph: &mut tunable::Graph, now: usize, raw: u16) -> Out {
    graph.push_sample(raw).unwrap();
    graph.tick_at(now as u64).unwrap();
    (graph.pop_level(), graph.pop_led(), graph.pop_rises())
}

#[test]
fn tunable_matches_compiled_then_follows_its_parameters() {
    let (mut compiled, mut graph) = (Graph::new(), tunable::Graph::new());
    let ((), allocated) = counted(|| {
        for (now, raw) in signal().enumerate() {
            assert_eq!(
                tunable_step(&mut graph, now, raw),
                compiled_step(&mut compiled, now, raw)
            );
        }
    });
    assert_eq!(allocated, 0);
    assert_eq!(
        tunable::PARAM_NAMES,
        [
            "lowpass.alpha",
            "threshold.on",
            "threshold.off",
            "blink.period"
        ]
    );

    assert_eq!(graph.set_lowpass_alpha(1.5), Err(ParamError::Range));
    assert_eq!(graph.set_lowpass_alpha(f32::NAN), Err(ParamError::Range));
    assert_eq!(graph.set_param(3, Scalar::F64(2.5)), Err(ParamError::Type));
    assert_eq!(graph.set_param(3, Scalar::I64(0)), Err(ParamError::Range));
    assert_eq!(graph.set_param(9, Scalar::U32(1)), Err(ParamError::Id));
    assert_eq!(graph.param(0), Some(Scalar::F32(0.25)));

    let encode = |name, value| encode_update(&tunable::PARAM_NAMES, &tunable::PARAMS, name, value);
    let message = encode("threshold.on", Scalar::F32(2.5)).unwrap();
    assert_eq!(message[message.len() - 4..], 2.5_f32.to_le_bytes());
    assert_eq!(
        encode("threshold.on", Scalar::F32(9.0)),
        Err(ParamError::Range)
    );
    assert_eq!(
        graph.apply_update(&message[..2]),
        Err(ParamError::Malformed)
    );

    let mut graph = tunable::Graph::new();
    let ((), allocated) = counted(|| {
        graph.set_lowpass_alpha(0.5).unwrap();
        graph.set_param(3, Scalar::I64(2)).unwrap();
        graph.apply_update(&message).unwrap();
    });
    assert_eq!(allocated, 0, "parameter updates allocate nothing");
    assert_eq!(graph.param(1), Some(Scalar::F32(2.5)));

    let mut reference = Reference::tuned();
    for (now, raw) in signal().enumerate() {
        assert_eq!(tunable_step(&mut graph, now, raw), reference.step(raw));
    }
}

#[test]
fn loaded_plan_matches_compiled_without_allocating() {
    for plan in [PLAN_A, PLAN_B] {
        let needed = Interpreter::required(&LIBRARY, plan);
        assert!(needed.is_ok_and(|size| size <= FANIN_ARENA), "{needed:?}");
    }
    let mut loaded = Loaded::new(PLAN_A);
    let mut compiled = Graph::new();
    let ((), allocated) = counted(|| {
        for (now, raw) in signal().chain(signal()).enumerate() {
            assert_eq!(loaded.step(raw), compiled_step(&mut compiled, now, raw));
        }
    });
    assert_eq!(allocated, 0, "pushes, ticks and pops allocate nothing");

    // `rises` holds 8 and drops the oldest, as in the compiled graph.
    let mut loaded = Loaded::new(PLAN_A);
    let [sample, level, led, rises] = loaded.ports;
    for _ in 0..10 {
        for raw in [4000_u16, 0] {
            for _ in 0..8 {
                loaded.running.push(sample, raw).unwrap();
                loaded.running.tick().unwrap();
                loaded.running.pop::<f32>(level).unwrap();
                loaded.running.pop::<bool>(led).unwrap();
            }
        }
    }
    let kept: Vec<u32> = std::iter::from_fn(|| loaded.running.pop(rises).unwrap()).collect();
    assert_eq!(kept, (3..=10).collect::<Vec<_>>());

    // Host ports are typed.
    assert_eq!(
        loaded.running.push(sample, 1.0_f32),
        Err(RunError::Port { port: sample })
    );
    assert_eq!(
        loaded.running.pop::<u16>(rises),
        Err(RunError::Port { port: rises })
    );
}

#[test]
fn swaps_to_another_plan_at_run_time() {
    let mut loaded = Loaded::new(PLAN_A);
    let mut reference = Reference::a();
    for raw in signal().take(50) {
        assert_eq!(loaded.step(raw), reference.step(raw));
    }
    let first = loaded.running.plan_hash();
    let ((), allocated) = counted(|| loaded.load(PLAN_B));
    assert_eq!(allocated, 0, "loading allocates nothing");
    assert_ne!(loaded.running.plan_hash(), first);
    let mut reference = Reference::b();
    for raw in signal() {
        assert_eq!(loaded.step(raw), reference.step(raw));
    }
}

#[test]
fn rejects_invalid_blobs_and_keeps_the_running_plan() {
    let mut loaded = Loaded::new(PLAN_A);
    let mut reference = Reference::a();
    for raw in signal().take(30) {
        assert_eq!(loaded.step(raw), reference.step(raw));
    }
    let running = loaded.running.plan_hash();

    let mut version = PLAN_A.to_vec();
    version[8..10].copy_from_slice(&9_u16.to_le_bytes());
    let body = &PLAN_A[..PLAN_A.len() - 4];
    let other = {
        let hash = LIBRARY.hash.to_le_bytes();
        let at = body.windows(8).position(|part| part == hash).unwrap();
        let mut other = body.to_vec();
        other[at] ^= 1;
        sealed(&other)
    };
    let cases: [(&[u8], LoadError); 5] = [
        (&version, LoadError::Version { found: 9 }),
        (&PLAN_A[1..], LoadError::Malformed),
        // Sealed again, so the checksum holds and only the structure is wrong.
        (&sealed(&body[..body.len() - 1]), LoadError::Malformed),
        (&sealed(&body[..40]), LoadError::Malformed),
        (
            &other,
            LoadError::Library {
                expected: LIBRARY.hash,
                found: LIBRARY.hash ^ 1,
            },
        ),
    ];
    for (blob, error) in cases {
        assert_eq!(loaded.running.load(&LIBRARY, blob), Err(error));
        assert_eq!(loaded.running.plan_hash(), running);
    }

    // Corrupt or cut-short transfers fail the checksum before anything else is read.
    let mut corrupt = PLAN_A.to_vec();
    corrupt[40] ^= 0x10;
    let padded = [PLAN_A, &[0]].concat();
    for blob in [&corrupt[..], &PLAN_A[..PLAN_A.len() - 1], &padded[..]] {
        assert!(matches!(
            loaded.running.load(&LIBRARY, blob),
            Err(LoadError::Checksum { .. })
        ));
        assert_eq!(loaded.running.plan_hash(), running);
    }

    let needed = Interpreter::required(&LIBRARY, PLAN_A).unwrap();
    let mut small = Box::new(Runner::<64>::new());
    assert_eq!(
        small.load(&LIBRARY, PLAN_A),
        Err(LoadError::Arena {
            needed: needed as u32
        })
    );
    assert_eq!(small.tick(), Ok(()), "with no plan, a tick does nothing");

    // The running plan carries on where it was.
    for raw in signal().skip(30) {
        assert_eq!(loaded.step(raw), reference.step(raw));
    }
}
