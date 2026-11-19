//! Values by reference of any type (Linux): whether a value goes as a place is decided by its size
//! alone, never by its type. A large list goes as a place and comes back held where the worker
//! wrote it, with its items on an aligned address; a small one stays in its packet; a worker
//! that agreed values by reference for `bytes` alone is sent the list in its packet; an opaque
//! value with a codec is placed like any other; and a value with no bytes is refused, with the
//! reason, before anything crosses.

#![cfg(target_os = "linux")]

use crate::support;

use std::sync::Arc;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{OWNER_HANDED, Slot, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_backend_process::ProcessBackend;
use daedalus_runner::put_typed;
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey, Typed};
use daedalus_worker_conformance::runner::registry;
use support::{spec, step_of};

/// A `list<u32>` value as its cell holds it: the wire form, its count then its items.
fn list(items: u32) -> Vec<u8> {
    let mut bytes = items.to_le_bytes().to_vec();
    for item in 0..items {
        bytes.extend_from_slice(&item.wrapping_mul(2_654_435_761).to_le_bytes());
    }
    bytes
}

fn items(bytes: &[u8]) -> Vec<u32> {
    let (items, _) = bytes[4..].as_chunks::<4>();
    items.iter().map(|item| u32::from_le_bytes(*item)).collect()
}

/// Runs a one-input, one-output step on `value`, and gives the output cell to let go.
fn run(step: &mut Step, value: &mut [u8]) -> Result<Slot, String> {
    let len = value.len();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: value.as_mut_ptr(),
            len,
            cap: len,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx = StepCx::new();
    match run_once(step, &mut Slots::of(&mut cells), &mut cx) {
        StepEnd::Done => Ok(std::mem::replace(&mut cells[1], Slot::EMPTY)),
        _ => Err(cx.reason().to_owned()),
    }
}

fn reversed_list(step: &mut Step, count: u32) -> Slot {
    let mut value = list(count);
    let out = run(step, &mut value).expect("the list comes back");
    let mut want = items(&value);
    want.reverse();
    assert_eq!(items(out.bytes().expect("bytes")), want, "reversed");
    out
}

#[test]
fn a_large_list_goes_by_place_and_comes_back_held_and_aligned() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&[]), &registry())
        .expect("registers");
    let mut step = step_of(&backend, "fixture.list_reverse");
    for _ in 0..3 {
        let out = reversed_list(&mut step, 64 << 10);
        assert_eq!(out.owner, OWNER_HANDED, "held where the worker wrote it");
        // The items, after the four-byte count, start on a 64-byte boundary.
        assert_eq!((out.data as usize + 4) % 64, 0, "{:p}", out.data);
        daedalus_runner::drop_owned(out);
    }
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.placed_in, 3, "{telemetry:?}");
    assert_eq!(telemetry.placed_out, 3, "{telemetry:?}");
    assert!(telemetry.bytes_in < 4096, "{telemetry:?}");
    release(step);
}

#[test]
fn a_small_list_stays_in_its_packet() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&[]), &registry())
        .expect("registers");
    let mut step = step_of(&backend, "fixture.list_reverse");
    let out = reversed_list(&mut step, 1000);
    assert_ne!(out.owner, OWNER_HANDED);
    daedalus_runner::drop_owned(out);
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.agreed, 1, "{telemetry:?}");
    assert_eq!((telemetry.placed_in, telemetry.placed_out), (0, 0));
    release(step);
}

/// The threshold follows what the worker agreed: 4 KiB for one that takes any type by reference
/// (where the board measured a placed list beat the socket), 2 KiB for any other, and what the
/// spec names over both. A worker that takes bytes alone by reference never gets a list as a
/// place, whatever its size.
#[test]
fn the_threshold_follows_what_the_worker_agreed() {
    let placed = |worker: daedalus_backend_process::WorkerSpec| {
        let mut backend = ProcessBackend::new();
        backend.add_worker(worker, &registry()).expect("registers");
        let mut step = step_of(&backend, "fixture.list_reverse");
        // 16 KiB of items: past either threshold.
        let out = reversed_list(&mut step, 4 << 10);
        daedalus_runner::drop_owned(out);
        release(step);
        let telemetry = backend.telemetry();
        (telemetry.placed_in, telemetry.placed_out)
    };
    assert_eq!(placed(spec(&[])), (1, 1));
    assert_eq!(placed(spec(&[]).shared_from(64 << 10)), (0, 0));
    assert_eq!(
        placed(spec(&[]).env("DAEDALUS_FIXTURE_BYTES_ONLY", "1")),
        (0, 0)
    );
}

#[test]
fn a_worker_that_takes_bytes_alone_by_reference_gets_a_list_in_its_packet() {
    let mut backend = ProcessBackend::new();
    let worker = spec(&[]).env("DAEDALUS_FIXTURE_BYTES_ONLY", "1");
    backend.add_worker(worker, &registry()).expect("registers");
    let mut step = step_of(&backend, "fixture.list_reverse");
    let out = reversed_list(&mut step, 64 << 10);
    assert_ne!(out.owner, OWNER_HANDED);
    daedalus_runner::drop_owned(out);
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.agreed, 1, "{telemetry:?}");
    assert_eq!((telemetry.placed_in, telemetry.placed_out), (0, 0));
    release(step);
}

#[test]
fn an_opaque_value_with_a_codec_is_placed_like_any_other() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&[]), &registry())
        .expect("registers");
    let mut step = step_of(&backend, "fixture.custom_type_key");
    let mut value: Vec<u8> = (0..256u32 << 10).map(|at| at as u8).collect();
    let out = run(&mut step, &mut value).expect("it comes back");
    assert_eq!(out.bytes(), Some(value.as_slice()));
    assert_eq!(out.owner, OWNER_HANDED);
    daedalus_runner::drop_owned(out);
    let telemetry = backend.telemetry();
    assert_eq!((telemetry.placed_in, telemetry.placed_out), (1, 1));
    release(step);
}

/// A value held in its cell as a Rust value, with no bytes and no buffer behind it.
struct Shot(#[allow(dead_code)] u32);

impl Typed for Shot {
    const KEY: TypeKey = TypeKey::new("t:shot");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:shot"));
}

#[test]
fn a_value_with_no_bytes_is_refused_with_the_reason() {
    let mut backend = ProcessBackend::new();
    let worker = spec(&[]).env("DAEDALUS_FIXTURE_PROBE", "1");
    let mut registry = registry();
    backend
        .add_worker_declaring(worker, &mut registry)
        .expect("registers");
    let mut step = step_of(&backend, "fixture.dmabuf_probe");
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    put_typed(
        &mut cells[0],
        TypeIndex(0),
        SpaceIndex(0),
        Arc::new(Shot(7)),
    );
    let mut cx = StepCx::new();
    let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Fault), "refused");
    let reason = cx.reason();
    assert!(
        reason.contains("shot of fixture.dmabuf_probe: a t:shot value has no bytes"),
        "{reason}"
    );
    assert!(reason.contains("byte codec"), "{reason}");
    assert_eq!(backend.telemetry().runs, 0, "nothing crossed");
    for cell in cells {
        daedalus_runner::drop_owned(cell);
    }
    release(step);
}
