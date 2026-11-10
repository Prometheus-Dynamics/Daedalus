//! An author's own type across the host boundary: what the backend lists for it, and the two
//! steps run on a real device.

mod common;

use std::sync::Arc;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx, SpaceKind, TransferCost, find_transfer};
use daedalus_runner::{drop_owned, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey, Typed};
use daedalus_wgpu::{
    HOST_SPACE, HostBytes, Options, SPACE_DEVICE, SPACE_HOST_MAPPED, Storage, WgpuBackend,
};

#[derive(Debug, PartialEq, Eq)]
struct Samples(Vec<u8>);

impl Typed for Samples {
    const KEY: TypeKey = TypeKey::new("test:samples");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("test:samples"));
}

impl HostBytes for Samples {
    fn bytes(&self) -> &[u8] {
        &self.0
    }

    fn from_bytes(bytes: Vec<u8>) -> Samples {
        Samples(bytes)
    }
}

const TY: TypeIndex = TypeIndex(0);
const SPACE: SpaceIndex = SpaceIndex(0);

fn cx() -> CompileCx<'static> {
    CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 1,
        merge_allowed: true,
        freeze_params: false,
    }
}

fn through(step: &mut Step, input: Slot) -> (StepEnd, Slot, String) {
    let mut cells = [input, Slot::EMPTY];
    let mut cx = StepCx::new();
    let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
    drop_owned(cells[0]);
    (end, cells[1], cx.reason().to_string())
}

#[test]
fn host_ports_are_on_the_host_unless_the_options_say_the_device() {
    let backend = WgpuBackend::open(Options::default());
    let memory = backend.memory();
    assert_eq!(
        memory[0].id, SPACE_DEVICE,
        "outputs still live on the device"
    );
    let host = memory.last().expect("spaces");
    assert_eq!(
        (&*host.id, host.kind, host.readable),
        (HOST_SPACE, SpaceKind::Host, false)
    );
    assert_eq!(
        memory
            .iter()
            .filter(|space| space.kind == SpaceKind::Host)
            .count(),
        1
    );

    let on_device = WgpuBackend::open(Options {
        host_ports_on_device: true,
        ..Options::default()
    });
    assert!(
        on_device
            .memory()
            .iter()
            .all(|space| space.kind != SpaceKind::Host)
    );
}

#[test]
fn a_type_that_is_bytes_gets_its_own_moves_which_the_planner_prefers() {
    let plain = WgpuBackend::open(Options::default());
    let typed = WgpuBackend::open(Options::default()).with_bytes::<Samples>();
    assert_eq!(typed.transfers().len(), plain.transfers().len() + 2);
    // Told twice, it lists them once.
    let typed = typed.with_bytes::<Samples>();
    let table = typed.transfers();
    assert_eq!(table.len(), plain.transfers().len() + 2);

    let up = find_transfer(&table, &Samples::KEY, HOST_SPACE, SPACE_DEVICE).expect("an upload");
    assert_eq!(up.id, "wgpu.upload:test:samples");
    assert_eq!(
        (up.key.as_ref(), up.cost),
        (Some(&Samples::KEY), TransferCost::Copy)
    );
    let down = find_transfer(&table, &Samples::KEY, SPACE_HOST_MAPPED, HOST_SPACE).expect("a map");
    assert_eq!(down.id, "wgpu.map:test:samples");
    // Any other type still gets the moves for any type.
    let other = TypeKey::new("test:other");
    assert_eq!(
        find_transfer(&table, &other, HOST_SPACE, SPACE_DEVICE).map(|t| t.id),
        Some("wgpu.upload")
    );
    assert_eq!(
        find_transfer(&table, &other, SPACE_HOST_MAPPED, HOST_SPACE).map(|t| t.id),
        Some("wgpu.map")
    );

    // On a named device they are that device's.
    let named = WgpuBackend::open_named("typed", Options::default())
        .expect("a free name")
        .with_bytes::<Samples>();
    let table = named.transfers();
    let up =
        find_transfer(&table, &Samples::KEY, HOST_SPACE, "wgpu.typed.buffer").expect("an upload");
    assert_eq!(up.id, "wgpu.typed.upload:test:samples");
}

#[test]
fn a_typed_value_goes_up_as_its_bytes_and_comes_back_as_the_type() {
    let Some(record) = common::adapter() else {
        return;
    };
    let backend = WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    })
    .with_bytes::<Samples>();
    backend.start().expect("opens");
    let step = |id: &str| -> Step {
        let table = backend.transfers();
        let found = table.iter().find(|t| t.id == id).expect("offered");
        (found.make)(&cx()).expect("a step")
    };
    let (mut upload, mut readback, mut map) = (
        step("wgpu.upload:test:samples"),
        step("wgpu.readback"),
        step("wgpu.map:test:samples"),
    );
    // Seven bytes: not a whole number of words.
    let sent = Samples(vec![1, 2, 3, 4, 5, 6, 7]);
    let before = backend.transfer_stats().expect("open");
    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, Arc::new(sent));
    let (end, mut out, why) = through(&mut upload, cell);
    assert!(matches!(end, StepEnd::Done), "{why}");
    let on_device = take_typed::<Storage>(&mut out).expect("a buffer");
    assert_eq!(on_device.size(), 8, "padded to a word");
    assert_eq!(
        backend.transfer_stats().expect("open").uploads - before.uploads,
        1
    );

    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, Arc::clone(&on_device));
    let (end, mut out, why) = through(&mut readback, cell);
    assert!(matches!(end, StepEnd::Pending(_)), "{why}");
    let readable = take_typed::<Storage>(&mut out).expect("a buffer");
    // The runner holds the next step until the copy is done. Here a device read does.
    let mut seen = [0; 8];
    on_device.read_into(0, &mut seen).expect("a readback");

    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, readable);
    let (end, mut out, why) = through(&mut map, cell);
    assert!(matches!(end, StepEnd::Done), "{why}");
    let back = take_typed::<Samples>(&mut out).expect("the type");
    assert_eq!(
        *back,
        Samples(vec![1, 2, 3, 4, 5, 6, 7, 0]),
        "the bytes, and the pad"
    );

    // A value of another type is refused by the typed upload, by reason.
    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, Arc::new(5u64));
    let (end, _, why) = through(&mut upload, cell);
    assert!(matches!(end, StepEnd::Fault));
    assert!(why.contains("no bytes"), "{why}");
    for step in [upload, readback, map] {
        release(step);
    }
}
