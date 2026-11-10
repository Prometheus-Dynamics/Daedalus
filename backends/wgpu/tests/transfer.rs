//! The moves between the host and the device: what the backend offers the planner, and each
//! step run on a real device.

mod common;

use std::sync::Arc;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx, TransferCost, find_transfer};
use daedalus_runner::{drop_owned, put_inline, put_shared, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeIndex, TypeKey};
use daedalus_wgpu::{
    Format, HOST_SPACE, MAP, Options, READBACK, SPACE_DEVICE, SPACE_HOST_MAPPED, Storage, Texture,
    TextureDesc, TextureError, UPLOAD, UPLOAD_MAPPED, WgpuBackend,
};

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

fn private() -> Option<WgpuBackend> {
    let record = common::adapter()?;
    let backend = WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    });
    // Uploads go to the device a backend opened last.
    backend.start().expect("opens");
    Some(backend)
}

fn step(backend: &WgpuBackend, id: &str) -> Step {
    let transfers = backend.transfers();
    let transfer = transfers.iter().find(|t| t.id == id).expect("offered");
    (transfer.make)(&cx()).expect("a step")
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at * 17 + 9) as u8).collect()
}

/// Runs a two-cell step and returns what it left in the output cell.
fn through(step: &mut Step, input: Slot) -> (StepEnd, Slot, String) {
    let mut cells = [input, Slot::EMPTY];
    let mut cx = StepCx::new();
    let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
    drop_owned(cells[0]);
    (end, cells[1], cx.reason().to_string())
}

#[test]
fn the_planner_is_offered_one_way_each_way() {
    let backend = WgpuBackend::open(Options::default());
    let table = backend.transfers();
    let key = TypeKey::new("anything");
    // Host to device is one upload, and device to host is a readback and then a map.
    let up = find_transfer(&table, &key, HOST_SPACE, SPACE_DEVICE).expect("an upload");
    assert_eq!((up.id, up.cost), (UPLOAD, TransferCost::Copy));
    let up = find_transfer(&table, &key, HOST_SPACE, SPACE_HOST_MAPPED).expect("an upload");
    assert_eq!((up.id, up.cost), (UPLOAD_MAPPED, TransferCost::Copy));
    let back = find_transfer(&table, &key, SPACE_DEVICE, SPACE_HOST_MAPPED).expect("a readback");
    assert_eq!((back.id, back.cost), (READBACK, TransferCost::Copy));
    let map = find_transfer(&table, &key, SPACE_HOST_MAPPED, HOST_SPACE).expect("a map");
    assert_eq!((map.id, map.cost), (MAP, TransferCost::Import));
    assert!(
        find_transfer(&table, &key, SPACE_DEVICE, HOST_SPACE).is_none(),
        "never in one hop"
    );
    // Every space the backend names but the device's own is one hop from the host.
    let buffers = backend
        .memory()
        .into_iter()
        .filter(|space| space.id != "wgpu.texture");
    for space in buffers.skip(1) {
        assert!(
            find_transfer(&table, &key, &space.id, HOST_SPACE).is_some(),
            "{}",
            space.id
        );
    }
    let ids: Vec<&str> = table.iter().map(|t| t.id).collect();
    let mut unique = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), ids.len(), "every id is its own");
}

#[test]
fn bytes_go_up_come_back_and_are_counted_once_each() {
    let Some(backend) = private() else { return };
    let (mut upload, mut readback, mut map) = (
        step(&backend, UPLOAD),
        step(&backend, READBACK),
        step(&backend, MAP),
    );
    let bytes = pattern(4001);
    let before = backend.transfer_stats().expect("open");

    // Host bytes become a device buffer, a whole number of words long.
    let mut cell = Slot::EMPTY;
    put_shared(&mut cell, TY, SPACE, Arc::from(bytes.as_slice()));
    let (end, mut uploaded, why) = through(&mut upload, cell);
    assert!(matches!(end, StepEnd::Done), "{why}");
    let on_device = take_typed::<Storage>(&mut uploaded).expect("a buffer");
    assert_eq!((on_device.size(), on_device.space()), (4004, SPACE_DEVICE));

    // The device buffer is copied to one the host can read, through a fence.
    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, Arc::clone(&on_device));
    let (end, mut copied, why) = through(&mut readback, cell);
    assert!(matches!(end, StepEnd::Pending(_)), "{why}");
    let readable = take_typed::<Storage>(&mut copied).expect("a buffer");

    // And handed to the host as it is.
    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, Arc::clone(&readable));
    let (end, mut mapped, why) = through(&mut map, cell);
    assert!(matches!(end, StepEnd::Done), "{why}");
    let host = take_typed::<Storage>(&mut mapped).expect("a buffer");
    assert!(Arc::ptr_eq(&host, &readable), "a map moves nothing");

    // A device read waits for the work before it, so by now the copy is done.
    let mut seen = vec![0; 4004];
    on_device.read_into(0, &mut seen).expect("a readback");
    let mut direct = vec![0; 4004];
    host.read_into(0, &mut direct).expect("in range");
    assert_eq!(seen[..4001], bytes);
    assert_eq!(seen[4001..], [0, 0, 0], "the tail is zeroes");
    if host.is_host_mapped() {
        assert_eq!(direct, seen, "the host reads the copy in place");
    }

    let stats = backend.transfer_stats().expect("open");
    assert_eq!(stats.uploads - before.uploads, 1, "exactly one upload");
    assert_eq!(stats.upload_bytes - before.upload_bytes, 4001);
    // The step's copy, and the device read this test made to wait for it.
    let step_downloads = stats.downloads - before.downloads - 1;
    assert_eq!(step_downloads, 1, "exactly one readback");

    for step in [upload, readback, map] {
        release(step);
    }
}

#[test]
fn small_values_upload_too_and_buffers_are_reused() {
    let Some(backend) = private() else { return };
    let mut upload = step(&backend, UPLOAD_MAPPED);
    // An inline value, as a CPU node writes a small Copy type.
    let mut first = None;
    for round in 0..6u32 {
        let mut cell = Slot::EMPTY;
        put_inline(&mut cell, TY, SPACE, &round.to_le_bytes()).expect("fits inline");
        let (end, mut out, why) = through(&mut upload, cell);
        assert!(matches!(end, StepEnd::Done), "{why}");
        let storage = take_typed::<Storage>(&mut out).expect("a buffer");
        let mut seen = [0; 4];
        storage.read_into(0, &mut seen).expect("a readback");
        assert_eq!(u32::from_le_bytes(seen), round);
        let id = Arc::as_ptr(&storage);
        // Dropped before the next round, so the next round gets the same buffer.
        assert_eq!(
            *first.get_or_insert(id),
            id,
            "round {round} reused the buffer"
        );
    }

    // A buffer that is already on the device passes through, and nothing else is a fault.
    let on_device = Arc::new(Storage::new(&backend, 64).expect("a buffer"));
    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, Arc::clone(&on_device));
    let (end, mut out, _) = through(&mut upload, cell);
    assert!(matches!(end, StepEnd::Done));
    assert!(Arc::ptr_eq(
        &take_typed::<Storage>(&mut out).expect("a buffer"),
        &on_device
    ));
    let (end, _, why) = through(&mut upload, Slot::EMPTY);
    assert!(matches!(end, StepEnd::Fault));
    assert!(why.contains("no bytes"), "{why}");
    let mut readback = step(&backend, READBACK);
    let (end, _, why) = through(&mut readback, Slot::EMPTY);
    assert!(matches!(end, StepEnd::Fault));
    assert!(why.contains("isn't a device buffer"), "{why}");
    release(upload);
    release(readback);
}

#[test]
fn gpu_storage_reads_back_into_caller_buffers() {
    let Some(backend) = private() else { return };
    let storage = Storage::new(&backend, 1 << 16).expect("a buffer");
    storage.write(0, &pattern(1 << 16)).expect("in range");
    let mut whole = vec![0; 1 << 16];
    storage.read_into(0, &mut whole).expect("a readback");
    assert_eq!(whole, pattern(1 << 16));
    let mut part = [0; 256];
    storage.read_into(4096, &mut part).expect("a readback");
    assert_eq!(part[..], pattern(1 << 16)[4096..4352]);
}

#[test]
fn integer_formats_round_trip_exactly() {
    let Some(backend) = private() else { return };
    for format in Format::ALL {
        // A width whose rows need padding for a copy, and more than one row.
        let desc = TextureDesc {
            format,
            width: 13,
            height: 7,
        };
        let texture = Texture::new(&backend, desc).expect("a texture");
        let row = 13 * format.bytes_per_pixel();
        let floats = matches!(format, Format::R32F | Format::Rg32F);
        let bytes: Vec<u8> = if floats {
            // Finite floats, so nothing canonicalises them on the way.
            (0..row * 7 / 4)
                .flat_map(|at| (at as f32 * 0.5 - 3.0).to_le_bytes())
                .collect()
        } else {
            pattern((row * 7) as usize)
        };
        texture.write(&bytes, row).expect("valid bytes");
        let mut seen = vec![0; bytes.len()];
        texture.read_into(&mut seen).expect("a readback");
        assert_eq!(seen, bytes, "{format:?}");

        let short = texture.read_into(&mut seen[..8]);
        assert!(
            matches!(short, Err(TextureError::Length { .. })),
            "{format:?}"
        );
        let ragged = texture.write(&bytes, row + 1);
        assert!(ragged.is_err(), "{format:?}");
    }
}
