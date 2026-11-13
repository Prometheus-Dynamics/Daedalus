//! Several values in one dma-buf, each at its own offset: planes, and pooled buffers. Each is
//! imported where it is, with no copy, and found again when its buffer comes round.
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)] // what each adapter did is this test's record

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx};
use daedalus_dma_sys::{Access, DmaBuf, DmaHeap};
use daedalus_runner::{drop_owned, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey, Typed};
use daedalus_wgpu::{DmaBufBacked, Options, Storage, WgpuBackend};

/// One value's length, and how far apart the values are in the buffer: a page multiple, which
/// every device here binds at.
const BYTES: usize = 4096;
const PITCH: usize = 8192;
const FRAMES: usize = 3;

/// One value of several in a dma-buf.
struct Part {
    fd: OwnedFd,
    offset: u64,
}

impl Typed for Part {
    const KEY: TypeKey = TypeKey::new("t:part");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:part"));
}

impl DmaBufBacked for Part {
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        Some((self.fd.as_fd(), BYTES as u64))
    }
    fn offset(&self) -> u64 {
        self.offset
    }
}

/// The bytes of value `frame` in round `round`: different for every value and every round.
fn pattern(frame: usize, round: usize) -> Vec<u8> {
    (0..BYTES)
        .map(|at| (at * 7 + frame * 31 + round * 101 + 3) as u8)
        .collect()
}

/// What the producer does: writes every value of a round into the one buffer.
fn write_round(buf: &DmaBuf, round: usize) {
    let map = buf.as_ref().map().expect("a mapping");
    buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
    for frame in 0..FRAMES {
        map.write((frame * PITCH) as u64, &pattern(frame, round))
            .expect("in range");
    }
    // And one more copy of value 0, four bytes off a page: no device binds a buffer there.
    map.write((FRAMES * PITCH + 4) as u64, &pattern(0, round))
        .expect("in range");
    buf.as_ref().end_cpu_access(Access::Write).expect("end");
}

fn cx() -> CompileCx<'static> {
    CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 64,
        merge_allowed: true,
        freeze_params: false,
        sharing: &[],
    }
}

/// The device's bytes for a value.
fn read(storage: &Storage) -> Vec<u8> {
    let mut bytes = vec![1; BYTES];
    storage.read_into(0, &mut bytes).expect("a readback");
    bytes
}

/// Runs `step`, the planned import, on the value at `offset` in `buf`, and gives the device's
/// bytes for it.
fn imported(step: &mut Step, buf: &DmaBuf, offset: usize) -> Vec<u8> {
    read(&import_part(step, buf, offset))
}

/// Runs `step`, the planned import, on the value at `offset` in `buf`, and gives the device
/// buffer.
fn import_part(step: &mut Step, buf: &DmaBuf, offset: usize) -> Arc<Storage> {
    let part = Part {
        fd: buf.try_clone().expect("a second fd").into_fd(),
        offset: offset as u64,
    };
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    put_typed(&mut cells[0], TypeIndex(0), SpaceIndex(0), Arc::new(part));
    let mut context = StepCx::new();
    let end = {
        let mut slots = Slots::of(&mut cells);
        run_once(step, &mut slots, &mut context)
    };
    assert!(matches!(end, StepEnd::Done), "{}", context.reason());
    let storage = take_typed::<Storage>(&mut cells[1]).expect("a device buffer");
    drop_owned(cells[0]);
    storage
}

/// With a bound of two imports and three values, the one imported longest ago is let go, and
/// imported again when it comes, while a holder of its first buffer still has it: two imports of
/// the same memory at once, both reading what the producer writes.
fn past_the_bound(index: usize, heap: &DmaHeap) {
    let backend = WgpuBackend::open(Options {
        index: Some(index),
        kept_imports: 2,
        ..Options::default()
    })
    .with_dmabuf::<Part>();
    backend.start().expect("the adapter opened a moment ago");
    let name = backend.caps().expect("open").adapter.name.clone();
    let transfers = backend.transfers();
    let transfer = transfers
        .iter()
        .find(|transfer| transfer.id == "wgpu.import:t:part")
        .expect("the import is planned");
    let mut step = (transfer.make)(&cx()).expect("a step");
    let buf = heap
        .allocate(((FRAMES + 2) * PITCH) as u64)
        .expect("a buffer");
    let stats = || backend.transfer_stats().expect("open");

    write_round(&buf, 0);
    let before = stats();
    // Value 0's buffer is held on, as a step downstream would hold it.
    let first = import_part(&mut step, &buf, 0);
    assert_eq!(read(&first), pattern(0, 0), "{name}");
    for frame in 1..FRAMES {
        assert_eq!(
            imported(&mut step, &buf, frame * PITCH),
            pattern(frame, 0),
            "{name}: value {frame}"
        );
    }
    // Value 2 is still kept, and found. Value 0 was let go for it.
    assert_eq!(
        imported(&mut step, &buf, 2 * PITCH),
        pattern(2, 0),
        "{name}"
    );
    assert_eq!(
        stats().imports - before.imports,
        FRAMES as u64,
        "{name}: three imports, and one found"
    );
    // So value 0 is imported again, beside the import still held.
    write_round(&buf, 1);
    let before = stats();
    let again = import_part(&mut step, &buf, 0);
    let after = stats();
    assert_eq!(
        (
            after.imports - before.imports,
            after.uploads - before.uploads
        ),
        (1, 0),
        "{name}: imported again past the bound, not copied"
    );
    assert!(!Arc::ptr_eq(&first, &again), "{name}: a second import");
    assert_eq!(read(&again), pattern(0, 1), "{name}: the second import");
    assert_eq!(read(&first), pattern(0, 1), "{name}: the first, still held");
    // Either goes without the other.
    drop(again);
    assert_eq!(read(&first), pattern(0, 1), "{name}: the first alone");
    eprintln!("offsets on {name}: past a bound of 2 a value is imported again, beside the first");
    release(step);
}

#[test]
fn values_at_offsets_in_one_dma_buf_are_imported_where_they_are() {
    let Ok(heap) = DmaHeap::open("system") else {
        assert!(
            std::env::var_os("DAEDALUS_DMA_HEAP_REQUIRED").is_none(),
            "this lane needs /dev/dma_heap/system"
        );
        return;
    };
    for index in 0..8 {
        let backend = WgpuBackend::open(Options {
            index: Some(index),
            ..Options::default()
        })
        .with_dmabuf::<Part>();
        if backend.start().is_err() {
            break;
        }
        let name = backend.caps().expect("open").adapter.name.clone();
        if backend.dmabuf_import(false) != Some(true) {
            eprintln!("offsets on {name}: the device imports no dma-buf, nothing to check");
            continue;
        }
        let transfers = backend.transfers();
        let transfer = transfers
            .iter()
            .find(|transfer| transfer.id == "wgpu.import:t:part")
            .expect("the import is planned");
        let mut step = (transfer.make)(&cx()).expect("a step");
        // Room past the last value for the device's padding.
        let buf = heap
            .allocate(((FRAMES + 2) * PITCH) as u64)
            .expect("a buffer");
        let stats = || backend.transfer_stats().expect("open");

        // Round 0: each value is imported at its own offset, with no copy.
        write_round(&buf, 0);
        let before = stats();
        for frame in 0..FRAMES {
            let got = imported(&mut step, &buf, frame * PITCH);
            assert_eq!(got, pattern(frame, 0), "{name}: value {frame}");
        }
        let after = stats();
        assert_eq!(
            (
                after.imports - before.imports,
                after.uploads - before.uploads
            ),
            (FRAMES as u64, 0),
            "{name}: three imports, no copy"
        );

        // Round 1: the producer writes the same buffer again. Each value is the buffer already
        // imported, so nothing is imported again, and the device has the new bytes.
        write_round(&buf, 1);
        let before = stats();
        for frame in 0..FRAMES {
            let got = imported(&mut step, &buf, frame * PITCH);
            assert_eq!(
                got,
                pattern(frame, 1),
                "{name}: value {frame}, written again"
            );
        }
        let after = stats();
        assert_eq!(
            (
                after.imports - before.imports,
                after.uploads - before.uploads
            ),
            (0, 0),
            "{name}: found again, nothing imported and nothing copied"
        );

        // A value four bytes off a page: no buffer is bound there, so it is copied, and the
        // answer for the device doesn't change.
        let before = stats();
        let got = imported(&mut step, &buf, FRAMES * PITCH + 4);
        assert_eq!(
            got,
            pattern(0, 1),
            "{name}: a value at an offset the device can't bind"
        );
        let after = stats();
        let how = match (
            after.imports - before.imports,
            after.uploads - before.uploads,
        ) {
            (1, 0) => "imported",
            (0, 1) => "copied",
            other => panic!("{name}: an odd offset gave {other:?}"),
        };
        assert_eq!(backend.dmabuf_import(false), Some(true), "{name}");
        eprintln!(
            "offsets on {name}: three values imported in place, found again; +4 bytes: {how}"
        );
        release(step);
        past_the_bound(index, &heap);
    }
}
