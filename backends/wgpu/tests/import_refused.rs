//! A dma-buf type reaches a device that refuses its kind of fd, with nothing else listed for it.
//!
//! A software renderer maps a buffer for writing, so it refuses a dma-buf whose fd only allows
//! reading. The type's own move is then costed as the copy its step makes, and the planner still
//! takes it: the any-type upload reads a cell's bytes, and a typed value has none there, so a plan
//! through it would fault on the first value.
#![cfg(target_os = "linux")]

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx, TransferCost, find_transfer};
use daedalus_dma_sys::DmaHeap;
use daedalus_runner::{drop_owned, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey, Typed};
use daedalus_wgpu::{
    DmaBufBacked, HOST_SPACE, HostBytes, Options, SPACE_DEVICE, Storage, WgpuBackend,
};

const BYTES: usize = 4096;

/// A read-only dma-buf value, or, where none was made, its bytes alone.
struct Frame {
    fd: Option<OwnedFd>,
    bytes: Option<Vec<u8>>,
}

impl Typed for Frame {
    const KEY: TypeKey = TypeKey::new("t:refused_frame");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:refused_frame"));
}

impl DmaBufBacked for Frame {
    const READ_ONLY: bool = true;
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        self.fd.as_ref().map(|fd| (fd.as_fd(), BYTES as u64))
    }
    fn bytes(&self) -> Option<&[u8]> {
        self.bytes.as_deref()
    }
}

/// The same, for a type that also has a byte upload of its own.
struct WithBytes(Vec<u8>);

impl Typed for WithBytes {
    const KEY: TypeKey = TypeKey::new("t:refused_with_bytes");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:refused_with_bytes"));
}

impl DmaBufBacked for WithBytes {
    const READ_ONLY: bool = true;
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        None
    }
}

impl HostBytes for WithBytes {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
    fn from_bytes(bytes: Vec<u8>) -> WithBytes {
        WithBytes(bytes)
    }
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
        lent: &[],
        offered: &[],
    }
}

/// Runs the move the planner takes for a `Frame` from the host to the device, and gives the
/// bytes the device has for it.
fn through_the_planned_move(backend: &WgpuBackend, value: Frame) -> Vec<u8> {
    let table = backend.transfers();
    let chosen = find_transfer(&table, &Frame::KEY, HOST_SPACE, SPACE_DEVICE)
        .expect("a move from the host to the device");
    assert!(
        chosen.id.starts_with("wgpu.import:") && chosen.key == Some(Frame::KEY),
        "the type's own move is taken, not {}",
        chosen.id
    );
    assert_eq!(
        chosen.cost,
        TransferCost::Copy,
        "costed as the copy it makes"
    );
    let mut step = (chosen.make)(&cx()).expect("a step");
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    put_typed(&mut cells[0], TypeIndex(0), SpaceIndex(0), Arc::new(value));
    let mut context = StepCx::new();
    let end = {
        let mut slots = Slots::of(&mut cells);
        run_once(&mut step, &mut slots, &mut context)
    };
    assert!(matches!(end, StepEnd::Done), "{}", context.reason());
    let storage = take_typed::<Storage>(&mut cells[1]).expect("a device buffer");
    let mut bytes = vec![1; BYTES];
    storage.read_into(0, &mut bytes).expect("a readback");
    drop_owned(cells[0]);
    release(step);
    bytes
}

/// The default device is the process's, and its answer for read-only dma-bufs is shared: the cases
/// run in turn here, in a process of their own, and the answer it had is put back after.
#[test]
fn a_read_only_dma_buf_type_reaches_a_device_that_refuses_its_kind() {
    let probe = WgpuBackend::open(Options::default());
    if probe.start().is_err() {
        return;
    }
    let before = probe.dmabuf_import(true);
    a_type_without_an_upload_takes_its_own_move();
    a_type_with_its_own_upload_keeps_it_on_a_device_that_refuses();
    if let Some(answer) = before {
        probe.assume_dmabuf_import(true, answer);
    }
}

/// A type with no byte upload of its own takes its dma-buf move, costed as the copy it makes.
fn a_type_without_an_upload_takes_its_own_move() {
    let backend = WgpuBackend::open(Options::default()).with_dmabuf::<Frame>();
    // As a software renderer answers, whatever this device would.
    backend.assume_dmabuf_import(true, false);
    if backend.start().is_err() {
        return;
    }
    let uploads = backend.transfer_stats().expect("open").uploads;

    // A value with no dma-buf of its own: its bytes are copied.
    let pattern: Vec<u8> = (0..BYTES).map(|at| (at * 7 + 3) as u8).collect();
    let got = through_the_planned_move(
        &backend,
        Frame {
            fd: None,
            bytes: Some(pattern.clone()),
        },
    );
    assert_eq!(got, pattern, "the value's bytes");

    // A read-only dma-buf: read through a read-only mapping and copied. Nothing can write one,
    // so its bytes are the heap's zeroes.
    if let Ok(heap) = DmaHeap::open("system") {
        let fd = heap
            .allocate_read_only((BYTES + 8192) as u64)
            .expect("a buffer")
            .into_fd();
        let got = through_the_planned_move(
            &backend,
            Frame {
                fd: Some(fd),
                bytes: None,
            },
        );
        assert_eq!(got, vec![0; BYTES], "the dma-buf's bytes");
    }
    let stats = backend.transfer_stats().expect("open");
    assert!(stats.uploads > uploads, "copied, not imported");
    assert_eq!(
        stats.imports, 0,
        "nothing imported on a device that refuses"
    );
}

/// A type with a byte upload of its own is copied by that on a device that refuses its kind, as
/// it says; its dma-buf move is planned only where the device imports.
fn a_type_with_its_own_upload_keeps_it_on_a_device_that_refuses() {
    let backend = WgpuBackend::open(Options::default())
        .with_bytes::<WithBytes>()
        .with_dmabuf::<WithBytes>();
    backend.assume_dmabuf_import(true, false);
    if backend.start().is_err() {
        return;
    }
    let table = backend.transfers();
    let chosen = find_transfer(&table, &WithBytes::KEY, HOST_SPACE, SPACE_DEVICE)
        .expect("a move from the host to the device");
    assert!(
        chosen.id.starts_with("wgpu.upload:"),
        "the type's own upload, not {}",
        chosen.id
    );
    backend.assume_dmabuf_import(true, true);
    let table = backend.transfers();
    let chosen = find_transfer(&table, &WithBytes::KEY, HOST_SPACE, SPACE_DEVICE)
        .expect("a move from the host to the device");
    assert!(
        chosen.id.starts_with("wgpu.import:"),
        "the import where the device takes it, not {}",
        chosen.id
    );
}
