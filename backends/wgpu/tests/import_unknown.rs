//! A process that can't allocate from a dma-heap doesn't know whether its device imports a
//! dma-buf until it is handed one. The import is planned anyway, the first value settles it, and
//! a value the device refuses is copied: the graph gets its bytes either way.
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)] // what each adapter answered is this test's record

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx};
use daedalus_dma_sys::{Access, DmaHeap};
use daedalus_runner::{drop_owned, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey, Typed};
use daedalus_wgpu::{DmaBufBacked, Options, Storage, WgpuBackend};

const BYTES: usize = 4096;

/// A value in a dma-buf whose fd can be written through.
struct Writable(OwnedFd);
/// A value in a dma-buf whose fd only allows reading.
struct OnlyRead(OwnedFd);

impl Typed for Writable {
    const KEY: TypeKey = TypeKey::new("t:unknown_writable");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:unknown_writable"));
}
impl Typed for OnlyRead {
    const KEY: TypeKey = TypeKey::new("t:unknown_only_read");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:unknown_only_read"));
}
impl DmaBufBacked for Writable {
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        Some((self.0.as_fd(), BYTES as u64))
    }
}
impl DmaBufBacked for OnlyRead {
    const READ_ONLY: bool = true;
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        Some((self.0.as_fd(), BYTES as u64))
    }
}

fn pattern() -> Vec<u8> {
    (0..BYTES).map(|at| (at * 7 + 3) as u8).collect()
}

/// What the producer does, with the heap access this process happens to have: a buffer with
/// room for any device's padding, filled with the pattern where its fd can be written.
fn produced(heap: &DmaHeap, read_only: bool) -> OwnedFd {
    let len = (BYTES + 8192) as u64;
    if read_only {
        return heap.allocate_read_only(len).expect("a buffer").into_fd();
    }
    let buf = heap.allocate(len).expect("a buffer");
    let map = buf.as_ref().map().expect("a mapping");
    buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
    map.write(0, &pattern()).expect("in range");
    buf.as_ref().end_cpu_access(Access::Write).expect("end");
    buf.into_fd()
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

fn listed(backend: &WgpuBackend, key: &str) -> bool {
    backend
        .transfers()
        .iter()
        .any(|transfer| transfer.id.starts_with("wgpu.import:") && transfer.id.ends_with(key))
}

/// Runs the planned import of `value` and gives the bytes the device has for it.
fn through_the_import<T: DmaBufBacked>(backend: &WgpuBackend, value: T) -> Vec<u8> {
    let id = format!("wgpu.import:{}", T::KEY.as_str());
    let transfers = backend.transfers();
    let transfer = transfers
        .iter()
        .find(|transfer| transfer.id == id)
        .expect("the import is planned");
    let mut step = (transfer.make)(&cx()).expect("a step");
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    put_typed(&mut cells[0], TypeIndex(0), SpaceIndex(0), Arc::new(value));
    let mut context = StepCx::new();
    let end = {
        let mut slots = Slots::of(&mut cells);
        run_once(&mut step, &mut slots, &mut context)
    };
    assert!(
        matches!(end, StepEnd::Done),
        "an import that can't be done is copied, never faulted: {}",
        context.reason()
    );
    let storage = take_typed::<Storage>(&mut cells[1]).expect("a device buffer");
    let mut bytes = vec![1; storage.size() as usize];
    storage.read_into(0, &mut bytes).expect("a readback");
    drop_owned(cells[0]);
    release(step);
    bytes
}

#[test]
fn the_first_dma_buf_settles_whether_a_device_imports_and_a_refused_one_is_copied() {
    let Ok(heap) = DmaHeap::open("system") else {
        assert!(
            std::env::var_os("DAEDALUS_DMA_HEAP_REQUIRED").is_none(),
            "this lane needs /dev/dma_heap/system, to stand in for the producer"
        );
        return;
    };
    for index in 0..8 {
        // A device of a process that can't allocate from a heap: nothing is tried at open.
        let backend = WgpuBackend::open(Options {
            index: Some(index),
            no_heap_access: true,
            ..Options::default()
        })
        .with_dmabuf::<Writable>()
        .with_dmabuf::<OnlyRead>();
        if backend.start().is_err() {
            break;
        }
        let caps = backend.caps().expect("open");
        let name = caps.adapter.name.clone();
        assert!(!caps.dma_heap, "{name}: nothing it can allocate from");
        for (read_only, key) in [(false, "t:unknown_writable"), (true, "t:unknown_only_read")] {
            let before = backend.dmabuf_import(read_only);
            if before == Some(false) {
                // The device has no dma-buf import at all, and that is known without trying.
                assert!(!listed(&backend, key), "{name}: no import to plan");
                continue;
            }
            assert_eq!(before, None, "{name}: unknown until a dma-buf arrives");
            assert!(
                listed(&backend, key),
                "{name}: the import is planned while unknown"
            );
            let version = backend.version();
            let uploads = backend.transfer_stats().expect("open").uploads;

            // The first value is the trial, and the graph gets its bytes whatever the answer.
            let expected = if read_only { vec![0; BYTES] } else { pattern() };
            let first = if read_only {
                through_the_import(&backend, OnlyRead(produced(&heap, true)))
            } else {
                through_the_import(&backend, Writable(produced(&heap, false)))
            };
            assert_eq!(
                first, expected,
                "{name}, read-only {read_only}: the first value"
            );

            let answer = backend.dmabuf_import(read_only);
            assert!(answer.is_some(), "{name}: the first value settled it");
            assert_ne!(backend.version(), version, "{name}: a plan is made again");
            let copied = backend.transfer_stats().expect("open").uploads - uploads;
            if answer == Some(true) {
                assert_eq!(copied, 0, "{name}: imported in place");
                assert!(listed(&backend, key));
            } else {
                assert_eq!(copied, 1, "{name}: refused, so copied once");
                assert!(
                    !listed(&backend, key),
                    "{name}: the next plan doesn't import"
                );
            }
            eprintln!(
                "unknown import on {name}, read-only {read_only}: {}",
                if answer == Some(true) {
                    "imports"
                } else {
                    "refused, copied"
                }
            );
        }
    }
}

#[test]
fn a_host_can_say_or_try_before_the_first_value() {
    let Ok(heap) = DmaHeap::open("system") else {
        return;
    };
    for index in 0..8 {
        let open = || {
            WgpuBackend::open(Options {
                index: Some(index),
                no_heap_access: true,
                ..Options::default()
            })
            .with_dmabuf::<Writable>()
        };
        // Said before the device opens: it holds once it does.
        let said = open();
        said.assume_dmabuf_import(false, false);
        if said.start().is_err() {
            break;
        }
        assert_eq!(said.dmabuf_import(false), Some(false));
        assert!(
            !listed(&said, "t:unknown_writable"),
            "said no: nothing to plan"
        );
        drop(said);

        // Tried with a buffer the producer supplies: the answer is the device's own.
        let tried = open();
        let answer = tried
            .try_dmabuf_import(produced(&heap, false), false)
            .expect("opens");
        assert_eq!(tried.dmabuf_import(false), Some(answer));
        assert_eq!(listed(&tried, "t:unknown_writable"), answer);
        // Where it imports, a value that follows goes in place. Where it doesn't, no import is
        // planned, and the value takes the copy the planner has for its type.
        if answer {
            let bytes = through_the_import(&tried, Writable(produced(&heap, false)));
            assert_eq!(bytes, pattern());
        }
    }
}

/// A value with no dma-buf, as one made where no heap opens: only its bytes.
struct NoFd(Option<Vec<u8>>);

impl Typed for NoFd {
    const KEY: TypeKey = TypeKey::new("t:unknown_no_fd");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:unknown_no_fd"));
}
impl DmaBufBacked for NoFd {
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        None
    }
    fn bytes(&self) -> Option<&[u8]> {
        self.0.as_deref()
    }
}

/// While nothing has tried, the import is planned, and a value can reach it that has no dma-buf
/// at all: made on a machine whose heap doesn't open. It is copied from its bytes, and says
/// nothing about what the device imports.
#[test]
fn a_value_with_no_dma_buf_is_copied_from_its_bytes() {
    let backend = WgpuBackend::open(Options {
        index: Some(0),
        no_heap_access: true,
        ..Options::default()
    })
    .with_dmabuf::<NoFd>();
    if backend.start().is_err() {
        assert!(
            std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
            "this lane needs an adapter"
        );
        return;
    }
    if backend.dmabuf_import(false) == Some(false) {
        // No dma-buf import on this device at all: nothing is planned, and the value is uploaded.
        return;
    }
    let before = backend.transfer_stats().expect("open");
    // A length that isn't a whole number of words is padded.
    for len in [BYTES, 10] {
        let bytes = pattern()[..len].to_vec();
        let got = through_the_import(&backend, NoFd(Some(bytes.clone())));
        assert_eq!(got[..len], bytes[..], "{len} bytes");
    }
    let after = backend.transfer_stats().expect("open");
    assert_eq!(after.uploads - before.uploads, 2, "each was a copy");
    assert_eq!(after.imports, before.imports, "nothing was imported");
    assert_eq!(
        backend.dmabuf_import(false),
        None,
        "and nothing was learned"
    );

    // With neither a dma-buf nor bytes there is nothing to give the device: a fault that says so.
    let id = format!("wgpu.import:{}", NoFd::KEY.as_str());
    let transfers = backend.transfers();
    let transfer = transfers.iter().find(|one| one.id == id).expect("planned");
    let mut step = (transfer.make)(&cx()).expect("a step");
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    put_typed(
        &mut cells[0],
        TypeIndex(0),
        SpaceIndex(0),
        Arc::new(NoFd(None)),
    );
    let mut context = StepCx::new();
    let end = {
        let mut slots = Slots::of(&mut cells);
        run_once(&mut step, &mut slots, &mut context)
    };
    assert!(matches!(end, StepEnd::Fault));
    assert!(
        context.reason().contains("no dma-buf and no bytes"),
        "{}",
        context.reason()
    );
    drop_owned(cells[0]);
    release(step);
}
