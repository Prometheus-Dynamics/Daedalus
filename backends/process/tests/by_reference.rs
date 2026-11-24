//! Values by reference (Linux): a large `bytes` input goes as a place in lent memory and the
//! worker's output is held by its cell where the worker wrote it, and goes back in as the place it
//! is at, with no copy; a value buffer cells still hold
//! is not named for the next run; a steady run allocates nothing; and a dma-buf value reaches the
//! worker's node with no copy, mapped read-only, read inside the kernel's sync brackets, sent once
//! per buffer, with the buffers a worker keeps bounded.

#![cfg(target_os = "linux")]

use crate::support;

use std::os::fd::BorrowedFd;
use std::sync::Arc;

use daedalus_abi::counting::counted;
use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{OWNER_HANDED, Slot, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_backend_process::ProcessBackend;
use daedalus_dma_sys::{Access, DmaBuf, DmaHeap};
use daedalus_dmabuf::DmaBufBacked;
use daedalus_runner::put_typed;
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex};
use daedalus_types::{TypeKey, Typed};
use daedalus_worker_conformance::runner::registry;
use support::{spec, step_of};

const LARGE: usize = 256 << 10;

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at * 31 + 7) as u8).collect()
}

/// Runs `fixture.raw_io` on `payload` and gives the output cell, which the caller lets go.
fn raw_io(step: &mut Step, payload: &mut [u8]) -> Slot {
    let len = payload.len();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: payload.as_mut_ptr(),
            len,
            cap: len,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx = StepCx::new();
    let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Done), "{}", cx.reason());
    let back = cells[1].bytes().expect("bytes come back");
    assert!(back.iter().rev().eq(payload.iter()), "reversed");
    core::mem::replace(&mut cells[1], Slot::EMPTY)
}

#[test]
fn a_large_input_goes_by_place_and_its_output_is_held_where_the_worker_wrote_it() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&[]), &registry())
        .expect("registers");
    let mut step = step_of(&backend, "fixture.raw_io");
    let mut bytes = payload(LARGE);
    for _ in 0..3 {
        let out = raw_io(&mut step, &mut bytes);
        assert_eq!(out.owner, OWNER_HANDED, "the cell holds lent memory");
        daedalus_runner::drop_owned(out);
    }
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.agreed, 1, "{telemetry:?}");
    assert_eq!(telemetry.placed_in, 3, "{telemetry:?}");
    assert_eq!(telemetry.placed_out, 3, "{telemetry:?}");
    assert_eq!(
        telemetry.shared_in, 0,
        "a run with its values placed is small"
    );
    assert!(
        telemetry.bytes_in < 4096,
        "no value's bytes went in a packet: {telemetry:?}"
    );
    assert_eq!(telemetry.fd_sends, 0, "{telemetry:?}");
    release(step);
}

#[test]
fn a_workers_output_goes_back_in_as_the_place_it_is_at_with_no_copy() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&[]), &registry())
        .expect("registers");
    let mut step = step_of(&backend, "fixture.raw_io");
    let mut bytes = payload(LARGE);
    for round in 1..=3u64 {
        // Its output, in a value buffer, is the next run's input: the worker reads it there.
        let reversed = raw_io(&mut step, &mut bytes);
        assert_eq!(reversed.owner, OWNER_HANDED);
        let mut cells = [reversed, Slot::EMPTY];
        let mut cx = StepCx::new();
        let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut cx);
        assert!(matches!(end, StepEnd::Done), "{}", cx.reason());
        assert_eq!(cells[1].bytes(), Some(&bytes[..]), "reversed twice");
        let telemetry = backend.telemetry();
        assert_eq!(telemetry.passed_in, round, "{telemetry:?}");
        assert_eq!(telemetry.placed_in, round * 2, "{telemetry:?}");
        for cell in cells {
            daedalus_runner::drop_owned(cell);
        }
    }
    release(step);
}

#[test]
fn a_value_buffer_cells_still_hold_is_not_named_until_they_let_go() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&[]), &registry())
        .expect("registers");
    let mut step = step_of(&backend, "fixture.raw_io");
    let mut bytes = payload(LARGE);
    // Each held output keeps its value buffer: four runs hold all four.
    let held: Vec<Slot> = (0..4).map(|_| raw_io(&mut step, &mut bytes)).collect();
    assert!(held.iter().all(|cell| cell.owner == OWNER_HANDED));
    let fifth = raw_io(&mut step, &mut bytes);
    assert_ne!(
        fifth.owner, OWNER_HANDED,
        "with none free it came in the packet"
    );
    assert_eq!(backend.telemetry().outs_full, 1);
    daedalus_runner::drop_owned(fifth);
    for cell in held {
        daedalus_runner::drop_owned(cell);
    }
    let out = raw_io(&mut step, &mut bytes);
    assert_eq!(
        out.owner, OWNER_HANDED,
        "let go, the buffers are named again"
    );
    daedalus_runner::drop_owned(out);
    assert_eq!(backend.telemetry().outs_full, 1);
    release(step);
}

#[test]
fn a_steady_run_by_reference_allocates_nothing() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&[]), &registry())
        .expect("registers");
    let mut step = step_of(&backend, "fixture.raw_io");
    let mut bytes = payload(LARGE);
    for _ in 0..8 {
        daedalus_runner::drop_owned(raw_io(&mut step, &mut bytes));
    }
    let (_, allocations) = counted(|| {
        for _ in 0..8 {
            daedalus_runner::drop_owned(raw_io(&mut step, &mut bytes));
        }
    });
    assert_eq!(allocations, 0);
    release(step);
}

/// A value that is a buffer by descriptor, as a camera's would be.
struct Shot {
    buf: DmaBuf,
    len: u64,
}

impl Typed for Shot {
    const KEY: TypeKey = TypeKey::new("t:shot");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:shot"));
}

impl DmaBufBacked for Shot {
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        Some((self.buf.as_fd(), self.len))
    }
}

/// Where the test's buffers come from: the system dma-heap when this machine has one the test
/// may open, and a memfd otherwise.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Source {
    Heap,
    Memfd,
}

fn source() -> Source {
    if DmaHeap::why_not("system").is_none() {
        Source::Heap
    } else {
        Source::Memfd
    }
}

/// A new buffer of `len` bytes holding `fill`'s bytes.
fn shot(len: usize, fill: u8) -> Arc<Shot> {
    shot_from(source(), len, fill)
}

fn shot_from(source: Source, len: usize, fill: u8) -> Arc<Shot> {
    let buf = match source {
        Source::Heap => DmaHeap::open("system")
            .and_then(|heap| heap.allocate(len as u64))
            .expect("a heap buffer"),
        Source::Memfd => DmaBuf::memfd("shot", len as u64).expect("a memfd"),
    };
    let bytes: Vec<u8> = (0..len).map(|at| (at as u8).wrapping_add(fill)).collect();
    let mapping = buf.as_ref().map().expect("maps");
    let bracketed = buf.as_ref().begin_cpu_access(Access::Write).is_ok();
    mapping.write(0, &bytes).expect("writes");
    if bracketed {
        buf.as_ref().end_cpu_access(Access::Write).expect("ends");
    }
    Arc::new(Shot {
        buf,
        len: len as u64,
    })
}

fn sum_of(len: usize, fill: u8) -> u64 {
    (0..len).fold(0u64, |sum, at| {
        sum.wrapping_add(u64::from((at as u8).wrapping_add(fill)))
    })
}

/// The probe's report for `shot`.
fn probe(step: &mut Step, value: &Arc<Shot>) -> String {
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    put_typed(
        &mut cells[0],
        TypeIndex(0),
        SpaceIndex(0),
        Arc::clone(value),
    );
    let mut cx = StepCx::new();
    let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Done), "{}", cx.reason());
    let report = String::from_utf8(cells[1].bytes().expect("a report").to_vec()).expect("text");
    for cell in cells {
        daedalus_runner::drop_owned(cell);
    }
    report
}

fn field<'a>(report: &'a str, name: &str) -> &'a str {
    report
        .split_whitespace()
        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
        .unwrap_or_else(|| panic!("no {name} in {report}"))
}

fn probing(plain: bool) -> (ProcessBackend, Step) {
    let mut backend = ProcessBackend::new().with_dmabuf::<Shot>();
    let mut worker = spec(&[]).env("DAEDALUS_FIXTURE_PROBE", "1");
    if plain {
        worker = worker.env("DAEDALUS_FIXTURE_PLAIN", "1");
    }
    let mut registry = registry();
    backend
        .add_worker_declaring(worker, &mut registry)
        .expect("registers");
    let step = step_of(&backend, "fixture.dmabuf_probe");
    (backend, step)
}

#[test]
fn a_dmabuf_reaches_the_node_with_no_copy_read_only_and_bracketed() {
    // A memfd always; the system heap too when this machine lets the test open it.
    let mut sources = vec![Source::Memfd];
    if source() == Source::Heap {
        sources.push(Source::Heap);
    }
    for source in sources {
        zero_copy_from(source);
    }
}

fn zero_copy_from(source: Source) {
    let (backend, mut step) = probing(false);
    let len = 3 << 20;
    let value = shot_from(source, len, 5);
    let mut report = String::new();
    for _ in 0..3 {
        report = probe(&mut step, &value);
    }
    let report = format!("{source:?}: {report}");
    assert_eq!(
        field(&report, "sum"),
        sum_of(len, 5).to_string(),
        "{report}"
    );
    assert_eq!(field(&report, "len"), len.to_string());
    // The node read the buffer through the worker's own mapping of it, shared and read-only:
    // not a copy, and not writable.
    assert_eq!(field(&report, "held"), "r--s", "{report}");
    let telemetry = backend.telemetry();
    assert_eq!(
        telemetry.fd_sends, 1,
        "one buffer is sent once: {telemetry:?}"
    );
    assert_eq!(telemetry.placed_in, 3, "{telemetry:?}");
    assert!(
        telemetry.bytes_in < 4096,
        "no bytes in a packet: {telemetry:?}"
    );
    assert_eq!(field(&report, "attached"), "1");
    // On a heap buffer each run's read is inside DMA_BUF_IOCTL_SYNC START and END (READ, as the
    // mapping is read-only). The report is written inside the third run's brackets, so its END
    // comes after: every earlier START has its END (see the next test for a failing node). That proves the calls
    // are made and accepted, not that a cache was flushed: the system heap is cached and coherent
    // on x86. A memfd refuses the call (it has no device to sync with): the worker counts it as
    // unbracketed and reads it as it is.
    match source {
        Source::Heap => {
            assert_eq!(field(&report, "begun"), "3", "{report}");
            assert_eq!(field(&report, "ended"), "2", "{report}");
            assert_eq!(field(&report, "unbracketed"), "0", "{report}");
        }
        Source::Memfd => {
            assert_eq!(field(&report, "begun"), "0", "{report}");
            assert_eq!(field(&report, "unbracketed"), "3", "{report}");
        }
    }
    release(step);
}

/// The brackets the worker opened and closed, read by a probe from inside its own run: that run's
/// START is the one open.
fn open_brackets(report: &str) -> (u64, u64) {
    let begun = field(report, "begun").parse().expect("a count");
    let ended: u64 = field(report, "ended").parse().expect("a count");
    (begun, ended)
}

#[test]
fn every_start_has_its_end_even_when_a_node_fails() {
    let source = source();
    let (backend, mut step) = probing(false);
    let mut fail = step_of(&backend, "fixture.dmabuf_fail");
    let value = shot_from(source, 1 << 20, 3);
    let mut faults = 0;
    for round in 0..3u64 {
        let report = format!("{source:?}: {}", probe(&mut step, &value));
        let (begun, ended) = open_brackets(&report);
        match source {
            // This run's START is open, and every earlier one, the failed runs' too, is closed.
            Source::Heap => {
                assert_eq!(begun, round * 3 + 1, "{report}");
                assert_eq!(ended, begun - 1, "{report}");
            }
            Source::Memfd => assert_eq!((begun, ended), (0, 0), "{report}"),
        }
        for _ in 0..2 {
            let mut cells = [Slot::EMPTY, Slot::EMPTY];
            put_typed(
                &mut cells[0],
                TypeIndex(0),
                SpaceIndex(0),
                Arc::clone(&value),
            );
            let mut cx = StepCx::new();
            let end = run_once(&mut fail, &mut Slots::of(&mut cells), &mut cx);
            assert!(matches!(end, StepEnd::Fault), "the node fails");
            assert!(
                cx.reason().contains("failed after reading"),
                "{}",
                cx.reason()
            );
            faults += 1;
            for cell in cells {
                daedalus_runner::drop_owned(cell);
            }
        }
    }
    assert_eq!(faults, 6);
    assert_eq!(
        backend.telemetry().spawns,
        1,
        "a node's fault is not the worker's death"
    );
    release(fail);
    release(step);
}

#[test]
fn each_buffer_is_sent_once_and_a_worker_keeps_at_most_sixteen() {
    let (backend, mut step) = probing(false);
    let shots: Vec<Arc<Shot>> = (0..=daedalus_process_shm::SLOTS)
        .map(|at| shot(4096, at as u8))
        .collect();
    for (at, value) in shots.iter().enumerate() {
        let report = probe(&mut step, value);
        assert_eq!(field(&report, "sum"), sum_of(4096, at as u8).to_string());
    }
    let sends = shots.len() as u64;
    assert_eq!(backend.telemetry().fd_sends, sends);
    // The last ones are still kept: no send.
    probe(&mut step, &shots[shots.len() - 1]);
    assert_eq!(backend.telemetry().fd_sends, sends);
    // The first was let go to make room for the seventeenth, so it goes again.
    let report = probe(&mut step, &shots[0]);
    assert_eq!(field(&report, "sum"), sum_of(4096, 0).to_string());
    assert_eq!(backend.telemetry().fd_sends, sends + 1);
    release(step);
}

#[test]
fn a_plain_worker_is_sent_a_dmabufs_bytes() {
    let (backend, mut step) = probing(true);
    let len = 128 << 10;
    let value = shot(len, 9);
    let report = probe(&mut step, &value);
    assert_eq!(field(&report, "sum"), sum_of(len, 9).to_string());
    assert_eq!(field(&report, "held"), "copy");
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.fd_sends, 0, "{telemetry:?}");
    assert_eq!(telemetry.plain_workers, 1, "{telemetry:?}");
    release(step);
}
