//! The device's fence on outside dma-bufs, on a real device.
//!
//! A software device can't import a dma-buf, so these put a dma-buf beside a plain device buffer,
//! as an import step does beside an imported one: the fence is attached to the dma-buf and the
//! owner kept just the same. A host gate keeps the submit pending for as long as a test needs,
//! so nothing here races the device.

use std::any::Any;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use daedalus_dma_sys::{Access, DmaBuf, DmaHeap};
use daedalus_vk_sys::gate::HostGate;

use crate::context::Context;
use crate::explicit_sync::External;
use crate::shader::{BindingKind, BindingSpec, Bound, Dispatch, Run, Shader, ShaderSpec};
use crate::{Options, Storage, Ticket, WgpuBackend};

static BUMP: ShaderSpec = ShaderSpec {
    name: "bump",
    entry: "main",
    source: "
        @group(0) @binding(0) var<storage, read_write> dst: array<u32>;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x >= arrayLength(&dst) { return; }
            dst[id.x] = dst[id.x] + 1u;
        }
    ",
    workgroup: [64, 1, 1],
    bindings: &[BindingSpec {
        index: 0,
        name: "dst",
        kind: BindingKind::Storage {
            read_only: false,
            stride: 4,
        },
    }],
    dispatch: Dispatch::PerElement {
        binding: 0,
        stride: 4,
    },
    inputs: &[],
    outputs: &[],
};

const BYTES: u64 = 4096;
const PATIENT: Duration = Duration::from_secs(120);

/// A backend with a device of its own, so its counts are this test's alone.
fn private(options: Options) -> Option<(WgpuBackend, Arc<Context>)> {
    let shared = WgpuBackend::open(Options::default());
    let Ok(record) = shared.start() else {
        assert!(std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none());
        return None;
    };
    let backend = WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        wait_timeout: PATIENT,
        ..options
    });
    let context = backend.context().expect("the adapter opened a moment ago");
    Some((backend, context))
}

/// A dma-buf from the system heap, or `None` where there is none to open: the tests that need a
/// real dma-buf then have nothing to attach to.
fn heap_buffer() -> Option<DmaBuf> {
    let heap = DmaHeap::open("system").ok()?;
    Some(heap.allocate(BYTES).expect("a heap buffer"))
}

/// A value's buffer with `dmabuf` from outside under it, owned by `owner`.
fn value(
    base: &Storage,
    dmabuf: &DmaBuf,
    read_only: bool,
    owner: Arc<dyn Any + Send + Sync>,
    context: &Context,
) -> Storage {
    let own = dmabuf.try_clone().expect("an fd of its own");
    Storage::of_value(base, External::new(own, read_only, Some(owner), context))
}

fn run(shader: &Shader, storage: &Storage) -> Ticket {
    match shader.run(&[Bound::Storage(storage)]).expect("runs") {
        Run::Started(ticket) => ticket,
        Run::Busy(_) => panic!("the device had room"),
    }
}

fn signalled(dmabuf: &DmaBuf, access: Access) -> bool {
    let fence = dmabuf.as_ref().export_fence(access).expect("a fence");
    fence.is_signaled().expect("polls")
}

fn poll_until(what: &str, context: &Context, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENT;
    while !done() {
        assert!(Instant::now() < deadline, "{what} never happened");
        context.poll();
        std::thread::yield_now();
    }
}

#[test]
fn a_value_dropped_while_a_submit_that_reads_it_is_pending_holds_its_owner_until_it_is_done() {
    // The owner is held whether or not a fence is attached. Attaching is off here because a
    // software device's sync fd export waits for its queue to reach the signal, which the gate
    // holds back; a hardware driver hands the fd over at once.
    let Some((backend, context)) = private(Options {
        no_fence_attach: true,
        ..Options::default()
    }) else {
        return;
    };
    let shader = Shader::load(&backend, &BUMP).expect("loads");
    let base = Storage::new(&backend, BYTES).expect("a buffer");
    let dmabuf = DmaBuf::memfd("external", BYTES).expect("memfd");
    let gate = HostGate::new(&context.device, &context.queue, &context.submit).expect("a gate");
    gate.hold().expect("the gate holds the queue");
    let owner: Arc<dyn Any + Send + Sync> = Arc::new(());
    let kept: Weak<dyn Any + Send + Sync> = Arc::downgrade(&owner);
    let storage = value(&base, &dmabuf, true, owner, &context);
    let ticket = run(&shader, &storage);
    // The value goes while the submit that used it can't have finished.
    drop(storage);
    for _ in 0..8 {
        context.poll();
    }
    assert!(!ticket.is_done(), "the gate holds the submit");
    assert!(
        kept.upgrade().is_some(),
        "the owner outlives a pending submit"
    );
    gate.open().expect("opens");
    ticket.wait().expect("the submit finishes");
    poll_until("the owner being let go", &context, || {
        kept.upgrade().is_none()
    });
}

#[test]
fn every_submit_that_binds_an_outside_buffer_attaches_its_fence_to_it() {
    let Some((backend, _context)) = private(Options::default()) else {
        return;
    };
    let Some(read) = heap_buffer() else {
        return;
    };
    let write = heap_buffer().expect("a second heap buffer");
    let shader = Shader::load(&backend, &BUMP).expect("loads");
    let base = Storage::new(&backend, BYTES).expect("a buffer");
    let owner: Arc<dyn Any + Send + Sync> = Arc::new(());
    let reads = value(
        &base,
        &read,
        true,
        Arc::clone(&owner),
        &backend.context().expect("open"),
    );
    let writes = value(
        &base,
        &write,
        false,
        owner,
        &backend.context().expect("open"),
    );
    run(&shader, &reads).wait().expect("finishes");
    run(&shader, &writes).wait().expect("finishes");
    let stats = backend.sync_stats().expect("a device is open");
    assert_eq!((stats.attaches, stats.buffers, stats.host_waits), (2, 2, 0));
    // Both buffers' fences signal once the work is done.
    for dmabuf in [&read, &write] {
        let fence = dmabuf
            .as_ref()
            .export_fence(Access::Write)
            .expect("a fence");
        assert!(fence.wait(PATIENT).expect("polls"));
    }
}

#[test]
fn a_value_whose_fd_only_allows_reading_is_attached_as_a_read() {
    let Some((_backend, context)) = private(Options::default()) else {
        return;
    };
    let memfd = || DmaBuf::memfd("external", BYTES).expect("memfd");
    assert_eq!(
        External::new(memfd(), true, None, &context).access,
        Access::Read
    );
    assert_eq!(
        External::new(memfd(), false, None, &context).access,
        Access::Write
    );
}

#[test]
fn a_submit_that_binds_no_outside_buffer_attaches_nothing() {
    let Some((backend, _context)) = private(Options::default()) else {
        return;
    };
    let shader = Shader::load(&backend, &BUMP).expect("loads");
    let plain = Storage::new(&backend, BYTES).expect("a buffer");
    run(&shader, &plain).wait().expect("finishes");
    let stats = backend.sync_stats().expect("a device is open");
    assert_eq!(stats, crate::SyncStats::default());
}

#[test]
fn a_fence_that_cant_be_attached_makes_the_host_wait_and_says_why() {
    let Some((backend, context)) = private(Options::default()) else {
        return;
    };
    let shader = Shader::load(&backend, &BUMP).expect("loads");
    let base = Storage::new(&backend, BYTES).expect("a buffer");
    // A memfd is no dma-buf: the kernel refuses the fence, so the host waits for the device.
    let memfd = DmaBuf::memfd("not-a-dma-buf", BYTES).expect("memfd");
    let storage = value(&base, &memfd, false, Arc::new(()), &context);
    let ticket = run(&shader, &storage);
    let stats = backend.sync_stats().expect("a device is open");
    assert_eq!((stats.attaches, stats.host_waits), (0, 1));
    let why = stats.host_wait_reason.expect("the wait is named");
    assert!(why.contains("refused the fence"), "{why}");
    // The host waited for the device before the run gave its ticket.
    assert!(ticket.is_done());
}

#[test]
fn a_host_that_turns_attaching_off_gets_no_fence_and_a_count_of_it() {
    let Some((backend, context)) = private(Options {
        no_fence_attach: true,
        ..Options::default()
    }) else {
        return;
    };
    let Some(dmabuf) = heap_buffer() else {
        return;
    };
    let shader = Shader::load(&backend, &BUMP).expect("loads");
    let base = Storage::new(&backend, BYTES).expect("a buffer");
    let gate = HostGate::new(&context.device, &context.queue, &context.submit).expect("a gate");
    gate.hold().expect("the gate holds the queue");
    let storage = value(&base, &dmabuf, false, Arc::new(()), &context);
    let ticket = run(&shader, &storage);
    // Nothing was attached, so the buffer looks idle while the device still has it.
    assert!(signalled(&dmabuf, Access::Write));
    let stats = backend.sync_stats().expect("a device is open");
    assert_eq!((stats.attaches, stats.skipped), (0, 1));
    gate.open().expect("opens");
    ticket.wait().expect("finishes");
}
