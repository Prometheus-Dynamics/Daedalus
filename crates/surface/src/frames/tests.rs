//! Frame calls on Linux, raw ABI: a round trip through a passthrough graph (its frame is the same
//! object coming back), descriptor ownership, shared-fd planes with an acquire fence, panic
//! cleanup and stale leases. Dma-buf work needs `/dev/dma_heap/system`; without it each test says
//! so and passes.

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd};
use std::ptr;
use std::sync::{Mutex, MutexGuard, PoisonError};

use daedalus::GraphDocument;
use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::transport::{FRAME_INTERFACE_KEY, fourcc};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use lemnos_abi::{BufferDesc, FenceDesc, Handle, MessageSink, PlaneDesc, Status};
use lemnos_linux::dma::{Access, DmaBuf, DmaBufRef, DmaHeap};

use super::dma::arm_panic;
use crate::alloc::thread_allocations;
use crate::frames::{
    daedalus_surface_frame_release, daedalus_surface_push_frame, daedalus_surface_take_frame,
};
use crate::graph::daedalus_surface_graph_compile;
use crate::host::{daedalus_surface_host_create, daedalus_surface_host_release};
use crate::io::daedalus_surface_tick;
use crate::state::lock;

const R8: u32 = fourcc(b"R8  ");
const SIDE: u32 = 256;
pub(super) const FRAME_LEN: usize = (SIDE * SIDE) as usize;

/// Serializes the tests: the fd counts are process-wide, so two tests opening descriptors at once
/// would skew each other's counts.
pub(super) fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The `stable_abi` plugin's `frame_pass` node, which hands a `SharedFrame` straight back out:
/// the graph `frame` -> `pass` -> `out`.
pub(super) fn passthrough() -> String {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    let frame = TypeExpr::opaque(FRAME_INTERFACE_KEY);
    let graph = registry
        .graph_builder()
        .unwrap()
        .node_id("stable_abi:frame_pass", "pass")
        .input_as("frame", frame.clone())
        .output_as("out", frame)
        .connect("frame", "pass.frame")
        .connect("pass.frame", "out")
        .build();
    GraphDocument::new(graph).to_json().unwrap()
}

/// The `stable_abi` plugin's `frame_info` node, which reads the frame's bytes and outputs where it
/// read them (`ptr`), not the frame: the graph `frame` -> `info`, its `ptr` -> `ptr`.
pub(super) fn consumer() -> String {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    let graph = registry
        .graph_builder()
        .unwrap()
        .node_id("stable_abi:frame_info", "info")
        .input_as("frame", TypeExpr::opaque(FRAME_INTERFACE_KEY))
        .output_as("ptr", TypeExpr::Scalar(ValueType::Int))
        .connect("frame", "info.frame")
        .connect("info.ptr", "ptr")
        .build();
    GraphDocument::new(graph).to_json().unwrap()
}

/// A host with the plugin installed, and a compiled passthrough graph.
fn graph() -> (Handle, Handle) {
    graph_with(&passthrough())
}

/// A host with the plugin installed, and `document` compiled on it.
pub(super) fn graph_with(document: &str) -> (Handle, Handle) {
    let mut host = Handle::NULL;
    // Safety: a valid out-pointer.
    assert_eq!(
        unsafe { daedalus_surface_host_create(&mut host) },
        Status::OK
    );
    {
        let mut tables = lock();
        let host_state = tables.hosts.get_mut(host).unwrap();
        host_state
            .registry
            .install_plugin(&StableAbiPlugin::new())
            .unwrap();
    }
    let mut graph = Handle::NULL;
    // Safety: the document and out-pointer are valid for the call.
    let status = unsafe {
        daedalus_surface_graph_compile(
            host,
            document.as_ptr(),
            document.len(),
            &mut graph,
            MessageSink::NONE,
        )
    };
    assert_eq!(status, Status::OK, "the graph compiles");
    (host, graph)
}

pub(super) fn release(graph: Handle, host: Handle) {
    // Safety: both are this test's live handles.
    unsafe {
        assert_eq!(
            crate::graph::daedalus_surface_graph_release(graph),
            Status::OK
        );
        assert_eq!(daedalus_surface_host_release(host), Status::OK);
    }
}

/// The system heap, or `None` (with a note on stderr) when this machine has none to use.
#[allow(clippy::print_stderr)]
pub(super) fn system_heap() -> Option<DmaHeap> {
    match DmaHeap::open("system") {
        Ok(heap) => Some(heap),
        Err(error) => {
            eprintln!("SKIP frame tests: /dev/dma_heap/system is not accessible: {error}");
            None
        }
    }
}

/// The number of open descriptors of this process.
pub(super) fn open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

/// A dma-buf of one frame, filled with `seed`-dependent bytes through a mapping.
pub(super) fn filled(heap: &DmaHeap, len: usize, seed: u8) -> DmaBuf {
    let buf = heap.allocate(len).unwrap();
    let mut map = buf.map().unwrap();
    buf.begin_cpu_access(Access::ReadWrite).unwrap();
    for (i, byte) in map.as_mut_slice().iter_mut().enumerate() {
        *byte = (i as u8).wrapping_mul(7).wrapping_add(seed);
    }
    buf.end_cpu_access(Access::ReadWrite).unwrap();
    buf
}

/// The bytes of `fd`, read through a mapping of its dma-buf (as a consumer would).
fn read_through_mapping(fd: BorrowedFd<'_>, len: usize) -> Vec<u8> {
    let buf = DmaBufRef::from_fd(fd).unwrap();
    let map = buf.map_read_only().unwrap();
    buf.begin_cpu_access(Access::Read).unwrap();
    let bytes = map.as_slice()[..len].to_vec();
    buf.end_cpu_access(Access::Read).unwrap();
    bytes
}

pub(super) fn identity(fd: BorrowedFd<'_>) -> (u64, u64) {
    DmaBufRef::from_fd(fd).unwrap().identity().unwrap()
}

/// A one-plane R8 frame over `buf`.
pub(super) fn r8_desc(buf: &DmaBuf) -> BufferDesc {
    BufferDesc::image(SIDE, SIDE, R8, 0)
        .with_plane(PlaneDesc {
            fd: buf.as_fd().as_raw_fd(),
            reserved: 0,
            offset: 0,
            stride: u64::from(SIDE),
            size: FRAME_LEN as u64,
        })
        .unwrap()
}

/// Pushes `desc` (with `fence`) into the graph's `frame` input.
pub(super) fn push(graph: Handle, desc: &BufferDesc, fence: *const FenceDesc) -> Status {
    let port = "frame";
    // Safety: the port and descriptors are valid for the call; the descriptor is borrowed.
    unsafe { daedalus_surface_push_frame(graph, port.as_ptr(), port.len(), desc, fence) }
}

/// Takes the graph's `out` frame: the lease, its descriptor and its fence.
pub(super) fn take(graph: Handle) -> Result<(Handle, BufferDesc, FenceDesc), Status> {
    let port = "out";
    let mut desc = BufferDesc::image(0, 0, 0, 0);
    let mut fence = FenceDesc::none();
    let mut lease = Handle::NULL;
    // Safety: the port and out-pointers are valid for the call.
    let status = unsafe {
        daedalus_surface_take_frame(
            graph,
            port.as_ptr(),
            port.len(),
            &mut desc,
            &mut fence,
            &mut lease,
        )
    };
    if status == Status::OK {
        Ok((lease, desc, fence))
    } else {
        Err(status)
    }
}

/// The owned plane descriptors of a taken frame, as `OwnedFd`s the test closes.
pub(super) fn owned_planes(desc: &BufferDesc) -> Vec<OwnedFd> {
    desc.planes()
        .iter()
        // Safety: a taken frame's descriptors are the caller's, duplicated for it.
        .map(|plane| unsafe { OwnedFd::from_raw_fd(plane.fd) })
        .collect()
}

/// The data pointer of the frame the lease holds (its `handle().data()`).
pub(super) fn lease_data(lease: Handle) -> usize {
    let tables = lock();
    tables.frames.get(lease).unwrap().handle().data() as usize
}

/// Whether `data` is one of the frame objects of input `index` (a pushed frame's identity).
fn pooled(graph: Handle, index: usize, data: usize) -> bool {
    let tables = lock();
    let graph = tables.graphs.get(graph).unwrap();
    graph.frame_spare[index]
        .as_ref()
        .is_some_and(|pool| pool.holds(data as *const std::ffi::c_void))
}

#[test]
fn a_frame_round_trips_with_its_pixels_its_dma_buf_and_its_identity() {
    let _serial = serial();
    let Some(heap) = system_heap() else { return };
    let (host, graph) = graph();
    let buf = filled(&heap, FRAME_LEN, 3);
    let source_identity = buf.identity().unwrap();
    // The port keeps one frame object (and so one dup of the descriptor) once it has pushed.
    let warm = r8_desc(&buf);
    assert_eq!(push(graph, &warm, ptr::null()), Status::OK);
    assert_eq!(
        unsafe { daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::OK
    );
    let (lease, _, _) = take(graph).unwrap();
    assert_eq!(unsafe { daedalus_surface_frame_release(lease) }, Status::OK);
    let before = open_fds();
    let mut allocations = 0;

    for round in 0..50 {
        let desc = r8_desc(&buf);
        let counted = thread_allocations();
        assert_eq!(push(graph, &desc, ptr::null()), Status::OK, "push {round}");
        // Safety: the handle is the surface's.
        assert_eq!(
            unsafe { daedalus_surface_tick(graph, MessageSink::NONE) },
            Status::OK
        );
        let (lease, out, fence) = take(graph).expect("the frame comes back");
        allocations += thread_allocations() - counted;
        assert_eq!(fence.kind, FenceDesc::NONE, "a take hands out no fence");
        assert_eq!(out.flags & BufferDesc::OWNED, BufferDesc::OWNED);
        assert_eq!((out.width, out.height, out.fourcc), (SIDE, SIDE, R8));
        assert_eq!(out.plane_count, 1);
        let owned = owned_planes(&out);
        let plane = owned[0].as_fd();
        // The same dma-buf: a duplicate, not a copy.
        assert_eq!(
            identity(plane),
            source_identity,
            "round {round}: same inode and device"
        );
        assert_eq!(
            read_through_mapping(plane, FRAME_LEN),
            read_through_mapping(buf.as_fd(), FRAME_LEN),
            "round {round}: the pixels are the same bytes"
        );
        assert!(
            pooled(graph, 0, lease_data(lease)),
            "round {round}: the frame keeps its identity through the graph"
        );
        drop(owned);
        let counted = thread_allocations();
        assert_eq!(unsafe { daedalus_surface_frame_release(lease) }, Status::OK);
        allocations += thread_allocations() - counted;
    }
    assert_eq!(
        open_fds(),
        before,
        "no descriptor is left open after 50 frames"
    );
    assert_eq!(
        allocations, 0,
        "a warm frame (push, tick, take, release) allocates nothing"
    );

    release(graph, host);
}

#[test]
fn a_borrowed_descriptor_stays_the_callers_and_an_owned_one_is_taken() {
    let _serial = serial();
    let Some(heap) = system_heap() else { return };
    let (host, graph) = graph();
    let buf = filled(&heap, FRAME_LEN, 9);

    // Borrowed: the caller's descriptor is still open after the push.
    let desc = r8_desc(&buf);
    assert_eq!(push(graph, &desc, ptr::null()), Status::OK);
    assert_eq!(identity(buf.as_fd()), buf.identity().unwrap());

    // Owned: the descriptor is the surface's now. One dup exists before the push and the same
    // one after it: the surface took it over rather than dup'ing it.
    let raw = buf.as_fd().try_clone_to_owned().unwrap().into_raw_fd();
    let mut owned = r8_desc(&buf);
    owned.flags = BufferDesc::OWNED;
    owned.planes[0].fd = raw;
    let before = open_fds();
    assert_eq!(push(graph, &owned, ptr::null()), Status::OK);
    // The port's previous frame (the borrowed push's dup) is released by this push, so the count
    // may drop, but the owned descriptor is taken over, never duplicated.
    assert!(
        open_fds() <= before,
        "the owned descriptor was taken, not duplicated"
    );
    // Tick and take the frame so the graph lets go of its input.
    assert_eq!(
        unsafe { daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::OK
    );
    let (lease, _, _) = take(graph).unwrap();
    assert_eq!(unsafe { daedalus_surface_frame_release(lease) }, Status::OK);

    release(graph, host);
}

#[test]
fn owned_descriptors_are_closed_when_the_call_fails() {
    let _serial = serial();
    let Some(heap) = system_heap() else { return };
    let (host, graph) = graph();
    let buf = filled(&heap, FRAME_LEN, 5);
    let before = open_fds();

    // A descriptor that does not validate (a struct too small to read) is closed regardless.
    let raw = buf.as_fd().try_clone_to_owned().unwrap().into_raw_fd();
    let mut bad = r8_desc(&buf);
    bad.flags = BufferDesc::OWNED;
    bad.planes[0].fd = raw;
    bad.struct_size = 1;
    assert_eq!(push(graph, &bad, ptr::null()), Status::INVALID_ARGUMENT);
    assert_eq!(open_fds(), before, "a refused owned descriptor is closed");

    // A port that is not a frame port, and an unknown port, close owned descriptors too.
    let raw = buf.as_fd().try_clone_to_owned().unwrap().into_raw_fd();
    let mut wrong_port = r8_desc(&buf);
    wrong_port.flags = BufferDesc::OWNED;
    wrong_port.planes[0].fd = raw;
    let port = "nope";
    // Safety: the port and descriptor are valid for the call.
    let status = unsafe {
        daedalus_surface_push_frame(graph, port.as_ptr(), port.len(), &wrong_port, ptr::null())
    };
    assert_eq!(status, Status::NOT_FOUND);
    assert_eq!(open_fds(), before);

    release(graph, host);
}

#[test]
fn shared_fd_planes_and_an_acquire_fence_are_carried() {
    let _serial = serial();
    let Some(heap) = system_heap() else { return };
    let (host, graph) = graph();
    // NV12-like: a luma plane and a half-height chroma plane, both in one dma-buf.
    let len = FRAME_LEN + FRAME_LEN / 2;
    let buf = filled(&heap, len, 1);
    let source = buf.identity().unwrap();
    let fd = buf.as_fd().as_raw_fd();
    let desc = BufferDesc::image(SIDE, SIDE, fourcc(b"NV12"), 0)
        .with_plane(PlaneDesc {
            fd,
            reserved: 0,
            offset: 0,
            stride: u64::from(SIDE),
            size: FRAME_LEN as u64,
        })
        .unwrap()
        .with_plane(PlaneDesc {
            fd,
            reserved: 0,
            offset: FRAME_LEN as u64,
            stride: u64::from(SIDE),
            size: (FRAME_LEN / 2) as u64,
        })
        .unwrap();
    // An acquire fence from the buffer's own work (idle, so already signalled).
    let fence = buf.export_fence(Access::Write).unwrap();
    let fence_desc = FenceDesc::sync_file(fence.as_fd().as_raw_fd());
    assert_eq!(push(graph, &desc, &fence_desc), Status::OK);
    assert_eq!(
        unsafe { daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::OK
    );

    let (lease, out, _) = take(graph).unwrap();
    assert_eq!(out.plane_count, 2);
    let owned = owned_planes(&out);
    assert_ne!(
        owned[0].as_raw_fd(),
        owned[1].as_raw_fd(),
        "each plane has its own descriptor"
    );
    for plane in &owned {
        assert_eq!(
            identity(plane.as_fd()),
            source,
            "both planes are the same dma-buf"
        );
    }
    assert_eq!(out.planes()[1].offset, FRAME_LEN as u64);
    drop(owned);
    assert_eq!(unsafe { daedalus_surface_frame_release(lease) }, Status::OK);

    // A syncobj fence is not carried.
    let syncobj = FenceDesc::syncobj(fence.as_fd().as_raw_fd(), 1);
    assert_eq!(push(graph, &r8_desc(&buf), &syncobj), Status::NOT_SUPPORTED);

    release(graph, host);
}

#[test]
fn a_panic_in_push_or_take_returns_a_status_and_leaks_nothing() {
    let _serial = serial();
    let Some(heap) = system_heap() else { return };
    let (host, graph) = graph();
    let buf = filled(&heap, FRAME_LEN, 7);

    // A push that panics after its owned descriptors are taken.
    let raw = buf.as_fd().try_clone_to_owned().unwrap().into_raw_fd();
    let mut owned = r8_desc(&buf);
    owned.flags = BufferDesc::OWNED;
    owned.planes[0].fd = raw;
    let before_push = open_fds();
    arm_panic();
    assert_eq!(push(graph, &owned, ptr::null()), Status::PANIC);
    assert_eq!(
        open_fds(),
        before_push - 1,
        "the panicking push closed its descriptor"
    );

    // The graph still works after the panic.
    assert_eq!(push(graph, &r8_desc(&buf), ptr::null()), Status::OK);
    assert_eq!(
        unsafe { daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::OK
    );
    let before = open_fds();

    // A take that panics after its descriptors are duplicated: the dups are closed with it.
    arm_panic();
    let mut out_desc = BufferDesc::image(0, 0, 0, 0);
    let mut out_fence = FenceDesc::none();
    let mut lease = Handle::NULL;
    let port = "out";
    // Safety: the port and out-pointers are valid for the call.
    let status = unsafe {
        daedalus_surface_take_frame(
            graph,
            port.as_ptr(),
            port.len(),
            &mut out_desc,
            &mut out_fence,
            &mut lease,
        )
    };
    assert_eq!(status, Status::PANIC);
    // The dups are closed, and so is the frame's own descriptor: the panic dropped the frame,
    // and nothing else held its buffer.
    assert_eq!(
        open_fds(),
        before - 1,
        "the panicking take leaks no descriptor"
    );

    release(graph, host);
}

#[test]
fn stale_leases_and_null_pointers_are_refused() {
    let _serial = serial();
    let Some(heap) = system_heap() else { return };
    let (host, graph) = graph();
    let buf = filled(&heap, FRAME_LEN, 2);

    assert_eq!(push(graph, &r8_desc(&buf), ptr::null()), Status::OK);
    assert_eq!(
        unsafe { daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::OK
    );
    let (lease, out, _) = take(graph).unwrap();
    drop(owned_planes(&out));
    assert_eq!(unsafe { daedalus_surface_frame_release(lease) }, Status::OK);
    assert_eq!(
        unsafe { daedalus_surface_frame_release(lease) },
        Status::STALE_HANDLE,
        "a released lease is stale"
    );
    assert_eq!(
        unsafe { daedalus_surface_frame_release(Handle::NULL) },
        Status::STALE_HANDLE
    );

    // Null descriptors are refused before anything is taken or pushed.
    let port = "frame";
    assert_eq!(
        unsafe {
            daedalus_surface_push_frame(graph, port.as_ptr(), port.len(), ptr::null(), ptr::null())
        },
        Status::INVALID_ARGUMENT
    );
    let mut lease = Handle::NULL;
    let mut fence = FenceDesc::none();
    assert_eq!(
        unsafe {
            daedalus_surface_take_frame(
                graph,
                "out".as_ptr(),
                3,
                ptr::null_mut(),
                &mut fence,
                &mut lease,
            )
        },
        Status::INVALID_ARGUMENT
    );

    release(graph, host);
}
