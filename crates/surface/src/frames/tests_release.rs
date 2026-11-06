//! A frame's dma-bufs are released as soon as the graph and the caller are done with it: after
//! the tick that consumed it, or at the take's release, never held for the next push.
//!
//! Each check counts the references this process has to the test's dma-buf (descriptors in
//! `/proc/self/fd` and mappings in `/proc/self/maps` of its inode) against the count before any
//! frame: the surface's frame objects are recycled, but a recycled object holds no buffer.

use std::ffi::c_void;
use std::os::unix::fs::MetadataExt;
use std::ptr;

use lemnos_abi::{Handle, MessageSink, Status};

use super::tests::{
    FRAME_LEN, consumer, filled, graph_with, lease_data, owned_planes, push, r8_desc, release,
    serial, system_heap, take,
};
use crate::frames::daedalus_surface_frame_release;
use crate::io::{daedalus_surface_take_i64, daedalus_surface_tick};
use crate::state::lock;

const ROUNDS: usize = 40;

/// This process's references to the dma-buf `(device, inode)`: open descriptors and mappings.
fn references((device, inode): (u64, u64)) -> (usize, usize) {
    let fds = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|entry| std::fs::metadata(entry.ok()?.path()).ok())
        .filter(|meta| meta.dev() == device && meta.ino() == inode)
        .count();
    let maps = std::fs::read_to_string("/proc/self/maps")
        .unwrap()
        .lines()
        .filter(|line| {
            let mut fields = line.split_whitespace().skip(4);
            fields.next() == Some(&inode.to_string()) && line.contains("dmabuf")
        })
        .count();
    (fds, maps)
}

fn tick(graph: Handle) {
    // Safety: the test's live graph handle.
    assert_eq!(
        unsafe { daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::OK
    );
}

/// Releases `lease` after closing the descriptors the take handed out in `desc`.
fn done(lease: Handle, desc: &lemnos_abi::BufferDesc) {
    drop(owned_planes(desc));
    // Safety: the test's own lease.
    assert_eq!(unsafe { daedalus_surface_frame_release(lease) }, Status::OK);
}

/// Frames the graph consumes (`frame_info` reads their bytes and outputs where): the buffer is
/// back to its own references after every tick, mapping included.
#[test]
fn a_consumed_frame_lets_go_of_its_dma_buf_at_the_tick() {
    let _serial = serial();
    let Some(heap) = system_heap() else { return };
    let (host, graph) = graph_with(&consumer());
    let buf = filled(&heap, FRAME_LEN, 4);
    let identity = buf.identity().unwrap();
    let baseline = references(identity);
    for round in 0..ROUNDS {
        assert_eq!(push(graph, &r8_desc(&buf), ptr::null()), Status::OK);
        assert_ne!(
            references(identity),
            baseline,
            "round {round}: the queued frame holds the buffer"
        );
        tick(graph);
        let mut read_at = 0;
        let port = "ptr";
        // Safety: the port and out-pointer are valid for the call.
        let status =
            unsafe { daedalus_surface_take_i64(graph, port.as_ptr(), port.len(), &mut read_at) };
        assert_eq!(status, Status::OK);
        assert_ne!(
            read_at, 0,
            "round {round}: the node mapped and read the frame"
        );
        assert_eq!(
            references(identity),
            baseline,
            "round {round}: nothing holds the buffer once the tick consumed the frame"
        );
    }
    release(graph, host);
}

/// Frames the graph passes out: the take's lease holds the buffer until it is released, and
/// leases held over several pushes each let go at their own release.
#[test]
fn a_taken_frame_lets_go_of_its_dma_buf_at_the_release() {
    let _serial = serial();
    let Some(heap) = system_heap() else { return };
    let (host, graph) = graph_with(&super::tests::passthrough());
    let bufs = [filled(&heap, FRAME_LEN, 5), filled(&heap, FRAME_LEN, 6)];
    let identities = bufs.each_ref().map(|buf| buf.identity().unwrap());
    let baseline = identities.map(references);
    for round in 0..ROUNDS {
        // One lease is kept while the next frame goes through: two frames out at once.
        let mut leases = Vec::new();
        for buf in &bufs {
            assert_eq!(push(graph, &r8_desc(buf), ptr::null()), Status::OK);
            tick(graph);
            let (lease, desc, _) = take(graph).expect("the frame comes back");
            leases.push((lease, desc));
        }
        let data = leases
            .iter()
            .map(|(lease, _)| lease_data(*lease) as *const c_void);
        assert!(
            data.collect::<Vec<_>>()
                .windows(2)
                .all(|pair| pair[0] != pair[1]),
            "round {round}: two frames out at once are two objects"
        );
        for (i, (lease, desc)) in leases.into_iter().enumerate() {
            assert_ne!(references(identities[i]), baseline[i], "held by the lease");
            done(lease, &desc);
            assert_eq!(
                references(identities[i]),
                baseline[i],
                "round {round}: the release lets go of buffer {i}"
            );
        }
    }
    {
        // The port's recycled objects are empty: they hold no descriptor.
        let tables = lock();
        let pool = tables.graphs.get(graph).unwrap().frame_spare[0].as_ref();
        assert!(
            pool.is_some_and(|pool| pool.kept() == 2),
            "two objects kept and reused"
        );
    }
    assert_eq!(identities.map(references), baseline);
    release(graph, host);
}
