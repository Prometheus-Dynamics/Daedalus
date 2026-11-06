//! Board test: an imported acquire fence delays the frame's consumer until the GPU is done.
//!
//! A real `sync_file` comes from a long GPU job ([`GpuJob`], Vulkan, `SYNC_FD` export) and is
//! pushed with a dma-buf frame into `frame` -> `frame_info`, a node that reads the frame's bytes
//! (`plane_data`, so `DMA_BUF_IOCTL_SYNC` start). With the fence, the tick returns only once the
//! job has completed (its `sync_file` is signalled when the tick returns), and a poll of the
//! dma-buf (`DMA_BUF_IOCTL_EXPORT_SYNC_FILE` for a read) shows the write pending meanwhile.
//! Without it, the same tick returns while the job still runs.
//!
//! Ignored: it needs a Vulkan device that exports `SYNC_FD` and the kernel's dma-buf sync-file
//! ioctls. Run on Raze #1 (CM5, v3dv): `cargo test -p daedalus-surface --lib fence -- --ignored
//! --nocapture` (cross-built for the board; results in `docs/benchmarks-log.md`).

use std::os::fd::{AsFd, AsRawFd};
use std::time::{Duration, Instant};

use lemnos_abi::{FenceDesc, MessageSink, Status};
use lemnos_linux::dma::{Access, fence_is_signaled};

use super::gpu_job::GpuJob;
use super::tests::{
    FRAME_LEN, consumer, filled, graph_with, push, r8_desc, release, serial, system_heap,
};
use crate::io::{daedalus_surface_take_i64, daedalus_surface_tick};

/// The GPU job's length: long enough that a wait is plain against a tick's microseconds.
const JOB: Duration = Duration::from_millis(250);
const ROUNDS: usize = 5;

fn tick(graph: lemnos_abi::Handle) {
    // Safety: the test's live graph handle.
    assert_eq!(
        unsafe { daedalus_surface_tick(graph, MessageSink::NONE) },
        Status::OK
    );
}

/// The `ptr` output: where `frame_info` read the frame's bytes (non-zero once it mapped them).
fn read_at(graph: lemnos_abi::Handle) -> i64 {
    let port = "ptr";
    let mut value = 0;
    // Safety: the port and out-pointer are valid for the call.
    let status = unsafe { daedalus_surface_take_i64(graph, port.as_ptr(), port.len(), &mut value) };
    assert_eq!(status, Status::OK);
    value
}

#[test]
#[ignore = "board: needs a Vulkan device exporting SYNC_FD (Raze #1, v3dv)"]
#[allow(clippy::print_stderr)]
fn a_gpu_acquire_fence_delays_the_frame_reader_until_the_job_is_done() {
    let _serial = serial();
    let heap = system_heap().expect("the board has /dev/dma_heap/system");
    let mut gpu = GpuJob::new().expect("a Vulkan device exporting SYNC_FD");
    let job = gpu.calibrate(JOB).expect("the job runs");
    eprintln!("GPU: {}; job {:.1} ms", gpu.name, job.as_secs_f64() * 1e3);
    assert!(
        job >= JOB / 2,
        "the job is long enough to see a wait: {job:?}"
    );

    let (host, graph) = graph_with(&consumer());
    let buf = filled(&heap, FRAME_LEN, 9);
    let desc = r8_desc(&buf);
    // Warm: the graph has run once and mapped the frame.
    assert_eq!(push(graph, &desc, std::ptr::null()), Status::OK);
    tick(graph);
    assert_ne!(read_at(graph), 0, "frame_info read the frame's bytes");

    let (mut fenced, mut unfenced, mut jobs) = (Vec::new(), Vec::new(), Vec::new());
    for round in 0..ROUNDS {
        // With the fence: the reader waits for the job.
        let (sync, pending) = gpu.submit().expect("submit");
        assert!(
            !fence_is_signaled(sync.as_fd()).unwrap(),
            "round {round}: the job is still running at the push"
        );
        let fence = FenceDesc::sync_file(sync.as_raw_fd());
        let start = Instant::now();
        assert_eq!(push(graph, &desc, &fence), Status::OK, "round {round}");
        // The dma-buf now carries the job's fence: a reader polling the buffer sees it pending.
        let readers = buf
            .export_fence(Access::Read)
            .expect("export the read fence");
        assert!(
            !readers.is_signaled().unwrap(),
            "round {round}: the buffer's pending write is the job's"
        );
        tick(graph);
        let waited = start.elapsed();
        assert!(
            fence_is_signaled(sync.as_fd()).unwrap(),
            "round {round}: the tick returned before the GPU job completed ({waited:?})"
        );
        assert!(readers.is_signaled().unwrap());
        assert_ne!(read_at(graph), 0);
        jobs.push(gpu.finish(pending).expect("the job completes"));
        fenced.push(waited);

        // Without: the reader does not wait, and the job is still running when it is done.
        let (sync, pending) = gpu.submit().expect("submit");
        let start = Instant::now();
        assert_eq!(push(graph, &desc, std::ptr::null()), Status::OK);
        tick(graph);
        let took = start.elapsed();
        assert!(
            !fence_is_signaled(sync.as_fd()).unwrap(),
            "round {round}: with no fence the tick still waited ({took:?})"
        );
        assert_ne!(read_at(graph), 0);
        unfenced.push(took);
        jobs.push(gpu.finish(pending).expect("the job completes"));
    }
    release(graph, host);

    let ms = |all: &[Duration]| {
        let mut all: Vec<f64> = all.iter().map(|d| d.as_secs_f64() * 1e3).collect();
        all.sort_by(f64::total_cmp);
        (all[0], all[all.len() / 2], all[all.len() - 1])
    };
    let (f, u, j) = (ms(&fenced), ms(&unfenced), ms(&jobs));
    eprintln!(
        "fence wait over {ROUNDS} rounds (min/median/max ms): push+tick with the GPU's fence \
         {:.2}/{:.2}/{:.2}, without {:.3}/{:.3}/{:.3}; GPU job {:.1}/{:.1}/{:.1}",
        f.0, f.1, f.2, u.0, u.1, u.2, j.0, j.1, j.2
    );
}
