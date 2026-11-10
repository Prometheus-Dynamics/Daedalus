//! Against the running kernel. The memfd tests run anywhere Linux does. The dma-heap tests need
//! `/dev/dma_heap/system` and say why they skipped when it isn't there.

#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)]

use std::fs::File;
use std::io::Write;
use std::os::fd::{AsFd, OwnedFd};
use std::time::{Duration, Instant};

use daedalus_dma_sys::{
    Access, DmaBuf, DmaBufRef, DmaHeap, Error, Identity, SyncFence, is_sync_file,
};

/// A file with no name, which maps like a dma-buf does.
fn memfd(len: usize) -> OwnedFd {
    let mut file = tempfile();
    file.write_all(&vec![0; len]).expect("sized");
    OwnedFd::from(file)
}

fn tempfile() -> File {
    let path = std::env::temp_dir().join(format!(
        "dma-sys-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .expect("a scratch file");
    std::fs::remove_file(&path).expect("unlinked");
    file
}

#[test]
fn mmap_read_write_and_identity_on_a_plain_fd() {
    let buf = DmaBuf::from_fd(memfd(8192));
    let view = buf.as_ref();
    assert_eq!(view.len(), Ok(8192));
    assert_eq!(view.is_empty(), Ok(false));
    assert_eq!(view.len(), Ok(8192), "the position was put back");

    let map = view.map().expect("a mapping");
    assert_eq!((map.len(), map.is_empty()), (8192, false));
    map.write(4000, b"through the mapping").expect("in range");
    let mut seen = [0; 19];
    map.read(4000, &mut seen).expect("in range");
    assert_eq!(&seen, b"through the mapping");
    assert_eq!(map.write(8190, &[0; 4]), Err(Error::Range));
    assert_eq!(map.read(u64::MAX, &mut seen), Err(Error::Range));

    // A second mapping of the same buffer sees the bytes, and a read-only one refuses writes.
    let other = view.map_read_only().expect("a mapping");
    other.read(4000, &mut seen).expect("in range");
    assert_eq!(&seen, b"through the mapping");
    assert_eq!(other.write(0, &[1]), Err(Error::Os(libc_eacces())));

    // Two fds of one buffer are the same buffer, and another file isn't.
    let again = buf.try_clone().expect("a second fd");
    let borrowed = DmaBufRef::from_fd(again.as_fd());
    assert_eq!(borrowed.identity(), view.identity());
    assert_eq!(Identity::of(buf.as_fd()), view.identity());
    let different = DmaBuf::from_fd(memfd(16));
    assert_ne!(different.as_ref().identity(), view.identity());
    drop(buf.into_fd());
}

fn libc_eacces() -> i32 {
    13
}

#[test]
fn a_pipe_is_not_a_fence_and_nothing_waits_on_it() {
    let (read, write) = std::io::pipe().expect("a pipe");
    let started = Instant::now();
    assert!(!is_sync_file(read.as_fd()));
    let refused = SyncFence::from_fd(OwnedFd::from(read));
    assert_eq!(refused.err(), Some(Error::NotSyncFile));
    assert!(!is_sync_file(write.as_fd()));
    assert!(!is_sync_file(memfd(16).as_fd()));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the check never blocks"
    );

    // A plain file has no fences to export or import either.
    let plain = DmaBuf::from_fd(memfd(4096));
    assert!(plain.as_ref().export_fence(Access::Read).is_err());
    assert!(plain.as_ref().begin_cpu_access(Access::Read).is_err());
    assert!(Error::Os(25).is_unsupported(), "ENOTTY");
    assert!(!Error::NotSyncFile.is_unsupported());
}

fn heap() -> Option<DmaHeap> {
    match DmaHeap::open("system") {
        Ok(heap) => Some(heap),
        Err(error) => {
            eprintln!("skipped: /dev/dma_heap/system: {error}");
            None
        }
    }
}

#[test]
fn a_heap_buffer_allocates_maps_and_syncs() {
    let Some(heap) = heap() else { return };
    assert_eq!(heap.allocate(0).err(), Some(Error::Range));
    let buf = heap.allocate(1 << 20).expect("a megabyte");
    let view = buf.as_ref();
    assert!(view.len().expect("a size") >= 1 << 20);
    let map = view.map().expect("a mapping");
    let pattern: Vec<u8> = (0..4096u32).map(|at| (at * 31) as u8).collect();
    view.begin_cpu_access(Access::Write).expect("begin");
    map.write(4096, &pattern).expect("in range");
    view.end_cpu_access(Access::Write).expect("end");
    let mut seen = vec![0; 4096];
    view.begin_cpu_access(Access::Read).expect("begin");
    map.read(4096, &mut seen).expect("in range");
    view.end_cpu_access(Access::Read).expect("end");
    assert_eq!(seen, pattern);
    assert_ne!(
        heap.allocate(4096).expect("another").as_ref().identity(),
        view.identity()
    );
    assert!(DmaHeap::open("no-such-heap").is_err());
}

#[test]
fn an_idle_buffer_exports_a_fence_that_has_signaled() {
    let Some(heap) = heap() else { return };
    let buf = heap.allocate(4096).expect("a page");
    let fence = match buf.as_ref().export_fence(Access::ReadWrite) {
        Ok(fence) => fence,
        Err(error) if error.is_unsupported() => {
            eprintln!("skipped: this kernel has no fence export ({error})");
            return;
        }
        Err(error) => panic!("export: {error}"),
    };
    assert!(is_sync_file(fence.as_fd()));
    assert_eq!(
        fence.is_signaled(),
        Ok(true),
        "nothing is pending on a new buffer"
    );
    assert_eq!(fence.wait(Duration::from_millis(100)), Ok(true));
    // A fence goes back on a buffer, and round-trips through its fd.
    buf.as_ref()
        .import_fence(&fence, Access::Write)
        .expect("import");
    let again = SyncFence::from_fd(fence.into_fd()).expect("still a sync_file");
    assert_eq!(again.is_signaled(), Ok(true));
}
