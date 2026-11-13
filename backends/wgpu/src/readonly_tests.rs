//! A dma-buf fd that only allows reading, imported and read on every adapter.
//!
//! A producer may hand readers a read-only descriptor of a buffer it keeps writing, so a reader
//! can't map it and write the bytes. What a device does with such an fd is its driver's: this
//! records it for each adapter, and checks that the host's own reads work and that a refusal
//! is only an error.
#![allow(clippy::print_stderr)] // what each adapter did is this test's record

use std::fs::OpenOptions;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;

use daedalus_dma_sys::{Access, DmaBufRef, DmaHeap};

use crate::context::Context;
use crate::{Options, Storage};

const BYTES: usize = 4096;

fn pattern() -> Vec<u8> {
    (0..BYTES).map(|at| (at * 7 + 3) as u8).collect()
}

/// The access mode the kernel has for an fd of this process: 0 is read-only, 2 is read-write.
fn access_mode(fd: &OwnedFd) -> Option<u32> {
    let info = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", fd.as_raw_fd())).ok()?;
    let flags = info.lines().find_map(|line| line.strip_prefix("flags:"))?;
    Some(u32::from_str_radix(flags.trim(), 8).ok()? & 3)
}

/// Opening a dma-buf's entry in `/proc/self/fd` read-only, which the kernel refuses.
fn reopened_read_only(fd: &OwnedFd) -> std::io::Result<OwnedFd> {
    let path = format!("/proc/self/fd/{}", fd.as_raw_fd());
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(0o2000000) // O_CLOEXEC
        .open(path)?;
    Ok(OwnedFd::from(file))
}

#[test]
fn a_read_only_dma_buf_is_read_by_the_host_and_imported_by_each_device() {
    let Ok(heap) = DmaHeap::open("system") else {
        assert!(
            std::env::var_os("DAEDALUS_DMA_HEAP_REQUIRED").is_none(),
            "this lane needs /dev/dma_heap/system"
        );
        return;
    };
    for index in 0..8 {
        let Ok(context) = Context::open_own(&Options {
            index: Some(index),
            ..Options::default()
        }) else {
            break;
        };
        let name = context.record.adapter.name.clone();
        let need = daedalus_vk_sys::memory::dma_buf_bytes_needed(&context.device, BYTES as u64)
            .unwrap_or(BYTES as u64);
        // A writable fd can't be turned into a read-only one from here: the kernel refuses to
        // open a dma-buf's `/proc/self/fd` entry again. Only the buffer's exporter can make one.
        let writable = heap
            .allocate(need.max(BYTES as u64))
            .expect("a buffer")
            .into_fd();
        assert_eq!(
            access_mode(&writable),
            Some(2),
            "the heap's own fd is read-write"
        );
        let reopened = reopened_read_only(&writable);
        assert_eq!(
            reopened.err().and_then(|error| error.raw_os_error()),
            Some(6),
            "ENXIO: a dma-buf can't be opened again"
        );
        drop(writable);

        // The heap gives a read-only fd when it is asked for one at allocation.
        let buf = heap
            .allocate_read_only(need.max(BYTES as u64))
            .expect("a read-only buffer");
        let only_read = buf.into_fd();
        assert_eq!(access_mode(&only_read), Some(0), "it only allows reading");
        let view = DmaBufRef::from_fd(only_read.as_fd());
        // A reader can't map it for writing, and can map it for reading and sync its reads.
        assert!(view.map().is_err(), "a writable mapping of a read-only fd");
        let map = Arc::new(view.map_read_only().expect("a read-only mapping"));
        view.begin_cpu_access(Access::Read).expect("a read sync");
        let mut seen = vec![1; BYTES];
        map.read(0, &mut seen).expect("in range");
        view.end_cpu_access(Access::Read)
            .expect("the read sync ends");
        assert_eq!(seen, vec![0; BYTES], "{name}: a new buffer is zeroed");
        let host_view = only_read
            .try_clone()
            .expect("a second fd, for the host's reads");

        // The device imports it, where it imports dma-bufs at all, and reads the same bytes.
        let imported = crate::dmabuf_value::import_bytes(&context, only_read, 0, BYTES as u64);
        match imported {
            Ok(storage) => {
                assert!(
                    context.record.dma_read_only,
                    "{name}: imported, so the record must say it takes a read-only fd"
                );
                let storage: Arc<Storage> = storage;
                let mut back = vec![1; BYTES];
                storage.read_into(0, &mut back).expect("a readback");
                assert_eq!(back, vec![0; BYTES], "{name}: the device reads the buffer");
                // The fd being read-only stops a host mapping from writing. It doesn't stop a
                // device that imported it: what the device writes, the host reads.
                storage.write(0, &pattern()).expect("the device writes");
                storage.read_into(0, &mut back).expect("a readback");
                assert_eq!(back, pattern(), "{name}: the device reads what it wrote");
                let view = DmaBufRef::from_fd(host_view.as_fd());
                view.begin_cpu_access(Access::Read).expect("a read sync");
                map.read(0, &mut seen).expect("in range");
                view.end_cpu_access(Access::Read)
                    .expect("the read sync ends");
                let host_sees = if seen == pattern() {
                    "the host sees it"
                } else {
                    "the host doesn't see it"
                };
                eprintln!(
                    "read-only dma-buf on {name}: imported, read back, device write: {host_sees}"
                );
            }
            // A refusal is an error and nothing more. A device that imports a writable fd may
            // still refuse a read-only one: a software renderer maps the buffer for writing.
            Err(error) => {
                assert!(
                    !context.record.dma_read_only,
                    "{name}: the record says it takes a read-only fd, and refused one: {error}"
                );
                let writable = if context.record.dma_heap {
                    "though it imports a writable one"
                } else {
                    "as a writable one isn't"
                };
                eprintln!("read-only dma-buf on {name}: not imported ({error}), {writable}");
            }
        }
    }
}
