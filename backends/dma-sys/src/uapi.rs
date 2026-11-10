//! The kernel's numbers and argument structs, from `linux/dma-buf.h`, `linux/dma-heap.h` and
//! `linux/sync_file.h`. A test checks the numbers against those headers.

/// `struct dma_heap_allocation_data`.
#[repr(C)]
#[derive(Default)]
pub(crate) struct HeapAllocation {
    pub(crate) len: u64,
    pub(crate) fd: u32,
    pub(crate) fd_flags: u32,
    pub(crate) heap_flags: u64,
}

/// `struct dma_buf_sync`.
#[repr(C)]
pub(crate) struct BufSync {
    pub(crate) flags: u64,
}

/// `struct dma_buf_export_sync_file` and `struct dma_buf_import_sync_file`, which are alike.
#[repr(C)]
pub(crate) struct SyncFileArg {
    pub(crate) flags: u32,
    pub(crate) fd: i32,
}

/// `struct sync_file_info`.
#[repr(C)]
#[derive(Default)]
pub(crate) struct SyncFileInfo {
    pub(crate) name: [u8; 32],
    pub(crate) status: i32,
    pub(crate) flags: u32,
    pub(crate) num_fences: u32,
    pub(crate) pad: u32,
    pub(crate) sync_fence_info: u64,
}

pub(crate) const DMA_BUF_SYNC_READ: u64 = 1;
pub(crate) const DMA_BUF_SYNC_WRITE: u64 = 2;
pub(crate) const DMA_BUF_SYNC_END: u64 = 4;

const WRITE: u64 = 1;
const READ: u64 = 2;

/// The kernel's `_IOC` for the common layout: 8 bits of number, 8 of type, 14 of size, 2 of
/// direction.
const fn ioc(direction: u64, kind: u8, number: u64, size: usize) -> u64 {
    (direction << 30) | ((size as u64) << 16) | ((kind as u64) << 8) | number
}

pub(crate) const DMA_HEAP_IOCTL_ALLOC: u64 =
    ioc(READ | WRITE, b'H', 0, size_of::<HeapAllocation>());
pub(crate) const DMA_BUF_IOCTL_SYNC: u64 = ioc(WRITE, b'b', 0, size_of::<BufSync>());
pub(crate) const DMA_BUF_IOCTL_EXPORT_SYNC_FILE: u64 =
    ioc(READ | WRITE, b'b', 2, size_of::<SyncFileArg>());
pub(crate) const DMA_BUF_IOCTL_IMPORT_SYNC_FILE: u64 =
    ioc(WRITE, b'b', 3, size_of::<SyncFileArg>());
pub(crate) const SYNC_IOC_FILE_INFO: u64 = ioc(READ | WRITE, b'>', 4, size_of::<SyncFileInfo>());

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    const PROGRAM: &str = r#"
#include <stdio.h>
#include <linux/dma-buf.h>
#include <linux/dma-heap.h>
#include <linux/sync_file.h>
int main(void) {
    printf("%lu %lu %lu %lu %lu %lu %lu %lu\n",
        (unsigned long)DMA_HEAP_IOCTL_ALLOC, (unsigned long)DMA_BUF_IOCTL_SYNC,
        (unsigned long)DMA_BUF_IOCTL_EXPORT_SYNC_FILE, (unsigned long)DMA_BUF_IOCTL_IMPORT_SYNC_FILE,
        (unsigned long)SYNC_IOC_FILE_INFO, (unsigned long)DMA_BUF_SYNC_READ,
        (unsigned long)DMA_BUF_SYNC_WRITE, (unsigned long)DMA_BUF_SYNC_END);
    return 0;
}
"#;

    const OURS: [u64; 8] = [
        DMA_HEAP_IOCTL_ALLOC,
        DMA_BUF_IOCTL_SYNC,
        DMA_BUF_IOCTL_EXPORT_SYNC_FILE,
        DMA_BUF_IOCTL_IMPORT_SYNC_FILE,
        SYNC_IOC_FILE_INFO,
        DMA_BUF_SYNC_READ,
        DMA_BUF_SYNC_WRITE,
        DMA_BUF_SYNC_END,
    ];

    #[test]
    fn the_numbers_are_the_ones_every_kernel_has() {
        // These layouts are the same on every architecture Linux gives dma-buf to.
        assert_eq!(
            OURS,
            [
                0xC018_4800,
                0x4008_6200,
                0xC008_6202,
                0x4008_6203,
                0xC038_3E04,
                1,
                2,
                4
            ]
        );
    }

    /// Compiles a C program against the installed kernel headers and compares what the header
    /// macros give. A machine with no C compiler or no headers can't check, and says so.
    #[test]
    fn ioctl_numbers_equal_the_header_macros() {
        let dir = std::env::temp_dir().join(format!("dma-sys-uapi-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a scratch directory");
        let (source, binary) = (dir.join("uapi.c"), dir.join("uapi"));
        std::fs::write(&source, PROGRAM).expect("the program is written");
        let built = Command::new("cc")
            .arg(&source)
            .arg("-o")
            .arg(&binary)
            .output();
        let checked = match built {
            Ok(output) if output.status.success() => {
                let printed = Command::new(&binary).output().expect("the program runs");
                let text = String::from_utf8(printed.stdout).expect("numbers");
                let theirs: Vec<u64> = text
                    .split_whitespace()
                    .map(|number| number.parse().expect("a number"))
                    .collect();
                assert_eq!(theirs, OURS, "the headers disagree with uapi.rs");
                true
            }
            _ => false,
        };
        let _ = std::fs::remove_dir_all(&dir);
        if !checked {
            // In CI the compiler and headers are there, and a skip would hide a wrong number.
            assert!(
                std::env::var_os("CI").is_none(),
                "no C compiler or kernel headers to check the ioctl numbers against"
            );
        }
    }
}
