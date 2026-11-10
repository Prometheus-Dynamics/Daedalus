# daedalus-dma-sys

The only crate that calls the kernel's dma-buf, dma-heap and sync_file interfaces. Linux only: on other systems the crate is empty. It depends on `libc` and on nothing in `core/`, and its API is plain Rust with its own error type.

| Call | What the kernel does |
| --- | --- |
| `DmaHeap::open`, `open_path` | Opens `/dev/dma_heap/<name>` |
| `DmaHeap::allocate` | `DMA_HEAP_IOCTL_ALLOC`, which returns a dma-buf fd |
| `DmaBuf::memfd` | `memfd_create` and `ftruncate`: memory that maps and shares like a dma-buf, with no device |
| `DmaBuf::from_fd`, `into_fd` | Wraps or releases an owned fd |
| `DmaBufRef::len` | `lseek` to the end and back |
| `DmaBufRef::identity` | `fstat`: the device and inode, which name the buffer however many fds it has |
| `DmaBufRef::map`, `map_read_only` | `mmap` shared, unmapped on drop |
| `begin_cpu_access`, `end_cpu_access` | `DMA_BUF_IOCTL_SYNC` |
| `export_fence` | `DMA_BUF_IOCTL_EXPORT_SYNC_FILE`, Linux 6.0 and later |
| `import_fence` | `DMA_BUF_IOCTL_IMPORT_SYNC_FILE` |
| `SyncFence::from_fd`, `is_sync_file` | `SYNC_IOC_FILE_INFO` |
| `SyncFence::wait`, `is_signaled` | `poll` for `POLLIN` |

A `SyncFence` can only be made from an fd the kernel says is a sync_file. Handing a GPU driver some other fd as a fence has hung one: a pipe never signals.

The ioctl numbers are written out in `uapi.rs` and a test compiles a C program against the kernel's headers to check them.
