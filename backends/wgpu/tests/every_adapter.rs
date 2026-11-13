//! The dma-buf import on every adapter of the machine, not only the best one: a driver that
//! refuses it must leave a refusal behind, never a closed fd closed again.
#![cfg(target_os = "linux")]

use daedalus_wgpu::{Options, Storage, WgpuBackend};

/// One test at a time: each opens devices, and one of them counts this process's fds.
static ONE: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn turn() -> std::sync::MutexGuard<'static, ()> {
    ONE.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// How many fds this process has open.
fn open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").map_or(0, Iterator::count)
}

#[test]
fn a_dma_heap_buffer_is_imported_or_refused_on_every_adapter() {
    let _turn = turn();
    let mut seen = 0;
    for index in 0..8 {
        let backend = WgpuBackend::open(Options {
            index: Some(index),
            ..Options::default()
        });
        // Past the last adapter there is nothing to open.
        if backend.start().is_err() {
            break;
        }
        seen += 1;
        let caps = backend.caps().expect("open");
        let name = caps.adapter.name.clone();
        let before = open_fds();
        for round in 0..4 {
            match Storage::new_dma_heap(&backend, 4096) {
                Ok(storage) => {
                    assert!(
                        caps.dma_heap,
                        "{name}: imported, so the record must say it can"
                    );
                    storage.write(0, &[7; 4096]).expect("in range");
                    let mut back = [0; 4096];
                    storage.read_into(0, &mut back).expect("in range");
                    assert_eq!(back, [7; 4096], "{name}, round {round}");
                }
                Err(error) => {
                    assert!(
                        !caps.dma_heap,
                        "{name}: the record says it imports, and the import was refused: {error}"
                    );
                }
            }
        }
        // Whoever closed a refused fd, it was closed once: nothing leaked, and this process's
        // other fds are all still there. The driver doesn't always do the same with one, so the
        // rounds above are what make this a check.
        assert_eq!(open_fds(), before, "{name}: fds before and after");
    }
    assert!(
        seen > 0 || std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
        "this lane needs an adapter"
    );
}

mod kinds {
    use std::os::fd::BorrowedFd;

    use daedalus_types::{TypeExpr, TypeKey, Typed};
    use daedalus_wgpu::DmaBufBacked;

    /// A type whose dma-bufs can be written through their fd.
    pub struct Writable;
    /// A type whose dma-bufs only allow reading.
    pub struct OnlyRead;

    impl Typed for Writable {
        const KEY: TypeKey = TypeKey::new("t:writable");
        const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:writable"));
    }
    impl Typed for OnlyRead {
        const KEY: TypeKey = TypeKey::new("t:only_read");
        const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:only_read"));
    }
    impl DmaBufBacked for Writable {
        fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
            None
        }
    }
    impl DmaBufBacked for OnlyRead {
        const READ_ONLY: bool = true;
        fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
            None
        }
    }
}

/// An import is listed for a type only where the device takes that type's kind of fd: a device
/// that imports writable dma-bufs and not read-only ones lists the first and not the second, so
/// a read-only value is planned as a copy there and never as an import that would be refused.
#[test]
fn an_import_is_listed_only_for_the_kind_of_fd_the_device_takes() {
    use daedalus_backend::Backend;
    let _turn = turn();
    for index in 0..8 {
        let backend = WgpuBackend::open(Options {
            index: Some(index),
            ..Options::default()
        })
        .with_dmabuf::<kinds::Writable>()
        .with_dmabuf::<kinds::OnlyRead>();
        if backend.start().is_err() {
            break;
        }
        let caps = backend.caps().expect("open");
        let name = caps.adapter.name.clone();
        let listed = |key: &str| {
            backend
                .transfers()
                .iter()
                .any(|transfer| transfer.id.contains("import") && transfer.id.ends_with(key))
        };
        // Listed where the device takes that kind of fd, and where nothing has tried: on a
        // machine whose heap doesn't open, the first dma-buf it is handed is the trial.
        let may = |read_only: bool| backend.dmabuf_import(read_only) != Some(false);
        assert_eq!(listed("t:writable"), may(false), "{name}: a writable type");
        assert_eq!(listed("t:only_read"), may(true), "{name}: a read-only type");
        // Where the heap opens, the answers are known when the device opens.
        if caps.dma_heap {
            assert_eq!(backend.dmabuf_import(false), Some(true), "{name}");
            assert_eq!(
                backend.dmabuf_import(true),
                Some(caps.dma_read_only),
                "{name}"
            );
        }
        assert!(
            caps.dma_heap || !caps.dma_read_only,
            "{name}: read-only import without any import"
        );
    }
}
