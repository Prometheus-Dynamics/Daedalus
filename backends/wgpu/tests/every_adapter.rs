//! The dma-buf import on every adapter of the machine, not only the best one: a driver that
//! refuses it must leave a refusal behind, never a closed fd closed again.
#![cfg(target_os = "linux")]

use daedalus_wgpu::{Options, Storage, WgpuBackend};

/// How many fds this process has open.
fn open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").map_or(0, Iterator::count)
}

#[test]
fn a_dma_heap_buffer_is_imported_or_refused_on_every_adapter() {
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
