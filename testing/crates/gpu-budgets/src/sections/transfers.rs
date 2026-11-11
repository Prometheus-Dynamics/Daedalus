//! Upload and readback through the backend's transfer steps, at 4 KiB, 256 KiB and 4 MiB, into
//! device memory, into host-mapped memory and into dma-heap memory.

use std::sync::Arc;

use daedalus_abi::Slot;
use daedalus_abi::testing::release;
use daedalus_gpu_budgets::{through, transfer};
use daedalus_runner::{put_shared, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_wgpu::{Storage, StorageError, WgpuBackend};

use super::{RUNS, WARM, median, median_with_setup, ms, pattern, word_size};

const SIZES: [usize; 4] = [4 << 10, 256 << 10, 4 << 20, 16 << 20];

pub fn run(backend: &WgpuBackend) {
    let (ty, space) = (TypeIndex(0), SpaceIndex(0));
    let mut upload = transfer(backend, daedalus_wgpu::UPLOAD);
    let mut mapped_upload = transfer(backend, daedalus_wgpu::UPLOAD_MAPPED);
    let mut readback = transfer(backend, daedalus_wgpu::READBACK);
    for bytes in SIZES {
        let payload: Arc<[u8]> = pattern(bytes, 13).into();
        let kib = bytes >> 10;
        let up = median(WARM, RUNS, || {
            let mut cell = Slot::EMPTY;
            put_shared(&mut cell, ty, space, Arc::clone(&payload));
            let mut out = through(&mut upload, cell);
            drop(take_typed::<Storage>(&mut out));
        });
        let up_mapped = median(WARM, RUNS, || {
            let mut cell = Slot::EMPTY;
            put_shared(&mut cell, ty, space, Arc::clone(&payload));
            let mut out = through(&mut mapped_upload, cell);
            drop(take_typed::<Storage>(&mut out));
        });
        let mut out_bytes = vec![0u8; bytes];
        // Each run takes a device buffer the step consumes, filled before the clock starts.
        let down = median_with_setup(
            WARM,
            RUNS,
            || {
                let device = Storage::new(backend, word_size(bytes as u64)).expect("a buffer");
                device.write(0, &payload).expect("in range");
                device
            },
            |device| {
                let mut cell = Slot::EMPTY;
                put_typed(&mut cell, ty, space, Arc::new(device));
                let mut back = through(&mut readback, cell);
                let mapped = take_typed::<Storage>(&mut back).expect("a buffer");
                mapped.read_into(0, &mut out_bytes).expect("a readback");
            },
        );
        // The same bytes straight to and from each kind of memory, without a step.
        let mut direct = |make: &dyn Fn(u64) -> Result<Storage, StorageError>| {
            let storage = make(word_size(bytes as u64)).expect("a buffer");
            let write = median(WARM, RUNS, || storage.write(0, &payload).expect("in range"));
            let read = median(WARM, RUNS, || {
                storage.read_into(0, &mut out_bytes).expect("a readback")
            });
            (write, read)
        };
        let (dev_w, dev_r) = direct(&|size| Storage::new(backend, size));
        let (map_w, map_r) = direct(&|size| Storage::new_host_mapped(backend, size));
        let heap = direct_heap(backend, bytes, &payload, &mut out_bytes);
        println!(
            "{kib} KiB: step upload to device {:.3} ms; step upload to mapped {:.3} ms; step readback device to mapped {:.3} ms (with the host read)",
            ms(up),
            ms(up_mapped),
            ms(down),
        );
        println!(
            "{kib} KiB direct: device write {:.3} ms, read {:.3} ms; mapped write {:.3} ms, read {:.3} ms; dma-heap {}",
            ms(dev_w),
            ms(dev_r),
            ms(map_w),
            ms(map_r),
            heap
        );
    }
    release(upload);
    release(mapped_upload);
    release(readback);
}

fn direct_heap(backend: &WgpuBackend, bytes: usize, payload: &[u8], out: &mut [u8]) -> String {
    let storage = match Storage::new_dma_heap(backend, word_size(bytes as u64)) {
        Ok(storage) => storage,
        Err(error) => return format!("skipped: {error}"),
    };
    let write = median(WARM, RUNS, || storage.write(0, payload).expect("in range"));
    let read = median(WARM, RUNS, || {
        storage.read_into(0, out).expect("a readback")
    });
    format!("write {:.3} ms, read {:.3} ms", ms(write), ms(read))
}
