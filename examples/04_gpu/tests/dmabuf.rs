//! A dma-buf from a CPU node to a shader node. The planner takes the import where the device
//! imports dma-bufs, and the upload where it doesn't. Either way the shader's words are the CPU's.
//! A machine with no GPU fails this where the GPU lane runs it (`DAEDALUS_GPU_REQUIRED=1`), and
//! skips it elsewhere.

#![cfg(target_os = "linux")]

use daedalus_example_gpu::{dmabuf, gpu};

/// The examples share the process's one device: a test at a time.
static ONE: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn has_gpu() -> Option<std::sync::MutexGuard<'static, ()>> {
    let turn = ONE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match gpu() {
        Ok(_) => Some(turn),
        Err(error) => {
            assert!(
                std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
                "{error}"
            );
            None
        }
    }
}

/// The value's route, and what the device counted over one run of the graph.
#[test]
fn a_heap_value_reaches_the_shader_by_the_route_the_device_can_run() {
    let Some(_turn) = has_gpu() else { return };
    let device = gpu().expect("a device");
    let imports = device.caps().expect("open").dma_heap;
    for count in [4u32, 64, 4096, 65_536] {
        let imported_before = device.transfer_stats().map_or(0, |stats| stats.imports);
        let uploads_before = device.transfer_stats().map_or(0, |stats| stats.uploads);
        let (words, route) = dmabuf::through_the_graph(count).expect("runs");
        assert_eq!(words, dmabuf::on_cpu(count), "{count} bytes");
        let imported = device.transfer_stats().map_or(0, |stats| stats.imports) - imported_before;
        let uploads = device.transfer_stats().map_or(0, |stats| stats.uploads) - uploads_before;
        if imports {
            assert_eq!(route, ["wgpu.import:gpu:heap_bytes"], "{count} bytes");
            assert_eq!(
                (imported, uploads),
                (1, 0),
                "{count} bytes: one import, no upload"
            );
        } else {
            assert_eq!(route, ["wgpu.upload:gpu:heap_bytes"], "{count} bytes");
            assert_eq!(
                (imported, uploads),
                (0, 1),
                "{count} bytes: one upload, no import"
            );
        }
    }
}

/// A device with only the byte moves: the value is uploaded, whatever the device can import, and
/// the shader's words are the CPU's.
#[test]
fn a_heap_value_is_uploaded_on_a_device_planned_without_a_dma_heap() {
    let Some(_turn) = has_gpu() else { return };
    let device = dmabuf::gpu_without_dma_heap().expect("a device");
    assert!(
        !device.caps().expect("open").dma_heap,
        "the device is planned without a heap"
    );
    for count in [4u32, 4096, 65_536] {
        let uploads_before = device.transfer_stats().map_or(0, |stats| stats.uploads);
        let imported_before = device.transfer_stats().map_or(0, |stats| stats.imports);
        let (words, route) = dmabuf::through_the_graph_on(&device, count).expect("runs");
        assert_eq!(words, dmabuf::on_cpu(count), "{count} bytes");
        assert_eq!(route, ["wgpu.upload:gpu:heap_bytes"], "{count} bytes");
        let uploads = device.transfer_stats().map_or(0, |stats| stats.uploads) - uploads_before;
        let imported = device.transfer_stats().map_or(0, |stats| stats.imports) - imported_before;
        assert_eq!(
            (uploads, imported),
            (1, 0),
            "{count} bytes: one upload, no import"
        );
    }
}
