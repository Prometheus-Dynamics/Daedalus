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

/// One run of the graph whose value is made in the dma-buf space: both readers get the CPU's
/// numbers, and the route and the device's counts are what `expected` says.
fn placed_case(device: &daedalus_wgpu::WgpuBackend, route: &[&str], moved: (u64, u64)) {
    for count in [4u32, 4096, 65_536] {
        let before = device.transfer_stats().expect("open");
        let placed = dmabuf::from_the_dmabuf_space(device, count).expect("runs");
        let after = device.transfer_stats().expect("open");
        assert_eq!(placed.words, dmabuf::on_cpu(count), "{count} bytes");
        assert_eq!(placed.sum, dmabuf::sum_on_cpu(count), "{count} bytes");
        assert_eq!(placed.route, route, "{count} bytes");
        assert_eq!(
            (
                after.imports - before.imports,
                after.uploads - before.uploads
            ),
            moved,
            "{count} bytes: imports and uploads"
        );
    }
}

const TO_HOST_THEN_UPLOAD: [&str; 2] = [
    "dmabuf.to_host:gpu:heap_bytes",
    "wgpu.upload:gpu:heap_bytes",
];

/// A value a node makes in the dma-buf space reaches the shader by an import from that space,
/// where the device imports dma-bufs. The CPU node beside its maker reads it with no move.
#[test]
fn a_value_made_in_the_dmabuf_space_is_imported_from_there() {
    let Some(_turn) = has_gpu() else { return };
    let device = gpu().expect("a device");
    if device.caps().expect("open").dma_heap {
        placed_case(&device, &["wgpu.import.dmabuf:gpu:heap_bytes"], (1, 0));
    } else {
        placed_case(&device, &TO_HOST_THEN_UPLOAD, (0, 1));
    }
}

/// On a device planned without dma-buf import, the same value is handed from the dma-buf space to
/// the host, with no copy, and uploaded from there.
#[test]
fn a_value_made_in_the_dmabuf_space_is_handed_to_the_host_and_uploaded() {
    let Some(_turn) = has_gpu() else { return };
    let device = dmabuf::gpu_without_dma_heap().expect("a device");
    placed_case(&device, &TO_HOST_THEN_UPLOAD, (0, 1));
}

/// A node that puts its output in a space no backend supplies doesn't plan, and the error names
/// the port and the space.
#[test]
fn a_port_in_a_space_nobody_supplies_fails_to_plan() {
    let Some(_turn) = has_gpu() else { return };
    let device = gpu().expect("a device");
    let refused = dmabuf::planned_without_the_space(&device).unwrap_err();
    assert!(refused.contains("heap.make_placed.value"), "{refused}");
    assert!(refused.contains("`dmabuf`"), "{refused}");
}

/// A host's lease on the GPU, counting its calls. They must come in pairs.
#[derive(Default)]
struct Counted {
    acquired: std::sync::atomic::AtomicUsize,
    released: std::sync::atomic::AtomicUsize,
}

impl daedalus_wgpu::GpuLease for Counted {
    fn acquire(&self) {
        use std::sync::atomic::Ordering;
        let acquired = self.acquired.fetch_add(1, Ordering::AcqRel) + 1;
        assert_eq!(
            acquired,
            self.released.load(Ordering::Acquire) + 1,
            "acquired while already held"
        );
    }
    fn release(&self) {
        use std::sync::atomic::Ordering;
        let released = self.released.fetch_add(1, Ordering::AcqRel) + 1;
        assert_eq!(
            released,
            self.acquired.load(Ordering::Acquire),
            "released while not held"
        );
    }
}

/// A graph run asks the host's lease for the device's work, the planner's moves and the region
/// alike, and has given it back by the time the graph is done.
#[test]
fn a_graph_run_holds_the_hosts_lease_and_gives_it_back() {
    use std::sync::atomic::Ordering;
    let Some(_turn) = has_gpu() else { return };
    let device = gpu().expect("a device");
    let lease = std::sync::Arc::new(Counted::default());
    device.set_lease(Some(lease.clone()));
    let ran = dmabuf::through_the_graph_on(&device, 4096);
    let (acquired, released) = (
        lease.acquired.load(Ordering::Acquire),
        lease.released.load(Ordering::Acquire),
    );
    // The device is shared with the other tests: it goes back to having no lease.
    device.set_lease(None);
    let (words, _) = ran.expect("runs");
    assert_eq!(words, dmabuf::on_cpu(4096));
    assert!(acquired > 0, "the graph's work asked for the lease");
    assert_eq!(
        acquired, released,
        "and it was given back when the graph was done"
    );
}
