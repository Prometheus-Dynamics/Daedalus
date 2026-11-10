//! 200 devices opened and closed on 4 threads in one process. Each opening makes an instance,
//! lists adapters and opens a device, and the Vulkan loader isn't safe against those racing. A
//! crash here is a path that misses the driver lock.

mod common;

use daedalus_wgpu::{Options, WgpuBackend, devices_created};

const THREADS: u64 = 4;
const PER_THREAD: u64 = 50;

#[test]
fn contexts_open_and_close_on_several_threads() {
    let Some(record) = common::adapter() else {
        return;
    };
    // A named adapter gets its own device each time, so every opening is a real one.
    let options = Options {
        adapter: Some(record.adapter.name.clone()),
        ..Options::default()
    };
    let before = devices_created();
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                for _ in 0..PER_THREAD {
                    let backend = WgpuBackend::open(options.clone());
                    let opened = backend.start().expect("a device");
                    assert_eq!(opened.adapter.name, record.adapter.name);
                }
            });
        }
    });
    assert_eq!(devices_created() - before, THREADS * PER_THREAD);
}
