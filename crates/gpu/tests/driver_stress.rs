//! Contexts opened and closed on several threads at once, in one process: 200 wgpu contexts over
//! 4 threads. Each opening creates an instance, enumerates adapters and creates a device. The
//! Vulkan loader is not safe against those calls racing each other (1.4.341's
//! `vkEnumeratePhysicalDevices` rescans the ICDs and `dlclose`s the previous scan while another
//! thread may be inside `vkGetDeviceProcAddr`), so they run under `driver_lock`. A crash here is a
//! path that misses it. Skips without an adapter.
#![allow(clippy::print_stderr)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_gpu::{GpuBackendKind, GpuOptions, select_backend};

const THREADS: usize = 4;
const PER_THREAD: usize = 50;

fn options() -> GpuOptions {
    GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        allow_software: true,
        ..GpuOptions::default()
    }
}

#[test]
fn contexts_open_and_close_on_several_threads() {
    match select_backend(&options()) {
        Ok(ctx) if ctx.backend_kind() == GpuBackendKind::Wgpu => {}
        _ => return eprintln!("skipping: no wgpu adapter"),
    }
    let opened = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let opened = Arc::clone(&opened);
            std::thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    let ctx = select_backend(&options()).expect("a wgpu context");
                    assert_eq!(ctx.backend_kind(), GpuBackendKind::Wgpu);
                    drop(ctx);
                    opened.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("a stress thread panicked");
    }
    assert_eq!(opened.load(Ordering::Relaxed), THREADS * PER_THREAD);
}
