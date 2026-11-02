//! Heap allocations on the raw Vulkan submit path, counted on the calling thread only: a
//! `RawSubmitter` submit + wait in steady state must allocate nothing, while `submit_raw` (the
//! wgpu path) allocates per submit. The number for `submit_raw` is printed as the "before".
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use daedalus_gpu::raw_vulkan::{RawSubmitter, ash::vk};
use daedalus_gpu::{GpuBackendKind, GpuContextHandle, GpuOptions, GpuStorage, select_backend};

struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: forwards to `System`, counting this thread's allocations while `COUNTING` is set.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = COUNTING.try_with(|on| {
            if on.get() {
                let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
            }
        });
        // SAFETY: as `System::alloc`.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as `System::dealloc`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Allocations this thread makes while `f` runs.
fn allocations(f: impl FnOnce()) -> usize {
    ALLOCS.with(|n| n.set(0));
    COUNTING.with(|on| on.set(true));
    f();
    COUNTING.with(|on| on.set(false));
    ALLOCS.with(|n| n.get())
}

const FRAMES: usize = 100;
const WARM_UP: usize = 20;

fn vulkan_ctx() -> Option<GpuContextHandle> {
    select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .ok()
}

#[test]
fn a_raw_submitter_frame_allocates_nothing() {
    let Some(ctx) = vulkan_ctx() else {
        return eprintln!("skipping: no GPU");
    };
    // SAFETY: only reads the handles; the fill below uses them under the sync contract.
    if unsafe { ctx.raw_vulkan() }.is_none() {
        return eprintln!("skipping: not a Vulkan context");
    }
    let submitter = RawSubmitter::new(&ctx).unwrap();
    let dst = GpuStorage::new_host_cached(&ctx, 4096, None).unwrap();
    // SAFETY: a buffer of this device; every submit below is waited for before the next.
    let buffer = unsafe { dst.raw_vk_buffer() }.unwrap();
    let fill = |d: &daedalus_gpu::raw_vulkan::ash::Device, cmd: vk::CommandBuffer| {
        // SAFETY: a fill of the storage, valid on its device.
        unsafe { d.cmd_fill_buffer(cmd, buffer.buffer, buffer.offset, buffer.size, 0x0101_0101) }
    };
    for _ in 0..WARM_UP {
        // SAFETY: as above.
        unsafe { submitter.submit(fill) }.unwrap().wait();
    }
    let count = allocations(|| {
        for _ in 0..FRAMES {
            // SAFETY: as above.
            unsafe { submitter.submit(fill) }.unwrap().wait();
        }
    });
    eprintln!("RawSubmitter: {count} allocations over {FRAMES} frames of submit + wait");
    assert_eq!(
        count, 0,
        "a steady-state RawSubmitter frame must not allocate"
    );
}

#[test]
fn submit_raw_frame_allocations_for_comparison() {
    let Some(ctx) = vulkan_ctx() else {
        return eprintln!("skipping: no GPU");
    };
    // SAFETY: only reads the handles.
    if unsafe { ctx.raw_vulkan() }.is_none() {
        return eprintln!("skipping: not a Vulkan context");
    }
    let dst = GpuStorage::new_host_cached(&ctx, 4096, None).unwrap();
    // SAFETY: a buffer of this device; each submit is waited for below.
    let buffer = unsafe { dst.raw_vk_buffer() }.unwrap();
    let fill = |d: &daedalus_gpu::raw_vulkan::ash::Device, cmd: vk::CommandBuffer| {
        // SAFETY: a fill of the storage, valid on its device.
        unsafe { d.cmd_fill_buffer(cmd, buffer.buffer, buffer.offset, buffer.size, 0x0101_0101) }
    };
    for _ in 0..WARM_UP {
        // SAFETY: as above.
        unsafe { ctx.submit_raw(fill) }.unwrap().wait();
    }
    let count = allocations(|| {
        for _ in 0..FRAMES {
            // SAFETY: as above.
            unsafe { ctx.submit_raw(fill) }.unwrap().wait();
        }
    });
    eprintln!("submit_raw: {count} allocations over {FRAMES} frames of submit + wait");
}
