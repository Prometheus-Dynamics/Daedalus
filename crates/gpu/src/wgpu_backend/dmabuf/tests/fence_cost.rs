//! What a pending acquire fence costs the *next* submission to the consumer's device, in each
//! GPU-side wait. The frame is an image a Vulkan producer exports in the driver's own renderable,
//! sampleable modifier, so this runs where the dma-heap (`LINEAR`) frames of `fences.rs` skip:
//! v3dv samples only `UIF`. The fence is a real `sync_file` from a GPU job on another device that
//! finishes about 200 ms later. `SyncFd` must not hold the next submission back on a kernel
//! driver; `Timeline` does on Mesa (wgpu-hal chains submissions with binary semaphores, and Mesa
//! runs a wait-before-signal submission on a submit thread), which is measured, not asserted.

use std::os::fd::AsFd;
use std::time::{Duration, Instant};

use ash::vk;

use super::late_writer::LateWriter;
use super::modifiers::{HEIGHT, WIDTH, export_image, renderable_modifiers};
use super::support::{
    device_poll, exclusive, import_backend, note, skip, use_fence_wait, wait_until,
};
use crate::external::sync_file_signaled;
use crate::{
    AcquireFenceWait, AcquireStatus, DrmFourcc, ExternalFrameDescriptor, ExternalPlane, GpuBackend,
    WgpuBackend,
};

const RUNS: usize = 5;

#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import"]
fn dmabuf_acquire_wait_cost_on_an_exported_frame() {
    let _gpu = exclusive();
    let Some(mut consumer) = import_backend() else {
        return;
    };
    if consumer
        .device_queue()
        .0
        .adapter_info()
        .driver
        .contains("llvmpipe")
    {
        // lavapipe's queue thread holds back the next submission in every mode.
        return skip("lavapipe holds back the next submission in every mode".into());
    }
    let format = vk::Format::B8G8R8A8_UNORM;
    let Some((modifier, planes)) = renderable_modifiers(&consumer, format).into_iter().next()
    else {
        return skip("no renderable, sampleable XRGB8888 modifier".into());
    };
    let usage = vk::ImageUsageFlags::SAMPLED
        | vk::ImageUsageFlags::TRANSFER_SRC
        | vk::ImageUsageFlags::COLOR_ATTACHMENT;
    let exported = match export_image(&consumer, format, modifier, planes, usage) {
        Ok(exported) => exported,
        Err(reason) => return skip(format!("export of 0x{modifier:x}: {reason}")),
    };
    let desc = || {
        let planes = exported
            .planes
            .iter()
            .map(|&(offset, pitch)| {
                ExternalPlane::from_borrowed(exported.fd.as_fd(), offset, pitch).unwrap()
            })
            .collect();
        ExternalFrameDescriptor::new(WIDTH, HEIGHT, DrmFourcc::XRGB8888, planes)
            .with_modifier(modifier)
    };
    let producer = WgpuBackend::new().expect("producer backend");
    let writer = LateWriter::on_own_texture(&producer, Duration::from_millis(200));

    let mut sync_fd = None;
    for wait in [AcquireFenceWait::SyncFd, AcquireFenceWait::Timeline] {
        if !use_fence_wait(&mut consumer, wait) {
            skip(format!("no {wait:?} wait"));
            continue;
        }
        let mut blocked = Vec::new();
        for _ in 0..RUNS {
            let fence = writer.write();
            let probe = fence.try_clone().unwrap();
            let handle = consumer
                .import_dmabuf(
                    desc()
                        .with_acquire_fence(fence)
                        .with_acquire_timeout(Duration::from_secs(2)),
                )
                .expect("import with a pending fence");
            let pending = !sync_file_signaled(probe.as_fd());
            let start = Instant::now();
            let next = consumer.import_dmabuf(desc()).expect("unfenced import");
            let took = start.elapsed();
            drop(next);
            // The status settles as the consumer's device makes progress.
            device_poll(&producer);
            wait_until(Duration::from_secs(3), "acquire", || {
                device_poll(&consumer);
                handle.acquire_status() != AcquireStatus::Pending
            });
            assert_eq!(handle.acquire_status(), AcquireStatus::Ready, "{wait:?}");
            drop(handle);
            device_poll(&consumer);
            if pending {
                blocked.push(took);
            }
        }
        if blocked.is_empty() {
            skip(format!(
                "{wait:?}: the producer finished before every import returned"
            ));
            continue;
        }
        blocked.sort();
        let median = blocked[blocked.len() / 2];
        note(format!(
            "{wait:?} on 0x{modifier:x}: the next submission behind a pending acquire took \
             {median:?} (median of {} runs; min {:?}, max {:?})",
            blocked.len(),
            blocked[0],
            blocked[blocked.len() - 1]
        ));
        if wait == AcquireFenceWait::SyncFd {
            sync_fd = Some(median);
        }
    }
    if let Some(median) = sync_fd {
        assert!(
            median < Duration::from_millis(50),
            "SyncFd held the next submission back for {median:?}"
        );
    }
}
