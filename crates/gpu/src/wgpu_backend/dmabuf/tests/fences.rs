use std::io::Write;
use std::os::fd::{AsFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ash::{khr, vk};
use wgpu::hal::api::Vulkan;

use super::support::{
    DmaBuf, consumer_backend, device_poll, exclusive, import_backend, note, pixel, skip,
    use_fence_wait, validated, wait_until,
};
use crate::external::sync_file_signaled;
use crate::{
    AcquireFenceMode, AcquireFenceWait, AcquireStatus, DEFAULT_ACQUIRE_TIMEOUT, DmabufAccess,
    DrmFourcc, ExternalFrameDescriptor, ExternalImportError, ExternalPlane, GpuBackend,
    GpuImageHandle, GpuUsage, WgpuBackend, export_dmabuf_fence,
};

const SYNC_FD: vk::ExternalSemaphoreHandleTypeFlags = vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD;
const WAITS: [AcquireFenceWait; 3] = [
    AcquireFenceWait::SyncFd,
    AcquireFenceWait::Timeline,
    AcquireFenceWait::Cpu,
];

/// A 64x32 XRGB8888 dma-heap frame holding [`pixel`].
struct Frame {
    buf: DmaBuf,
}

impl Frame {
    const WIDTH: u32 = 64;
    const HEIGHT: u32 = 32;
    const STRIDE: u64 = 256;

    fn new() -> Self {
        let buf = DmaBuf::alloc((Self::STRIDE * u64::from(Self::HEIGHT)) as usize)
            .expect("dma-heap alloc");
        buf.write(|bytes| {
            for (y, row) in bytes.chunks_exact_mut(Self::STRIDE as usize).enumerate() {
                for (x, px) in row[..Self::WIDTH as usize * 4]
                    .chunks_exact_mut(4)
                    .enumerate()
                {
                    px.copy_from_slice(&pixel(x as u32, y as u32));
                }
            }
        });
        Self { buf }
    }

    fn desc(&self, fourcc: DrmFourcc) -> ExternalFrameDescriptor {
        let plane = ExternalPlane::from_borrowed(self.buf.fd.as_fd(), 0, Self::STRIDE).unwrap();
        ExternalFrameDescriptor::single_plane(Self::WIDTH, Self::HEIGHT, fourcc, plane)
    }
}

fn assert_pattern(backend: &WgpuBackend, handle: &GpuImageHandle, context: &str) {
    let read = backend.read_texture(handle).expect("readback");
    for (index, px) in read.chunks_exact(4).enumerate() {
        let (x, y) = (index as u32 % handle.width, index as u32 / handle.width);
        assert_eq!(px, &pixel(x, y), "pixel ({x},{y}), {context}");
    }
}

/// A never-signaling stand-in for a fence: the read end of a pipe nobody writes to. The watcher
/// and the CPU wait only `poll` the fd, so it behaves like a pending `sync_file`; the `SyncFd`
/// wait cannot import it, which falls back as `AcquireFenceWaits::non_sync_file_fallback` says.
fn pipe_fence() -> (OwnedFd, std::io::PipeWriter) {
    let (reader, writer) = std::io::pipe().unwrap();
    (reader.into(), writer)
}

/// Signaled, implicit and never-signaling fences in every wait mode the device supports.
#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
fn dmabuf_import_waits_for_fences() {
    let _gpu = exclusive();
    let Some(mut backend) = import_backend() else {
        return;
    };
    let frame = Frame::new();
    for wait in WAITS {
        if !use_fence_wait(&mut backend, wait) {
            skip(format!("{wait:?} fence waits unsupported"));
            continue;
        }
        validated(&backend, || check_fences(&backend, &frame, wait));
    }
}

/// The default mode (`Auto`: `SyncFd` first), the per-import override, and where a fence that is
/// not a `sync_file` goes.
#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
fn dmabuf_fence_mode_selection() {
    let _gpu = exclusive();
    let Some(mut backend) = import_backend() else {
        return;
    };
    let support = backend.dmabuf_import_support();
    let waits = support.fence_waits().unwrap();
    assert_eq!(support.acquire_fence_mode(), Some(AcquireFenceMode::Auto));
    assert_eq!(support.acquire_fence_wait(), waits.iter().next());
    note(format!("fence waits: {waits:?}, default {support:?}"));
    let frame = Frame::new();
    let desc = || frame.desc(DrmFourcc::XRGB8888);
    let timeout = Duration::from_millis(30);

    // A never-signaling pipe is not a sync_file: `Auto` hands it to the timeline watcher (the GPU
    // goes ahead after the timeout), an explicit `SyncFd` or `Cpu` waits on the CPU.
    for mode in AcquireFenceMode::ALL {
        let (never, _writer) = pipe_fence();
        let start = Instant::now();
        let result = backend.import_dmabuf(
            desc()
                .with_acquire_fence(never)
                .with_acquire_timeout(timeout)
                .with_acquire_fence_mode(mode),
        );
        let elapsed = start.elapsed();
        if waits.non_sync_file_fallback(mode) == AcquireFenceWait::Timeline {
            let handle = result.expect("timeline wait for a non-sync_file fence");
            assert!(
                elapsed < timeout,
                "{mode:?}: the import blocked for {elapsed:?}"
            );
            wait_until(Duration::from_secs(2), "timeout", || {
                handle.acquire_status() != AcquireStatus::Pending
            });
            assert_eq!(handle.acquire_status(), AcquireStatus::TimedOut, "{mode:?}");
        } else {
            let err = result.unwrap_err();
            assert!(elapsed >= timeout, "{mode:?}: resolved after {elapsed:?}");
            assert!(
                matches!(err, ExternalImportError::FenceTimeout { .. }),
                "{mode:?}: {err}"
            );
        }
        device_poll(&backend);
    }

    // The per-import override wins over the backend default (and vice versa).
    if waits.timeline {
        backend.set_acquire_fence_mode(AcquireFenceMode::Cpu);
        let (never, _writer) = pipe_fence();
        let handle = backend
            .import_dmabuf(
                desc()
                    .with_acquire_fence(never)
                    .with_acquire_timeout(timeout)
                    .with_acquire_fence_mode(AcquireFenceMode::Timeline),
            )
            .expect("timeline override on a CPU-wait backend");
        assert_eq!(handle.acquire_status(), AcquireStatus::Pending);
        drop(handle);
        device_poll(&backend);
    }
    backend.set_acquire_fence_mode(AcquireFenceMode::Timeline);
    let (never, _writer) = pipe_fence();
    let err = backend
        .import_dmabuf(
            desc()
                .with_acquire_fence(never)
                .with_acquire_timeout(timeout)
                .with_acquire_fence_mode(AcquireFenceMode::Cpu),
        )
        .unwrap_err();
    assert!(matches!(err, ExternalImportError::FenceTimeout { .. }));
    assert_eq!(
        backend.dmabuf_import_support().acquire_fence_mode(),
        Some(AcquireFenceMode::Timeline)
    );
}

fn check_fences(backend: &WgpuBackend, frame: &Frame, wait: AcquireFenceWait) {
    let desc = || frame.desc(DrmFourcc::XRGB8888);
    // Explicit fence exported from the dmabuf's implicit fences (DMA_BUF_IOCTL_EXPORT_SYNC_FILE);
    // it may still be pending on the GPU work of the previous mode's imports.
    let fence = export_dmabuf_fence(frame.buf.fd.as_fd(), DmabufAccess::Read).expect("export");
    let handle = backend
        .import_dmabuf(desc().with_acquire_fence(fence))
        .expect("import with exported fence");
    assert_pattern(backend, &handle, "exported fence");
    assert_eq!(handle.acquire_status(), AcquireStatus::Ready);

    // Same through the descriptor helper, for a writer (read+write fences).
    let handle = backend
        .import_dmabuf(
            desc()
                .with_usage(GpuUsage::UPLOAD)
                .with_implicit_fence()
                .expect("implicit fence"),
        )
        .expect("import with implicit fence");
    assert_pattern(backend, &handle, "implicit fence");

    // A fence that never signals: the timeline path lets the GPU go ahead after the timeout and
    // flags the image; the others fail before any Vulkan object is created.
    let (never, _writer) = pipe_fence();
    let guard = Arc::new(());
    let timeout = Duration::from_millis(30);
    let start = Instant::now();
    let result = backend.import_dmabuf(
        desc()
            .with_keepalive(guard.clone())
            .with_acquire_fence(never)
            .with_acquire_timeout(timeout),
    );
    let elapsed = start.elapsed();
    if wait == AcquireFenceWait::Timeline {
        let handle = result.expect("timeline import with a stuck fence");
        assert!(elapsed < timeout, "the import blocked for {elapsed:?}");
        assert_eq!(handle.acquire_status(), AcquireStatus::Pending);
        let waited = wait_until(Duration::from_secs(2), "timeout", || {
            handle.acquire_status() != AcquireStatus::Pending
        });
        assert_eq!(handle.acquire_status(), AcquireStatus::TimedOut);
        assert!(elapsed + waited >= timeout, "resolved early: {waited:?}");
        // The GPU went ahead: readback completes (the dmabuf holds the pattern anyway).
        assert_pattern(backend, &handle, "after timeout");
        drop(handle);
        device_poll(backend);
    } else {
        let err = result.unwrap_err();
        assert!(
            matches!(err, ExternalImportError::FenceTimeout { .. }),
            "{err}"
        );
    }
    assert_eq!(Arc::strong_count(&guard), 1, "keepalive released");
}

/// Timeline path: a fence signaled within the timeout releases the GPU (and how fast the watcher
/// hop is), waits are released in import order, and dropping the backend with a stuck fence
/// neither hangs nor leaves the handle `Pending`.
#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
fn dmabuf_timeline_watcher() {
    let _gpu = exclusive();
    let Some(mut backend) = import_backend() else {
        return;
    };
    if !use_fence_wait(&mut backend, AcquireFenceWait::Timeline) {
        return skip("no timeline fence waits".into());
    }
    let frame = Frame::new();
    let desc = || {
        frame
            .desc(DrmFourcc::XRGB8888)
            .with_acquire_timeout(Duration::from_secs(5))
    };

    // Late fence within the timeout: correct pixels. Measure the watcher hop (fence -> status,
    // set right before the timeline is signaled) and fence -> readback done against the same
    // readback of an unfenced import.
    let median = |mut samples: Vec<Duration>| {
        samples.sort();
        samples[samples.len() / 2]
    };
    let (mut hops, mut fenced, mut unfenced) = (Vec::new(), Vec::new(), Vec::new());
    for _ in 0..20 {
        let handle = backend.import_dmabuf(desc()).expect("import");
        let start = Instant::now();
        assert_pattern(&backend, &handle, "no fence");
        unfenced.push(start.elapsed());

        let (fence, mut writer) = pipe_fence();
        let handle = backend
            .import_dmabuf(desc().with_acquire_fence(fence))
            .expect("import");
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(handle.acquire_status(), AcquireStatus::Pending);
        let signaled = Instant::now();
        writer.write_all(&[1]).unwrap();
        hops.push(wait_until(Duration::from_secs(1), "fence release", || {
            handle.acquire_status() != AcquireStatus::Pending
        }));
        assert_eq!(handle.acquire_status(), AcquireStatus::Ready);
        assert_pattern(&backend, &handle, "late fence");
        fenced.push(signaled.elapsed());
    }
    let max_hop = hops.iter().max().copied();
    let (hop, fenced, unfenced) = (median(hops), median(fenced), median(unfenced));
    note(format!(
        "timeline watcher hop: median {hop:?} (max {max_hop:?}); fence -> readback done: median \
         {fenced:?}, readback without fence: median {unfenced:?}"
    ));
    assert!(hop < Duration::from_millis(20), "{hop:?}");

    // A pending wait holds back the next submission on the device: Mesa runs a submission that
    // waits for an unsignaled timeline value on a submit thread, and wgpu chains submissions with
    // binary semaphores, which Mesa only accepts once the previous submission reached the kernel.
    // The block is bounded by the timeout. Releases also happen in import order: `second` signals
    // first, but its import (whose acquire submission waits behind `first`) only returns once
    // `first` timed out; releasing `second` early would have released `first` with it.
    let timeout = Duration::from_millis(100);
    let (first_fence, _first_writer) = pipe_fence();
    let first = backend
        .import_dmabuf(
            desc()
                .with_acquire_fence(first_fence)
                .with_acquire_timeout(timeout),
        )
        .unwrap();
    let start = Instant::now();
    let (second_fence, mut second_writer) = pipe_fence();
    let second_desc = desc().with_acquire_fence(second_fence);
    let (second, blocked) = std::thread::scope(|scope| {
        let importer = scope.spawn(|| (backend.import_dmabuf(second_desc), start.elapsed()));
        std::thread::sleep(Duration::from_millis(20));
        second_writer.write_all(&[1]).unwrap();
        importer.join().unwrap()
    });
    let second = second.unwrap();
    note(format!(
        "submission behind a pending timeline wait blocked for {blocked:?} (timeout {timeout:?})"
    ));
    assert_eq!(first.acquire_status(), AcquireStatus::TimedOut);
    assert_eq!(second.acquire_status(), AcquireStatus::Ready);
    assert!(
        blocked >= timeout * 3 / 4 && blocked < timeout * 5,
        "blocked {blocked:?}"
    );
    drop((first, second));

    // Shutdown with a fence that would block the GPU for a minute.
    let (stuck, _writer) = pipe_fence();
    let guard = Arc::new(());
    let handle = backend
        .import_dmabuf(
            desc()
                .with_keepalive(guard.clone())
                .with_acquire_fence(stuck)
                .with_acquire_timeout(Duration::from_secs(60)),
        )
        .unwrap();
    let start = Instant::now();
    drop(backend);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "backend drop took {elapsed:?}"
    );
    assert_eq!(handle.acquire_status(), AcquireStatus::TimedOut);
    drop(handle);
    assert_eq!(Arc::strong_count(&guard), 1, "keepalive released");
}

/// A producer device writes the dmabuf at the end of a long GPU job and hands over that job's
/// `sync_file`. With GPU-side waits the import returns while the fence is still pending,
/// consumers still read the producer's pixels, and a handle dropped before the fence signals
/// keeps its dmabuf until the GPU is done with the acquire. A timeline timeout shorter than the
/// job lets the consumer go ahead; with CPU waits the import blocks.
#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
fn dmabuf_import_gpu_waits_for_late_fence() {
    let _gpu = exclusive();
    let (mut consumer, independent) = consumer_backend();
    let support = consumer.dmabuf_import_support();
    if support
        .fence_waits()
        .is_none_or(|waits| !waits.sync_fd && !waits.timeline)
    {
        return skip(format!("no GPU-side fence wait: {support:?}"));
    }
    let producer = WgpuBackend::new().expect("producer backend");
    let frame = Frame::new();
    let desc = || frame.desc(DrmFourcc::XBGR8888);
    let target = match producer.import_dmabuf(desc().with_usage(GpuUsage::STORAGE)) {
        Ok(handle) => handle,
        Err(ExternalImportError::UnsupportedFormat { reason, .. }) => {
            return skip(format!("storage import for the producer: {reason}"));
        }
        Err(err) => panic!("producer import failed: {err}"),
    };
    let writer = LateWriter::new(&producer, &target, Duration::from_millis(300));

    for mode in [AcquireFenceWait::SyncFd, AcquireFenceWait::Timeline] {
        if !use_fence_wait(&mut consumer, mode) {
            skip(format!("consumer lacks {mode:?} fence waits"));
            continue;
        }
        for _ in 0..2 {
            frame.buf.write(|bytes| bytes.fill(0));
            let fence = writer.write();
            let probe = fence.try_clone().unwrap();
            let start = Instant::now();
            let handle = validated(&consumer, || {
                consumer
                    .import_dmabuf(desc().with_acquire_fence(fence))
                    .expect("import with pending fence")
            });
            let elapsed = start.elapsed();
            assert!(
                !sync_file_signaled(probe.as_fd()),
                "the producer finished before the import returned ({elapsed:?}); the import \
                 blocked or the producer job is too short"
            );
            assert_eq!(handle.acquire_status(), AcquireStatus::Pending);
            let context = format!("{mode:?}, independent: {independent}");
            assert_pattern(&consumer, &handle, &context);
            assert!(sync_file_signaled(probe.as_fd()));
            assert_eq!(handle.acquire_status(), AcquireStatus::Ready);
        }

        let fence = writer.write();
        let probe = fence.try_clone().unwrap();
        let guard = Arc::new(());
        let handle = consumer
            .import_dmabuf(
                desc()
                    .with_keepalive(guard.clone())
                    .with_acquire_fence(fence),
            )
            .expect("import with pending fence");
        drop(handle);
        let _ = consumer.device_queue().0.poll(wgpu::PollType::Poll);
        let held = Arc::strong_count(&guard);
        if sync_file_signaled(probe.as_fd()) {
            skip("producer finished before the keepalive check".into());
        } else {
            assert_eq!(held, 2, "dmabuf released while its acquire was pending");
        }
        device_poll(&consumer);
        assert_eq!(Arc::strong_count(&guard), 1, "keepalive released once idle");
    }

    // The default (`Auto` with a bounded timeout) and `Timeline` with an unbounded timeout take the
    // `SyncFd` wait. On a kernel driver (not lavapipe, whose queue thread holds back the next
    // submission either way) the imported sync_file is a kernel fence the next submission does not
    // wait for on the CPU.
    let mut hardware = WgpuBackend::new().expect("wgpu backend");
    let both = hardware
        .dmabuf_import_support()
        .fence_waits()
        .is_some_and(|waits| waits.sync_fd && waits.timeline);
    for (mode, timeout) in [
        (AcquireFenceMode::Auto, DEFAULT_ACQUIRE_TIMEOUT),
        (AcquireFenceMode::Timeline, Duration::MAX),
    ] {
        if !both {
            break;
        }
        hardware.set_acquire_fence_mode(mode);
        let fence = writer.write();
        let probe = fence.try_clone().unwrap();
        let handle = hardware
            .import_dmabuf(
                desc()
                    .with_acquire_fence(fence)
                    .with_acquire_timeout(timeout),
            )
            .expect("import");
        let start = Instant::now();
        let next = hardware.import_dmabuf(desc()).expect("unfenced import");
        let blocked = start.elapsed();
        assert!(
            !sync_file_signaled(probe.as_fd()),
            "{mode:?}: the next submission waited for the producer ({blocked:?})"
        );
        drop(next);
        assert_pattern(
            &hardware,
            &handle,
            &format!("{mode:?}, timeout {timeout:?}"),
        );
    }

    // A real sync_file that misses a short timeout: the consumer goes ahead without the producer.
    if use_fence_wait(&mut consumer, AcquireFenceWait::Timeline) {
        let fence = writer.write();
        let probe = fence.try_clone().unwrap();
        let timeout = Duration::from_millis(50);
        let handle = consumer
            .import_dmabuf(
                desc()
                    .with_acquire_fence(fence)
                    .with_acquire_timeout(timeout),
            )
            .expect("import");
        wait_until(Duration::from_secs(2), "timeout", || {
            handle.acquire_status() != AcquireStatus::Pending
        });
        assert_eq!(handle.acquire_status(), AcquireStatus::TimedOut);
        assert!(!sync_file_signaled(probe.as_fd()), "producer job too short");
        let _ = consumer.read_texture(&handle).expect("readback");
        if independent {
            assert!(
                !sync_file_signaled(probe.as_fd()),
                "the consumer waited for the producer despite the timeout"
            );
        }
        device_poll(&producer);
    }

    use_fence_wait(&mut consumer, AcquireFenceWait::Cpu);
    let fence = writer.write();
    let probe = fence.try_clone().unwrap();
    let handle = consumer
        .import_dmabuf(desc().with_acquire_fence(fence))
        .expect("blocking import");
    assert!(sync_file_signaled(probe.as_fd()));
    drop(handle);
    device_poll(&producer); // destroys the exported semaphores
}

/// Writes `pixel(x, y)` into a storage texture after a GPU spin of about the requested duration,
/// handing out a `sync_file` that signals once the write is done.
struct LateWriter<'a> {
    backend: &'a WgpuBackend,
    pipeline: wgpu::ComputePipeline,
    bind_group: wgpu::BindGroup,
    spins: wgpu::Buffer,
    iterations: u32,
}

impl<'a> LateWriter<'a> {
    fn new(backend: &'a WgpuBackend, target: &GpuImageHandle, duration: Duration) -> Self {
        let device = backend.device_queue().0;
        let texture = backend.get_texture(target).expect("registered texture");
        let view = texture.create_view(&Default::default());
        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("late-writer"),
            source: wgpu::ShaderSource::Wgsl(
                r#"
@group(0) @binding(0) var dst: texture_storage_2d<rgba8unorm, write>;
@group(0) @binding(1) var<uniform> spins: u32;
@compute @workgroup_size(1)
fn main() {
var acc = 1u;
for (var i = 0u; i < spins; i++) { acc = acc * 1664525u + 1013904223u; }
let dims = textureDimensions(dst);
for (var y = 0u; y < dims.y; y++) {
    for (var x = 0u; x < dims.x; x++) {
        var c = vec4<f32>(vec3<f32>(f32(x), f32(y), f32(x ^ y)) / 255.0, 1.0);
        if (acc == 7u) { c = vec4<f32>(0.0); }
        textureStore(dst, vec2<i32>(i32(x), i32(y)), c);
    }
}
}
"#
                .into(),
            ),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("late-writer"),
            layout: None,
            module: &shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        let spins = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("late-writer-spins"),
            size: 16,
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("late-writer"),
            layout: &pipeline.get_bind_group_layout(0),
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: wgpu::BindingResource::TextureView(&view),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: spins.as_entire_binding(),
                },
            ],
        });
        let mut writer = Self {
            backend,
            pipeline,
            bind_group,
            spins,
            iterations: 1 << 20,
        };
        // Calibrate the spin to about `duration` on this GPU.
        let start = std::time::Instant::now();
        writer.dispatch();
        device_poll(backend);
        let per_spin = start.elapsed().as_secs_f64() / f64::from(writer.iterations);
        writer.iterations = (duration.as_secs_f64() / per_spin).clamp(1e6, 2e9) as u32;
        writer
    }

    fn dispatch(&self) {
        let (device, queue) = self.backend.device_queue();
        queue.write_buffer(&self.spins, 0, &self.iterations.to_le_bytes());
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &self.bind_group, &[]);
            pass.dispatch_workgroups(1, 1, 1);
        }
        queue.submit(Some(encoder.finish()));
    }

    /// Submit the late write and export a `sync_file` of its completion.
    fn write(&self) -> OwnedFd {
        let (device, queue) = self.backend.device_queue();
        // SAFETY: the semaphore is created, signaled by the next submission, exported, and
        // destroyed once that submission completed.
        unsafe {
            let hal_dev = device.as_hal::<Vulkan>().expect("Vulkan device");
            let raw = hal_dev.raw_device().clone();
            let mut export = vk::ExportSemaphoreCreateInfo::default().handle_types(SYNC_FD);
            let semaphore = raw
                .create_semaphore(
                    &vk::SemaphoreCreateInfo::default().push_next(&mut export),
                    None,
                )
                .expect("exportable semaphore");
            queue
                .as_hal::<Vulkan>()
                .expect("Vulkan queue")
                .add_signal_semaphore(semaphore, None);
            self.dispatch();
            let fd_api = khr::external_semaphore_fd::Device::new(
                hal_dev.shared_instance().raw_instance(),
                &raw,
            );
            let info = vk::SemaphoreGetFdInfoKHR::default()
                .semaphore(semaphore)
                .handle_type(SYNC_FD);
            let fd = fd_api.get_semaphore_fd(&info).expect("export sync_file");
            queue.on_submitted_work_done(move || raw.destroy_semaphore(semaphore, None));
            OwnedFd::from_raw_fd(fd)
        }
    }
}
