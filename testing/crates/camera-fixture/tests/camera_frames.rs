#![cfg(target_os = "linux")]
#![allow(clippy::print_stdout)] // the timings and the camera's report are this test's output

//! Real camera frames on the device. A free capture node streams frames over its MMAP buffers,
//! which are exported as dma-bufs. Each frame is imported and read by a copy shader and by an
//! image copy, and both must give the bytes the host reads from the same memory.
//!
//! Skipped with its reason when no capture node is free, and required with
//! `DAEDALUS_CAMERA_REQUIRED=1`. Run on a board with `--nocapture` to see the timings.

use std::os::fd::OwnedFd;
use std::time::{Duration, Instant};

use daedalus_camera_fixture::{Camera, Error, capture_nodes};
use daedalus_dma_sys::{Access, DmaBuf};
use daedalus_wgpu::dmabuf::{ExternalImage, ExternalPlane, Fourcc, ImportError};
use daedalus_wgpu::image::{ExternalBuffer, ImageCopy};
use daedalus_wgpu::kernels::{Kernel, Kernels};
use daedalus_wgpu::{Format, Options, Storage, TextureDesc, WgpuBackend};

/// Frames streamed in rotation over the camera's buffers.
const FRAMES: usize = 400;
const WIDTH: u32 = 640;
const HEIGHT: u32 = 480;
/// YUYV is two bytes a pixel: the frame is read as one byte-wide image twice the width.
const YUYV: [u8; 4] = *b"YUYV";

fn device() -> Option<WgpuBackend> {
    let shared = WgpuBackend::open(Options::default());
    let record = match shared.start() {
        Ok(record) => record,
        Err(error) => {
            assert!(
                std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
                "{error}"
            );
            return None;
        }
    };
    Some(WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }))
}

/// The first capture node that is free, and what each busy or unusable node said.
fn free_camera(report: &mut Vec<String>) -> Option<Camera> {
    for node in capture_nodes() {
        match Camera::open(&node, YUYV, WIDTH, HEIGHT) {
            Ok(camera) => {
                report.push(format!("{}: free, streaming", node.display()));
                return Some(camera);
            }
            Err(Error::Busy { path, step }) => {
                report.push(format!("{}: busy ({step})", path.display()));
            }
            Err(Error::NotCapture(path)) => {
                report.push(format!("{}: not a streaming capture node", path.display()));
            }
            Err(error) => report.push(format!("{error}")),
        }
    }
    None
}

/// The bytes the host reads from the frame's memory, after the device has finished with it.
fn host_bytes(fd: &OwnedFd, len: usize) -> Vec<u8> {
    let buf = DmaBuf::from_fd(fd.try_clone().expect("a second fd"));
    buf.as_ref().begin_cpu_access(Access::Read).expect("begin");
    let map = buf.as_ref().map_read_only().expect("a mapping");
    let mut out = vec![0; len];
    map.read(0, &mut out).expect("in range");
    buf.as_ref().end_cpu_access(Access::Read).expect("end");
    out
}

fn read_back(storage: &Storage, len: usize) -> Vec<u8> {
    let mut out = vec![0; storage.size() as usize];
    storage.read_into(0, &mut out).expect("a readback");
    out.truncate(len);
    out
}

fn ms(time: Duration) -> f64 {
    time.as_secs_f64() * 1e3
}

fn median_of(times: &mut [Duration]) -> Duration {
    times.sort_unstable();
    times.get(times.len() / 2).copied().unwrap_or_default()
}

#[test]
fn real_camera_frames_import_and_read_the_bytes_the_host_sees() {
    let required = std::env::var_os("DAEDALUS_CAMERA_REQUIRED").is_some();
    let mut report = Vec::new();
    let Some(mut camera) = free_camera(&mut report) else {
        for line in &report {
            println!("camera: {line}");
        }
        assert!(
            !required,
            "DAEDALUS_CAMERA_REQUIRED is set, and no capture node is free"
        );
        println!("camera: skipped, no free capture node");
        return;
    };
    for line in &report {
        println!("camera: {line}");
    }
    let Some(backend) = device() else { return };
    let format = camera.format();
    let stride = u64::from(format.stride);
    println!(
        "camera: {} gave {:?} {}x{}, stride {stride}, sizeimage {}",
        camera.path().display(),
        format.fourcc.to_le_bytes().map(char::from),
        format.width,
        format.height,
        format.size
    );
    // YUYV as storage: one byte-wide image, `stride` bytes a row, over the same memory.
    let width = format.stride;
    let rows = format.height;
    let len = (stride * u64::from(rows)) as usize;
    let describe = |fd: OwnedFd| ExternalBuffer {
        fd,
        width,
        height: rows,
        fourcc: Fourcc::R8,
        planes: vec![(0, stride)],
    };
    let kernels = Kernels::new(&backend).expect("a device");
    let image = ImageCopy::new(
        &backend,
        TextureDesc {
            format: Format::R8,
            width,
            height: rows,
        },
        stride,
    )
    .expect("a copy");
    let out = Storage::new(&backend, (len as u64).next_multiple_of(4)).expect("a buffer");
    let mut import_times = Vec::new();
    let mut copy_times = Vec::new();
    let mut image_times = Vec::new();
    let mut seen = [false; 64];
    let (mut first_ok, mut first_bad, mut reuse_ok, mut reuse_bad) =
        (0usize, 0usize, 0usize, 0usize);
    let mut texture_bad = 0usize;

    for _ in 0..FRAMES {
        let frame = camera.next_frame().expect("a frame");
        let index = frame.index as usize;
        let fd = camera
            .dmabuf(frame.index)
            .expect("an export")
            .try_clone()
            .expect("a clone");
        let start = Instant::now();
        let imported = backend
            .import_storage(describe(fd))
            .unwrap_or_else(|error| {
                panic!("import of buffer {index} ({} bytes): {error}", format.size)
            });
        import_times.push(start.elapsed());
        let reused = seen[index % 64];
        seen[index % 64] = true;

        let start = Instant::now();
        kernels
            .run(Kernel::Copy, &imported.storage, &out, len as u64)
            .expect("runs")
            .wait()
            .expect("finishes");
        copy_times.push(start.elapsed());
        let on_device = read_back(&out, len);
        let host = host_bytes(camera.dmabuf(frame.index).expect("an export"), len);
        let device_ok = on_device == host;
        match (reused, device_ok) {
            (false, true) => first_ok += 1,
            (false, false) => first_bad += 1,
            (true, true) => reuse_ok += 1,
            (true, false) => reuse_bad += 1,
        }

        let start = Instant::now();
        image
            .run(&imported.storage, 0)
            .expect("copies")
            .wait()
            .expect("finishes");
        image_times.push(start.elapsed());
        let mut texel = vec![0u8; len];
        image.texture().read_into(&mut texel).expect("a readback");
        if texel != host {
            texture_bad += 1;
        }
        camera.requeue(frame.index).expect("requeued");
    }
    println!(
        "frames: {FRAMES} in rotation over {} buffers, {} bytes checked against the host",
        seen.iter().filter(|s| **s).count(),
        FRAMES * len
    );
    println!(
        "import (a copy through the CPU, see the stats): median {:.3} ms",
        ms(median_of(&mut import_times))
    );
    println!(
        "copy shader over the imported frame: median {:.3} ms; image copy into a texture: median {:.3} ms",
        ms(median_of(&mut copy_times)),
        ms(median_of(&mut image_times))
    );
    println!(
        "device against host, first use of a buffer: {first_ok} match, {first_bad} differ; reuse: {reuse_ok} match, {reuse_bad} differ; image copy texture differs on {texture_bad} frames"
    );
    if let Some(stats) = backend.storage_import_stats() {
        println!("storage import stats: {stats:?}");
    }

    // The linear import the in-place path takes: V3D refuses it, and the message is recorded.
    let first = camera
        .dmabuf(0)
        .expect("an export")
        .try_clone()
        .expect("a clone");
    let external = ExternalImage {
        width,
        height: rows,
        fourcc: Fourcc::R8,
        modifier: None,
        planes: vec![ExternalPlane {
            fd: first,
            offset: 0,
            stride,
        }],
        acquire_fence: None,
        acquire_timeout: daedalus_wgpu::dmabuf::DEFAULT_ACQUIRE_TIMEOUT,
    };
    match backend.import_external(external) {
        Ok(_) => println!("import_external: imported the linear frame as an image"),
        Err(ImportError::UnsupportedFormat { reason, .. }) => {
            println!("import_external: refused: {reason}")
        }
        Err(error) => println!("import_external: refused: {error}"),
    }
    assert_eq!(
        (first_bad, reuse_bad, texture_bad),
        (0, 0, 0),
        "the device must read the bytes the host sees in every frame"
    );
}
