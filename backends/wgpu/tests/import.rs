//! Importing external memory as a texture, against a real device and real dma-bufs from the
//! system heap. A device that can't import (lavapipe) and a machine with no heap each say so, and
//! the tests that need them stop there. `DAEDALUS_DMA_HEAP_REQUIRED=1` makes that a failure.

#![cfg(target_os = "linux")]

mod common;

use std::os::fd::OwnedFd;
use std::sync::Arc;

use daedalus_dma_sys::{Access, DmaBuf, DmaHeap};
use daedalus_wgpu::dmabuf::{ExternalImage, ExternalPlane, Fourcc, ImportError, MOD_LINEAR};
use daedalus_wgpu::fragment::FragmentPass;
use daedalus_wgpu::{Format, Options, Texture, WgpuBackend};

fn private() -> Option<WgpuBackend> {
    let record = common::adapter()?;
    Some(WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }))
}

fn must_import() -> bool {
    std::env::var_os("DAEDALUS_DMA_HEAP_REQUIRED").is_some()
}

/// A dma-buf of `len` bytes from the system heap, or `None` where there is no heap to use.
fn heap_buffer(len: u64) -> Option<DmaBuf> {
    match DmaHeap::open("system").and_then(|heap| heap.allocate(len)) {
        Ok(buf) => Some(buf),
        Err(error) => {
            assert!(!must_import(), "/dev/dma_heap/system: {error}");
            None
        }
    }
}

fn plane(buf: &DmaBuf, offset: u64, stride: u64) -> ExternalPlane {
    ExternalPlane {
        fd: buf.try_clone().expect("a second fd").into_fd(),
        offset,
        stride,
    }
}

/// Whether an error means this device or driver doesn't do this import at all.
fn unsupported(error: &ImportError) -> bool {
    let cannot = matches!(
        error,
        ImportError::Unsupported(_) | ImportError::UnsupportedFormat { .. }
    );
    assert!(!(cannot && must_import()), "{error}");
    cannot
}

#[test]
fn dmabuf_import_is_zero_copy() {
    let Some(backend) = private() else { return };
    let (width, height, stride) = (64u32, 16u32, 256u64);
    let Some(buf) = heap_buffer(stride * u64::from(height)) else {
        return;
    };
    let map = buf.as_ref().map().expect("a mapping");
    let write = |seed: u8| {
        let pixels: Vec<u8> = (0..stride as usize * height as usize)
            .map(|at| (at as u8).wrapping_mul(3).wrapping_add(seed))
            .collect();
        buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
        map.write(0, &pixels).expect("in range");
        buf.as_ref().end_cpu_access(Access::Write).expect("end");
        pixels
    };
    let first = write(1);

    let describe = || {
        let mut image = ExternalImage::new(
            width,
            height,
            Fourcc::ABGR8888,
            vec![plane(&buf, 0, stride)],
        );
        image.modifier = Some(MOD_LINEAR);
        image
    };
    let texture = match backend.import_external(describe()) {
        Ok(texture) => texture,
        Err(error) if unsupported(&error) => return,
        Err(error) => panic!("the import: {error}"),
    };
    let desc = texture.desc();
    assert_eq!(
        (desc.format, desc.width, desc.height),
        (Format::Rgba8, width, height)
    );
    assert!(texture.chroma().is_none());
    let mut seen = vec![0; first.len()];
    texture.read_into(&mut seen).expect("a readback");
    assert_eq!(seen, first, "the device reads the host's bytes");

    // No copy was made: what the host writes now is what the same texture holds.
    let second = write(77);
    texture.read_into(&mut seen).expect("a readback");
    assert_eq!(seen, second, "the texture is the dma-buf, not a copy of it");

    // The same memory described the same way is found, not imported again.
    let again = backend.import_external(describe()).expect("found");
    assert!(Arc::ptr_eq(&again, &texture));
    let stats = backend.import_stats().expect("open");
    assert_eq!((stats.imports, stats.hits, stats.evictions), (1, 1, 0));
    // Described another way, it is another import of the same memory.
    let mut narrow = describe();
    narrow.width = 32;
    let other = backend.import_external(narrow).expect("imports");
    assert!(!Arc::ptr_eq(&other, &texture));
    assert_eq!(backend.import_stats().expect("open").imports, 2);
}

#[test]
fn dmabuf_import_nv12_as_one_texture() {
    let Some(backend) = private() else { return };
    // A width whose rows are already as a driver aligns them, so the layout is one any takes.
    let (width, height) = (256u32, 64u32);
    let stride = u64::from(width);
    let luma = stride * u64::from(height);
    // The image is small, and a driver may still want a large buffer for NV12.
    let Some(buf) = heap_buffer(256 << 10) else {
        return;
    };
    let map = buf.as_ref().map().expect("a mapping");
    let y: Vec<u8> = (0..luma).map(|at| (at % 251) as u8).collect();
    let uv: Vec<u8> = (0..luma / 2).map(|at| (at % 199) as u8 + 20).collect();
    buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
    map.write(0, &y).expect("in range");
    map.write(luma, &uv).expect("in range");
    buf.as_ref().end_cpu_access(Access::Write).expect("end");

    let mut image = ExternalImage::new(
        width,
        height,
        Fourcc::NV12,
        vec![plane(&buf, 0, stride), plane(&buf, luma, stride)],
    );
    image.modifier = Some(MOD_LINEAR);
    let texture = match backend.import_external(image) {
        Ok(texture) => texture,
        Err(error) if unsupported(&error) => return,
        Err(error) => panic!("the import: {error}"),
    };
    // One image with two planes: this is Y, and its chroma is half the size each way.
    let desc = texture.desc();
    assert_eq!(
        (desc.format, desc.width, desc.height),
        (Format::R8, width, height)
    );
    let chroma = texture.chroma().expect("the UV plane").desc();
    assert_eq!(
        (chroma.format, chroma.width, chroma.height),
        (Format::Rg8, 128, 32)
    );

    // Each plane sampled through a pass and copied out holds what the host wrote.
    let copy = "@group(0) @binding(0) var src: texture_2d<f32>;
        @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {
            return textureLoad(src, vec2<i32>(pos.xy), 0);
        }";
    let sampled = |plane: &Texture| {
        let desc = plane.desc();
        let target = Texture::new(&backend, desc).expect("a target");
        let pass = FragmentPass::new(&backend, copy, &[desc.format]).expect("a pass");
        let binding = pass.bind(&[plane], &[]).expect("binds");
        pass.run_bound(&binding, &[&target])
            .expect("draws")
            .wait()
            .expect("finishes");
        let mut out = vec![0; (desc.width * desc.height * desc.format.bytes_per_pixel()) as usize];
        target.read_into(&mut out).expect("a readback");
        out
    };
    assert_eq!(sampled(&texture), y, "the Y plane");
    assert_eq!(
        sampled(texture.chroma().expect("the UV plane")),
        uv,
        "the UV plane"
    );
}

#[test]
fn dmabuf_import_rejects_non_dmabuf_and_short_buffers() {
    let Some(backend) = private() else { return };
    let not_a_dma_buf = || OwnedFd::from(std::fs::File::open("/dev/zero").expect("/dev/zero"));
    let image = |fd: OwnedFd, height: u32| {
        let plane = ExternalPlane {
            fd,
            offset: 0,
            stride: 256,
        };
        let mut image = ExternalImage::new(64, height, Fourcc::ABGR8888, vec![plane]);
        image.modifier = Some(MOD_LINEAR);
        image
    };
    // Something that isn't a dma-buf is refused with a reason, whatever the device.
    let refused = backend
        .import_external(image(not_a_dma_buf(), 16))
        .map(drop);
    assert!(refused.is_err(), "/dev/zero imported as an image");

    // A description that doesn't hold together is refused before any fd is looked at.
    let mut bad = image(not_a_dma_buf(), 16);
    bad.planes[0].stride = 255;
    let error = backend.import_external(bad).map(drop).unwrap_err();
    assert!(
        matches!(&error, ImportError::Invalid(why) if why.contains("stride of 255")),
        "{error}"
    );
    let mut fence = image(not_a_dma_buf(), 16);
    fence.acquire_fence = Some(not_a_dma_buf());
    // A fence that isn't a sync_file never reaches the driver. Which check fires first depends
    // on the device, and both are refusals.
    assert!(backend.import_external(fence).is_err());

    // A real dma-buf too short for the image is refused with the size that is needed.
    let Some(buf) = heap_buffer(4096) else { return };
    let short = backend
        .import_external(image(buf.try_clone().expect("an fd").into_fd(), 64))
        .map(drop);
    match short {
        Err(ImportError::Invalid(why)) => assert!(why.contains("needs 16384"), "{why}"),
        Err(error) if unsupported(&error) => {}
        other => panic!("a short buffer was accepted: {other:?}"),
    }
}

/// A value that is an image in a dma-buf, as a camera's frame type would be.
struct Frame {
    buf: DmaBuf,
    width: u32,
    height: u32,
    stride: u64,
}

impl daedalus_types::Typed for Frame {
    const KEY: daedalus_types::TypeKey = daedalus_types::TypeKey::new("test.frame");
    const EXPR: daedalus_types::TypeExpr = daedalus_types::TypeExpr::Opaque(Self::KEY);
}

impl daedalus_wgpu::dmabuf::ImportExternal for Frame {
    fn describe(&self) -> Result<ExternalImage, ImportError> {
        let mut image = ExternalImage::new(
            self.width,
            self.height,
            Fourcc::R8,
            vec![plane(&self.buf, 0, self.stride)],
        );
        image.modifier = Some(MOD_LINEAR);
        // The producer's own fence: whatever is still writing the buffer.
        image.acquire_fence = self
            .buf
            .as_ref()
            .export_fence(Access::Read)
            .ok()
            .map(daedalus_dma_sys::SyncFence::into_fd);
        Ok(image)
    }
}

#[test]
fn a_type_that_describes_itself_is_imported_by_its_transfer() {
    use daedalus_abi::testing::{release, run_once};
    use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
    use daedalus_backend::{CompileCx, TransferCost};
    use daedalus_runner::{put_typed, take_typed};
    use daedalus_types::{SpaceIndex, TypeIndex, Typed};

    let Some(backend) = private() else { return };
    backend.start().expect("opens");
    let transfer = daedalus_wgpu::dmabuf::import_transfer::<Frame>("test.import", "test.frames");
    assert_eq!(transfer.key, Some(Frame::KEY));
    assert_eq!(
        (&*transfer.from, &*transfer.to),
        ("test.frames", daedalus_wgpu::SPACE_TEXTURE)
    );
    assert_eq!(transfer.cost, TransferCost::Import);
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 1,
        merge_allowed: true,
        freeze_params: false,
    };
    let mut step = (transfer.make)(&cx).expect("a step");

    // What isn't a `Frame` faults, and says so.
    let mut cells = [Slot::EMPTY; 2];
    let mut step_cx = StepCx::new();
    let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut step_cx);
    assert!(matches!(end, StepEnd::Fault));
    assert!(
        step_cx.reason().contains("another type"),
        "{}",
        step_cx.reason()
    );

    let Some(buf) = heap_buffer(256 * 16) else {
        release(step);
        return;
    };
    let pixels: Vec<u8> = (0..256 * 16).map(|at| (at * 7) as u8).collect();
    let map = buf.as_ref().map().expect("a mapping");
    buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
    map.write(0, &pixels).expect("in range");
    buf.as_ref().end_cpu_access(Access::Write).expect("end");
    let frame = Arc::new(Frame {
        buf,
        width: 256,
        height: 16,
        stride: 256,
    });
    for round in 0..3 {
        let mut cells = [Slot::EMPTY; 2];
        put_typed(
            &mut cells[0],
            TypeIndex(0),
            SpaceIndex(0),
            Arc::clone(&frame),
        );
        let mut step_cx = StepCx::new();
        let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut step_cx);
        if matches!(end, StepEnd::Fault) {
            // A device that can't import says so through the step's fault.
            assert!(!must_import(), "{}", step_cx.reason());
            break;
        }
        assert!(matches!(end, StepEnd::Done));
        let texture = take_typed::<Texture>(&mut cells[1]).expect("a texture");
        assert_eq!(texture.desc().format, Format::R8);
        // The frame's texture holds the frame: the producer's buffer isn't free while a reader
        // still has the texture.
        assert!(texture.holds_its_source());
        assert_eq!(Arc::strong_count(&frame), 2, "ours, and the texture's");
        let mut seen = vec![0; pixels.len()];
        texture.read_into(&mut seen).expect("a readback");
        assert_eq!(seen, pixels, "round {round}");
    }
    // With the last reader gone the frame is the producer's again, though the import is kept.
    assert_eq!(Arc::strong_count(&frame), 1);
    // Three frames of one buffer are one import.
    let stats = backend.import_stats().expect("open");
    assert!(
        stats.imports <= 1 && stats.imports + stats.hits <= 3,
        "{stats:?}"
    );
    release(step);
}
