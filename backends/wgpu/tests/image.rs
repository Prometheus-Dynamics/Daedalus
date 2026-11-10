//! Images in buffers copied into textures, at strides that copy straight across, strides that
//! fold, and strides that go row by row. And a dma-buf imported as the buffer.

mod common;

use daedalus_wgpu::image::{ImageCopy, ImageError, Plan};
use daedalus_wgpu::{Format, Options, Storage, TextureDesc, WgpuBackend};

fn private() -> Option<WgpuBackend> {
    let record = common::adapter()?;
    Some(WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }))
}

/// An image's bytes at `stride`, with junk in the padding, and the pixels alone.
fn image(width: u32, height: u32, pixel: u32, stride: u64, seed: u8) -> (Vec<u8>, Vec<u8>) {
    let row = (width * pixel) as usize;
    let mut padded = vec![0xEE; (stride * u64::from(height)) as usize];
    let mut pixels = Vec::with_capacity(row * height as usize);
    for y in 0..height as usize {
        for x in 0..row {
            let value = (x as u8)
                .wrapping_mul(7)
                .wrapping_add((y as u8).wrapping_mul(31))
                .wrapping_add(seed);
            padded[y * stride as usize + x] = value;
            pixels.push(value);
        }
    }
    (padded, pixels)
}

fn storage_of(backend: &WgpuBackend, bytes: &[u8], lead: usize) -> Storage {
    let size = (lead + bytes.len()).next_multiple_of(4) as u64;
    let storage = Storage::new(backend, size).expect("a buffer");
    let mut all = vec![0x55; size as usize];
    all[lead..lead + bytes.len()].copy_from_slice(bytes);
    storage.write(0, &all).expect("in range");
    storage
}

#[test]
fn storage_frames_copy_into_textures() {
    let Some(backend) = private() else { return };
    let cases = [
        // Rows already 256 apart: one copy.
        (Format::R8, 200, 6, 256, Plan::Direct),
        (Format::Rgba8, 64, 5, 512, Plan::Direct),
        // 640-byte GREY rows: two fold into 1280, and a pass unfolds them.
        (Format::R8, 640, 8, 640, Plan::Folded { rows: 2 }),
        (Format::R8, 600, 8, 640, Plan::Folded { rows: 2 }),
        (Format::Rgba8, 80, 8, 320, Plan::Folded { rows: 4 }),
        (Format::Rgba16Uint, 30, 4, 384, Plan::Folded { rows: 2 }),
        // Nothing folds a stride of 100, or an odd height: one copy a row.
        (Format::R8, 90, 5, 100, Plan::Rows),
        (Format::R8, 640, 7, 640, Plan::Rows),
    ];
    for (format, width, height, stride, plan) in cases {
        let desc = TextureDesc {
            format,
            width,
            height,
        };
        let copy = ImageCopy::new(&backend, desc, stride).expect("a copy");
        assert_eq!(copy.plan(), plan, "{format:?} {width}x{height} at {stride}");
        assert_eq!(copy.texture().desc(), desc);
        let read = |seed: u8, lead: usize| {
            let (padded, pixels) = image(width, height, format.bytes_per_pixel(), stride, seed);
            let buffer = storage_of(&backend, &padded, lead);
            copy.run(&buffer, lead as u64)
                .expect("copies")
                .wait()
                .expect("finishes");
            let mut seen = vec![0; pixels.len()];
            copy.texture().read_into(&mut seen).expect("a readback");
            assert_eq!(
                seen, pixels,
                "{format:?} {width}x{height} at {stride}, from {lead}"
            );
        };
        // The same copy runs for each image, from the start of a buffer or from inside it.
        read(1, 0);
        read(90, 512);
    }
}

#[test]
fn a_folded_copy_that_would_read_past_the_buffer_goes_row_by_row() {
    let Some(backend) = private() else { return };
    let desc = TextureDesc {
        format: Format::R8,
        width: 600,
        height: 8,
    };
    let copy = ImageCopy::new(&backend, desc, 640).expect("a copy");
    assert_eq!(copy.plan(), Plan::Folded { rows: 2 });
    // The buffer ends at the image's last pixel: the fold's last row would need its padding.
    let (padded, pixels) = image(600, 8, 1, 640, 3);
    let exact = &padded[..640 * 7 + 600];
    let buffer = storage_of(&backend, exact, 0);
    copy.run(&buffer, 0)
        .expect("copies")
        .wait()
        .expect("finishes");
    let mut seen = vec![0; pixels.len()];
    copy.texture().read_into(&mut seen).expect("a readback");
    assert_eq!(seen, pixels);

    // What doesn't fit is refused, and says what.
    let short = copy.run(&buffer, 8).map(drop);
    assert!(
        matches!(&short, Err(ImageError::Invalid(why)) if why.contains("doesn't fit")),
        "{short:?}"
    );
    let narrow = ImageCopy::new(&backend, desc, 500).map(drop);
    assert!(matches!(&narrow, Err(ImageError::Invalid(why)) if why.contains("shorter than a row")));
}

#[cfg(target_os = "linux")]
#[test]
fn frames_import_once_per_dmabuf_and_read_in_place() {
    use daedalus_dma_sys::{Access, DmaHeap};
    use daedalus_wgpu::dmabuf::{Fourcc, ImportError};
    use daedalus_wgpu::image::ExternalBuffer;

    let Some(backend) = private() else { return };
    let must = std::env::var_os("DAEDALUS_DMA_HEAP_REQUIRED").is_some();
    let (width, height, stride) = (640u32, 8u32, 640u64);
    let buf =
        match DmaHeap::open("system").and_then(|heap| heap.allocate(stride * u64::from(height))) {
            Ok(buf) => buf,
            Err(error) => {
                assert!(!must, "/dev/dma_heap/system: {error}");
                return;
            }
        };
    let map = buf.as_ref().map().expect("a mapping");
    let write = |seed: u8| {
        let (padded, pixels) = image(width, height, 1, stride, seed);
        buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
        map.write(0, &padded).expect("in range");
        buf.as_ref().end_cpu_access(Access::Write).expect("end");
        pixels
    };
    let describe = |fourcc: Fourcc, height: u32| ExternalBuffer {
        fd: buf.try_clone().expect("a second fd").into_fd(),
        width,
        height,
        fourcc,
        planes: vec![(0, stride)],
    };
    let grey = Fourcc::from_bytes(*b"GREY");
    let first = write(5);
    let imported = match backend.import_storage(describe(grey, height)) {
        Ok(imported) => imported,
        // lavapipe is here: it can't import a dma-buf.
        Err(ImportError::Unsupported(why)) => {
            assert!(!must, "{why}");
            return;
        }
        Err(error) => panic!("the import: {error}"),
    };
    assert_eq!(
        imported.planes,
        [(0, stride)],
        "each plane's place is reported as described"
    );

    // The camera's bytes become a texture with one folded copy, and nothing was copied to get
    // them onto the device.
    let desc = TextureDesc {
        format: Format::R8,
        width,
        height,
    };
    let copy = ImageCopy::new(&backend, desc, stride).expect("a copy");
    let mut seen = vec![0; first.len()];
    copy.run(&imported.storage, 0)
        .expect("copies")
        .wait()
        .expect("finishes");
    copy.texture().read_into(&mut seen).expect("a readback");
    assert_eq!(seen, first);
    // The next frame in the same buffer: the import is found, and the device reads the new
    // bytes in place.
    let second = write(200);
    let again = backend
        .import_storage(describe(grey, height))
        .expect("found");
    assert!(std::sync::Arc::ptr_eq(&again.storage, &imported.storage));
    copy.run(&again.storage, 0)
        .expect("copies")
        .wait()
        .expect("finishes");
    copy.texture().read_into(&mut seen).expect("a readback");
    assert_eq!(seen, second);
    let stats = backend.storage_import_stats().expect("open");
    assert_eq!((stats.imports, stats.hits, stats.evictions), (1, 1, 0));

    // What isn't pixels is refused, and so is a buffer too short for what it is said to hold.
    for raw in [b"RGGB", b"pBAA", b"MJPG", b"H264"] {
        let refused = backend.import_storage(describe(Fourcc::from_bytes(*raw), height));
        assert!(
            matches!(&refused, Err(ImportError::UnsupportedFormat { .. })),
            "{} was imported",
            Fourcc::from_bytes(*raw)
        );
    }
    let short = backend.import_storage(describe(Fourcc::R8, 64));
    assert!(
        matches!(&short, Err(ImportError::Invalid(why)) if why.contains("this device needs")),
        "{short:?}"
    );
}
