//! Images in buffers copied into textures, in each plan, and a dma-buf imported as a buffer: the
//! first import, and the cache hit that the next import of the same buffer finds.

use daedalus_dma_sys::DmaHeap;
use daedalus_wgpu::dmabuf::Fourcc;
use daedalus_wgpu::image::{ExternalBuffer, ImageCopy};
use daedalus_wgpu::{Format, Storage, TextureDesc, WgpuBackend};

use super::{RUNS, WARM, median, ms, pattern, word_size};

/// One image copy, with the stride its buffer has: the plan is what the copy takes.
fn copy_case(
    backend: &WgpuBackend,
    label: &str,
    format: Format,
    width: u32,
    height: u32,
    stride: u64,
) {
    let copy = ImageCopy::new(
        backend,
        TextureDesc {
            format,
            width,
            height,
        },
        stride,
    )
    .expect("a copy");
    let len = stride * u64::from(height);
    let buffer = Storage::new(backend, word_size(len)).expect("a buffer");
    buffer
        .write(0, &pattern(len as usize, 5))
        .expect("in range");
    let time = median(WARM, RUNS, || {
        copy.run(&buffer, 0)
            .expect("copies")
            .wait()
            .expect("finishes");
    });
    println!(
        "image copy {label} {width}x{height} at stride {stride}: plan {:?}, {:.3} ms",
        copy.plan(),
        ms(time)
    );
}

pub fn run(backend: &WgpuBackend) {
    copy_case(backend, "XRGB (Rgba8)", Format::Rgba8, 640, 480, 640 * 4);
    copy_case(backend, "XRGB (Rgba8)", Format::Rgba8, 1280, 720, 1280 * 4);
    copy_case(backend, "GREY", Format::R8, 640, 480, 640);
    copy_case(backend, "GREY", Format::R8, 640, 480, 768);
    // A stride that nothing folds: row by row.
    copy_case(backend, "GREY", Format::R8, 640, 480, 644);
    import(backend);
}

/// The first import of a dma-buf, then the cache hit of the same buffer again.
fn import(backend: &WgpuBackend) {
    let (width, height, stride) = (640u32, 480u32, 640u64);
    let len = stride * u64::from(height);
    // V3D reads 256 bytes past the last plane, so an import needs that slack after the image.
    let slack = 256;
    let buf = match DmaHeap::open("system").and_then(|heap| heap.allocate(len + slack)) {
        Ok(buf) => buf,
        Err(error) => {
            println!("import storage: skipped, /dev/dma_heap/system: {error}");
            return;
        }
    };
    let describe = |buf: &daedalus_dma_sys::DmaBuf| ExternalBuffer {
        fd: buf.try_clone().expect("a second fd").into_fd(),
        width,
        height,
        fourcc: Fourcc::R8,
        planes: vec![(0, stride)],
    };
    // The same image in a buffer with exactly its own length is refused, and the message says
    // how long the buffer needs to be.
    let tight = DmaHeap::open("system").and_then(|heap| heap.allocate(len));
    if let Ok(tight) = tight {
        match backend.import_storage(describe(&tight)) {
            Ok(_) => println!("import storage, exact length {len}: imported"),
            Err(error) => println!("import storage, exact length {len}: refused: {error}"),
        }
    }
    let first = match backend.import_storage(describe(&buf)) {
        Ok(imported) => imported,
        Err(error) => {
            println!("import storage: refused: {error}");
            return;
        }
    };
    let hit = median(WARM, RUNS, || {
        let again = backend.import_storage(describe(&buf)).expect("found");
        std::hint::black_box(&again);
    });
    let again = backend.import_storage(describe(&buf)).expect("found");
    let same = std::sync::Arc::ptr_eq(&first.storage, &again.storage);
    println!(
        "import storage, cache hit of a {width}x{height} GREY dma-buf: {:.4} ms ({:.1} us), same storage: {same}",
        ms(hit),
        hit.as_secs_f64() * 1e6
    );
    // Rewrite the buffer from the host, then read it through the cached import: does the device
    // see the new bytes? The host's view of the buffer is the reference.
    let kernels = daedalus_wgpu::kernels::Kernels::new(backend).expect("a device");
    let out = daedalus_wgpu::Storage::new(backend, len).expect("a buffer");
    let map = buf.as_ref().map().expect("a mapping");
    for seed in [1u8, 2, 3] {
        let fresh: Vec<u8> = pattern(len as usize, u32::from(seed) + 40);
        buf.as_ref()
            .begin_cpu_access(daedalus_dma_sys::Access::Write)
            .expect("begin");
        map.write(0, &fresh).expect("in range");
        buf.as_ref()
            .end_cpu_access(daedalus_dma_sys::Access::Write)
            .expect("end");
        kernels
            .run(
                daedalus_wgpu::kernels::Kernel::Copy,
                &first.storage,
                &out,
                len,
            )
            .expect("runs")
            .wait()
            .expect("finishes");
        let mut seen = vec![0u8; len as usize];
        out.read_into(0, &mut seen).expect("a readback");
        println!(
            "rewrite {seed} of the dma-heap buffer: the device reads the new bytes through the cached import: {}",
            seen == fresh
        );
    }
    if let Some(stats) = backend.storage_import_stats() {
        println!("storage import stats: {stats:?}");
    }
}
