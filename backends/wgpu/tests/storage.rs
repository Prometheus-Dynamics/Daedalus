//! Storage buffers in each memory space, the readback pool and the scratch pool, on a real device.

mod common;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_wgpu::{
    Options, SPACE_DEVICE, SPACE_HOST_MAPPED, StagingStats, Storage, StorageError, StoragePool,
    Texture, TextureDesc, TextureError, WgpuBackend,
};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// A backend with a device of its own, so its counters are this test's alone.
fn private() -> Option<WgpuBackend> {
    let record = common::adapter()?;
    Some(WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }))
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at * 7 + 3) as u8).collect()
}

#[test]
fn host_mapped_storage_round_trips_without_staging() {
    let Some(backend) = private() else { return };
    for storage in [
        Storage::new_host_mapped(&backend, 4096).expect("host-mapped"),
        Storage::new_host_cached(&backend, 4096).expect("host-cached"),
    ] {
        if !storage.is_host_mapped() {
            // Off Vulkan there is no mapping, and the buffer is a plain device one.
            assert_eq!(storage.space(), SPACE_DEVICE);
            continue;
        }
        assert_ne!(storage.space(), SPACE_DEVICE);
        storage.write(0, &pattern(4096)).expect("in range");
        let mut seen = vec![0; 1024];
        storage.read_into(512, &mut seen).expect("in range");
        assert_eq!(seen, pattern(4096)[512..1536]);
    }
    let stats = backend.staging_stats().expect("a device is open");
    assert_eq!(stats, StagingStats::default(), "no staging buffer was used");
}

#[test]
fn uninit_storage_takes_kernel_output() {
    let Some(backend) = private() else { return };
    let storage = Storage::new_uninit(&backend, 1 << 16).expect("uninit");
    assert_eq!((storage.size(), storage.space()), (1 << 16, SPACE_DEVICE));
    assert!(!storage.is_host_mapped());
    // The producer writes all of it before anything reads it.
    storage.write(0, &pattern(1 << 16)).expect("in range");
    let mut seen = vec![0; 1 << 16];
    storage.read_into(0, &mut seen).expect("a readback");
    assert_eq!(seen, pattern(1 << 16));
}

#[test]
fn staging_pool_reuses_readback_buffers_and_tracks_stats() {
    let Some(backend) = private() else { return };
    let storage = Storage::new(&backend, 4096).expect("a buffer");
    let mut seen = vec![1; 4096];
    storage.read_into(0, &mut seen).expect("a readback");
    assert_eq!(seen, vec![0; 4096], "a new buffer is zeroed");
    storage.write(8, &[5, 6, 7, 8]).expect("in range");
    storage.read_into(0, &mut seen).expect("a readback");
    assert_eq!(seen[8..12], [5, 6, 7, 8]);
    // A much smaller read doesn't map the 4 KiB buffer.
    storage.read_into(8, &mut seen[..4]).expect("a readback");
    assert_eq!(seen[..4], [5, 6, 7, 8]);
    let stats = backend.staging_stats().expect("a device is open");
    assert_eq!((stats.misses, stats.hits, stats.returned), (2, 1, 3));
    assert_eq!((stats.size_classes, stats.pooled_buffers), (2, 2));
    assert_eq!(stats.pooled_bytes, 4096 + 8);

    let outside = storage.read_into(4092, &mut seen[..8]);
    assert!(matches!(outside, Err(StorageError::Range { .. })));
    assert!(matches!(
        storage.write(2, &[0; 4]),
        Err(StorageError::Range { .. })
    ));
    assert!(matches!(
        Storage::new(&backend, 6),
        Err(StorageError::Size { .. })
    ));
    assert!(matches!(
        Storage::new_uninit(&backend, 0),
        Err(StorageError::Size { .. })
    ));
}

#[test]
fn storage_pool_reuses_within_its_cap() {
    let Some(backend) = private() else { return };
    let pool = StoragePool::new(&backend, 4096).expect("a pool");
    assert_eq!(
        pool.fill(1000, 8).expect("buffers"),
        4,
        "the cap holds four 1 KiB buffers"
    );
    assert!(pool.take(4096 * 4).is_none(), "nothing of that class");
    assert!(pool.take(6).is_none(), "not a multiple of 4");

    // Warm-up, then nothing on the take and give path allocates.
    for _ in 0..8 {
        let taken = pool.take(1000).expect("a filled class");
        assert_eq!(taken.size(), 1000);
        pool.give(taken);
    }
    let ((), allocated) = counted(|| {
        for _ in 0..1000 {
            let (first, second) = (pool.take(600), pool.take(1024));
            if let (Some(first), Some(second)) = (first, second) {
                pool.give(first);
                pool.give(second);
            }
        }
    });
    assert_eq!(allocated, 0);
    let stats = pool.stats();
    assert_eq!((stats.created, stats.reused, stats.dropped), (4, 2008, 0));
    assert_eq!((stats.missed, stats.pooled_bytes), (1, 4096));

    // A give beyond the cap is dropped and counted.
    let extra = Storage::new(&backend, 1024).expect("a buffer");
    assert_eq!(extra.device_key(), pool.device_key());
    pool.give(extra);
    assert_eq!(pool.stats().dropped, 1);
    pool.clear();
    assert_eq!(pool.stats().pooled_bytes, 0);
    assert!(pool.take(1000).is_none());
}

#[test]
fn textures_are_made_at_setup_and_checked() {
    let Some(backend) = private() else { return };
    for format in daedalus_wgpu::Format::ALL {
        let desc = TextureDesc {
            format,
            width: 16,
            height: 8,
        };
        let texture = Texture::new(&backend, desc).expect("a texture");
        assert_eq!(texture.desc(), desc);
    }
    let huge = TextureDesc {
        format: daedalus_wgpu::Format::Rgba8,
        width: u32::MAX,
        height: 1,
    };
    assert!(matches!(
        Texture::new(&backend, huge),
        Err(TextureError::Size { .. })
    ));
}

#[test]
fn the_backend_names_its_memory_spaces() {
    use daedalus_backend::Backend;
    let spaces = WgpuBackend::open(Options::default()).memory();
    let ids: Vec<&str> = spaces.iter().map(|space| &*space.id).collect();
    assert_eq!(
        ids,
        [
            SPACE_DEVICE,
            SPACE_HOST_MAPPED,
            "wgpu.cached",
            "wgpu.dmaheap"
        ]
    );
}

/// Copies `src` to `dst` on the device.
static COPY: daedalus_wgpu::shader::ShaderSpec = {
    use daedalus_wgpu::shader::{BindingKind, BindingSpec, Dispatch, ShaderSpec};
    ShaderSpec {
        name: "copy",
        entry: "main",
        source: "
            @group(0) @binding(0) var<storage, read> src: array<u32>;
            @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
            @compute @workgroup_size(64)
            fn main(@builtin(global_invocation_id) id: vec3<u32>) {
                if id.x < arrayLength(&dst) { dst[id.x] = src[id.x]; }
            }
        ",
        workgroup: [64, 1, 1],
        bindings: &[
            BindingSpec {
                index: 0,
                name: "src",
                kind: BindingKind::Storage {
                    read_only: true,
                    stride: 4,
                },
            },
            BindingSpec {
                index: 1,
                name: "dst",
                kind: BindingKind::Storage {
                    read_only: false,
                    stride: 4,
                },
            },
        ],
        dispatch: Dispatch::PerElement {
            binding: 1,
            stride: 4,
        },
        inputs: &[],
        outputs: &[],
    }
};

#[test]
fn a_dma_heap_buffer_is_one_memory_for_the_host_and_the_device() {
    use daedalus_wgpu::shader::{Bound, Run, Shader};
    let go = |shader: &Shader, bound: &[Bound<'_>]| loop {
        match shader.run(bound).expect("runs") {
            Run::Started(ticket) => break ticket.wait().expect("finishes"),
            Run::Busy(oldest) => oldest.wait().expect("finishes"),
        }
    };
    let Some(backend) = private() else { return };
    let heap = match Storage::new_dma_heap(&backend, 1 << 16) {
        Ok(heap) => heap,
        // No heap on this machine, or a device like lavapipe that can't import one. A board run
        // sets the variable to make that a failure.
        Err(StorageError::Unsupported(what)) => {
            assert!(
                std::env::var_os("DAEDALUS_DMA_HEAP_REQUIRED").is_none(),
                "{what}"
            );
            return;
        }
        Err(other) => panic!("a dma-heap buffer: {other}"),
    };
    assert_eq!(heap.space(), daedalus_wgpu::SPACE_DMA_HEAP);
    assert!(heap.is_host_mapped());
    // The host writes, the device reads it in place and copies it out.
    heap.write(0, &pattern(1 << 16)).expect("in range");
    let copy = Storage::new(&backend, 1 << 16).expect("a buffer");
    let shader = Shader::load(&backend, &COPY).expect("loads");
    go(&shader, &[Bound::Storage(&heap), Bound::Storage(&copy)]);
    let mut seen = vec![0; 1 << 16];
    copy.read_into(0, &mut seen).expect("a readback");
    assert_eq!(seen, pattern(1 << 16));

    // The device writes, and the host reads it in place once the work is done.
    let source = Storage::new(&backend, 1 << 16).expect("a buffer");
    let reversed: Vec<u8> = pattern(1 << 16).into_iter().rev().collect();
    source.write(0, &reversed).expect("in range");
    go(&shader, &[Bound::Storage(&source), Bound::Storage(&heap)]);
    source
        .read_into(0, &mut seen[..4])
        .expect("waits for the device's work");
    heap.read_into(0, &mut seen).expect("in range");
    assert_eq!(seen, reversed);
}
