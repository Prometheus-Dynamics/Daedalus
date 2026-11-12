//! The timings the board procedures asked for that had no harness: the fragment form of an element
//! chain beside its buffer forms at 1280x800, the busy answer of a run, and the host-cached
//! buffer at 16 MiB. Each is the median of the runs after warm-up.

use std::sync::Arc;
use std::time::Instant;

use daedalus_macros::node;
use daedalus_registry::NodeRecord;
use daedalus_wgpu::merge::{ElementSpec, chain_shader, fragment_pass};
use daedalus_wgpu::shader::{Bound, Run};
use daedalus_wgpu::{Format, Storage, Texture, TextureDesc, WgpuBackend};
use daedalus_wgpu_conformance::{Net, filled, port, run};

use super::{MIB, RUNS, WARM, median, median_of, ms, us};

/// The target the element forms run on, as the procedures name it.
const WIDTH: u32 = 1280;
const HEIGHT: u32 = 800;

#[node(id = "gaps.inc4", inputs(c), outputs(out), backend = daedalus_wgpu_author(element, c: u32, out: u32))]
const INC4: &str = "fn apply(c: vec4<u32>) -> vec4<u32> { return c + vec4<u32>(1u, 2u, 3u, 4u); }";

#[node(id = "gaps.twice4", inputs(c), outputs(out), backend = daedalus_wgpu_author(element, c: u32, out: u32))]
const TWICE4: &str = "fn apply(c: vec4<u32>) -> vec4<u32> { return c * 2u; }";

/// `count` nodes, alternating: each is an input of the next.
fn nodes(count: usize) -> Vec<&'static NodeRecord> {
    (0..count)
        .map(|at| {
            if at % 2 == 0 {
                &INC4_NODE
            } else {
                &TWICE4_NODE
            }
        })
        .collect()
}

fn spec(record: &'static NodeRecord) -> &'static ElementSpec {
    (record.body.code as &dyn std::any::Any)
        .downcast_ref()
        .expect("an element body")
}

/// The element forms of chains of 4 and 8 nodes on a 1280x800 target: one fragment pass over a
/// texture, one merged compute pass over a buffer, and one compute region per node.
pub(crate) fn element_forms(backend: &WgpuBackend) {
    let elements = (WIDTH * HEIGHT) as usize;
    for count in [4usize, 8] {
        let records = nodes(count);
        // Compute: one u32 per word, four words an element.
        let words_in: Vec<u32> = (0..elements * 4)
            .map(|at| (at as u32).wrapping_mul(7) & 0xfff)
            .collect();
        let input = filled(backend, &words_in);
        let mut merged = Net::line(
            &records
                .iter()
                .map(|record| record.body.code as &'static (dyn std::any::Any + Send + Sync))
                .collect::<Vec<_>>(),
        )
        .compile(backend, &[port(count as u32 - 1, 0)])
        .expect("compiles");
        let compute_merged = median(WARM, RUNS, || {
            let ran = run(
                backend,
                &mut merged,
                &[daedalus_wgpu_conformance::In::Buffer(&input)],
                1,
            );
            let out = ran.outputs.into_iter().next().flatten().expect("an output");
            // A four-byte read waits for the device's work, as the fragment's wait does.
            let mut first = [0u8; 4];
            out.read_into(0, &mut first).expect("finishes");
            std::hint::black_box(first);
        });

        // One region per node: each node's output is the next node's input, every region run and
        // waited for before the next.
        let mut units: Vec<_> = records
            .iter()
            .map(|record| {
                Net::line(&[record.body.code as &'static (dyn std::any::Any + Send + Sync)])
                    .compile(backend, &[port(0, 0)])
                    .expect("compiles")
            })
            .collect();
        let compute_apart = median(WARM, RUNS, || {
            let mut current = Arc::clone(&input);
            for unit in units.iter_mut() {
                let ran = run(
                    backend,
                    unit,
                    &[daedalus_wgpu_conformance::In::Buffer(&current)],
                    1,
                );
                current = ran.outputs.into_iter().next().flatten().expect("an output");
            }
            let mut first = [0u8; 4];
            current.read_into(0, &mut first).expect("finishes");
            std::hint::black_box(first);
        });

        // Fragment: the same chain drawn over a texture of the same elements.
        let chain: Vec<&ElementSpec> = records.iter().map(|record| spec(record)).collect();
        let pass = fragment_pass(backend, &chain, Format::Rgba16Uint).expect("a pass");
        let desc = TextureDesc {
            format: Format::Rgba16Uint,
            width: WIDTH,
            height: HEIGHT,
        };
        let source = Texture::new(backend, desc).expect("a texture");
        let target = Texture::new(backend, desc).expect("a texture");
        let texels: Vec<u8> = words_in
            .iter()
            .flat_map(|&word| (word as u16).to_le_bytes())
            .collect();
        source.write(&texels, WIDTH * 8).expect("valid bytes");
        let binding = pass.bind(&[&source], &[]).expect("binds");
        let fragment = median(WARM, RUNS, || {
            pass.run_bound(&binding, &[&target])
                .expect("draws")
                .wait()
                .expect("finishes");
        });
        println!(
            "element chain of {count} nodes, {WIDTH}x{HEIGHT}: fragment pass {:.3} ms; merged compute pass {:.3} ms; {count} compute regions {:.3} ms",
            ms(fragment),
            ms(compute_merged),
            ms(compute_apart)
        );
    }
}

/// The busy answer of a run: a shader with a run in flight answers `Busy` to the next run, and
/// the time that answer takes is what a step pays when the device is full.
pub fn busy(backend: &WgpuBackend) {
    let n = 4 * MIB / 4;
    let src = Storage::new(backend, n * 4).expect("a buffer");
    let dst = Storage::new(backend, n * 4).expect("a buffer");
    src.write(0, &vec![1u8; (n * 4) as usize])
        .expect("in range");
    let shader = chain_shader(backend, &[&daedalus_wgpu_conformance::TRIPLE]).expect("merges");
    let mut per_call = Vec::with_capacity(RUNS);
    let mut answered = 0usize;
    for _ in 0..(WARM + RUNS) {
        // A run that is answered Busy is retried until one starts.
        let ticket = loop {
            match shader
                .run(&[Bound::Storage(&src), Bound::Storage(&dst)])
                .expect("runs")
            {
                Run::Started(ticket) => break ticket,
                Run::Busy(_) => std::thread::yield_now(),
            }
        };
        let calls = 1000usize;
        let start = Instant::now();
        let mut busy_now = 0usize;
        for _ in 0..calls {
            if let Run::Busy(_) = shader
                .run(&[Bound::Storage(&src), Bound::Storage(&dst)])
                .expect("runs")
            {
                busy_now += 1;
            }
        }
        let took = start.elapsed();
        ticket.wait().expect("finishes");
        answered += busy_now;
        per_call.push(took / calls as u32);
    }
    let per = median_of(&mut per_call[WARM..].to_vec());
    println!(
        "busy answer: {:.3} us per call (median of {RUNS}); {answered} of {} calls were answered Busy",
        us(per),
        (WARM + RUNS) * 1000
    );
}

/// The host-cached buffer at 16 MiB: the host writes it and reads it back.
pub(crate) fn host_cached(backend: &WgpuBackend) {
    let bytes = 16 * MIB as usize;
    let payload = vec![7u8; bytes];
    let mut out = vec![0u8; bytes];
    let made = Storage::new_host_cached(backend, bytes as u64);
    let Ok(storage) = made else {
        println!("host-cached 16 MiB: not made");
        return;
    };
    println!("host-cached 16 MiB buffer is in space {}", storage.space());
    let write = median(WARM, RUNS, || storage.write(0, &payload).expect("in range"));
    let read = median(WARM, RUNS, || {
        storage.read_into(0, &mut out).expect("a readback")
    });
    println!(
        "host-cached 16 MiB: host write {:.3} ms, host read {:.3} ms",
        ms(write),
        ms(read)
    );
}

/// The 16 MiB dma-heap read, split: the storage's own read, a raw read of the same kind of
/// memory with no device buffer on it, and the raw read again while the device buffer is alive.
/// The order is part of the measurement: the device buffer may change what the host reads cost.
#[cfg(target_os = "linux")]
pub(crate) fn heap_read(backend: &WgpuBackend) {
    use daedalus_dma_sys::{Access, DmaHeap};
    let bytes = 16 * MIB as usize;
    let payload = vec![9u8; bytes];
    let mut out = vec![0u8; bytes];
    let Ok(storage) = Storage::new_dma_heap(backend, bytes as u64) else {
        println!("heap read: the device can't import a dma-heap buffer");
        return;
    };
    storage.write(0, &payload).expect("in range");
    let storage_read = median(WARM, RUNS, || {
        storage.read_into(0, &mut out).expect("a readback")
    });
    // A four-byte read is the storage's sync, its begin and end of CPU access, and four bytes of copy.
    let storage_sync = median(WARM, RUNS, || {
        storage.read_into(0, &mut out[..4]).expect("a sync")
    });
    println!(
        "16 MiB dma-heap storage one-byte read (sync and four bytes): {:.3} ms",
        ms(storage_sync)
    );
    let Ok(heap) = DmaHeap::open("system") else {
        println!("heap read: skipped, no /dev/dma_heap/system");
        return;
    };
    let mut raw_read = |label: &str| {
        let buf = heap.allocate(bytes as u64).expect("a dma-heap buffer");
        let map = buf.as_ref().map().expect("a mapping");
        buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
        map.write(0, &payload).expect("in range");
        buf.as_ref().end_cpu_access(Access::Write).expect("end");
        let read = median(WARM, RUNS, || {
            buf.as_ref().begin_cpu_access(Access::Read).expect("begin");
            map.read(0, &mut out).expect("in range");
            buf.as_ref().end_cpu_access(Access::Read).expect("end");
        });
        let sync = median(WARM, RUNS, || {
            buf.as_ref().begin_cpu_access(Access::Read).expect("begin");
            buf.as_ref().end_cpu_access(Access::Read).expect("end");
        });
        println!("16 MiB dma-heap raw read ({label}): {:.3} ms", ms(read));
        println!(
            "16 MiB dma-heap raw sync only ({label}): {:.3} ms",
            ms(sync)
        );
    };
    println!(
        "16 MiB dma-heap storage read_into: {:.3} ms",
        ms(storage_read)
    );
    // The same read again, after the one-byte reads, as a check on what the first read left behind.
    // The reference reads after a GPU kernel has written the imported buffer. Here the device
    // writes the storage (a copy from a host-written source), then the same sync and read run.
    let copy = daedalus_wgpu_conformance::Op::new(backend, &daedalus_wgpu_conformance::COPY);
    let source = Storage::new(backend, bytes as u64).expect("a buffer");
    source.write(0, &payload).expect("in range");
    let gpu_write = |storage: &Storage| {
        copy.run(&source, storage).wait().expect("finishes");
    };
    gpu_write(&storage);
    let timed_after_gpu = |bytes_out: &mut [u8]| -> std::time::Duration {
        let mut times = Vec::new();
        for round in 0..(WARM + RUNS) {
            gpu_write(&storage);
            let start = std::time::Instant::now();
            storage.read_into(0, bytes_out).expect("a readback");
            if round >= WARM {
                times.push(start.elapsed());
            }
        }
        median_of(&mut times)
    };
    let mut gpu_out = vec![0u8; bytes];
    let after_gpu_sync = timed_after_gpu(&mut gpu_out[..4]);
    let after_gpu_read = timed_after_gpu(&mut gpu_out);
    println!(
        "16 MiB dma-heap storage after a GPU write: one-byte read {:.3} ms, full read {:.3} ms",
        ms(after_gpu_sync),
        ms(after_gpu_read)
    );
    let mut again_out = vec![0u8; bytes];
    let storage_again = median(WARM, RUNS, || {
        storage.read_into(0, &mut again_out).expect("a readback")
    });
    println!(
        "16 MiB dma-heap storage read_into, repeated: {:.3} ms",
        ms(storage_again)
    );
    raw_read("no device buffer on it");
    drop(storage);
    raw_read("device buffer dropped");
}

/// The 16 MiB dma-heap read split by what surrounds the copy: the destination (reused, or a fresh
/// zeroed Vec each run, which page-faults on first touch), the sync (once, or per 64 KiB chunk),
/// and the storage's own read against the raw mapping's, with the same copy.
#[cfg(target_os = "linux")]
pub(crate) fn heap_probe(backend: &WgpuBackend) {
    use daedalus_dma_sys::{Access, DmaHeap};
    let bytes = 16 * MIB as usize;
    let payload = vec![9u8; bytes];
    let Ok(storage) = Storage::new_dma_heap(backend, bytes as u64) else {
        println!("probe: no dma-heap storage");
        return;
    };
    storage.write(0, &payload).expect("in range");
    let heap = DmaHeap::open("system").expect("heap");
    let buf = heap.allocate(bytes as u64).expect("buffer");
    let map = buf.as_ref().map().expect("mapping");
    buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
    map.write(0, &payload).expect("in range");
    buf.as_ref().end_cpu_access(Access::Write).expect("end");

    let mut reused = vec![0u8; bytes];
    let storage_reused = median(WARM, RUNS, || {
        storage.read_into(0, &mut reused).expect("read")
    });
    let raw_reused = median(WARM, RUNS, || {
        buf.as_ref().begin_cpu_access(Access::Read).expect("begin");
        map.read(0, &mut reused).expect("read");
        buf.as_ref().end_cpu_access(Access::Read).expect("end");
    });
    let raw_nosync = median(WARM, RUNS, || map.read(0, &mut reused).expect("read"));
    let storage_fresh = median(WARM, RUNS, || {
        let mut fresh = vec![0u8; bytes];
        storage.read_into(0, &mut fresh).expect("read");
        std::hint::black_box(&fresh);
    });
    let raw_fresh = median(WARM, RUNS, || {
        let mut fresh = vec![0u8; bytes];
        buf.as_ref().begin_cpu_access(Access::Read).expect("begin");
        map.read(0, &mut fresh).expect("read");
        buf.as_ref().end_cpu_access(Access::Read).expect("end");
        std::hint::black_box(&fresh);
    });
    let raw_chunks = median(WARM, RUNS, || {
        for (at, chunk) in reused.chunks_mut(64 * 1024).enumerate() {
            buf.as_ref().begin_cpu_access(Access::Read).expect("begin");
            map.read((at * 64 * 1024) as u64, chunk).expect("read");
            buf.as_ref().end_cpu_access(Access::Read).expect("end");
        }
    });
    println!(
        "probe storage read into a reused Vec: {:.3} ms",
        ms(storage_reused)
    );
    println!(
        "probe raw sync, copy, sync into a reused Vec: {:.3} ms",
        ms(raw_reused)
    );
    println!(
        "probe raw copy with no sync into a reused Vec: {:.3} ms",
        ms(raw_nosync)
    );
    println!(
        "probe storage read into a fresh zeroed Vec: {:.3} ms",
        ms(storage_fresh)
    );
    println!(
        "probe raw sync, copy, sync into a fresh zeroed Vec: {:.3} ms",
        ms(raw_fresh)
    );
    println!(
        "probe raw sync per 64 KiB chunk into a reused Vec: {:.3} ms",
        ms(raw_chunks)
    );
}

/// One variant alone in its process: the storage's read, or the raw mapping's read, each into its
/// own reused Vec, with nothing else allocated. Run as separate processes, so no order effect.
#[cfg(target_os = "linux")]
pub(crate) fn heap_alone(backend: &WgpuBackend, which: &str) {
    use daedalus_dma_sys::{Access, DmaHeap};
    let bytes = 16 * MIB as usize;
    let payload = vec![9u8; bytes];
    let mut out = vec![0u8; bytes];
    match which {
        "storage" => {
            let storage = Storage::new_dma_heap(backend, bytes as u64).expect("storage");
            storage.write(0, &payload).expect("in range");
            let read = median(WARM, RUNS, || storage.read_into(0, &mut out).expect("read"));
            println!("alone storage read_into: {:.3} ms", ms(read));
        }
        "raw" | "rawpad" | "raw2m" => {
            let heap = DmaHeap::open("system").expect("heap");
            // "rawpad" allocates what the storage allocates: its bytes plus the device's slack.
            let len = if which == "rawpad" {
                bytes as u64 + 256
            } else {
                bytes as u64
            };
            let buf = heap.allocate(len).expect("buffer");
            let map = buf.as_ref().map().expect("mapping");
            buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
            map.write(0, &payload).expect("in range");
            buf.as_ref().end_cpu_access(Access::Write).expect("end");
            let read = median(WARM, RUNS, || {
                buf.as_ref().begin_cpu_access(Access::Read).expect("begin");
                map.read(0, &mut out).expect("read");
                buf.as_ref().end_cpu_access(Access::Read).expect("end");
            });
            println!("alone raw sync, copy, sync: {:.3} ms", ms(read));
        }
        other => println!("alone: unknown variant {other}"),
    }
}
