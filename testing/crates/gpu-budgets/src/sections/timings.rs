//! The timings the board records asked for that had no section: a region of small shader nodes
//! against the same nodes submitted one at a time, one fragment pass, the record and submit of one dispatch, the time source a timed pass reports, and a 1 MiB
//! dma-heap buffer's allocate, map, read and fence. Each is the median of the timed runs after
//! warm-up, as the record's procedure asks.

use std::time::{Duration, Instant};

use daedalus_wgpu::fragment::FragmentPass;
use daedalus_wgpu::shader::{Bound, Run, Shader};
use daedalus_wgpu::{Format, Storage, Texture, TextureDesc, TimeSource, WgpuBackend};
use daedalus_wgpu_conformance::{COPY, In, Net, Op, filled, port, run};

use super::region_batch::TRIPLE;
#[cfg(target_os = "linux")]
use super::{MIB, pattern};
use super::{RUNS, WARM, line_ms, measure, median, median_of, ms, summary, us};

/// The raw submit timings: 20 warm-up runs, then the median of 200.
const SUBMIT_WARM: usize = 20;
const SUBMIT_TIMED: usize = 200;

/// Elements of the small ops, as the region test uses them.
const ELEMENTS: u64 = 1000;
/// Nodes in the region.
const NODES: usize = 8;

/// A region of eight triple nodes over 1000 words, one submit; then the same eight ops as eight
/// separate submits. The record's procedure is the add and triple ops of the region test: add
/// needs two inputs, which the chain here doesn't give, so the section runs triple alone.
pub fn region8(backend: &WgpuBackend) {
    let n = ELEMENTS * 4;
    let work: Vec<(Storage, Storage)> = (0..NODES)
        .map(|_| {
            (
                Storage::new(backend, n).expect("a buffer"),
                Storage::new(backend, n).expect("a buffer"),
            )
        })
        .collect();
    let op = Op::new(backend, &TRIPLE);
    let codes: Vec<&'static (dyn std::any::Any + Send + Sync)> = vec![&TRIPLE; NODES];
    let net = Net::line(&codes);
    let mut unit = net
        .compile(backend, &[port(NODES as u32 - 1, 0)])
        .expect("compiles");
    let input = filled(backend, &vec![1u32; ELEMENTS as usize]);
    let eight = median(WARM, RUNS, || {
        let mut last = None;
        for (src, dst) in &work {
            last = Some(op.run(src, dst));
        }
        last.expect("eight ops").wait().expect("finishes");
    });
    let one = median(WARM, RUNS, || {
        let ran = run(backend, &mut unit, &[In::Buffer(&input)], 1);
        if let daedalus_abi::StepEnd::Pending(fence) = &ran.end {
            daedalus_abi::testing::wait_fence(fence, u64::MAX);
        }
        std::hint::black_box(&ran);
    });
    daedalus_abi::testing::release(unit.step);
    println!(
        "region of 8 triple ops over {ELEMENTS} words, 8 submits: {:.3} ms",
        ms(eight)
    );
    println!(
        "region of 8 triple ops over {ELEMENTS} words, 1 submit: {:.3} ms",
        ms(one)
    );
}

/// One fragment pass over a 1280x800 one-channel target: each texel is computed from the input's
/// texel at the same place and no other. Submit to done, median of 200 after 20 warm-up runs.
pub fn fragment_pass(backend: &WgpuBackend) {
    const WGSL: &str = "@group(0) @binding(0) var src: texture_2d<f32>;
         @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {
             let v = textureLoad(src, vec2<i32>(pos.xy), 0).r;
             return vec4<f32>(v * 0.5 + 0.25, 0.0, 0.0, 1.0);
         }";
    let desc = TextureDesc {
        format: Format::R8,
        width: 1280,
        height: 800,
    };
    let src = Texture::new(backend, desc).expect("a texture");
    let dst = Texture::new(backend, desc).expect("a texture");
    let pass = match FragmentPass::new(backend, WGSL, &[Format::R8]) {
        Ok(pass) => pass,
        Err(error) => {
            println!("fragment pass: refused: {error}");
            return;
        }
    };
    let binding = pass.bind(&[&src], &[]).expect("binds");
    let time = median(20, 200, || {
        pass.run_bound(&binding, &[&dst])
            .expect("draws")
            .wait()
            .expect("finishes");
    });
    println!(
        "fragment pass, r8 1280x800: {:.3} ms (median of 200 after 20)",
        ms(time)
    );
}

/// The record and submit of one dispatch, with no wait, two ways: through the backend's own
/// submit (`Op`), and through a raw Vulkan chain of one stage, which records the dispatch into a
/// command buffer it reuses. Each is the median of 200 runs after 20 warm-up runs.
pub fn raw_submit(backend: &WgpuBackend) {
    let copy = Op::new(backend, &COPY);
    let (src, dst) = (
        Storage::new(backend, 4).expect("a buffer"),
        Storage::new(backend, 4).expect("a buffer"),
    );
    let mut wgpu_submit = Vec::with_capacity(SUBMIT_TIMED);
    for round in 0..(SUBMIT_WARM + SUBMIT_TIMED) {
        let start = Instant::now();
        let ticket = copy.run(&src, &dst);
        let took = start.elapsed();
        ticket.wait().expect("finishes");
        if round >= SUBMIT_WARM {
            wgpu_submit.push(took);
        }
    }
    println!(
        "one dispatch, wgpu record and submit (no wait): {}",
        line_ms(summary(&mut wgpu_submit))
    );
    raw_chain_submit(backend);
}

/// A word-add shader, one stage, run through a raw chain on the backend's device.
#[cfg(target_os = "linux")]
const BUMP: &str = "@group(0) @binding(0) var<storage, read_write> data: array<u32>;
    @compute @workgroup_size(64)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x < arrayLength(&data) { data[id.x] = data[id.x] + 1u; }
    }";

/// One dispatch recorded and submitted by a raw Vulkan chain, with no wait. Waiting for the run
/// before the next one is done before the clock starts, so the time is the record and submit.
#[cfg(target_os = "linux")]
fn raw_chain_submit(backend: &WgpuBackend) {
    use daedalus_wgpu::Arg;
    let mut chain = match backend.raw_chain(&[(BUMP, "main")], 1) {
        Ok(chain) => chain,
        Err(error) => {
            println!("raw submit (vk chain): refused: {error}");
            return;
        }
    };
    // The buffer is outside the chain's reach until the chain drops, so it is declared first.
    let buffer = Storage::new(backend, 256).expect("a buffer");
    buffer.write(0, &[0u8; 256]).expect("in range");
    // A write reaches the device with wgpu's next submit, which a readback makes: the chain's
    // submits do not make one, so the write is flushed here, before the first run.
    let mut flushed = [0u8; 4];
    buffer.read_into(0, &mut flushed).expect("a readback");
    let mut times = Vec::with_capacity(SUBMIT_TIMED);
    for round in 0..(SUBMIT_WARM + SUBMIT_TIMED) {
        assert!(chain.wait(Duration::from_secs(10)), "the last run finished");
        let start = Instant::now();
        chain.begin();
        chain
            .dispatch(
                0,
                &[Arg::Storage {
                    buffer: buffer.wgpu_buffer(),
                    size: 256,
                }],
                [1, 1, 1],
            )
            .expect("dispatches");
        chain.submit().expect("submits");
        let took = start.elapsed();
        if round >= SUBMIT_WARM {
            times.push(took);
        }
    }
    assert!(chain.wait(Duration::from_secs(10)), "the last run finished");
    // Each run adds one to every word, so the buffer counts the runs: a check that the chain ran.
    let mut first = [0u8; 4];
    buffer.read_into(0, &mut first).expect("a readback");
    let counted = u32::from_le_bytes(first);
    let runs = (SUBMIT_WARM + SUBMIT_TIMED) as u32;
    println!(
        "one dispatch, raw Vulkan chain record and submit (no wait): {} (buffer counts {counted} of {runs} runs)",
        line_ms(summary(&mut times))
    );
}

#[cfg(not(target_os = "linux"))]
fn raw_chain_submit(_backend: &WgpuBackend) {
    println!("raw submit (vk chain): skipped, raw Vulkan is a Linux path");
}

/// Which time source a timed pass reports on this device, and its median: timestamps the device
/// wrote around the pass, or submit to done when the device has none.
pub fn timer_source(backend: &WgpuBackend) {
    let shader = Shader::load(backend, &TRIPLE)
        .expect("loads")
        .with_timestamps();
    let (src, dst) = (
        Storage::new(backend, 4 * ELEMENTS).expect("a buffer"),
        Storage::new(backend, 4 * ELEMENTS).expect("a buffer"),
    );
    let mut sources = Vec::new();
    let mut times = Vec::new();
    for round in 0..(WARM + RUNS) {
        let bound = [Bound::Storage(&src), Bound::Storage(&dst)];
        let ticket = match shader.run(&bound).expect("runs") {
            Run::Started(ticket) => ticket,
            Run::Busy(ticket) => {
                ticket.wait().expect("finishes");
                continue;
            }
        };
        ticket.wait().expect("finishes");
        let deadline = Instant::now() + Duration::from_secs(10);
        let measured = loop {
            match ticket.gpu_time() {
                Some(measured) => break Some(measured),
                None if Instant::now() > deadline => break None,
                None => {
                    // Asking whether the ticket is done lets the device deliver the timestamps.
                    let _ = ticket.is_done();
                }
            }
        };
        // The warm-up runs are not reported.
        if let (Some((took, source)), true) = (measured, round >= WARM) {
            sources.push(source);
            times.push(took);
        }
    }
    let timestamps = sources
        .iter()
        .filter(|s| **s == TimeSource::Timestamps)
        .count();
    println!(
        "timed passes: {} of {} reported timestamps, the rest submit to done",
        timestamps,
        sources.len()
    );
    if !times.is_empty() {
        println!(
            "pass time, median of {}: {:.1} us ({})",
            times.len(),
            us(median_of(&mut times)),
            if timestamps == sources.len() {
                "timestamps"
            } else {
                "mixed sources"
            }
        );
    }
}

/// A 1 MiB dma-heap buffer: allocate and map, then a CPU write and read bracketed by the sync,
/// each the median of 50 runs after 5; then the fence the buffer exports after a CPU write, and
/// how long that fence takes to signal.
#[cfg(target_os = "linux")]
pub fn dma_1mib() {
    use daedalus_dma_sys::{Access, DmaHeap};
    let bytes = MIB as usize;
    let payload = pattern(bytes, 7);
    let mut out = vec![0u8; bytes];
    let Ok(heap) = DmaHeap::open("system") else {
        println!("dma 1 MiB: skipped, no /dev/dma_heap/system");
        return;
    };
    let alloc_map = measure(WARM, RUNS, || {
        let buf = heap.allocate(bytes as u64).expect("a dma-heap buffer");
        let map = buf.as_ref().map().expect("a mapping");
        std::hint::black_box(&map);
    });
    let buf = heap.allocate(bytes as u64).expect("a dma-heap buffer");
    let map = buf.as_ref().map().expect("a mapping");
    let write = measure(WARM, RUNS, || {
        buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
        map.write(0, &payload).expect("in range");
        buf.as_ref().end_cpu_access(Access::Write).expect("end");
    });
    let read = measure(WARM, RUNS, || {
        buf.as_ref().begin_cpu_access(Access::Read).expect("begin");
        map.read(0, &mut out).expect("in range");
        buf.as_ref().end_cpu_access(Access::Read).expect("end");
    });
    println!("dma 1 MiB allocate and map: {}", line_ms(alloc_map));
    println!("dma 1 MiB write (sync, copy, sync): {}", line_ms(write));
    println!("dma 1 MiB read (sync, copy, sync): {}", line_ms(read));
    // The fence the buffer exports after a CPU write: time from the export to its signal.
    let mut signal = Vec::new();
    let mut unsignalled = 0usize;
    for round in 0..(WARM + RUNS) {
        buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
        map.write(0, &payload).expect("in range");
        buf.as_ref().end_cpu_access(Access::Write).expect("end");
        let start = Instant::now();
        let fence = buf.as_ref().export_fence(Access::Read).expect("exports");
        let done = fence.wait(Duration::from_secs(5)).expect("waits");
        if round >= WARM {
            if done {
                signal.push(start.elapsed());
            } else {
                unsignalled += 1;
            }
        }
    }
    if signal.is_empty() {
        println!("dma 1 MiB export_fence: no fence signalled in {RUNS} runs");
    } else {
        println!(
            "dma 1 MiB export_fence to signal, {} signalled, {unsignalled} not within 5 s: {}",
            signal.len(),
            line_ms(summary(&mut signal))
        );
    }
}
