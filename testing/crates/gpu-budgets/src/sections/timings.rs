//! The timings the board records asked for that had no section: a region of small shader nodes
//! against the same nodes submitted one at a time, a fragment pass with a box filter at 3x3 and
//! 5x5, the record and submit of one dispatch, the time source a timed pass reports, and a 1 MiB
//! dma-heap buffer's allocate, map, read and fence. Each is the median of the timed runs after
//! warm-up, as the record's procedure asks.

use std::time::{Duration, Instant};

use daedalus_wgpu::fragment::FragmentPass;
use daedalus_wgpu::shader::{BindingKind, BindingSpec, Bound, Dispatch, Run, Shader, ShaderSpec};
use daedalus_wgpu::{Format, Storage, Texture, TextureDesc, TimeSource, WgpuBackend};
use daedalus_wgpu_conformance::{COPY, In, Net, Op, filled, port, run};

use super::{MIB, RUNS, WARM, median, median_of, ms, pattern, us};

/// Elements of the small ops, as the region test uses them.
const ELEMENTS: u64 = 1000;
/// Nodes in the region.
const NODES: usize = 8;

const fn storage(index: u32, name: &'static str, read_only: bool) -> BindingSpec {
    BindingSpec {
        index,
        name,
        kind: BindingKind::Storage {
            read_only,
            stride: 4,
        },
    }
}

/// `out = src * 3`, element by element: the triple op of the region test.
static TRIPLE: ShaderSpec = ShaderSpec {
    name: "triple",
    entry: "main",
    source: "
        @group(0) @binding(0) var<storage, read> src: array<u32>;
        @group(0) @binding(1) var<storage, read_write> out: array<u32>;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x < arrayLength(&out) { out[id.x] = src[id.x] * 3u; }
        }
    ",
    workgroup: [64, 1, 1],
    bindings: &[storage(0, "src", true), storage(1, "out", false)],
    dispatch: Dispatch::PerElement {
        binding: 1,
        stride: 4,
    },
    inputs: &[0],
    outputs: &[1],
};

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

/// A box filter over the input's texels, as WGSL: the mean of the `radius`-neighbourhood, clamped at
/// the edges, in one red channel. Nothing in the crate names a box filter; the record needs one.
fn box_wgsl(radius: i32, width: u32, height: u32) -> String {
    format!(
        "@group(0) @binding(0) var src: texture_2d<f32>;
         @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{
             let p = vec2<i32>(pos.xy);
             var sum = 0.0;
             for (var dy = -{radius}; dy <= {radius}; dy++) {{
                 for (var dx = -{radius}; dx <= {radius}; dx++) {{
                     let q = clamp(p + vec2<i32>(dx, dy), vec2<i32>(0), vec2<i32>({w}, {h}));
                     sum += textureLoad(src, q, 0).r;
                 }}
             }}
             let n = f32(({side}) * ({side}));
             return vec4<f32>(sum / n, 0.0, 0.0, 1.0);
         }}",
        w = width - 1,
        h = height - 1,
        side = 2 * radius + 1,
    )
}

/// A box filter on an R8 target of 1280x800, 3x3 and 5x5: submit to done, median of 200 after 20
/// warm-up runs.
pub fn fragment_box(backend: &WgpuBackend) {
    let (width, height) = (1280u32, 800u32);
    let desc = TextureDesc {
        format: Format::R8,
        width,
        height,
    };
    let src = Texture::new(backend, desc).expect("a texture");
    let dst = Texture::new(backend, desc).expect("a texture");
    for radius in [1, 2] {
        let label = if radius == 1 { "3x3" } else { "5x5" };
        let pass = match FragmentPass::new(backend, &box_wgsl(radius, width, height), &[Format::R8])
        {
            Ok(pass) => pass,
            Err(error) => {
                println!("box {label}: refused: {error}");
                continue;
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
            "box {label} fragment pass, r8 1280x800: {:.3} ms (median of 200 after 20)",
            ms(time)
        );
    }
}

/// The record and submit of one dispatch, with no wait, against the wait for it: the floor a
/// node's submit pays.
pub fn raw_submit(backend: &WgpuBackend) {
    let copy = Op::new(backend, &COPY);
    let (src, dst) = (
        Storage::new(backend, 4).expect("a buffer"),
        Storage::new(backend, 4).expect("a buffer"),
    );
    let mut submit = Vec::new();
    for round in 0..(WARM + RUNS) {
        let start = Instant::now();
        let ticket = copy.run(&src, &dst);
        let took = start.elapsed();
        ticket.wait().expect("finishes");
        if round >= WARM {
            submit.push(took);
        }
    }
    println!(
        "one dispatch, record and submit (no wait): {:.1} us (median of {RUNS})",
        us(median_of(&mut submit))
    );
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
    for _ in 0..(WARM + RUNS) {
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
        if let Some((took, source)) = measured {
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
/// each the median of 50; then the sync fence the device would signal for it, and how long
/// the fence takes to signal after a CPU write.
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
    let alloc_map = median(WARM, RUNS, || {
        let buf = heap.allocate(bytes as u64).expect("a dma-heap buffer");
        let map = buf.as_ref().map().expect("a mapping");
        std::hint::black_box(&map);
    });
    let buf = heap.allocate(bytes as u64).expect("a dma-heap buffer");
    let map = buf.as_ref().map().expect("a mapping");
    let write = median(WARM, RUNS, || {
        buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
        map.write(0, &payload).expect("in range");
        buf.as_ref().end_cpu_access(Access::Write).expect("end");
    });
    let read = median(WARM, RUNS, || {
        buf.as_ref().begin_cpu_access(Access::Read).expect("begin");
        map.read(0, &mut out).expect("in range");
        buf.as_ref().end_cpu_access(Access::Read).expect("end");
    });
    println!("dma 1 MiB allocate and map: {:.4} ms", ms(alloc_map));
    println!("dma 1 MiB write (sync, copy, sync): {:.4} ms", ms(write));
    println!("dma 1 MiB read (sync, copy, sync): {:.4} ms", ms(read));
    // The fence the buffer exports after a CPU write: time from the export to its signal.
    let mut signal = Vec::new();
    for round in 0..(WARM + RUNS) {
        buf.as_ref().begin_cpu_access(Access::Write).expect("begin");
        map.write(0, &payload).expect("in range");
        buf.as_ref().end_cpu_access(Access::Write).expect("end");
        let start = Instant::now();
        let fence = buf.as_ref().export_fence(Access::Read).expect("exports");
        let done = fence.wait(Duration::from_secs(5)).expect("waits");
        if round >= WARM && done {
            signal.push(start.elapsed());
        }
    }
    if signal.is_empty() {
        println!("dma 1 MiB export_fence: no fence signalled");
    } else {
        println!(
            "dma 1 MiB export_fence to signal, median of {}: {:.1} us",
            signal.len(),
            us(median_of(&mut signal))
        );
    }
}
