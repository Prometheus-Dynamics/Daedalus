//! The measurement tool for the timed GPU budgets. It runs on boards and by hand, not in the
//! gate: `cargo run --release -p daedalus-gpu-budgets -- [section ...]`, with the sections
//! `limits`, `batching`, `allocation`, `dispatch-floor` and `readback`, or none for all.
//! Times are the best of the runs, since a budget is what the device can do.
#![allow(clippy::print_stdout)] // the measurements are this program's output

use std::time::{Duration, Instant};

use daedalus_abi::counting::CountingAlloc;
use daedalus_gpu_budgets::{boundaries, merged_chain, shader_line};
use daedalus_wgpu::kernels::{Kernel, Kernels};
use daedalus_wgpu::{Options, Storage, WgpuBackend};

#[global_allocator]
static COUNTING: CountingAlloc = CountingAlloc;

const SECTIONS: [&str; 5] = [
    "limits",
    "batching",
    "allocation",
    "dispatch-floor",
    "readback",
];
const RUNS: usize = 200;

fn best(mut body: impl FnMut()) -> Duration {
    (0..RUNS)
        .map(|_| {
            let start = Instant::now();
            body();
            start.elapsed()
        })
        .min()
        .unwrap_or_default()
}

fn ms(time: Duration) -> f64 {
    time.as_secs_f64() * 1e3
}

fn limits(backend: &WgpuBackend) {
    let Some(record) = backend.caps() else { return };
    println!("adapter: {}", record.adapter.name);
    println!("{:#?}", record.compute);
}

fn median(times: &mut [Duration]) -> Duration {
    times.sort_unstable();
    times.get(times.len() / 2).copied().unwrap_or_default()
}

/// Eight small ops, as eight submits and as one: the workload the design's budget row is
/// measured on. Each op is a shader copy of 64 KiB. The eight submits are made one after the
/// other and waited for once, at the end. The median of 35 frames after 5 to warm up.
fn batching(backend: &WgpuBackend) {
    let kernels = Kernels::new(backend).expect("a device");
    let n = 64 * 1024;
    let work: Vec<(Storage, Storage)> = (0..8)
        .map(|_| {
            let src = Storage::new(backend, n).expect("a buffer");
            let dst = Storage::new(backend, n).expect("a buffer");
            (src, dst)
        })
        .collect();
    let (mut apart, mut together) = (Vec::new(), Vec::new());
    for round in 0..40 {
        let start = Instant::now();
        let mut last = None;
        for (src, dst) in &work {
            last = Some(kernels.run(Kernel::Copy, src, dst, n).expect("runs"));
        }
        // Work on a device finishes in the order it was submitted: the last one is all of it.
        if let Some(last) = last {
            last.wait().expect("finishes");
        }
        let eight = start.elapsed();

        let start = Instant::now();
        let mut batch = kernels.batch();
        for (src, dst) in &work {
            batch.add(Kernel::Copy, src, dst, n).expect("fits");
        }
        batch.submit().wait().expect("finishes");
        if round >= 5 {
            apart.push(eight);
            together.push(start.elapsed());
        }
    }
    println!("8 ops, 8 submits: {:.3} ms", ms(median(&mut apart)));
    println!("8 ops, 1 submit:  {:.3} ms", ms(median(&mut together)));
}

fn allocation(backend: &WgpuBackend) {
    for nodes in [1, 2, 4, 8] {
        let counted = shader_line(backend, nodes, 4096);
        println!(
            "a region of {nodes} shader nodes: {} allocations in a warm run",
            counted.allocations
        );
    }
    // The least this backend does for a submit: one op, recorded and submitted, with no
    // region around it. What a region allocates beyond this is the region's own.
    let kernels = Kernels::new(backend).expect("a device");
    let (src, dst) = (
        Storage::new(backend, 4096).expect("a buffer"),
        Storage::new(backend, 4096).expect("a buffer"),
    );
    let one = |ops: usize| {
        let mut count = 0;
        for _ in 0..8 {
            let ((), allocations) = daedalus_abi::counting::counted(|| {
                let mut batch = kernels.batch();
                for _ in 0..ops {
                    batch.add(Kernel::Copy, &src, &dst, 4096).expect("fits");
                }
                batch.submit().wait().expect("finishes");
            });
            count = allocations;
        }
        count
    };
    println!(
        "one op recorded and submitted alone: {} allocations, eight in one batch: {}",
        one(1),
        one(8)
    );
    let (_, warm) = merged_chain(backend, 2, 4096);
    println!(
        "a merged chain of 4 pixel bodies: {} allocations in a warm run",
        warm.allocations
    );
}

/// The least a run costs: one op over four bytes, submitted and waited for.
fn dispatch_floor(backend: &WgpuBackend) {
    let kernels = Kernels::new(backend).expect("a device");
    let (src, dst) = (
        Storage::new(backend, 4).expect("a buffer"),
        Storage::new(backend, 4).expect("a buffer"),
    );
    let floor = best(|| {
        kernels
            .run(Kernel::Copy, &src, &dst, 4)
            .expect("runs")
            .wait()
            .expect("finishes");
    });
    println!(
        "one pass, submit and wait: {:.1} us",
        floor.as_secs_f64() * 1e6
    );
}

fn readback(backend: &WgpuBackend) {
    for bytes in [4 * 1024, 256 * 1024, 4 * 1024 * 1024] {
        let storage = Storage::new(backend, bytes).expect("a buffer");
        let mut out = vec![0; bytes as usize];
        let read = best(|| storage.read_into(0, &mut out).expect("a readback"));
        println!("read back {} KiB: {:.3} ms", bytes / 1024, ms(read));
    }
    let counted = boundaries(backend, 64 * 1024);
    println!(
        "64 KiB up and back: {} upload, {} download",
        counted.uploads, counted.downloads
    );
}

fn main() -> std::process::ExitCode {
    let asked: Vec<String> = std::env::args().skip(1).collect();
    if let Some(unknown) = asked.iter().find(|name| !SECTIONS.contains(&name.as_str())) {
        println!("no section `{unknown}`: the sections are {SECTIONS:?}");
        return std::process::ExitCode::FAILURE;
    }
    let backend = WgpuBackend::open(Options::default());
    if let Err(error) = backend.start() {
        println!("no device: {error}");
        return std::process::ExitCode::FAILURE;
    }
    for section in SECTIONS {
        if !asked.is_empty() && !asked.iter().any(|name| name == section) {
            continue;
        }
        println!("== {section}");
        match section {
            "limits" => limits(&backend),
            "batching" => batching(&backend),
            "allocation" => allocation(&backend),
            "dispatch-floor" => dispatch_floor(&backend),
            _ => readback(&backend),
        }
    }
    std::process::ExitCode::SUCCESS
}
