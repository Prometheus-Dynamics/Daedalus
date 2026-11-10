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

/// Eight ops over 64 KiB each: one submit for all, against a submit and a wait for each.
fn batching(backend: &WgpuBackend) {
    let kernels = Kernels::new(backend).expect("a device");
    let n = 64 * 1024;
    let work: Vec<(Storage, Storage)> = (0..8)
        .map(|_| {
            let src = Storage::new(backend, n).expect("a buffer");
            let dst = Storage::new(backend, Kernel::HexLower.output_len(n)).expect("a buffer");
            (src, dst)
        })
        .collect();
    let together = best(|| {
        let mut encoder = kernels.encoder();
        for (src, dst) in &work {
            kernels
                .encode(&mut encoder, Kernel::HexLower, src, dst, n)
                .expect("fits");
        }
        kernels.submit(encoder).wait().expect("finishes");
    });
    let apart = best(|| {
        for (src, dst) in &work {
            kernels
                .run(Kernel::HexLower, src, dst, n)
                .expect("runs")
                .wait()
                .expect("finishes");
        }
    });
    println!("8 ops, 1 submit:  {:.3} ms", ms(together));
    println!("8 ops, 8 submits: {:.3} ms", ms(apart));
}

fn allocation(backend: &WgpuBackend) {
    for nodes in [1, 2, 4, 8] {
        let counted = shader_line(backend, nodes, 4096);
        println!(
            "a region of {nodes} shader nodes: {} allocations in a warm run",
            counted.allocations
        );
    }
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
