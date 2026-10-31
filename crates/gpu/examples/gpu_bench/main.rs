//! GPU budget of the host: compute limits and features, dispatch latency, kernel throughput
//! against the CPU, and the per-frame cost of getting a `LINEAR` dmabuf camera frame to a compute
//! shader (image import + copy, against importing the dmabuf as a storage buffer).
//!
//! ```text
//! cargo run --release -p daedalus-gpu --features gpu-dmabuf --example gpu_bench
//! DAEDALUS_DMA_HEAP=/dev/dma_heap/linux,cma ...   # heap for the dmabuf frames
//! GPU_BENCH_ITERS=200 ...                         # samples per timing (default 100)
//! ```
//!
//! Every section reports its error and the report continues. Timings are wall-clock from submit
//! to the fence signalling (`poll(Wait)`), so they include the driver's submit and wake-up cost.
#![allow(clippy::print_stdout)] // the report is the program's output

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("gpu_bench: needs Linux");
}

#[cfg(target_os = "linux")]
fn main() {
    linux::run();
}

#[cfg(target_os = "linux")]
mod linux;
