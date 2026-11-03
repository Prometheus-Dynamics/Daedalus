//! CUDA and Vulkan on one NVIDIA GPU: a device-local buffer allocated on the wgpu device with
//! exportable memory is imported into CUDA and scaled there in place, and Vulkan reads the result
//! from the same memory. The handoff copies nothing (the backends' transfer counters do not move),
//! so the round trip shows the two APIs addressing one allocation. Skips (passes, printing why)
//! without libcuda, a CUDA device, a wgpu device on NVIDIA, or the external-memory extension.
#![cfg(all(feature = "gpu-cuda", feature = "gpu-vulkan-raw", target_os = "linux"))]
#![allow(clippy::print_stderr)] // the skip reasons and the driver's answers

use std::time::Instant;

use daedalus_gpu::interop::{ShareKind, SharedBuffer, SharedSemaphore};
use daedalus_gpu::raw_vulkan::RawSubmitter;
use daedalus_gpu::{
    CudaBackend, CudaImport, CudaSemaphore, GpuBackend, GpuContextHandle, select_wgpu_on_vendor,
};

/// PCI vendor id of NVIDIA.
const NVIDIA: u32 = 0x10DE;
const COUNT: usize = 1000;

fn f32_bytes(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_ne_bytes()).collect()
}

fn f32_values(bytes: &[u8]) -> Vec<f32> {
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| f32::from_ne_bytes(*c))
        .collect()
}

/// The CUDA backend and a wgpu context on the same NVIDIA GPU, or `None` after saying why.
fn devices(test: &str) -> Option<(CudaBackend, GpuContextHandle)> {
    let cuda = match CudaBackend::new() {
        Ok(cuda) => cuda,
        Err(e) => {
            eprintln!("skipped {test}: {e}");
            return None;
        }
    };
    let ctx = match select_wgpu_on_vendor(NVIDIA) {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("skipped {test}: no wgpu device on NVIDIA: {e}");
            return None;
        }
    };
    Some((cuda, ctx))
}

#[test]
fn a_vulkan_buffer_is_scaled_by_cuda_and_read_back_with_no_copy() {
    let test = "a_vulkan_buffer_is_scaled_by_cuda_and_read_back_with_no_copy";
    let Some((cuda, ctx)) = devices(test) else {
        return;
    };
    let bytes = (COUNT * 4) as u64;
    let shared = match SharedBuffer::create(&ctx, bytes) {
        Ok(shared) => shared,
        Err(e) => {
            eprintln!("skipped {test}: shared buffer: {e}");
            return;
        }
    };
    // The memory is shared only with the CUDA device of the same GPU.
    if cuda.device_uuid().expect("CUDA device UUID") != shared.device_uuid() {
        eprintln!(
            "skipped {test}: the wgpu device {} is not the CUDA device",
            ctx.adapter_info().name
        );
        return;
    }
    let before_cuda = cuda.stats();
    let before_wgpu = ctx.stats();

    // Round 1: Vulkan writes, CUDA scales in place, Vulkan reads.
    let input: Vec<f32> = (0..COUNT).map(|i| i as f32 * 0.5 - 100.0).collect();
    shared.write(&f32_bytes(&input)).expect("write");
    let import = cuda
        .import_opaque_fd(shared.export_fd().expect("export"), bytes)
        .expect("CUDA imports the Vulkan memory");
    import
        .scale_f32(3.0)
        .expect("launch on the import")
        .wait()
        .expect("kernel completes");
    drop(import);
    let out = f32_values(&shared.read().expect("read"));
    for (i, (got, src)) in out.iter().zip(&input).enumerate() {
        assert_eq!(*got, src * 3.0, "round 1, element {i}");
    }

    // Round 2 on the same memory: Vulkan writes over CUDA's result, CUDA scales again.
    let again: Vec<f32> = (0..COUNT).map(|i| 1.0 + i as f32).collect();
    shared.write(&f32_bytes(&again)).expect("write");
    let import = cuda
        .import_opaque_fd(shared.export_fd().expect("export"), bytes)
        .expect("second import");
    import
        .scale_f32(0.25)
        .expect("launch")
        .wait()
        .expect("wait");
    drop(import);
    let out = f32_values(&shared.read().expect("read"));
    for (i, (got, src)) in out.iter().zip(&again).enumerate() {
        assert_eq!(*got, src * 0.25, "round 2, element {i}");
    }

    // The handoffs moved nothing through either backend's transfer counters.
    assert_eq!(cuda.stats(), before_cuda, "CUDA transfer counters moved");
    assert_eq!(ctx.stats(), before_wgpu, "wgpu transfer counters moved");
    eprintln!(
        "{test}: {COUNT} f32 on {}, two round trips, 0 copies at the CUDA handoff",
        cuda.adapter_info().name
    );
}

#[test]
fn a_dmabuf_fd_is_probed_against_the_cuda_import() {
    let test = "a_dmabuf_fd_is_probed_against_the_cuda_import";
    let Some((cuda, _ctx)) = devices(test) else {
        return;
    };
    let len = 4096usize;
    let heap = match lemnos_linux::dma::DmaHeap::open_path("/dev/dma_heap/system") {
        Ok(heap) => heap,
        Err(e) => {
            eprintln!("skipped {test}: no dma-heap: {e}");
            return;
        }
    };
    let fd = match heap.allocate(len) {
        Ok(buf) => buf.into_fd(),
        Err(e) => {
            eprintln!("skipped {test}: dma-heap allocation: {e}");
            return;
        }
    };
    // The driver either accepts a dma-buf as an opaque fd or refuses it with its own error.
    match cuda.import_opaque_fd(fd, len as u64) {
        Ok(import) => eprintln!("{test}: imported: {import:?}"),
        Err(e) => eprintln!("{test}: refused: {e}"),
    }
}

/// A dma-buf exported by Vulkan from the NVIDIA device, imported into CUDA: accepted (then checked
/// with a kernel and a read) or refused with the driver's error. Prints which.
#[test]
fn a_vulkan_dmabuf_is_probed_against_the_cuda_import() {
    let test = "a_vulkan_dmabuf_is_probed_against_the_cuda_import";
    let Some((cuda, ctx)) = devices(test) else {
        return;
    };
    let bytes = (COUNT * 4) as u64;
    let shared = match SharedBuffer::create_for(&ctx, bytes, ShareKind::DmaBuf) {
        Ok(shared) => shared,
        Err(e) => {
            eprintln!("{test}: Vulkan refused a dma-buf allocation: {e}");
            return;
        }
    };
    let fd = match shared.export_fd() {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("{test}: Vulkan refused the dma-buf export: {e}");
            return;
        }
    };
    match cuda.import_opaque_fd(fd, bytes) {
        Ok(import) => {
            let input: Vec<f32> = (0..COUNT).map(|i| i as f32).collect();
            shared.write(&f32_bytes(&input)).expect("write");
            import.scale_f32(2.0).expect("launch").wait().expect("wait");
            drop(import);
            let out = f32_values(&shared.read().expect("read"));
            assert!(out.iter().zip(&input).all(|(o, i)| *o == i * 2.0));
            eprintln!("{test}: CUDA imported the Vulkan dma-buf; 0 copies at the handoff");
        }
        Err(e) => eprintln!("{test}: CUDA refused the Vulkan dma-buf: {e}"),
    }
}

/// One CUDA device and one wgpu device on it, with the objects of a Vulkan<->CUDA handoff: a shared
/// buffer, a timeline semaphore on each side, and the CUDA import of the buffer. Fields drop in
/// order: the CUDA objects first, then the memory they refer to.
struct Handoff {
    import: CudaImport,
    csem: CudaSemaphore,
    ring: RawSubmitter,
    sem: SharedSemaphore,
    shared: SharedBuffer,
    cuda: CudaBackend,
    ctx: GpuContextHandle,
}

/// The handoff objects on NVIDIA, or `None` after saying why (the test then passes as skipped).
fn handoff(test: &str) -> Option<Handoff> {
    let (cuda, ctx) = devices(test)?;
    let bytes = (COUNT * 4) as u64;
    let shared = match SharedBuffer::create(&ctx, bytes) {
        Ok(shared) => shared,
        Err(e) => {
            eprintln!("skipped {test}: shared buffer: {e}");
            return None;
        }
    };
    if cuda.device_uuid().expect("CUDA device UUID") != shared.device_uuid() {
        eprintln!("skipped {test}: the wgpu device is not the CUDA device");
        return None;
    }
    let sem = match SharedSemaphore::create(&ctx) {
        Ok(sem) => sem,
        Err(e) => {
            eprintln!("skipped {test}: timeline semaphore: {e}");
            return None;
        }
    };
    let csem = match cuda.import_timeline_semaphore(sem.export_fd().expect("export semaphore")) {
        Ok(csem) => csem,
        Err(e) => {
            eprintln!("skipped {test}: CUDA refused the timeline semaphore: {e}");
            return None;
        }
    };
    let import = cuda
        .import_opaque_fd(shared.export_fd().expect("export buffer"), bytes)
        .expect("CUDA imports the Vulkan buffer");
    let ring = RawSubmitter::with_slots(&ctx, 4).expect("raw submitter");
    Some(Handoff {
        import,
        csem,
        ring,
        sem,
        shared,
        cuda,
        ctx,
    })
}

/// Vulkan and CUDA hand one buffer back and forth on one timeline semaphore, with no host wait at
/// the boundaries: Vulkan sets value `v`, CUDA waits for `v`, scales the buffer in place and sets
/// `v + 1`, and Vulkan's next submit waits for `v + 1`. The host waits once per round, at the end,
/// and reads the result back.
#[test]
fn a_timeline_semaphore_hands_a_buffer_between_vulkan_and_cuda() {
    let test = "a_timeline_semaphore_hands_a_buffer_between_vulkan_and_cuda";
    let Some(h) = handoff(test) else {
        return;
    };
    let before_cuda = h.cuda.stats();
    let before_wgpu = h.ctx.stats();
    let mut value = 0u64;
    for (round, factor) in [3.0f32, 0.25, -2.0].into_iter().enumerate() {
        let input: Vec<f32> = (0..COUNT).map(|i| i as f32 * 0.5 + round as f32).collect();
        h.shared.write(&f32_bytes(&input)).expect("write");
        let ready = value + 1;
        // Vulkan: the data is in place, set the semaphore (no host wait).
        // SAFETY: no commands are recorded; the semaphore is live until the ticket below completes.
        let _signal = unsafe {
            h.ring
                .submit_synced(|_, _| {}, &[], &[(h.sem.raw_handle(), ready)])
        }
        .expect("Vulkan signals");
        // CUDA: wait for it on the stream, scale in place, set the next value.
        h.csem.wait(ready).expect("CUDA waits");
        let fence = h.import.scale_f32(factor).expect("launch");
        h.csem.signal(ready + 1).expect("CUDA signals");
        // Vulkan: wait for CUDA's signal, then the host waits for this submission and reads back.
        // SAFETY: as above.
        unsafe {
            h.ring
                .submit_synced(|_, _| {}, &[(h.sem.raw_handle(), ready + 1)], &[])
        }
        .expect("Vulkan waits")
        .wait();
        drop(fence);
        value = ready + 1;
        let out = f32_values(&h.shared.read().expect("read"));
        for (i, (got, src)) in out.iter().zip(&input).enumerate() {
            assert_eq!(*got, src * factor, "round {round}, element {i}");
        }
    }
    assert_eq!(h.cuda.stats(), before_cuda, "CUDA transfer counters moved");
    assert_eq!(h.ctx.stats(), before_wgpu, "wgpu transfer counters moved");
    eprintln!(
        "{test}: 3 rounds of {COUNT} f32 on {}, semaphore-ordered, values checked",
        h.cuda.adapter_info().name
    );
}

/// Per-handoff latency, Vulkan -> CUDA -> Vulkan, on the RTX 4060: the host-fence path (the host
/// waits on each side's completion before the next side starts) against the semaphore path (the
/// same work, ordered on the GPU by a timeline semaphore, with the host waiting once at the end).
/// Both do two Vulkan submits and one CUDA kernel per round. Ignored by default: run with
/// `cargo test -p daedalus-gpu --features gpu-cuda,gpu-vulkan-raw --test cuda_interop -- --ignored --nocapture`.
#[test]
#[ignore = "benchmark: run with --ignored --nocapture on an NVIDIA GPU"]
fn handoff_latency_host_fence_vs_semaphore() {
    const WARMUP: usize = 200;
    const ROUNDS: usize = 2000;
    let test = "handoff_latency_host_fence_vs_semaphore";
    let Some(h) = handoff(test) else {
        return;
    };
    let input: Vec<f32> = (0..COUNT).map(|i| i as f32).collect();
    h.shared.write(&f32_bytes(&input)).expect("write");
    let mut value = 0u64;
    let mut fence_ns = Vec::with_capacity(ROUNDS);
    let mut sem_ns = Vec::with_capacity(ROUNDS);
    for round in 0..WARMUP + ROUNDS {
        // Host fences: Vulkan completes, then CUDA runs, then Vulkan.
        let t = Instant::now();
        // SAFETY: no commands are recorded.
        unsafe { h.ring.submit(|_, _| {}) }
            .expect("Vulkan submit")
            .wait();
        h.import
            .scale_f32(1.0)
            .expect("launch")
            .wait()
            .expect("CUDA completes");
        // SAFETY: as above.
        unsafe { h.ring.submit(|_, _| {}) }
            .expect("Vulkan submit")
            .wait();
        let fence = t.elapsed();

        // Semaphores: the same three steps ordered on the GPU, the host waits once at the end.
        let t = Instant::now();
        let ready = value + 1;
        // SAFETY: as above; the semaphore outlives the round.
        let _signal = unsafe {
            h.ring
                .submit_synced(|_, _| {}, &[], &[(h.sem.raw_handle(), ready)])
        }
        .expect("Vulkan signals");
        h.csem.wait(ready).expect("CUDA waits");
        let _kernel = h.import.scale_f32(1.0).expect("launch");
        h.csem.signal(ready + 1).expect("CUDA signals");
        // SAFETY: as above.
        unsafe {
            h.ring
                .submit_synced(|_, _| {}, &[(h.sem.raw_handle(), ready + 1)], &[])
        }
        .expect("Vulkan waits")
        .wait();
        let sem = t.elapsed();
        value = ready + 1;

        if round >= WARMUP {
            fence_ns.push(fence.as_nanos() as u64);
            sem_ns.push(sem.as_nanos() as u64);
        }
    }
    // Scale by 1.0 each round, so the data is unchanged by the whole run.
    let out = f32_values(&h.shared.read().expect("read"));
    assert!(
        out.iter().zip(&input).all(|(a, b)| a == b),
        "data changed in the benchmark"
    );

    let stats = |mut v: Vec<u64>| {
        v.sort_unstable();
        let at =
            |q: f64| v[((v.len() as f64 * q).ceil() as usize - 1).min(v.len() - 1)] as f64 / 1000.0;
        let mean = v.iter().sum::<u64>() as f64 / v.len() as f64 / 1000.0;
        (at(0.5), at(0.99), mean)
    };
    let (fm, fp, fmean) = stats(fence_ns);
    let (sm, sp, smean) = stats(sem_ns);
    eprintln!(
        "{test}: {ROUNDS} round trips (after {WARMUP} warm-up) on {}\n  host fence: median {fm:.1} us, p99 {fp:.1} us, mean {fmean:.1} us\n  semaphore:  median {sm:.1} us, p99 {sp:.1} us, mean {smean:.1} us",
        h.cuda.adapter_info().name
    );
}
