//! The no-Daedalus floors for two board rows: E2, a storage's first write, and E3, a readback
//! into host-mapped memory with the host's read. Each Daedalus row is timed next to the same work
//! done with raw Vulkan calls, in one process, round by round: A, then B, round after round, so a
//! drift on the device reaches both sides alike. An overhead line is the median of the per-round
//! differences, which is the point of interleaving; it is not the difference of two medians.

use std::sync::Arc;
use std::time::{Duration, Instant};

use daedalus_abi::Slot;
use daedalus_abi::testing::release;
use daedalus_gpu_budgets::{read_through, transfer};
use daedalus_runner::{put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_wgpu::{RawBlock, RawPlacement, Storage, WgpuBackend, raw_copy};

use super::{MIB, WARM, device_with, line_ms, ms, pattern, summary};

const SIZE: u64 = 16 * MIB;
/// The timed rounds, unless `DAEDALUS_BASELINE_ROUNDS` names another count.
const ROUNDS: usize = 31;

/// The timed rounds: `DAEDALUS_BASELINE_ROUNDS` when it is a positive number, else 31.
fn rounds() -> usize {
    let Ok(text) = std::env::var("DAEDALUS_BASELINE_ROUNDS") else {
        return ROUNDS;
    };
    match text.trim().parse::<usize>() {
        Ok(count) if count > 0 => count,
        _ => {
            println!(
                "DAEDALUS_BASELINE_ROUNDS is `{text}`, which is not a positive number: {ROUNDS} rounds"
            );
            ROUNDS
        }
    }
}

/// One row: the median of its timed rounds, in the house format.
fn row(label: &str, times: &mut [Duration]) -> String {
    format!("{label}: {}", line_ms(summary(times)))
}

/// The per-round differences `minuend - subtrahend`, paired by round. The line gives their median,
/// that median as a percent of the subtrahend's median, and the spread: the minimum, the maximum
/// and the quartiles. The quartiles are the values at a quarter and three quarters of the sorted
/// rounds, so they are an estimate with few rounds.
fn overhead(label: &str, minuend: &[Duration], subtrahend: &[Duration]) -> String {
    let mut diffs: Vec<f64> = minuend
        .iter()
        .zip(subtrahend)
        .map(|(a, b)| ms(*a) - ms(*b))
        .collect();
    let mut base: Vec<f64> = subtrahend.iter().map(|b| ms(*b)).collect();
    if diffs.is_empty() || base.is_empty() {
        return format!("{label}: no rounds were timed");
    }
    diffs.sort_by(f64::total_cmp);
    base.sort_by(f64::total_cmp);
    let n = diffs.len();
    let at = |num: usize| diffs[(n * num / 4).min(n - 1)];
    let median = diffs[n / 2];
    let base_median = base[base.len() / 2];
    let percent = 100.0 * median / base_median;
    format!(
        "{label}: median {median:+.4} ms, {percent:+.1} % of the baseline's median {base_median:.4} ms; per-round min {:+.4} ms, max {:+.4} ms, IQR {:+.4} to {:+.4} ms (width {:.4} ms), {n} rounds",
        diffs[0],
        diffs[n - 1],
        at(1),
        at(3),
        at(3) - at(1),
    )
}

/// `Storage::new` then a first write of `bytes`, timed; the storage is dropped after the clock,
/// which leaves it as the spare buffer the next `Storage::new` of this size takes.
fn time_storage(device: &WgpuBackend, bytes: &[u8]) -> Result<Duration, String> {
    let start = Instant::now();
    let storage = Storage::new(device, SIZE).map_err(|error| error.to_string())?;
    storage.write(0, bytes).expect("in range");
    let took = start.elapsed();
    drop(storage);
    Ok(took)
}

/// A write of the whole of `bytes` into a block kept across rounds: the steady-state floor.
fn time_kept(block: &RawBlock, bytes: &[u8]) -> Duration {
    let start = Instant::now();
    block.write(0, bytes).expect("in range");
    start.elapsed()
}

/// The same memory as `Storage::new` places in, made by the raw calls, mapped, and written once.
/// The block is freed after the clock: the cold cost of an allocation.
fn time_raw(device: &WgpuBackend, bytes: &[u8]) -> Duration {
    let start = Instant::now();
    let block = device
        .raw_block(SIZE, RawPlacement::DeviceReachable)
        .expect("a block");
    block.write(0, bytes).expect("in range");
    let took = start.elapsed();
    drop(block);
    took
}

/// A heap `Vec<u8>` of the same size, allocated and written once: the cost of first-touching
/// ordinary pages, with no device memory in it.
fn time_heap(bytes: &[u8]) -> Duration {
    let start = Instant::now();
    let mut heap = Vec::with_capacity(bytes.len());
    heap.extend_from_slice(bytes);
    let took = start.elapsed();
    std::hint::black_box(&heap);
    took
}

/// E2 on a device of its own, so no spare buffer is idle when the first `Storage::new` is made.
/// A is `Storage::new` then a first write. B-kept is a raw block of the same placement, kept across
/// rounds and written in full each round: the steady-state floor. B makes and frees a block each
/// round: the cold cost. C is a heap `Vec`. B and B-kept are skipped, with the reason, where the
/// storage's memory isn't mapped on the device.
fn e2(rounds: usize) {
    println!(
        "E2 pair: 16 MiB, Storage::new then a first write (A); a raw block of the same placement kept across rounds and written in full (B-kept); a raw block made, mapped and written each round (B); a heap Vec written once (C)"
    );
    println!(
        "E2 note: Storage::new takes a spare buffer of the same size when one is idle, so in steady state A is compared with B-kept, and B shows what a cold allocation costs."
    );
    let Some(device) = device_with(|options| options) else {
        println!("E2 pair: skipped, no device");
        return;
    };
    let kept = match device.raw_block(SIZE, RawPlacement::DeviceReachable) {
        Ok(block) if block.is_mapped() => {
            println!(
                "E2 B memory: type {} ({}), the type Storage::new places 16 MiB in on this device, mapped",
                block.memory_type(),
                block.memory_flags()
            );
            Some(block)
        }
        Ok(block) => {
            println!(
                "E2 B and B-kept: skipped, the type Storage::new places 16 MiB in is {} ({}) and this device doesn't map it, so there is no mapped first write to time",
                block.memory_type(),
                block.memory_flags()
            );
            None
        }
        Err(error) => {
            println!("E2 B and B-kept: skipped, no raw block on this device: {error}");
            None
        }
    };
    let bytes = vec![1u8; SIZE as usize];
    let (mut a, mut b_kept, mut b, mut c) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut cold = None;
    for round in 0..(WARM + rounds) {
        let a_took = match time_storage(&device, &bytes) {
            Ok(took) => took,
            Err(error) => {
                println!("E2 A: skipped, Storage::new failed: {error}");
                return;
            }
        };
        if round == 0 {
            // The first Storage::new on this device: no spare buffer can exist yet.
            cold = Some(a_took);
        }
        let kept_took = kept.as_ref().map(|block| time_kept(block, &bytes));
        let b_took = kept.is_some().then(|| time_raw(&device, &bytes));
        let c_took = time_heap(&bytes);
        if round >= WARM {
            a.push(a_took);
            b_kept.extend(kept_took);
            b.extend(b_took);
            c.push(c_took);
        }
    }
    if let Some(cold) = cold {
        println!(
            "E2 A first Storage::new on this device, with no spare buffer: {:.4} ms (one run, not a median)",
            ms(cold)
        );
    }
    println!(
        "{}",
        row(
            "E2 A Storage::new then a first write 16 MiB, steady state",
            &mut a
        )
    );
    if kept.is_some() {
        println!(
            "{}",
            row(
                "E2 B-kept raw block of the same placement, kept, written in full 16 MiB",
                &mut b_kept
            )
        );
        println!(
            "{}",
            overhead("E2 overhead, A minus B-kept, same round", &a, &b_kept)
        );
        println!(
            "{}",
            row(
                "E2 B raw block made, mapped and written once 16 MiB (a cold allocation)",
                &mut b
            )
        );
        println!(
            "{}",
            overhead(
                "E2 cold allocation, B minus B-kept, same round",
                &b,
                &b_kept
            )
        );
    }
    println!(
        "{}",
        row(
            "E2 C heap Vec<u8> 16 MiB, allocated and written once",
            &mut c
        )
    );
    if kept.is_some() {
        println!(
            "{}",
            overhead(
                "E2 device memory against heap, B minus C, same round",
                &b,
                &c
            )
        );
    }
}

/// A readback step's run on `device`, the wait for its fence, then the host's read of its target,
/// timed from the call to the read. This is `read_through`, the same as the `transfers` section's
/// readback row. The device buffer is consumed by the step, so its drop is timed too.
fn time_readback(readback: &mut daedalus_abi::Step, device: Storage, out: &mut [u8]) -> Duration {
    let start = Instant::now();
    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TypeIndex(0), SpaceIndex(0), Arc::new(device));
    let mut back = read_through(readback, cell);
    let mapped = take_typed::<Storage>(&mut back).expect("a buffer");
    mapped.read_into(0, out).expect("a readback");
    drop(mapped);
    start.elapsed()
}

/// E3: the readback step then the host read (A), which is the `transfers` section's row; a raw
/// copy of the same device buffer into a host-mapped block, submitted and waited for, then the host
/// read (B); and the host read of that block alone, after a copy has filled it (B2). B and B2 are
/// skipped, with the reason, where the raw path or the host-mapped block isn't there.
fn e3(backend: &WgpuBackend, rounds: usize) {
    println!(
        "E3 pair: 16 MiB readback to host-mapped memory with the host read (A, the transfers row: the same wait on the step's fence, then the read); a raw copy into the same memory type with the host read (B); the host read alone (B2)"
    );
    let payload = pattern(SIZE as usize, 13);
    let mut out = vec![0u8; SIZE as usize];
    let mut readback = transfer(backend, daedalus_wgpu::READBACK);
    let raw = match (
        backend.raw_block(SIZE, RawPlacement::HostMapped),
        backend.raw_submitter(2),
    ) {
        (Ok(block), Ok(submitter)) if block.is_mapped() => {
            println!(
                "E3 B memory: type {} ({}), host-mapped, the memory the readback's target is placed in",
                block.memory_type(),
                block.memory_flags()
            );
            Some((block, submitter))
        }
        (Ok(_), Ok(_)) => {
            println!("E3 B and B2: skipped, the host-mapped memory isn't mapped on this device");
            None
        }
        (Err(error), _) | (_, Err(error)) => {
            println!("E3 B and B2: skipped, no raw path on this device: {error}");
            None
        }
    };
    // Untimed checks, once: each path returns the bytes written, so no row times a wrong copy.
    // The raw copy also fills the block, which is what B2 then reads.
    let device = Storage::new(backend, SIZE).expect("a buffer");
    device.write(0, &payload).expect("in range");
    time_readback(&mut readback, device, &mut out);
    println!(
        "E3 check: the readback step returns the 16 MiB written: {}",
        if out == payload { "yes" } else { "no" }
    );
    let raw = match raw {
        Some((block, submitter)) => {
            let source = Storage::new(backend, SIZE).expect("a buffer");
            source.write(0, &payload).expect("in range");
            raw_copy(&submitter, &source, &block, SIZE).expect("a copy");
            let mut check = vec![0u8; SIZE as usize];
            block.read(0, &mut check).expect("a readback");
            if check == payload {
                println!("E3 check: the raw copy returns the 16 MiB written: yes");
                Some((block, submitter))
            } else {
                println!("E3 B and B2: skipped, the raw copy did not return the bytes written");
                None
            }
        }
        None => None,
    };
    let (mut a, mut b, mut b2) = (Vec::new(), Vec::new(), Vec::new());
    for round in 0..(WARM + rounds) {
        let keep = round >= WARM;
        let device = Storage::new(backend, SIZE).expect("a buffer");
        device.write(0, &payload).expect("in range");
        let took_a = time_readback(&mut readback, device, &mut out);
        if let Some((block, submitter)) = &raw {
            let device = Storage::new(backend, SIZE).expect("a buffer");
            device.write(0, &payload).expect("in range");
            let start = Instant::now();
            raw_copy(submitter, &device, block, SIZE).expect("a copy");
            block.read(0, &mut out).expect("a readback");
            drop(device);
            let took_b = start.elapsed();
            let start = Instant::now();
            block.read(0, &mut out).expect("a readback");
            let took_b2 = start.elapsed();
            if keep {
                b.push(took_b);
                b2.push(took_b2);
            }
        }
        if keep {
            a.push(took_a);
        }
    }
    release(readback);
    println!(
        "{}",
        row(
            "E3 A step readback device to host-mapped 16 MiB, then the host read",
            &mut a
        )
    );
    if raw.is_some() {
        println!(
            "{}",
            row(
                "E3 B raw copy device to host-mapped 16 MiB (submit, wait), then the host read",
                &mut b
            )
        );
        println!(
            "{}",
            row(
                "E3 B2 host read of the host-mapped 16 MiB alone, no copy",
                &mut b2
            )
        );
        println!("{}", overhead("E3 overhead, A minus B, same round", &a, &b));
        println!(
            "{}",
            overhead("E3 raw copy and submit, B minus B2, same round", &b, &b2)
        );
    }
}

/// The two no-Daedalus floors, E2 and E3, run in one process. The rounds are interleaved.
pub fn run(backend: &WgpuBackend) {
    let rounds = rounds();
    println!(
        "{rounds} timed rounds after {WARM} warm-up rounds. Each round runs its rows in order (A, then B); a row's line is the median of the rounds, with the p99 and the best of the same runs; an overhead line is the median of the per-round differences."
    );
    e2(rounds);
    e3(backend, rounds);
}
