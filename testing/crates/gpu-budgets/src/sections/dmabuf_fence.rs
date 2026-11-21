//! E9: the device's fence on an imported dma-buf, with planted early readers that prove the figure
//! can tell a reader that waits from one that doesn't.
//!
//! A 1 MiB dma-heap buffer is imported, prefilled with a sentinel by the CPU, and written by the
//! GPU with a write long enough to outlive a read made right after its submit. The backend attaches
//! the submit's fence to the buffer. Then, 50 runs each:
//! - (a) a CPU read inside the kernel's sync bracket, with no other wait, must see the GPU's values
//!   every time: the bracket waits on the attached fence;
//! - (b) planted: a plain read of the mapping right after the submit, with no bracket, must see the
//!   sentinel in at least 45 runs, or the write is too short to catch an early reader;
//! - (c) planted: the same as (a) on a device opened with attaching off: the fence exported at the
//!   submit must be signalled already and the bracketed read must see the sentinel, or the figure
//!   can't tell an attached fence from none.
//!
//! It also prints what attaching costs: the host's time per submit from the backend's counts, and
//! the submit-to-done time of a short write with attaching on and off. Then E14's device-side wait
//! runs on the same buffer.

use std::time::{Duration, Instant};

use daedalus_dma_sys::{Access, DmaBuf, DmaHeap, Mapping, is_sync_file};
use daedalus_wgpu::shader::Shader;
use daedalus_wgpu::{Options, Storage, Ticket, WgpuBackend};

use super::dmabuf::{
    BYTES, FILL, LONG_ENOUGH, ROOM, ROUNDS_CAP, ROUNDS_START, SAMPLE_STEP, cpu_read,
    device_side_wait, fill, import_in_place, open_importing, sampled_correct,
};
use super::{RUNS, WARM, median_of, ms};

/// How many times over the calibrated write is taken. On V3D (2026-11-20) the bare calibration gave
/// 16 rounds, and a plain read right after the submit still saw the GPU's values in 12 of 50.
const MARGIN: u32 = 8;

/// What the CPU writes before each run. No `FILL` value at every sampled word is this.
pub(super) const SENTINEL: u32 = 0x5e17_1e1a;

/// Whether every sampled word of `bytes` is still the sentinel.
pub(super) fn saw_sentinel(bytes: &[u8]) -> bool {
    let words = bytes.as_chunks::<4>().0;
    (0..words.len())
        .step_by(SAMPLE_STEP)
        .all(|index| u32::from_le_bytes(words[index]) == SENTINEL)
}

/// Writes the sentinel over the value, inside a write sync, which waits for what is attached.
pub(super) fn prefill(buf: &DmaBuf, map: &Mapping) -> Result<(), String> {
    let bytes: Vec<u8> = (0..BYTES / 4)
        .flat_map(|_| SENTINEL.to_le_bytes())
        .collect();
    let view = buf.as_ref();
    view.begin_cpu_access(Access::Write)
        .map_err(|error| error.to_string())?;
    let wrote = map.write(0, &bytes).map_err(|error| error.to_string());
    view.end_cpu_access(Access::Write)
        .map_err(|error| error.to_string())?;
    wrote
}

/// The value's first 1 MiB through the mapping, with no sync at all: an early reader.
fn plain_read(map: &Mapping, out: &mut [u8]) -> Result<(), String> {
    map.read(0, out).map_err(|error| error.to_string())
}

/// The rounds for which the GPU's write is still running after a 1 MiB read right after its
/// submit: they double until three runs in a row are, and are then taken `MARGIN` times over, so a
/// write that is only just long enough doesn't let a few early reads through. `Err` with the reason
/// when even a write of `LONG_ENOUGH` or the cap has finished by then.
fn calibrate(shader: &Shader, storage: &Storage, map: &Mapping) -> Result<u32, String> {
    let mut rounds = ROUNDS_START;
    let mut bytes = vec![0u8; BYTES as usize];
    loop {
        let (mut all, mut longest) = (true, Duration::ZERO);
        for seed in 0..3 {
            let started = Instant::now();
            let ticket = fill(shader, storage, rounds, seed)?;
            plain_read(map, &mut bytes)?;
            all &= !ticket.is_done();
            ticket.wait().map_err(|error| error.to_string())?;
            longest = longest.max(started.elapsed());
        }
        if all {
            return Ok(rounds.saturating_mul(MARGIN).min(ROUNDS_CAP));
        }
        if longest >= LONG_ENOUGH * 4 || rounds >= ROUNDS_CAP {
            return Err(format!(
                "a GPU write of {:.1} ms ({rounds} loop rounds) finished before a read right after its submit",
                longest.as_secs_f64() * 1e3
            ));
        }
        rounds *= 2;
    }
}

/// One device importing the buffer, with its fill loaded.
struct Writer {
    backend: WgpuBackend,
    storage: std::sync::Arc<Storage>,
    shader: Shader,
}

fn writer(options: Options, buf: &DmaBuf) -> Result<Writer, String> {
    let (backend, mut step) = open_importing(options)?;
    let storage = import_in_place(&backend, &mut step, buf)?;
    let shader = Shader::load(&backend, &FILL)
        .map_err(|error| format!("the GPU write doesn't load: {error}"))?;
    Ok(Writer {
        backend,
        storage,
        shader,
    })
}

/// Counts over the timed runs of one way of reading.
#[derive(Default)]
struct Tally {
    gpu: usize,
    sentinel: usize,
    unsignalled: usize,
    sync_files: usize,
    failures: Vec<String>,
}

/// `RUNS` timed runs (after `WARM`): prefill, the GPU's write, then `read` right after the
/// submit. `export` also takes the buffer's fence at the submit.
fn runs(
    writer: &Writer,
    buf: &DmaBuf,
    map: &Mapping,
    rounds: u32,
    seed: u32,
    read: impl Fn(&mut [u8]) -> Result<(), String>,
) -> Tally {
    let mut tally = Tally::default();
    let mut bytes = vec![0u8; BYTES as usize];
    for round in 0..(WARM + RUNS) {
        let seed = seed + round as u32;
        if let Err(reason) = prefill(buf, map) {
            tally.failures.push(format!("prefill: {reason}"));
            continue;
        }
        let ticket: Ticket = match fill(&writer.shader, &writer.storage, rounds, seed) {
            Ok(ticket) => ticket,
            Err(reason) => {
                tally.failures.push(format!("the GPU write: {reason}"));
                continue;
            }
        };
        // Sampled at once: a fence looked at after the read could have signalled since.
        let fence = buf.as_ref().export_fence(Access::Read).map(|fence| {
            (
                is_sync_file(fence.as_fd()),
                matches!(fence.is_signaled(), Ok(false)),
            )
        });
        let read = read(&mut bytes);
        ticket.wait().ok();
        if round < WARM {
            continue;
        }
        match fence {
            Ok((sync_file, unsignalled)) => {
                tally.sync_files += usize::from(sync_file);
                tally.unsignalled += usize::from(unsignalled);
            }
            Err(error) => tally.failures.push(format!("export_fence: {error}")),
        }
        match read {
            Ok(()) => {
                tally.gpu += usize::from(sampled_correct(&bytes, rounds, seed));
                tally.sentinel += usize::from(saw_sentinel(&bytes));
            }
            Err(reason) => tally.failures.push(format!("read: {reason}")),
        }
    }
    tally
}

/// The submit-to-done time of a one-round write, `RUNS` times after `WARM`.
fn short_write(writer: &Writer) -> Result<Duration, String> {
    let mut times = Vec::with_capacity(RUNS);
    for round in 0..(WARM + RUNS) {
        let started = Instant::now();
        fill(&writer.shader, &writer.storage, 1, round as u32)?
            .wait()
            .map_err(|error| error.to_string())?;
        if round >= WARM {
            times.push(started.elapsed());
        }
    }
    Ok(median_of(&mut times))
}

fn failures(label: &str, tally: &Tally) {
    if let Some(last) = tally.failures.last() {
        println!(
            "dma-fence: {label} failures {}, the last: {last}",
            tally.failures.len()
        );
    }
}

/// E9, then the device-side wait of E14, on a 1 MiB dma-heap buffer. Prints `dma-fence:` and
/// `dma-wait:` lines.
pub fn fence_after_write() {
    let skip = |reason: String| {
        println!("dma-fence: skipped: {reason}");
        println!("dma-wait: skipped: no E9 buffer to wait on: {reason}");
    };
    let heap = match DmaHeap::open("system") {
        Ok(heap) => heap,
        Err(error) => return skip(format!("no /dev/dma_heap/system: {error}")),
    };
    let buf = match heap.allocate(BYTES + ROOM) {
        Ok(buf) => buf,
        Err(error) => return skip(format!("no dma-heap buffer: {error}")),
    };
    let map = match buf.as_ref().map() {
        Ok(map) => map,
        Err(error) => return skip(format!("the dma-buf doesn't map: {error}")),
    };
    let on = match writer(Options::default(), &buf) {
        Ok(writer) => writer,
        Err(reason) => return skip(reason),
    };
    println!("dma-fence: import: the device imported the 1 MiB dma-heap buffer in place");
    let rounds = match calibrate(&on.shader, &on.storage, &map) {
        Ok(rounds) => rounds,
        Err(reason) => return skip(reason),
    };
    println!(
        "dma-fence: GPU write: {rounds} loop rounds per word, sized so the write outlives a read right after its submit"
    );

    let before = on.backend.sync_stats().unwrap_or_default();
    let waits = runs(&on, &buf, &map, rounds, 1000, |out| {
        cpu_read(&buf, &map, out)
    });
    let after = on.backend.sync_stats().unwrap_or_default();
    let early = runs(&on, &buf, &map, rounds, 2000, |out| plain_read(&map, out));
    let attaches = after.attaches - before.attaches;
    let host_waits = after.host_waits - before.host_waits;
    println!(
        "dma-fence: fence attached: {attaches} of {} submits; host waits {host_waits}",
        WARM + RUNS
    );
    if let Some(why) = after.host_wait_reason.filter(|_| host_waits > 0) {
        println!("dma-fence: host wait: {why}");
    }
    println!(
        "dma-fence: sync_file: {} of {RUNS} exports; unsignalled at export: {} of {RUNS}",
        waits.sync_files, waits.unsignalled
    );
    println!(
        "dma-fence: (a) the CPU read in the sync bracket, no other wait, sees the GPU's values: {} of {RUNS}",
        waits.gpu
    );
    failures("(a)", &waits);
    println!(
        "dma-fence: (b) planted: a plain read right after the submit sees the sentinel: {} of {RUNS}",
        early.sentinel
    );
    failures("(b)", &early);

    let adapter = on.backend.start().map(|record| record.adapter.name).ok();
    let off = writer(
        Options {
            adapter,
            no_fence_attach: true,
            ..Options::default()
        },
        &buf,
    );
    let off_time = match &off {
        Ok(off) => {
            let unattached = runs(off, &buf, &map, rounds, 3000, |out| {
                cpu_read(&buf, &map, out)
            });
            println!(
                "dma-fence: (c) planted: attaching off: signalled at export {} of {RUNS}; the bracketed read sees the sentinel: {} of {RUNS}",
                RUNS - unattached.unsignalled,
                unattached.sentinel
            );
            failures("(c)", &unattached);
            short_write(off)
        }
        Err(reason) => {
            println!("dma-fence: (c) skipped: {reason}");
            Err(reason.clone())
        }
    };
    let per_attach = if attaches > 0 {
        (after.host_nanos - before.host_nanos) as f64 / attaches as f64
    } else {
        0.0
    };
    match (short_write(&on), off_time) {
        (Ok(attached), Ok(not)) => println!(
            "dma-fence: attach cost: host {per_attach:.0} ns a submit; a short write, submit to done: median {:.4} ms attached, {:.4} ms not",
            ms(attached),
            ms(not)
        ),
        (Err(reason), _) | (_, Err(reason)) => println!(
            "dma-fence: attach cost: host {per_attach:.0} ns a submit; submit to done not measured: {reason}"
        ),
    }
    // The planted copy with no wait follows a write by a producer that attaches nothing.
    let planted = off.as_ref().ok().map(|off| super::dmabuf_plant::Producer {
        backend: &off.backend,
        shader: &off.shader,
        storage: &off.storage,
    });
    device_side_wait(
        &on.backend,
        &on.shader,
        &on.storage,
        &buf,
        &map,
        rounds,
        planted,
    );
}
