//! The dma-buf parts of two board figures, E9 and E14. Each imports dma-heap buffers into the
//! device with the backend's own dma-buf import (the step a `with_dmabuf` type plans), and keeps
//! its own fd of the same dma-buf to export fences from and to map. E9 itself is in
//! `dmabuf_fence`; this file has the imports and the GPU write it uses, and E14.
//!
//! The device-side wait (E14): a second device imports the dma-buf E9 writes, and a copy out of it
//! is submitted there waiting on the fence exported at the write's submit, with no host wait before
//! it. The backend attached the write's fence to the buffer, so the export carries it. A planted
//! copy with no wait, after a write by a device with attaching off, must see the sentinel the CPU
//! wrote before the write. With the fence attached, V3D's kernel makes even a job that doesn't wait
//! wait for it, so the plant needs a producer that attaches nothing.
//!
//! `dma-steady` (E14): eight buffers are imported in rotation for 1000 imports. It prints the
//! allocations per import (the counting allocator) and the cache hits, which are the rotations
//! the backend did not import again: the backend's own import count is the only number used.
//!
//! Where a step can't be taken here, its line says `skipped:` with the reason.

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use daedalus_abi::counting::counted;
use daedalus_abi::testing::run_once;
use daedalus_abi::{Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx};
use daedalus_dma_sys::{Access, DmaBuf, DmaHeap, Mapping, SyncFence};
use daedalus_runner::{drop_owned, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey, Typed};
use daedalus_wgpu::shader::{BindingKind, BindingSpec, Bound, Dispatch, Run, Shader, ShaderSpec};
use daedalus_wgpu::{
    DmaBufBacked, Options, RawPlacement, Storage, Ticket, WgpuBackend, raw_copy_after,
};

use super::{RUNS, WARM, line_ms, summary};

/// One value: 1 MiB, the size the record names.
pub(super) const BYTES: u64 = 1 << 20;
/// Allocated past the value, for the memory a device needs behind an imported buffer beyond its
/// bytes (V3D needs some).
pub(super) const ROOM: u64 = 64 << 10;
/// The CPU checks every 997th word, which spreads the samples over every workgroup.
pub(super) const SAMPLE_STEP: usize = 997;
/// Imports in the steady rotation, and how many buffers are in it.
const STEADY_IMPORTS: usize = 1000;
const STEADY_BUFFERS: usize = 8;
/// The calibration doubles the loop rounds from here, and stops at the cap, or once one run of the
/// write takes `LONG_ENOUGH`: a fence still signalled at export then doesn't follow the device.
pub(super) const ROUNDS_START: u32 = 16;
pub(super) const ROUNDS_CAP: u32 = 1 << 20;
pub(super) const LONG_ENOUGH: Duration = Duration::from_millis(50);
/// The longest a fence or a copy is waited for.
pub(super) const SIGNAL_BOUND: Duration = Duration::from_secs(5);

/// The GPU's write: each word is a hash of its index and the run's seed, with `rounds` loops. Its
/// work grows with `rounds`, so a long enough run is still going when its fence is exported.
pub(super) static FILL: ShaderSpec = ShaderSpec {
    name: "dmabuf-fill",
    entry: "main",
    source: "
        struct Config { rounds: u32, seed: u32 }
        @group(0) @binding(0) var<storage, read_write> dst: array<u32>;
        @group(0) @binding(1) var<uniform> config: Config;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x >= arrayLength(&dst) { return; }
            var x = id.x * 2654435761u + config.seed;
            for (var round = 0u; round < config.rounds; round++) {
                x = x * 1664525u + 1013904223u;
                x = x ^ (x >> 13u);
            }
            dst[id.x] = x;
        }
    ",
    workgroup: [64, 1, 1],
    bindings: &[
        BindingSpec {
            index: 0,
            name: "dst",
            kind: BindingKind::Storage {
                read_only: false,
                stride: 4,
            },
        },
        BindingSpec {
            index: 1,
            name: "config",
            kind: BindingKind::Uniform { size: 8 },
        },
    ],
    dispatch: Dispatch::PerElement {
        binding: 0,
        stride: 4,
    },
    inputs: &[],
    outputs: &[],
};

/// What `FILL` writes at `index` after `rounds` loops with `seed`, computed on the CPU in the same
/// wrapping arithmetic the device uses.
pub fn expected(index: u32, rounds: u32, seed: u32) -> u32 {
    let mut x = index.wrapping_mul(2_654_435_761).wrapping_add(seed);
    for _ in 0..rounds {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        x ^= x >> 13;
    }
    x
}

/// A dma-buf of one value, from the fd the import takes. The value is the buffer's first 1 MiB.
struct Part {
    fd: OwnedFd,
}

impl Typed for Part {
    const KEY: TypeKey = TypeKey::new("t:dmabuf-section");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:dmabuf-section"));
}

impl DmaBufBacked for Part {
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        Some((self.fd.as_fd(), BYTES))
    }
    fn offset(&self) -> u64 {
        0
    }
}

fn cx() -> CompileCx<'static> {
    CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 64,
        merge_allowed: true,
        freeze_params: false,
        sharing: &[],
        lent: &[],
        offered: &[],
    }
}

/// A device that imports this section's dma-bufs, opened with `options`, and the planned import
/// step for them.
pub(super) fn open_importing(options: Options) -> Result<(WgpuBackend, Step), String> {
    let backend = WgpuBackend::open(options).with_dmabuf::<Part>();
    backend
        .start()
        .map_err(|error| format!("no device: {error}"))?;
    let transfers = backend.transfers();
    let transfer = transfers
        .iter()
        .find(|transfer| transfer.id == "wgpu.import:t:dmabuf-section")
        .ok_or("the import is not planned")?;
    let step = (transfer.make)(&cx()).map_err(|_| "the import has no step".to_string())?;
    Ok((backend, step))
}

/// The value of `buf` in the device, through `step`. The device buffer it gives is the dma-buf's
/// memory once the device imports it; a device that copies gives a copy.
pub(super) fn import_whole(step: &mut Step, buf: &DmaBuf) -> Result<Arc<Storage>, String> {
    let part = Part {
        fd: buf
            .try_clone()
            .map_err(|error| error.to_string())?
            .into_fd(),
    };
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    put_typed(&mut cells[0], TypeIndex(0), SpaceIndex(0), Arc::new(part));
    let mut context = StepCx::new();
    let end = {
        let mut slots = Slots::of(&mut cells);
        run_once(step, &mut slots, &mut context)
    };
    if !matches!(end, StepEnd::Done) {
        return Err(context.reason().to_string());
    }
    let storage = take_typed::<Storage>(&mut cells[1]).ok_or("the import gave no buffer")?;
    drop_owned(cells[0]);
    Ok(storage)
}

/// Imports `buf` through `step`, and says why not unless the device took it in place: one more
/// import in the backend's count, and no copy.
pub(super) fn import_in_place(
    backend: &WgpuBackend,
    step: &mut Step,
    buf: &DmaBuf,
) -> Result<Arc<Storage>, String> {
    let count = |backend: &WgpuBackend| backend.transfer_stats().map(|stats| stats.imports);
    let before = count(backend);
    let storage =
        import_whole(step, buf).map_err(|reason| format!("the import was refused: {reason}"))?;
    match (before, count(backend)) {
        (Some(before), Some(after)) if after == before + 1 => Ok(storage),
        _ => Err(format!(
            "the device copied the dma-buf instead of importing it: {}",
            backend.dmabuf_status(false)
        )),
    }
}

/// Submits one run of `FILL` over `storage`, waiting out a device that is busy with another run.
pub(super) fn fill(
    shader: &Shader,
    storage: &Storage,
    rounds: u32,
    seed: u32,
) -> Result<Ticket, String> {
    let mut config = [0u8; 8];
    config[..4].copy_from_slice(&rounds.to_le_bytes());
    config[4..].copy_from_slice(&seed.to_le_bytes());
    loop {
        match shader
            .run(&[Bound::Storage(storage), Bound::Uniform(&config)])
            .map_err(|error| error.to_string())?
        {
            Run::Started(ticket) => return Ok(ticket),
            Run::Busy(oldest) => oldest.wait().map_err(|error| error.to_string())?,
        }
    }
}

/// Whether the first 1 MiB of `bytes` holds what `FILL` gives at the sampled words.
pub(super) fn sampled_correct(bytes: &[u8], rounds: u32, seed: u32) -> bool {
    let words = bytes.as_chunks::<4>().0;
    (0..words.len())
        .step_by(SAMPLE_STEP)
        .all(|index| u32::from_le_bytes(words[index]) == expected(index as u32, rounds, seed))
}

/// The dma-buf's first 1 MiB, read by the CPU through its own mapping, inside a read sync.
pub(super) fn cpu_read(buf: &DmaBuf, map: &Mapping, out: &mut [u8]) -> Result<(), String> {
    let buf = buf.as_ref();
    buf.begin_cpu_access(Access::Read)
        .map_err(|error| error.to_string())?;
    let read = map.read(0, out).map_err(|error| error.to_string());
    buf.end_cpu_access(Access::Read)
        .map_err(|error| error.to_string())?;
    read
}

/// E14's device-side wait: a second device imports the same dma-buf and copies it out, in a submit
/// that waits on the fence exported while the first device is still writing. The host waits for
/// nothing before that submit. Then the planted runs (`dmabuf_plant`): the same copy with no wait,
/// after a write by `planted`, a device with attaching off, held and not held, from this process
/// and from a second one.
pub(super) fn device_side_wait(
    producer: &WgpuBackend,
    shader: &Shader,
    storage: &Storage,
    buf: &DmaBuf,
    map: &Mapping,
    rounds: u32,
    planted: Option<super::dmabuf_plant::Producer>,
) {
    use super::dmabuf_fence::prefill;
    let skip = |reason: String| println!("dma-wait: skipped: {reason}");
    let adapter = match producer.start() {
        Ok(record) => record.adapter.name,
        Err(error) => return skip(format!("no producer device: {error}")),
    };
    let (consumer, mut step) = match open_importing(Options {
        adapter: Some(adapter.clone()),
        ..Options::default()
    }) {
        Ok(opened) => opened,
        Err(reason) => return skip(format!("no second device: {reason}")),
    };
    let source = match import_in_place(&consumer, &mut step, buf) {
        Ok(source) => source,
        Err(reason) => return skip(format!("the second device: {reason}")),
    };
    let submitter = match consumer.raw_submitter(2) {
        Ok(submitter) => submitter,
        Err(error) => return skip(format!("no raw submitter: {error}")),
    };
    let block = match consumer.raw_block(BYTES, RawPlacement::HostMapped) {
        Ok(block) => block,
        Err(error) => return skip(format!("no host-mapped block: {error}")),
    };
    let (mut submit, mut after_signal, mut whole) = (Vec::new(), Vec::new(), Vec::new());
    let (mut pending, mut correct, mut failures) = (0, 0, Vec::new());
    for round in 0..(WARM + RUNS) {
        let seed = 5000 + round as u32;
        if let Err(reason) = prefill(buf, map) {
            return skip(format!("the sentinel prefill: {reason}"));
        }
        let ticket = match fill(shader, storage, rounds, seed) {
            Ok(ticket) => ticket,
            Err(reason) => return skip(format!("the GPU write failed: {reason}")),
        };
        let watched = match buf.as_ref().export_fence(Access::Read) {
            Ok(fence) => fence,
            Err(error) => {
                ticket.wait().ok();
                return skip(format!("export_fence: {error}"));
            }
        };
        let waited = match watched.as_fd().try_clone_to_owned() {
            Ok(fd) => fd,
            Err(error) => {
                ticket.wait().ok();
                return skip(format!("the fence's fd doesn't duplicate: {error}"));
            }
        };
        let still = matches!(watched.is_signaled(), Ok(false));
        let semaphore = match consumer.import_sync_fd(waited) {
            Ok(semaphore) => semaphore,
            Err(error) => {
                ticket.wait().ok();
                return skip(format!(
                    "the second device can't wait on a sync_file: {error}"
                ));
            }
        };
        let host = Instant::now();
        let mut submitted = Duration::ZERO;
        let mut signalled = None;
        let copied = raw_copy_after(&submitter, &source, &block, BYTES, &[&semaphore], || {
            submitted = host.elapsed();
            signalled = signal_instant(&watched);
        });
        consumer.attach_after_submit();
        let done = Instant::now();
        ticket.wait().ok();
        let mut bytes = vec![0u8; BYTES as usize];
        let ok = match copied
            .map_err(|error| error.to_string())
            .and_then(|()| block.read(0, &mut bytes).map_err(|error| error.to_string()))
        {
            Ok(()) => sampled_correct(&bytes, rounds, seed),
            Err(reason) => {
                failures.push(reason);
                false
            }
        };
        if round < WARM {
            continue;
        }
        pending += usize::from(still);
        correct += usize::from(ok);
        submit.push(submitted);
        whole.push(done - host);
        if let Some(signalled) = signalled {
            after_signal.push(done.saturating_duration_since(signalled));
        }
    }
    println!("dma-wait: producer still writing at the consumer's submit: {pending} of {RUNS}");
    println!("dma-wait: the consumer's copy saw the GPU's values: {correct} of {RUNS}");
    println!(
        "dma-wait: host submit, no host wait before it: {}",
        line_ms(summary(&mut submit))
    );
    if !after_signal.is_empty() {
        println!(
            "dma-wait: copy done after the fence signalled (the device-side wait's cost and the copy): {}",
            line_ms(summary(&mut after_signal))
        );
    }
    println!(
        "dma-wait: submit to copy done, the producer's rest included: {}",
        line_ms(summary(&mut whole))
    );
    if let Some(last) = failures.last() {
        println!("dma-wait: failures {}, the last: {last}", failures.len());
    }
    let Some(planted) = planted else {
        println!("dma-wait: planted: skipped: no producer with attaching off");
        return;
    };
    let local = || {
        let copied = raw_copy_after(&submitter, &source, &block, BYTES, &[], || {});
        consumer.attach_after_submit();
        let mut bytes = vec![0u8; BYTES as usize];
        copied
            .map_err(|error| error.to_string())
            .and_then(|()| block.read(0, &mut bytes).map_err(|error| error.to_string()))
            .map(|()| bytes)
    };
    super::dmabuf_plant::run(&planted, buf, map, rounds, &adapter, &local);
}

/// When `fence` signals, seen from this thread, or `None` past the bound.
pub(super) fn signal_instant(fence: &SyncFence) -> Option<Instant> {
    fence
        .wait(SIGNAL_BOUND)
        .ok()
        .filter(|done| *done)
        .map(|_| Instant::now())
}

/// E14's steady state: eight 1 MiB dma-heap buffers imported in rotation for 1000 imports. Prints
/// `dma-steady:` lines.
pub fn steady_imports() {
    let skip = |reason: String| println!("dma-steady: skipped: {reason}");
    let heap = match DmaHeap::open("system") {
        Ok(heap) => heap,
        Err(error) => return skip(format!("no /dev/dma_heap/system: {error}")),
    };
    let bufs: Result<Vec<DmaBuf>, _> = (0..STEADY_BUFFERS)
        .map(|_| heap.allocate(BYTES + ROOM))
        .collect();
    let bufs = match bufs {
        Ok(bufs) => bufs,
        Err(error) => return skip(format!("no dma-heap buffers: {error}")),
    };
    let (backend, mut step) = match open_importing(Options::default()) {
        Ok(opened) => opened,
        Err(reason) => return skip(reason),
    };
    let Some(before) = backend.transfer_stats() else {
        return skip("no transfer count".to_string());
    };
    let mut allocations = Vec::with_capacity(STEADY_IMPORTS);
    for round in 0..STEADY_IMPORTS {
        let (made, allocated) = counted(|| import_whole(&mut step, &bufs[round % STEADY_BUFFERS]));
        match made {
            Ok(storage) => drop(storage),
            Err(reason) => return skip(format!("import {round} failed: {reason}")),
        }
        allocations.push(allocated as u64);
    }
    let Some(after) = backend.transfer_stats() else {
        return skip("no transfer count".to_string());
    };
    let imports = after.imports - before.imports;
    let copies = after.uploads - before.uploads;
    if imports == 0 {
        return skip(format!(
            "the device copied every dma-buf ({copies} copies) instead of importing it: {}",
            backend.dmabuf_status(false)
        ));
    }
    let hits = (STEADY_IMPORTS as u64).saturating_sub(imports);
    println!(
        "dma-steady: {STEADY_BUFFERS} buffers of 1 MiB, {STEADY_IMPORTS} imports in rotation: device imports {imports}, cache hits {hits}, copies {copies}"
    );
    let median = |values: &[u64]| {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        sorted[sorted.len() / 2]
    };
    let rest = &allocations[STEADY_BUFFERS..];
    println!(
        "dma-steady: allocations per import: first {STEADY_BUFFERS} imports {:?}; after them median {} (min {}, max {})",
        &allocations[..STEADY_BUFFERS],
        median(rest),
        rest.iter().min().copied().unwrap_or(0),
        rest.iter().max().copied().unwrap_or(0)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cpu_value_follows_the_seed_and_the_rounds() {
        assert_eq!(expected(0, 0, 7), 7);
        assert_ne!(expected(1, 3, 7), expected(1, 3, 8));
        assert_ne!(expected(1, 3, 7), expected(1, 4, 7));
        let bytes: Vec<u8> = (0..1024u32)
            .flat_map(|index| expected(index, 5, 9).to_le_bytes())
            .collect();
        assert!(sampled_correct(&bytes, 5, 9));
        assert!(!sampled_correct(&bytes, 5, 10));
    }
}
