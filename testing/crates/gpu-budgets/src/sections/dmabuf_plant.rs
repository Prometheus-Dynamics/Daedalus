//! E14's planted consumer: a copy out of the dma-buf that doesn't wait for the producer's write.
//!
//! The write comes from a device with attaching off, so nothing of Daedalus's orders the copy
//! after it. Two ways, each 50 runs, from this process and from a second one (`dmabuf_peer`):
//! - held: the producer's queue is held on its own device (`raw_hold`) before the write is
//!   submitted, the copy is submitted and finishes, and only then is the hold opened. A copy that
//!   doesn't wait has to see the sentinel the CPU wrote before the write. That is the plant: the
//!   figure catches a consumer that doesn't wait. If the copy instead can't finish until the hold
//!   opens (opened after `HOLD` regardless), something orders it after work that wasn't even
//!   submitted to the GPU yet, and the line says how many waited.
//! - not held: the write is submitted and running when the copy is submitted. On one V3D the
//!   copy then runs after the write (E14, 2026-11-20 and 2026-11-21): the line records that order.

use std::sync::mpsc;
use std::time::Duration;

use daedalus_dma_sys::{DmaBuf, Mapping};
use daedalus_wgpu::shader::Shader;
use daedalus_wgpu::{Storage, WgpuBackend};

use super::dmabuf::fill;
use super::dmabuf_fence::prefill;
use super::dmabuf_peer::{Peer, Seen, seen};
use super::{RUNS, WARM};

/// How long a held write is held at most, when the copy can't finish while it is.
const HOLD: Duration = Duration::from_secs(2);

/// The producer: a device with attaching off, importing the same buffer.
pub(super) struct Producer<'a> {
    pub backend: &'a WgpuBackend,
    pub shader: &'a Shader,
    pub storage: &'a Storage,
}

#[derive(Default)]
struct Count {
    sentinel: usize,
    gpu: usize,
    other: usize,
    waited: usize,
    submitted: usize,
    failures: Vec<String>,
}

impl Count {
    fn add(&mut self, seen: Result<Seen, String>, waited: bool) {
        match seen {
            Ok(Seen::Sentinel) => self.sentinel += 1,
            Ok(Seen::Gpu) => self.gpu += 1,
            Ok(Seen::Other) => self.other += 1,
            Err(reason) => self.failures.push(reason),
        }
        self.waited += usize::from(waited);
    }

    fn print(&self, label: &str, held: bool) {
        let held = match held {
            true => format!(
                "; it waited for the hold {}; the write was submitted before the copy {}",
                self.waited, self.submitted
            ),
            false => String::new(),
        };
        println!(
            "dma-wait: {label}: the copy with no wait saw the sentinel {} of {RUNS}, the GPU's values {}, neither {}{held}",
            self.sentinel, self.gpu, self.other
        );
        if let Some(last) = self.failures.last() {
            println!(
                "dma-wait: {label}: failures {}, the last: {last}",
                self.failures.len()
            );
        }
    }
}

/// The planted runs. `local` makes one copy with no wait on this process's consumer device and
/// returns its bytes; `adapter` names the device the second process opens.
pub(super) fn run(
    producer: &Producer,
    buf: &DmaBuf,
    map: &Mapping,
    rounds: u32,
    adapter: &str,
    local: &dyn Fn() -> Result<Vec<u8>, String>,
) {
    let held = rounds_of(
        producer,
        buf,
        map,
        rounds,
        7000,
        true,
        &mut |rounds, seed, gate| {
            held_locally(gate, || local().map(|bytes| seen(&bytes, rounds, seed)))
        },
    );
    held.print("planted, held, this process", true);
    let free = rounds_of(
        producer,
        buf,
        map,
        rounds,
        7500,
        false,
        &mut |rounds, seed, _| (local().map(|bytes| seen(&bytes, rounds, seed)), false),
    );
    free.print("order, not held, this process", false);

    let mut peer = match Peer::spawn(adapter, buf) {
        Ok(peer) => peer,
        Err(reason) => {
            println!("dma-wait: second process: skipped: {reason}");
            return;
        }
    };
    let held = rounds_of(
        producer,
        buf,
        map,
        rounds,
        8000,
        true,
        &mut |rounds, seed, gate| {
            if let Err(reason) = peer.copy(rounds, seed) {
                return (Err(reason), false);
            }
            match peer.seen(Some(HOLD)) {
                Some(seen) => {
                    gate.map(open);
                    (seen, false)
                }
                None => {
                    gate.map(open);
                    let seen = peer.seen(None).unwrap_or_else(|| Err("no answer".into()));
                    (seen, true)
                }
            }
        },
    );
    held.print("planted, held, second process", true);
    let free = rounds_of(
        producer,
        buf,
        map,
        rounds,
        8500,
        false,
        &mut |rounds, seed, _| {
            if let Err(reason) = peer.copy(rounds, seed) {
                return (Err(reason), false);
            }
            (
                peer.seen(None).unwrap_or_else(|| Err("no answer".into())),
                false,
            )
        },
    );
    free.print("order, not held, second process", false);
}

type Copy<'a> =
    dyn FnMut(u32, u32, Option<&daedalus_wgpu::RawGate>) -> (Result<Seen, String>, bool) + 'a;

/// How long the held write's thread is given to submit before the copy is made.
const SETTLE: Duration = Duration::from_millis(50);

/// `WARM + RUNS` rounds: the sentinel, the producer's write, then `copy`. Held, the write is
/// submitted from a thread of its own, behind the hold (the submit itself may block until the
/// hold opens: lavapipe's does), and `copy` opens the hold once its copy is made. The warm rounds
/// aren't counted.
fn rounds_of(
    producer: &Producer,
    buf: &DmaBuf,
    map: &Mapping,
    rounds: u32,
    seeds: u32,
    hold: bool,
    copy: &mut Copy<'_>,
) -> Count {
    let mut count = Count::default();
    for round in 0..(WARM + RUNS) {
        let seed = seeds + round as u32;
        let outcome = match hold {
            false => (|| {
                prefill(buf, map)?;
                let ticket = fill(producer.shader, producer.storage, rounds, seed)
                    .map_err(|reason| format!("the GPU write failed: {reason}"))?;
                let (seen, waited) = copy(rounds, seed, None);
                ticket.wait().map_err(|error| error.to_string())?;
                Ok((seen, waited, false))
            })(),
            true => held_round(producer, buf, map, rounds, seed, copy),
        };
        if round < WARM {
            continue;
        }
        match outcome {
            Ok((seen, waited, submitted)) => {
                count.add(seen, waited);
                count.submitted += usize::from(submitted);
            }
            Err(reason) => count.failures.push(reason),
        }
    }
    count
}

/// One held round: what the copy saw, whether it waited for the hold, and whether the write's
/// submit had returned before the copy was made.
fn held_round(
    producer: &Producer,
    buf: &DmaBuf,
    map: &Mapping,
    rounds: u32,
    seed: u32,
    copy: &mut Copy<'_>,
) -> Result<(Result<Seen, String>, bool, bool), String> {
    prefill(buf, map)?;
    let gate = producer
        .backend
        .raw_hold()
        .map_err(|error| format!("no hold: {error}"))?;
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| fill(producer.shader, producer.storage, rounds, seed));
        std::thread::sleep(SETTLE);
        let submitted = writer.is_finished();
        let (seen, waited) = copy(rounds, seed, Some(&gate));
        open(&gate);
        let ticket = writer
            .join()
            .map_err(|_| "the write's thread panicked".to_string())?
            .map_err(|reason| format!("the GPU write failed: {reason}"))?;
        ticket.wait().map_err(|error| error.to_string())?;
        Ok((seen, waited, submitted))
    })
}

/// Makes the copy on this thread while another opens `gate` once it is made, or after `HOLD`,
/// whichever is first: a copy that can't finish while the hold is closed still finishes.
fn held_locally(
    gate: Option<&daedalus_wgpu::RawGate>,
    copy: impl FnOnce() -> Result<Seen, String>,
) -> (Result<Seen, String>, bool) {
    let Some(gate) = gate else {
        return (copy(), false);
    };
    let (made, wait) = mpsc::channel::<()>();
    std::thread::scope(|scope| {
        let opener = scope.spawn(move || {
            let waited = wait.recv_timeout(HOLD).is_err();
            open(gate);
            waited
        });
        let seen = copy();
        let _ = made.send(());
        (seen, opener.join().unwrap_or(true))
    })
}

fn open(gate: &daedalus_wgpu::RawGate) {
    let _ = gate.open();
}
