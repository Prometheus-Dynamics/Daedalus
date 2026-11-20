//! The timeout and device-loss path on a real device, opt-in: `daedalus-gpu-budgets grind`. It is
//! left out of a run with no section named, and of every gate. A long grind shader runs past the
//! backend's wait bound, so the wait fails as a timeout and the device is marked lost. The next
//! run faults, the backend opens a new device, and a run of known work on that device is checked.
//! It writes nothing to disk and keeps nothing after it returns.
//!
//! The work is set by three environment variables. `DAEDALUS_GRIND_ROUNDS` is the loop count, a
//! `u32` (default 4000000000). `DAEDALUS_GRIND_REPEAT` repeats that loop in each invocation
//! (default 4): one loop of a `u32` count is about 2 s on one device, so the repeat is what takes
//! the work past the bound. `DAEDALUS_GRIND_ELEMENTS` is the number of invocations (default 64).
//! `DAEDALUS_GRIND_BOUND_MS` lowers the wait bound for a test of the path on a device whose driver
//! resets a long job before 5 s; the default is the backend's own bound.
//! If the wait does not pass the bound, the section says so and does not claim the path ran. Then
//! a known run on the device as it stands says whether a driver that reset the job early left the
//! device working. Either way the driver's own device-lost reports are printed, and one last line,
//! `grind: summary: wait=... lost_callback=... known_run=... same_device=... reopen_s=...`, is the
//! record a judge reads.

use std::time::{Duration, Instant};

use daedalus_wgpu::shader::{BindingKind, BindingSpec, Bound, Dispatch, Run, Shader, ShaderSpec};
use daedalus_wgpu::{DEFAULT_WAIT_TIMEOUT, Options, Storage, Ticket, WaitError, WgpuBackend};

use super::device_with;

/// How long the device's work may run after the wait fails, before the section stops watching.
const WATCH: Duration = Duration::from_secs(60);
/// The most tries at a run on the reopened device, with the backend's retry time between them.
const TRIES: usize = 6;

/// Work whose length is set by `rounds` and `repeat`: the fence tests' grind shader, with the
/// loop repeated so the work can pass the wait bound.
static GRIND: ShaderSpec = ShaderSpec {
    name: "grind",
    entry: "main",
    source: "
        struct Config { rounds: u32, repeat: u32 }
        @group(0) @binding(0) var<storage, read_write> dst: array<u32>;
        @group(0) @binding(1) var<uniform> config: Config;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x >= arrayLength(&dst) { return; }
            var x = id.x + dst[id.x];
            for (var outer = 0u; outer < config.repeat; outer++) {
                for (var round = 0u; round < config.rounds; round++) {
                    x = x * 1664525u + 1013904223u;
                    x = x ^ (x >> 13u);
                }
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

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// One run of the grind shader over `buffer`, with the loop `rounds` long and `repeat` times.
/// `Err` says what the backend answered, which is printed.
fn submit(
    backend: &WgpuBackend,
    buffer: &Storage,
    rounds: u32,
    repeat: u32,
) -> Result<Ticket, String> {
    let shader = Shader::load(backend, &GRIND).map_err(|error| error.to_string())?;
    let mut config = [0u8; 8];
    config[..4].copy_from_slice(&rounds.to_le_bytes());
    config[4..].copy_from_slice(&repeat.to_le_bytes());
    match shader
        .run(&[Bound::Storage(buffer), Bound::Uniform(&config)])
        .map_err(|error| error.to_string())?
    {
        Run::Started(ticket) => Ok(ticket),
        Run::Busy(_) => Err("the device is busy".into()),
    }
}

/// A run of known work on the backend as it is now: zeros in, the invocation's id out, with no
/// loops. Its result is checked word by word. `Err` is what the backend said on the way.
fn known_run(backend: &WgpuBackend, elements: u32) -> Result<bool, String> {
    let bytes = elements as usize * 4;
    let buffer = Storage::new(backend, bytes as u64).map_err(|error| error.to_string())?;
    buffer
        .write(0, &vec![0u8; bytes])
        .map_err(|error| error.to_string())?;
    submit(backend, &buffer, 0, 0)?
        .wait()
        .map_err(|error| error.to_string())?;
    let mut out = vec![0u8; bytes];
    buffer
        .read_into(0, &mut out)
        .map_err(|error| error.to_string())?;
    let got: Vec<u32> = out
        .as_chunks::<4>()
        .0
        .iter()
        .map(|word| u32::from_le_bytes(*word))
        .collect();
    Ok(got == (0..elements).collect::<Vec<u32>>())
}

/// Whether a storage made now sits on a different device from `before`.
fn device_changed(backend: &WgpuBackend, elements: u32, before: u64) -> bool {
    Storage::new(backend, u64::from(elements) * 4)
        .map(|storage| storage.device_key() != before)
        .unwrap_or(false)
}

/// What the known run gave: `correct`, `wrong`, or `none` when it could not run at all.
fn outcome_word(result: &Result<bool, String>) -> &'static str {
    match result {
        Ok(true) => "correct",
        Ok(false) => "wrong",
        Err(_) => "none",
    }
}

/// The driver's device-lost reports so far, one line each, printed as they stand.
fn report_losses(when: &str) -> bool {
    let losses = daedalus_wgpu::driver_losses();
    if losses.is_empty() {
        println!("grind: device-lost callback {when}: not fired");
    }
    for line in &losses {
        println!("grind: device-lost callback {when}: fired: {line}");
    }
    !losses.is_empty()
}

pub fn run() {
    let rounds = env_u32("DAEDALUS_GRIND_ROUNDS", 4_000_000_000);
    let repeat = env_u32("DAEDALUS_GRIND_REPEAT", 4);
    let elements = env_u32("DAEDALUS_GRIND_ELEMENTS", 64).max(1);
    let bound = match env_u32("DAEDALUS_GRIND_BOUND_MS", 0) {
        0 => DEFAULT_WAIT_TIMEOUT,
        ms => Duration::from_millis(u64::from(ms)),
    };
    println!(
        "grind: {elements} invocations, {rounds} loops repeated {repeat} times, wait bound {bound:?}"
    );
    // A device of this section's own, so nothing else's state is touched.
    let Some(backend) = device_with(|options| Options {
        wait_timeout: bound,
        ..options
    }) else {
        println!("grind: skipped, no device");
        return;
    };
    let buffer = match Storage::new(&backend, u64::from(elements) * 4) {
        Ok(buffer) => buffer,
        Err(error) => {
            println!("grind: skipped, no buffer: {error}");
            return;
        }
    };
    let before = buffer.device_key();
    let ticket = match submit(&backend, &buffer, rounds, repeat) {
        Ok(ticket) => ticket,
        Err(error) => {
            println!("grind: refused before the wait: {error}");
            return;
        }
    };
    let started = Instant::now();
    let waited = ticket.wait();
    let took = started.elapsed();
    let error = match waited {
        Ok(()) => {
            // The driver may have reset the hung job before the bound (V3D does, about half a
            // second in). Then the wait succeeds, and the question is whether the device still
            // works: the known run is on this same device, without a reopen.
            println!(
                "grind: the wait finished in {:.2} s, inside the bound: the timeout path did not run",
                took.as_secs_f64()
            );
            let lost = report_losses("after the wait");
            // A storage made now opens a new device if the old one was lost, so the time to the
            // known run's answer is the reopen time when the device changed.
            let reopen = Instant::now();
            let same = !device_changed(&backend, elements, before);
            println!(
                "grind: same device after the wait: {}",
                if same { "yes" } else { "no" }
            );
            let known = known_run(&backend, elements);
            let reopen_s = if same {
                "none".to_string()
            } else {
                format!("{:.3}", reopen.elapsed().as_secs_f64())
            };
            let which = if same {
                "the same device"
            } else {
                "a reopened device"
            };
            match &known {
                Ok(true) => println!("grind: known run on {which} after the wait: correct"),
                Ok(false) => println!("grind: known run on {which} after the wait: wrong values"),
                Err(error) => println!("grind: known run on {which} after the wait: {error}"),
            }
            let lost = report_losses("after the known run") || lost;
            println!(
                "grind: summary: wait=finished lost_callback={} known_run={} same_device={} reopen_s={reopen_s}",
                if lost { "fired" } else { "not-fired" },
                outcome_word(&known),
                if same { "yes" } else { "no" },
            );
            return;
        }
        Err(error) => error,
    };
    let failed = Instant::now();
    let timed_out = matches!(error, WaitError::TimedOut { .. });
    println!(
        "grind: the wait failed after {:.2} s: {error} (timeout: {timed_out})",
        took.as_secs_f64()
    );
    report_losses("at the failed wait");

    // The next run on the same shader and buffers: a fault, or the device's own refusal.
    match submit(&backend, &buffer, 0, 0) {
        Err(error) => println!("grind: the next run faults: {error}"),
        Ok(_) => println!("grind: the next run was accepted; the device was not marked lost"),
    }

    // Each try opens the device again if it has to, after the backend's own retry time.
    let reopen = Instant::now();
    let mut outcome = None;
    let mut last = Err("no try made".to_string());
    for attempt in 1..=TRIES {
        last = known_run(&backend, elements);
        match &last {
            Ok(true) => {
                outcome = Some(attempt);
                break;
            }
            Ok(false) => {
                println!("grind: try {attempt}: the known run gave wrong values");
                break;
            }
            Err(error) => println!("grind: try {attempt}: {error}"),
        }
        let pause = backend.retry_in().unwrap_or(Duration::from_secs(1));
        std::thread::sleep(pause.min(Duration::from_secs(5)));
    }
    let changed = device_changed(&backend, elements, before);
    let reopen_s = match outcome {
        Some(attempt) => {
            let reopen_s = reopen.elapsed().as_secs_f64();
            println!(
                "grind: the known run on the reopened device is correct on try {attempt}, {reopen_s:.3} s from the failed wait; a new device: {changed}"
            );
            format!("{reopen_s:.3}")
        }
        None => {
            println!("grind: the known run was not correct within {TRIES} tries");
            "none".to_string()
        }
    };

    // Whether the abandoned work on the old device ever finishes: watched for a while, not waited on.
    let mut finished = None;
    while failed.elapsed() < WATCH {
        if ticket.is_done() {
            finished = Some(failed.elapsed());
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if let Some(after) = finished {
        println!(
            "grind: the abandoned work finished {:.2} s after the failed wait (seen at the next poll)",
            after.as_secs_f64()
        );
    } else {
        println!("grind: the abandoned work did not finish within {WATCH:?} of watching");
    }
    let lost = report_losses("at the end");
    println!(
        "grind: summary: wait={} lost_callback={} known_run={} same_device={} reopen_s={reopen_s}",
        if timed_out { "timed-out" } else { "failed" },
        if lost { "fired" } else { "not-fired" },
        outcome_word(&last),
        if changed { "no" } else { "yes" },
    );
}
