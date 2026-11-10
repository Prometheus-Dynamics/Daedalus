//! Tickets, the in-flight limit, timers and the errors a device reports, on a real device.

use std::sync::Arc;
use std::time::{Duration, Instant};

use daedalus_abi::{FencePoll, FenceWord};

use crate::context::Context;
use crate::fence::{Polled, waiting};
use crate::shader::{
    BindingKind, BindingSpec, Bound, Dispatch, Run, Shader, ShaderError, ShaderSpec,
};
use crate::{Options, Storage, TimeSource, WaitError, WgpuBackend};

/// Work whose length the test sets, through `rounds`.
static GRIND: ShaderSpec = ShaderSpec {
    name: "grind",
    entry: "main",
    source: "
        struct Config { rounds: u32 }
        @group(0) @binding(0) var<storage, read_write> dst: array<u32>;
        @group(0) @binding(1) var<uniform> config: Config;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x >= arrayLength(&dst) { return; }
            var x = id.x + dst[id.x];
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
            kind: BindingKind::Uniform { size: 4 },
        },
    ],
    dispatch: Dispatch::PerElement {
        binding: 0,
        stride: 4,
    },
};

/// A shader and a buffer, with a fixed, small amount of work. Nothing here depends on how long
/// the device takes: a software device can finish a submission before the next call returns.
struct Grind {
    shader: Shader,
    buffer: Storage,
}

impl Grind {
    fn new(backend: &WgpuBackend) -> Grind {
        Grind {
            shader: Shader::load(backend, &GRIND).expect("loads"),
            buffer: Storage::new(backend, 1 << 16).expect("a buffer"),
        }
    }

    fn run(&self) -> Result<Run, ShaderError> {
        let bound = [Bound::Storage(&self.buffer), Bound::Uniform(&[64, 0, 0, 0])];
        self.shader.run(&bound)
    }
}

/// Polls until `done` says so. A poll never blocks, so this spins, with a deadline.
fn poll_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENT;
    while !done() {
        assert!(Instant::now() < deadline, "{what} never finished");
        std::thread::yield_now();
    }
}

/// A bound no test here should meet. On a software device with other tests grinding beside
/// this one, the work can take many times what it was measured at.
const PATIENT: Duration = Duration::from_secs(120);

fn patient() -> Options {
    Options {
        wait_timeout: PATIENT,
        ..Options::default()
    }
}

/// A backend with a device of its own, so its in-flight count and errors are this test's alone.
fn private(options: Options) -> Option<(WgpuBackend, Arc<Context>)> {
    let shared = WgpuBackend::open(Options::default());
    let Ok(record) = shared.start() else {
        assert!(std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none());
        return None;
    };
    let backend = WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..options
    });
    let context = backend.context().expect("the adapter opened a moment ago");
    Some((backend, context))
}

fn started(run: Result<Run, ShaderError>) -> crate::Ticket {
    match run.unwrap_or_else(|error| panic!("a run: {error}")) {
        Run::Started(ticket) => ticket,
        Run::Busy(_) => panic!("the device had room"),
    }
}

#[test]
fn tickets_complete_without_waiting_and_in_flight_is_bounded() {
    let Some((backend, context)) = private(patient()) else {
        return;
    };
    let grind = Grind::new(&backend);
    // Submit as fast as the device lets us. Whenever it is full, the answer is the oldest
    // unfinished submission and nothing was submitted, and polling that ticket is enough to get
    // going again: nothing here waits.
    let mut tickets = Vec::new();
    let mut told_to_wait = 0;
    while tickets.len() < 24 {
        let asked = Instant::now();
        match grind.run().expect("runs") {
            Run::Started(ticket) => tickets.push(ticket),
            Run::Busy(oldest) => {
                told_to_wait += 1;
                assert!(
                    context.inflight().unfinished() <= 2,
                    "never more than the limit"
                );
                poll_until("the oldest submission", || oldest.is_done());
            }
        }
        assert!(asked.elapsed() < PATIENT);
        assert!(
            context.inflight().unfinished() <= 2,
            "never more than the limit"
        );
    }
    for ticket in &tickets {
        poll_until("a submission", || ticket.is_done());
        let (took, source) = ticket.gpu_time().expect("done");
        assert_eq!(
            source,
            TimeSource::SubmitToDone,
            "no timestamps were asked for"
        );
        assert!(took > Duration::ZERO);
    }
    // On a device that runs work in the background some of the 24 met a full device. On one that
    // finishes within the submit none did, and both are right.
    assert!(told_to_wait <= 24 * 8);
}

#[test]
fn a_fence_resolves_once_and_is_then_forgotten() {
    let Some((backend, context)) = private(patient()) else {
        return;
    };
    let grind = Grind::new(&backend);
    let before = waiting();
    let word = started(grind.run()).into_fence().data.addr();
    // Pending for as long as the work runs, which may be no time at all, then ready, once.
    poll_until("the fence", || match Polled::poll(word) {
        FencePoll::Pending => false,
        FencePoll::Ready => true,
        FencePoll::Failed => panic!("the fence failed"),
    });
    assert_eq!(
        Polled::poll(word),
        FencePoll::Failed,
        "a resolved fence is forgotten"
    );
    assert!(waiting() <= before, "nothing is left in the table");

    // A fence fails when the device reported an error while its work ran.
    let fence = started(grind.run()).into_fence();
    context.errors.record("validation: bad".into());
    assert_eq!(Polled::poll(fence.data.addr()), FencePoll::Failed);
    assert_eq!(context.errors.take().as_deref(), Some("validation: bad"));
    context.wait("the test's work").expect("finishes");
}

#[test]
fn uncaptured_errors_fail_the_next_run_once() {
    let Some((backend, context)) = private(patient()) else {
        return;
    };
    let grind = Grind::new(&backend);

    // What the device reports outside any call lands in its handler.
    drop(context.device.create_buffer(&wgpu::BufferDescriptor {
        label: None,
        size: 16,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::MAP_WRITE,
        mapped_at_creation: false,
    }));
    context.poll();
    let Err(ShaderError::Device(reported)) = grind.run().map(drop) else {
        panic!("the device's error wasn't returned");
    };
    assert!(!reported.is_empty());
    // It was returned once, and nothing was submitted for it.
    started(grind.run()).wait().expect("finishes");

    // An error that arrives while work runs fails the wait for that work.
    let ticket = started(grind.run());
    context.errors.record("out of memory".into());
    assert_eq!(
        ticket.wait(),
        Err(WaitError::Device("out of memory".into()))
    );
}

#[test]
fn a_wait_that_outlives_its_bound_fails_and_marks_the_device_lost() {
    let Some((backend, context)) = private(patient()) else {
        return;
    };
    let grind = Grind::new(&backend);
    started(grind.run()).wait().expect("finishes");
    assert!(!context.is_lost());
    // What a wait does when its bound passes. Whether a real wait gets there depends on the
    // device, so the outcome is driven directly.
    let error = context.timed_out("a submission");
    assert!(error.is_timeout());
    assert_eq!(
        error,
        WaitError::TimedOut {
            what: "a submission",
            after: PATIENT
        }
    );
    assert!(context.is_lost());
    // Nothing more runs on a lost device, and a fence on it fails.
    let refused = grind.run().map(drop);
    assert_eq!(
        refused,
        Err(ShaderError::Device("the device is lost".into()))
    );
}

#[test]
fn timers_report_gpu_time_per_pass() {
    let Some((backend, context)) = private(patient()) else {
        return;
    };
    let has_timestamps = context
        .device
        .features()
        .contains(wgpu::Features::TIMESTAMP_QUERY);
    let shader = Shader::load(&backend, &GRIND)
        .expect("loads")
        .with_timestamps();
    let big = Storage::new(&backend, 1 << 20).expect("a buffer");
    let bound = [Bound::Storage(&big), Bound::Uniform(&[64, 0, 0, 0])];
    for _ in 0..4 {
        let ticket = started(shader.run(&bound));
        ticket.wait().expect("finishes");
        // The timestamps come back with a map, which lands on a poll after the work.
        let deadline = Instant::now() + Duration::from_secs(10);
        let (took, source) = loop {
            match ticket.gpu_time() {
                Some((took, TimeSource::Timestamps)) => break (took, TimeSource::Timestamps),
                Some(measured) if !has_timestamps || Instant::now() > deadline => break measured,
                _ => context.poll(),
            }
        };
        let expected = if has_timestamps {
            TimeSource::Timestamps
        } else {
            TimeSource::SubmitToDone
        };
        assert_eq!(source, expected);
        assert!(
            took > Duration::ZERO && took < Duration::from_secs(60),
            "{took:?}"
        );
        assert_eq!(
            ticket.gpu_time(),
            Some((took, source)),
            "read again, the same"
        );
    }
}
