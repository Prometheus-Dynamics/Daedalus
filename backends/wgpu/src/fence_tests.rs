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

/// A shader, a buffer, and a number of rounds that keeps this device busy for a while: long
/// enough that a submission is still running when the next calls are made, on any device.
struct Grind {
    shader: Shader,
    buffer: Storage,
    rounds: [u8; 4],
}

impl Grind {
    fn new(backend: &WgpuBackend) -> Grind {
        let mut grind = Grind {
            shader: Shader::load(backend, &GRIND).expect("loads"),
            buffer: Storage::new(backend, 1 << 20).expect("a buffer"),
            rounds: 16u32.to_le_bytes(),
        };
        loop {
            // The quicker of two, so a first run's setup doesn't count as work.
            let timed = || {
                let began = Instant::now();
                started(grind.run()).wait().expect("finishes");
                began.elapsed()
            };
            let took = timed().min(timed());
            let rounds = u32::from_le_bytes(grind.rounds);
            if took > Duration::from_millis(60) || rounds >= 1 << 22 {
                return grind;
            }
            grind.rounds = (rounds * 4).to_le_bytes();
        }
    }

    /// Makes the work four times longer. Tests running beside this one share the GPU, so the
    /// first guess can be too short, and a test that sees its work finish early asks for more.
    fn longer(&mut self) {
        let rounds = u32::from_le_bytes(self.rounds);
        assert!(rounds < 1 << 26, "no amount of work keeps this device busy");
        self.rounds = (rounds * 4).to_le_bytes();
    }

    fn run(&self) -> Result<Run, ShaderError> {
        let bound = [Bound::Storage(&self.buffer), Bound::Uniform(&self.rounds)];
        self.shader.run(&bound)
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
    let Some((backend, _context)) = private(Options::default()) else {
        return;
    };
    let mut grind = Grind::new(&backend);

    // Two fit. The third isn't submitted, and comes back as the oldest to wait on.
    let (first, second, oldest) = loop {
        let first = started(grind.run());
        let second = started(grind.run());
        let asked = Instant::now();
        match grind.run().expect("runs") {
            Run::Busy(oldest) => {
                assert!(
                    asked.elapsed() < Duration::from_secs(1),
                    "being told to wait doesn't wait"
                );
                break (first, second, oldest);
            }
            // The first had already finished, so there was room: the work was too short.
            Run::Started(third) => {
                assert!(first.is_done(), "three were in flight at once");
                for ticket in [&first, &second, &third] {
                    ticket.wait().expect("finishes");
                }
                grind.longer();
            }
        }
    };
    oldest.wait().expect("finishes");
    assert!(first.is_done(), "the oldest was the first");
    // With the oldest done there is room again.
    let third = started(grind.run());
    for ticket in [&second, &third] {
        ticket.wait().expect("finishes");
        assert!(ticket.is_done());
    }
    let (took, source) = first.gpu_time().expect("done");
    assert_eq!(
        source,
        TimeSource::SubmitToDone,
        "no timestamps were asked for"
    );
    assert!(took > Duration::ZERO);
}

#[test]
fn a_fence_is_pending_until_the_work_is_done_and_then_gone() {
    let Some((backend, context)) = private(Options::default()) else {
        return;
    };
    let mut grind = Grind::new(&backend);
    let before = waiting();
    // A poll never waits for the work: it says pending while the work runs.
    let word = loop {
        let word = started(grind.run()).into_fence().data.addr();
        match Polled::poll(word) {
            FencePoll::Pending => break word,
            // Already done by the first poll: the work was too short.
            FencePoll::Ready => grind.longer(),
            FencePoll::Failed => panic!("the fence failed"),
        }
    };
    context.wait("the test's work").expect("finishes");
    assert_eq!(Polled::poll(word), FencePoll::Ready);
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
    context.wait("the test's work").expect("finishes");
}

#[test]
fn uncaptured_errors_fail_the_next_run_once() {
    let Some((backend, context)) = private(Options::default()) else {
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
    let options = Options {
        wait_timeout: Duration::from_nanos(1),
        ..Options::default()
    };
    let Some((backend, context)) = private(options) else {
        return;
    };
    let shader = Shader::load(&backend, &GRIND).expect("loads");
    let big = Storage::new(&backend, 1 << 20).expect("a buffer");
    let bound = [Bound::Storage(&big), Bound::Uniform(&[0, 16, 0, 0])];
    let ticket = started(shader.run(&bound));
    assert!(!context.is_lost());
    let waited = ticket.wait();
    assert!(
        waited.as_ref().is_err_and(WaitError::is_timeout),
        "{waited:?}"
    );
    assert!(context.is_lost());
    // Nothing more runs on a lost device.
    let refused = shader.run(&bound).map(drop);
    assert_eq!(
        refused,
        Err(ShaderError::Device("the device is lost".into()))
    );
    // Let the work finish before the device goes, with a wait this test owns.
    assert!(crate::wait::wait_idle(
        &context.device,
        Duration::from_secs(60)
    ));
}

#[test]
fn timers_report_gpu_time_per_pass() {
    let Some((backend, context)) = private(Options::default()) else {
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
