//! `run_adaptive_in_place` goes parallel only when measured (or hinted) work pays for dispatch.
//!
//! A frame counts as serial when every handler ran on the calling thread. The executor reads its
//! timings from an injected [`Clock`]. Each thread has a virtual clock that handlers advance by
//! their modelled cost instead of sleeping, so the segment costs the cost model sees are exact and
//! the mode decisions do not depend on machine load.
//!
//! Virtual time says nothing about which thread runs a segment, and the calling thread also drains
//! the queue. So in frames the test expects to run in parallel, each handler waits for its whole
//! gang of [`GANG`] handlers (see [`Probe::rendezvous`]). That puts one segment on each thread and
//! keeps the calling thread's share, and so the measured dispatch overhead, the same on every run.
//! The only real-time check is `#[ignore]`d; see docs/testing.md.
//!
//! The tests keep their own binary instead of sharing `tests/it`'s threads.
#![cfg(feature = "threads")]

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use daedalus_core::platform::Clock;
use daedalus_data::model::Value;
use daedalus_planner::{ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::sync::Mutex;
use daedalus_runtime::{
    NODE_COST_META_KEY, NodeError, NodeHandler, OwnedExecutor, RuntimeNode, SchedulerConfig,
    build_runtime,
};

/// Segments per frame in every test graph; the gang size of [`Probe::rendezvous`].
const GANG: usize = 4;

/// Deadlock guard for [`Probe::rendezvous`]. A parallel frame gathers its gang well within this;
/// a serial frame, which has no gang, waits it out once per handler. Not an assertion.
const GANG_WATCHDOG: Duration = Duration::from_millis(200);

thread_local! {
    /// This thread's virtual time in nanoseconds. Handlers advance the time of the thread they
    /// run on, so a segment's measured cost is exactly the work it models.
    static VIRTUAL_NS: Cell<u64> = const { Cell::new(0) };
}

/// A clock reading the calling thread's virtual time.
fn virtual_clock() -> Clock {
    Clock::new(|| Duration::from_nanos(VIRTUAL_NS.with(Cell::get)))
}

fn advance(work: Duration) {
    VIRTUAL_NS.with(|now| now.set(now.get() + work.as_nanos() as u64));
}

#[derive(Default)]
struct Probe {
    threads: Mutex<Vec<ThreadId>>,
    /// Modelled work per handler call, in microseconds.
    work_us: AtomicU64,
    /// Sleep for the work instead of advancing virtual time (the real-time check).
    real_time: bool,
    /// Handlers join a gang of [`GANG`] before finishing; see [`Probe::rendezvous`].
    gang: AtomicBool,
    /// Gang arrivals so far; a round is complete at each multiple of [`GANG`].
    arrivals: AtomicUsize,
    /// The current round and its watchdog deadline, shared by the round's handlers.
    watchdog: Mutex<(usize, Option<Instant>)>,
}

impl Probe {
    /// Whether the frame since the last call ran on more than one thread; resets the record.
    fn take_parallel(&self) -> bool {
        let caller = thread::current().id();
        let threads = std::mem::take(&mut *self.threads.lock());
        threads.iter().any(|&id| id != caller)
    }

    /// Wait until this handler's gang of [`GANG`] has arrived, or the watchdog expires.
    ///
    /// Handlers of one parallel frame each hold one thread until all of them have started, so the
    /// frame's segments spread over the workers instead of being drained by the calling thread.
    fn rendezvous(&self) {
        let ticket = self.arrivals.fetch_add(1, Ordering::SeqCst);
        let round = ticket / GANG;
        let round_end = (round + 1) * GANG;
        // One deadline per round, so a serial frame (whose gang never forms) waits once, not once
        // per handler.
        let deadline = {
            let mut watchdog = self.watchdog.lock();
            if watchdog.0 != round || watchdog.1.is_none() {
                *watchdog = (round, Some(Instant::now() + GANG_WATCHDOG));
            }
            watchdog.1.expect("deadline set above")
        };
        while self.arrivals.load(Ordering::SeqCst) < round_end && Instant::now() < deadline {
            thread::yield_now();
        }
    }
}

struct ProbeHandler(Arc<Probe>);

impl NodeHandler for ProbeHandler {
    fn run(
        &self,
        _node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        let probe = &self.0;
        probe.threads.lock().push(thread::current().id());
        if probe.gang.load(Ordering::SeqCst) {
            probe.rendezvous();
        }
        let work = Duration::from_micros(probe.work_us.load(Ordering::SeqCst));
        if probe.real_time {
            thread::sleep(work);
        } else {
            advance(work);
        }
        Ok(())
    }
}

/// `GANG` independent nodes (one ready segment each), optionally hinted heavy.
fn executor_with(
    work: Duration,
    heavy: bool,
    real_time: bool,
) -> (OwnedExecutor<ProbeHandler>, Arc<Probe>) {
    let mut graph = Graph::default();
    for idx in 0..GANG {
        let mut node = NodeInstance::new(format!("n{idx}"));
        if heavy {
            node = node.with_metadata(NODE_COST_META_KEY, Value::String("heavy".into()));
        }
        graph.nodes.push(node);
    }
    let plan = build_runtime(
        &ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig::default(),
    );
    let probe = Arc::new(Probe {
        real_time,
        ..Probe::default()
    });
    probe
        .work_us
        .store(work.as_micros() as u64, Ordering::SeqCst);
    let clock = if real_time {
        Clock::default()
    } else {
        virtual_clock()
    };
    let exec = OwnedExecutor::new(Arc::new(plan), ProbeHandler(probe.clone()))
        .with_pool_size(Some(GANG))
        .with_clock(clock);
    (exec, probe)
}

#[test]
fn cheap_graph_stays_serial() {
    let mut graph = Graph::default();
    for idx in 0..8 {
        graph.nodes.push(NodeInstance::new(format!("n{idx}")));
    }
    let plan = build_runtime(
        &ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig::default(),
    );
    let probe = Arc::new(Probe::default());
    let mut exec = OwnedExecutor::new(Arc::new(plan), ProbeHandler(probe.clone()))
        .with_pool_size(Some(8))
        .with_clock(virtual_clock());
    for frame in 0..64 {
        let telemetry = exec.run_adaptive_in_place().expect("adaptive frame");
        assert_eq!(telemetry.nodes_executed, 8);
        assert!(!probe.take_parallel(), "frame {frame} ran in parallel");
    }
}

#[test]
fn heavy_fan_out_goes_parallel() {
    let (mut exec, probe) = executor_with(Duration::from_millis(3), false, false);
    // Nothing measured yet: the first frame runs serially and times the segments. The gang is on
    // for every frame, so a frame that goes parallel always shows it, and a serial one waits out
    // the watchdog once.
    probe.gang.store(true, Ordering::SeqCst);
    exec.run_adaptive_in_place().expect("first frame");
    assert!(!probe.take_parallel(), "unmeasured frame ran in parallel");

    for frame in 0..6 {
        exec.run_adaptive_in_place().expect("adaptive frame");
        assert!(probe.take_parallel(), "frame {frame} ran serially");
    }
}

#[test]
fn heavy_hint_runs_the_first_frame_in_parallel() {
    let (mut exec, probe) = executor_with(Duration::from_millis(2), true, false);
    probe.gang.store(true, Ordering::SeqCst);
    exec.run_adaptive_in_place().expect("first frame");
    assert!(probe.take_parallel());
}

#[test]
/// Real clock: going back to serial needs measured dispatch overhead to outweigh the cheap work,
/// and a virtual clock charges no overhead, so once the work is free the model (correctly) has no
/// reason to leave parallel. The heavy phase still uses the gang, so its frames are deterministic.
fn stays_parallel_while_heavy_and_returns_to_serial_once_cheap() {
    let (mut exec, probe) = executor_with(Duration::from_millis(2), false, true);
    let mut modes = Vec::new();
    for frame in 0..12 {
        // Frame 0 is the unmeasured serial one. Its gang would wait out the watchdog inside the
        // timed segment, and on a real clock that wait would count as the segment's cost.
        probe.gang.store(frame > 0, Ordering::SeqCst);
        exec.run_adaptive_in_place().expect("heavy frame");
        modes.push(probe.take_parallel());
    }
    assert!(!modes[0], "unmeasured frame ran in parallel");
    assert!(modes[1..].iter().all(|&parallel| parallel), "{modes:?}");

    // Cheap frames: no gang. Parallel cheap frames can finish on the calling thread alone, so only
    // the settled tail is checked.
    probe.gang.store(false, Ordering::SeqCst);
    probe.work_us.store(0, Ordering::SeqCst);
    let mut modes = Vec::new();
    for _ in 0..120 {
        exec.run_adaptive_in_place().expect("cheap frame");
        modes.push(probe.take_parallel());
    }
    assert!(
        modes.ends_with(&[false; 40]),
        "still parallel after the work got cheap: {modes:?}"
    );
}

#[test]
fn switches_to_parallel_when_work_gets_heavy() {
    let (mut exec, probe) = executor_with(Duration::ZERO, false, false);
    for _ in 0..20 {
        exec.run_adaptive_in_place().expect("cheap frame");
        assert!(!probe.take_parallel());
    }
    // Serial frames are timed every few frames, so the switch takes a handful of frames. The gang
    // is on throughout: the serial frames before the switch time out on the watchdog, which is
    // slow but harmless.
    probe.gang.store(true, Ordering::SeqCst);
    probe.work_us.store(2_000, Ordering::SeqCst);
    let modes: Vec<bool> = (0..12)
        .map(|_| {
            exec.run_adaptive_in_place().expect("heavy frame");
            probe.take_parallel()
        })
        .collect();
    assert!(modes.ends_with(&[true; 4]), "{modes:?}");
}

/// Real time, so `#[ignore]`d: run with `cargo test -p daedalus-runtime --test adaptive_mode --
/// --ignored`. Best of several attempts per mode, so a loaded machine only has to be quiet once.
#[test]
#[ignore = "real-time speed check; noisy on loaded machines"]
fn heavy_fan_out_is_faster_in_real_time() {
    let work = Duration::from_millis(3);
    let attempts = 5;
    let frames = 4;
    let mut best_parallel = Duration::MAX;
    let mut best_serial = Duration::MAX;
    for _ in 0..attempts {
        let (mut exec, probe) = executor_with(work, false, true);
        exec.run_adaptive_in_place().expect("first frame");
        let start = Instant::now();
        for _ in 0..frames {
            exec.run_adaptive_in_place().expect("adaptive frame");
            assert!(probe.take_parallel());
        }
        best_parallel = best_parallel.min(start.elapsed() / frames);

        // Serial reference: the same total work in one segment, which never runs in parallel.
        let mut graph = Graph::default();
        graph.nodes.push(NodeInstance::new("serial"));
        let plan = build_runtime(
            &ExecutionPlan::new(graph, vec![]),
            &SchedulerConfig::default(),
        );
        let probe = Arc::new(Probe {
            real_time: true,
            ..Probe::default()
        });
        probe
            .work_us
            .store((work * GANG as u32).as_micros() as u64, Ordering::SeqCst);
        let mut exec = OwnedExecutor::new(Arc::new(plan), ProbeHandler(probe.clone()));
        let start = Instant::now();
        for _ in 0..frames {
            exec.run_adaptive_in_place().expect("serial frame");
        }
        best_serial = best_serial.min(start.elapsed() / frames);
    }
    assert!(
        best_parallel < best_serial * 3 / 4,
        "parallel frames took {best_parallel:?}, serial {best_serial:?}"
    );
}
