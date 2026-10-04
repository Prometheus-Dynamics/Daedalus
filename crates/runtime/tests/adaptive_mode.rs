//! `run_adaptive_in_place` goes parallel only when measured (or hinted) work pays for dispatch.
//!
//! A frame counts as serial when every handler ran on the calling thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use daedalus_data::model::Value;
use daedalus_planner::{ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::{
    NODE_COST_META_KEY, NodeError, NodeHandler, OwnedExecutor, RuntimeNode, SchedulerConfig,
    build_runtime,
};
use parking_lot::Mutex;

#[derive(Default)]
struct Probe {
    active: AtomicUsize,
    max_active: AtomicUsize,
    threads: Mutex<Vec<ThreadId>>,
    /// Sleep per handler call, in microseconds.
    work_us: AtomicU64,
}

impl Probe {
    /// Whether the frame since the last call ran on more than one thread; resets the record.
    fn take_parallel(&self) -> bool {
        let caller = thread::current().id();
        let threads = std::mem::take(&mut *self.threads.lock());
        self.max_active.store(0, Ordering::SeqCst);
        threads.iter().any(|&id| id != caller)
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
        let active = probe.active.fetch_add(1, Ordering::SeqCst) + 1;
        probe.max_active.fetch_max(active, Ordering::SeqCst);
        probe.threads.lock().push(thread::current().id());
        let work = probe.work_us.load(Ordering::SeqCst);
        if work > 0 {
            thread::sleep(Duration::from_micros(work));
        }
        probe.active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

/// `count` independent nodes (one ready segment each), optionally hinted heavy.
fn executor(
    count: usize,
    work: Duration,
    heavy: bool,
) -> (OwnedExecutor<ProbeHandler>, Arc<Probe>) {
    let mut graph = Graph::default();
    for idx in 0..count {
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
    let probe = Arc::new(Probe::default());
    probe
        .work_us
        .store(work.as_micros() as u64, Ordering::SeqCst);
    let exec =
        OwnedExecutor::new(Arc::new(plan), ProbeHandler(probe.clone())).with_pool_size(Some(count));
    (exec, probe)
}

#[test]
fn cheap_graph_stays_serial() {
    let (mut exec, probe) = executor(8, Duration::ZERO, false);
    for frame in 0..64 {
        let telemetry = exec.run_adaptive_in_place().expect("adaptive frame");
        assert_eq!(telemetry.nodes_executed, 8);
        assert!(!probe.take_parallel(), "frame {frame} ran in parallel");
    }
}

#[test]
fn heavy_fan_out_goes_parallel_and_is_faster() {
    let work = Duration::from_millis(3);
    let (mut exec, probe) = executor(4, work, false);
    // Nothing measured yet: the first frame runs serially and times the segments.
    exec.run_adaptive_in_place().expect("first frame");
    assert!(!probe.take_parallel(), "unmeasured frame ran in parallel");

    let frames = 6;
    let start = Instant::now();
    for frame in 0..frames {
        exec.run_adaptive_in_place().expect("adaptive frame");
        assert!(probe.take_parallel(), "frame {frame} ran serially");
    }
    let per_frame = start.elapsed() / frames;
    let serial = work * 4;
    assert!(
        per_frame < serial * 3 / 4,
        "parallel frames took {per_frame:?}, serial would take {serial:?}"
    );
}

#[test]
fn heavy_hint_runs_the_first_frame_in_parallel() {
    let (mut exec, probe) = executor(4, Duration::from_millis(2), true);
    exec.run_adaptive_in_place().expect("first frame");
    assert!(probe.max_active.load(Ordering::SeqCst) > 1);
    assert!(probe.take_parallel());
}

#[test]
fn stays_parallel_while_heavy_and_returns_to_serial_once_cheap() {
    let (mut exec, probe) = executor(4, Duration::from_millis(2), false);
    let mut modes = Vec::new();
    for _ in 0..12 {
        exec.run_adaptive_in_place().expect("heavy frame");
        modes.push(probe.take_parallel());
    }
    assert!(!modes[0], "unmeasured frame ran in parallel");
    assert!(modes[1..].iter().all(|&parallel| parallel), "{modes:?}");

    // Cheap parallel frames can finish on the calling thread alone without `executor-pool`, so
    // only the settled tail is checked.
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
    let (mut exec, probe) = executor(4, Duration::ZERO, false);
    for _ in 0..20 {
        exec.run_adaptive_in_place().expect("cheap frame");
        assert!(!probe.take_parallel());
    }
    // Serial frames are timed every few frames, so the switch takes a handful of frames.
    probe.work_us.store(2_000, Ordering::SeqCst);
    let modes: Vec<bool> = (0..12)
        .map(|_| {
            exec.run_adaptive_in_place().expect("heavy frame");
            probe.take_parallel()
        })
        .collect();
    assert!(modes.ends_with(&[true; 4]), "{modes:?}");
}
