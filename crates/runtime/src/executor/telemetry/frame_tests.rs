use core::time::Duration;

use super::super::{ProbeCount, ProbeTime};
use super::{FrameOverheadWindow, FrameProbe};
use crate::plan::RuntimePlan;

fn empty_plan() -> RuntimePlan {
    RuntimePlan::try_from_execution(&daedalus_planner::ExecutionPlan::new(
        daedalus_planner::Graph::default(),
        vec![],
    ))
    .expect("empty plan")
}

#[test]
fn window_keeps_the_last_ticks_and_derives_the_remainder() {
    let probe = FrameProbe::for_plan(&empty_plan());
    let mut window = FrameOverheadWindow::new(4, &probe);
    for tick in 1..=6u64 {
        probe.add_time(ProbeTime::Collect, Duration::from_nanos(tick * 5));
        probe.add_time(ProbeTime::Handlers, Duration::from_nanos(tick * 10));
        probe.add_time(ProbeTime::NodeRuns, Duration::from_nanos(tick * 30));
        probe.add_count(ProbeCount::Nodes, 2);
        window.record(|sample, edges| {
            probe.finish_tick(Duration::from_nanos(tick * 100), sample, edges);
        });
    }
    assert_eq!((window.len(), window.recorded()), (4, 6));
    let latest = window.latest().expect("latest");
    assert_eq!(latest.tick_ns, 600);
    assert_eq!(latest.node_io_ns, 180 - 60);
    assert_eq!(latest.dispatch_ns, 600 - 30 - 180);
    assert_eq!(latest.graph_overhead_ns, 600 - 60);

    let report = window.report(&[], 7);
    let tick = report.stage("tick").expect("tick");
    // Ticks 3..=6 remain: 300, 400, 500, 600 ns.
    assert_eq!((tick.p50, tick.p99, tick.max), (400, 600, 600));
    assert!((tick.mean - 450.0).abs() < f64::EPSILON);
    assert_eq!(report.stage("take").expect("take").max, 7);
    assert_eq!(report.counter("nodes").expect("nodes").p50, 2);
    assert!(report.to_table().contains("graph_overhead"));

    window.clear();
    assert!(window.is_empty() && window.latest().is_none());
}

#[test]
fn finish_tick_resets_the_probe() {
    let probe = FrameProbe::for_plan(&empty_plan());
    probe.add_time(ProbeTime::Inject, Duration::from_nanos(50));
    probe.add_count(ProbeCount::SharedClones, 3);
    let mut sample = super::FrameTickSample::default();
    probe.finish_tick(Duration::from_nanos(100), &mut sample, &mut []);
    assert_eq!((sample.inject_ns, sample.shared_clones), (50, 3));
    probe.finish_tick(Duration::from_nanos(100), &mut sample, &mut []);
    assert_eq!((sample.inject_ns, sample.shared_clones), (0, 0));
}
