// The report is this example's output.
#![allow(clippy::print_stdout)]

//! Time saved by output reuse on an expensive node. `src -> heavy -> sink`, where `heavy` does a
//! fixed amount of work per call. Runs the same ticks with `heavy` declared `reusable` and not:
//!
//! - `unchanged`: the source re-sends the same payload every tick (a held input), so a reusable
//!   `heavy` runs once and replays;
//! - `changing`: the source sends a new value every tick, so a reusable `heavy` never replays and
//!   pays only the stamp check.
//!
//! `reuse_saving [TICKS] [WORK]`. WORK is the loop count of `heavy` (default 2,000,000).

use std::hint::black_box;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use daedalus::data::model::Value;
use daedalus::planner::{Edge, ExecutionPlan, Graph, NodeInstance};
use daedalus::runtime::build_runtime;
use daedalus::runtime::executor::OwnedExecutor;
use daedalus::runtime::io::NodeIo;
use daedalus::runtime::state::ExecutionContext;
use daedalus::runtime::{NODE_REUSABLE_META_KEY, NodeError, RuntimeNode, SchedulerConfig};
use daedalus::transport::Payload;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// The source's value and the payload it sends: the same payload while the value is unchanged.
struct Source {
    value: i32,
    held: Option<Payload>,
    changing: bool,
}

fn plan(reusable: bool) -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("src"));
    let mut heavy = NodeInstance::new("heavy");
    if reusable {
        heavy = heavy.with_metadata(NODE_REUSABLE_META_KEY, Value::Bool(true));
    }
    graph.nodes.push(heavy);
    graph.nodes.push(NodeInstance::new("sink"));
    graph.edges.push(Edge::new(0, "out", 1, "in"));
    graph.edges.push(Edge::new(1, "out", 2, "in"));
    ExecutionPlan::new(graph, vec![])
}

fn run(
    reusable: bool,
    changing: bool,
    ticks: usize,
    work: u64,
) -> Result<(f64, u64, usize), Error> {
    let source = Arc::new(Mutex::new(Source {
        value: 1,
        held: None,
        changing,
    }));
    let heavy_calls = Arc::new(AtomicU64::new(0));
    let heavy_calls_handler = heavy_calls.clone();
    let source_handler = source.clone();
    let handler = move |node: &RuntimeNode, _ctx: &ExecutionContext, io: &mut NodeIo| {
        match node.id.as_str() {
            "src" => {
                let mut source = source_handler.lock().unwrap();
                if source.changing {
                    source.value = source.value.wrapping_add(1);
                    source.held = None;
                }
                let value = source.value;
                let payload = source
                    .held
                    .get_or_insert_with(|| Payload::owned("test:i32", value))
                    .clone();
                io.push_payload("out", payload);
            }
            "heavy" => {
                heavy_calls_handler.fetch_add(1, Ordering::Relaxed);
                let seed = io.get_typed::<i32>("in").unwrap_or(0) as u64;
                let mut acc = seed;
                for i in 0..work {
                    acc = acc.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(i);
                }
                io.push_payload("out", Payload::owned("test:u64", black_box(acc)));
            }
            _ => {
                if let Some(value) = io.get_typed::<u64>("in") {
                    black_box(value);
                }
            }
        }
        Ok::<(), NodeError>(())
    };
    let runtime = Arc::new(build_runtime(&plan(reusable), &SchedulerConfig::default()));
    let mut exec = OwnedExecutor::new(runtime, handler);
    for _ in 0..10.min(ticks) {
        exec.run_in_place()?;
    }
    let start = Instant::now();
    let mut reused = 0usize;
    for _ in 0..ticks {
        let telemetry = exec.run_in_place()?;
        reused += telemetry.nodes_reused;
    }
    let per_tick_us = start.elapsed().as_secs_f64() * 1e6 / ticks as f64;
    Ok((per_tick_us, heavy_calls.load(Ordering::Relaxed), reused))
}

fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let ticks: usize = args.get(1).map_or(Ok(2000), |arg| arg.parse())?;
    let work: u64 = args.get(2).map_or(Ok(2_000_000), |arg| arg.parse())?;
    for (label, changing) in [("unchanged", false), ("changing", true)] {
        let (off_us, off_calls, _) = run(false, changing, ticks, work)?;
        let (on_us, on_calls, reused) = run(true, changing, ticks, work)?;
        println!(
            "{label}: no reuse {off_us:.1} us/tick ({off_calls} heavy calls); \
             reusable {on_us:.1} us/tick ({on_calls} heavy calls, {reused} replayed ticks)"
        );
    }
    Ok(())
}
