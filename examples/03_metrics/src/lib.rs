//! What a host records about a run, read back from it: the counters, one row per step, the
//! per-tick events, and the reports of what failed.
//!
//! ```text
//! host.x --> half (fails on an odd number) --> host.out
//! ```
//!
//! The clock here is a counter, so the times in the output are the same on every machine. An
//! application passes its own clock the same way.

#![forbid(unsafe_code)]

use std::sync::Arc;

use daedalus::prelude::*;
use daedalus_cpu::CpuBackend;
use daedalus_cpu_author::Fault;

/// A node can fail. The failure is counted and reported, and the graph keeps running.
#[node(id = "metrics.half", inputs(x), outputs(y))]
fn half(x: i64) -> Result<i64, Fault> {
    if x % 2 != 0 {
        return Err(Fault {
            code: 1,
            reason: "an odd number has no half",
        });
    }
    Ok(x / 2)
}

const GRAPH: &str = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {
    "nodes": [
      {"id": "host", "kind": "host_bridge", "port_types": {"x": "i64", "out": "i64"}},
      {"id": "metrics.half", "label": "half"}
    ],
    "edges": [
      {"from": {"node": 0, "port": "x"}, "to": {"node": 1, "port": "x"}},
      {"from": {"node": 1, "port": "y"}, "to": {"node": 0, "port": "out"}}
    ]
  }
}"#;

/// What the example read back.
pub struct Recorded {
    pub report: ExecutionReport,
    /// One per tick, from the event log: (tick, step calls, faults).
    pub events: Vec<(u32, u32, u32)>,
    /// One per failure: the node, and the reason it gave.
    pub faults: Vec<(String, String)>,
    pub outputs: Vec<i64>,
}

/// Runs four values through the graph and one tick with nothing to do, recording at `level`.
pub fn record(level: Level) -> Recorded {
    let mut registry = Registry::new();
    registry
        .register_all(&[&HALF_NODE])
        .expect("the node registers");
    let graph = GraphDoc::parse(GRAPH).expect("the graph parses").graph;
    let planned =
        plan(&graph, &registry, &[&CpuBackend], &Config::default()).expect("the graph plans");
    let mut loaded = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");

    // The host's telemetry wraps a tick: it reads the clock before and after.
    let mut telemetry = Telemetry::new(level);
    // The per-tick event log is kept only once it is turned on.
    telemetry.log_mut().set_on(true);
    let mut now = 0u64;
    let mut clock = || {
        now += 250;
        now
    };
    let mut outputs = Vec::new();
    for x in [8_i64, 3, 6, 5] {
        let host = loaded.host();
        host.feed_typed("x", Arc::new(x)).expect("a feed");
        telemetry.tick(host.runner(), &mut clock).expect("a tick");
        outputs.extend(host.take_copy::<i64>("out").expect("a take"));
        // The host keeps a report of each failure, gathered when its reports are read.
        let _ = loaded.fault_reports().count();
    }
    // A tick with nothing fed is idle, and is counted as such.
    telemetry
        .tick(loaded.host().runner(), &mut clock)
        .expect("a tick");

    Recorded {
        report: telemetry.report(loaded.host().runner()),
        events: telemetry
            .log()
            .recent()
            .map(|event| (event.tick, event.step_calls, event.faults))
            .collect(),
        faults: loaded
            .fault_reports()
            .map(|fault| {
                (
                    fault.node().unwrap_or("?").to_string(),
                    fault.reason().to_string(),
                )
            })
            .collect(),
        outputs,
    }
}

/// Runs the example. Returns the lines it prints.
pub fn run() -> String {
    let recorded = record(Level::Trace);
    let mut out = format!("metrics: outputs {:?}\n", recorded.outputs);
    out.push_str(&recorded.report.table());
    for (tick, calls, faults) in &recorded.events {
        out.push_str(&format!(
            "event: tick {tick}, {calls} step calls, {faults} faults\n"
        ));
    }
    for (node, reason) in &recorded.faults {
        out.push_str(&format!("fault: {node}: {reason}\n"));
    }
    out
}
