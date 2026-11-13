//! A host runs a graph whose node is served by a Rust worker process. The host feeds it, steps it,
//! reads its progress, and cancels a run that is waiting for one.
//!
//! ```text
//! host.x --> scale --> host.y
//! ```
//!
//! `scale` is a `#[node]` of the Rust worker SDK. It reports progress as it runs, and a negative
//! input makes it wait until the host cancels the run. The worker is `daedalus-example-rust-worker-serve`,
//! which serves the same node records this crate registers.

#![forbid(unsafe_code)]

use std::path::Path;
use std::time::{Duration, Instant};

use daedalus::prelude::*;
use daedalus_backend_process::{ProcessBackend, WorkerSpec};
use daedalus_lang_rust::{Ctx, Fault};
use daedalus_wire::FaultCode;

/// Triples its input. Reports progress before and after, and a negative input waits for a cancel.
#[node(id = "ffi.scale", inputs(x), outputs(y), backend = daedalus_lang_rust)]
pub fn scale(x: i64, cx: &Ctx) -> Result<i64, Fault> {
    cx.progress(0.25, "scaling");
    if x < 0 {
        // A cancel is awaited for at most a few seconds, so a run that is never cancelled still ends.
        let until = Instant::now() + Duration::from_secs(5);
        while !cx.cancelled() && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(2));
        }
        if cx.cancelled() {
            return Ok(0);
        }
        return Err(Fault {
            code: FaultCode::NodeError,
            reason: "no cancel came".to_owned(),
        });
    }
    cx.progress(1.0, "scaled");
    Ok(x * 3)
}

/// The node records the worker serves, and the host registers.
pub const NODES: &[&NodeRecord] = &[&SCALE_NODE];

/// The graph: the host's `x` into `scale`, and its output back to the host as `y`. A label names a
/// node for the host.
fn graph() -> GraphBody {
    let text = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {
    "nodes": [
      {"id": "host", "kind": "host_bridge", "label": "io", "port_types": {"x": "i64", "y": "i64"}},
      {"id": "ffi.scale", "label": "ffi.scale"}
    ],
    "edges": [
      {"from": {"node": 0, "port": "x"}, "to": {"node": 1, "port": "x"}},
      {"from": {"node": 1, "port": "y"}, "to": {"node": 0, "port": "y"}}
    ]
  }
}"#;
    GraphDoc::parse(text).expect("the graph parses").graph
}

/// The graph planned with the worker started from `worker`. The worker's declarations are checked
/// against the nodes registered here.
fn planned(worker: &Path) -> Planned {
    let mut registry = Registry::new();
    registry.register_all(NODES).expect("the node registers");
    let mut workers = ProcessBackend::new();
    workers
        .add_worker(WorkerSpec::new(worker), &registry)
        .expect("the worker starts and serves the node");
    plan(&graph(), &registry, &[&workers], &Config::default()).expect("the graph plans")
}

/// Runs the example, with the worker at `worker`. Returns the lines it prints.
pub fn run(worker: &Path) -> String {
    let mut host =
        LoadedHost::load(planned(worker), LoadOptions::default()).expect("the plan loads");
    let mut out = String::new();

    // A run that completes: the worker triples the input, and reports its progress. The handle
    // is taken before the tick, and could be read from another thread while the tick runs.
    let progress = host.progress_handle("ffi.scale");
    host.host()
        .feed_inline("x", &2i64.to_le_bytes())
        .expect("a feed");
    host.step().expect("a tick");
    let y = host.host().take_copy::<i64>("y").expect("a take");
    out.push_str(&format!(
        "host: 2 went to the worker and came back as {y:?}, its progress was {:?}\n",
        progress
            .as_ref()
            .map(daedalus::layers::host::NodeProgress::get)
    ));
    assert_eq!(
        progress.map(|handle| handle.get()),
        host.node_progress("ffi.scale"),
        "the handle and the host read the same cells"
    );

    // A run that waits for a cancel: the host cancels it from another thread. The cancelled run is
    // reported as a fault of its step, and the step's reason is what the host reads back.
    let cancel = host.cancel_handle();
    host.host()
        .feed_inline("x", &(-1i64).to_le_bytes())
        .expect("a feed");
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        cancel.cancel();
    });
    host.step().expect("a tick");
    canceller.join().expect("the canceller finishes");
    let reason = host
        .fault_reports()
        .next()
        .map(|report| report.reason().to_owned())
        .unwrap_or_default();
    out.push_str(&format!(
        "host: a run the host cancelled ended with the fault {reason:?}\n"
    ));
    out
}
