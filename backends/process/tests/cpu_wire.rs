//! A value between a CPU node and a worker crosses as its wire form, written and read by its own
//! type: the planner puts the CPU node's output in the worker's lent memory (`process.lent`), or
//! in `cpu.wire`, the CPU step writes it there through the type's `WireForm`, the worker's moves
//! hand the cell on as it is, and the CPU node after the worker reads it back the same way.
//!
//! The struct here has a `u8`, a `u32` and a `u16`: seven bytes on the wire, eight in a Rust
//! struct's memory, with padding and its fields in another order. Sent as its memory's bytes it
//! would not decode in the worker.

mod support;

use daedalus::layers::planner::explain::explain_plan;
use daedalus::prelude::*;
use daedalus_backend_process::ProcessBackend;
use daedalus_cpu::CpuBackend;
use support::spec;

/// The struct `fixture.padded_echo` declares, compared by its fields.
#[derive(Clone, Copy, Debug, Default, PartialEq, Type, WireForm)]
#[structural]
pub struct Padded {
    a: u8,
    b: u32,
    c: u16,
}

/// The same struct, with no wire form.
#[derive(Clone, Copy, Debug, Default, PartialEq, Type)]
#[structural]
pub struct Unwired {
    a: u8,
    b: u32,
    c: u16,
}

#[node(id = "edge.pad", inputs(x), outputs(out))]
pub fn pad(x: u32) -> Padded {
    Padded {
        a: x as u8,
        b: x * 1000,
        c: (x + 1) as u16,
    }
}

#[node(id = "edge.unpad", inputs("in"), outputs(y))]
pub fn unpad(r#in: Padded) -> u32 {
    u32::from(r#in.a) + r#in.b + u32::from(r#in.c)
}

#[node(id = "edge.pad_unwired", inputs(x), outputs(out))]
pub fn pad_unwired(x: u32) -> Unwired {
    Unwired {
        a: x as u8,
        b: x,
        c: 0,
    }
}

#[node(id = "edge.unpad_unwired", inputs("in"), outputs(y))]
pub fn unpad_unwired(r#in: Unwired) -> u32 {
    u32::from(r#in.a) + r#in.b + u32::from(r#in.c)
}

#[node(id = "edge.floats", inputs(x), outputs(out))]
pub fn floats(x: u32) -> Vec<f32> {
    (0..x).map(|at| at as f32 * 0.5).collect()
}

#[node(id = "edge.total", inputs(items), outputs(y))]
pub fn total(items: &[f32]) -> u32 {
    (items.iter().sum::<f32>() * 2.0) as u32
}

/// A graph whose host bridge, node 0, feeds `x` and takes `y` and `z`, around `nodes`.
fn graph(nodes: &[&str], edges: &[(u32, &str, u32, &str)]) -> GraphBody {
    let nodes: Vec<String> = nodes
        .iter()
        .map(|id| format!(r#"{{"id": "{id}"}}"#))
        .collect();
    let edges: Vec<String> = edges
        .iter()
        .map(|(from, out, to, input)| {
            format!(
                r#"{{"from": {{"node": {from}, "port": "{out}"}}, "to": {{"node": {to}, "port": "{input}"}}}}"#
            )
        })
        .collect();
    let text = format!(
        r#"{{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {{
    "nodes": [
      {{"id": "host", "kind": "host_bridge", "label": "io", "port_types": {{"x": "u32", "y": "u32", "z": "u32"}}}},
      {}
    ],
    "edges": [{}]
  }}
}}"#,
        nodes.join(",\n      "),
        edges.join(",\n      ")
    );
    GraphDoc::parse(&text).expect("the graph parses").graph
}

/// host.x → `producer` → `fixture.padded_echo` → `reader` → host.y
fn through_worker(producer: &str, reader: &str) -> GraphBody {
    graph(
        &[producer, "fixture.padded_echo", reader],
        &[
            (0, "x", 1, "x"),
            (1, "out", 2, "in"),
            (2, "out", 3, "in"),
            (3, "y", 0, "y"),
        ],
    )
}

fn planned(graph: &GraphBody) -> Result<Planned, String> {
    let mut registry = Registry::new();
    registry
        .register_all(&[
            &PAD_NODE,
            &UNPAD_NODE,
            &PAD_UNWIRED_NODE,
            &UNPAD_UNWIRED_NODE,
            &FLOATS_NODE,
            &TOTAL_NODE,
        ])
        .expect("the CPU nodes register");
    let mut workers = ProcessBackend::new();
    workers
        .add_worker_declaring(spec(&[]).env("DAEDALUS_FIXTURE_PROBE", "1"), &mut registry)
        .expect("the worker registers");
    plan(
        graph,
        &registry,
        &[&CpuBackend, &workers],
        &Config::default(),
    )
    .map_err(|error| error.to_string())
}

fn loaded(graph: &GraphBody) -> Result<LoadedHost, String> {
    LoadedHost::load(planned(graph)?, LoadOptions::default()).map_err(|error| error.to_string())
}

/// Feeds `x`, runs a tick, and takes `outputs`.
fn tick(host: &mut LoadedHost, x: u32, outputs: &[&str]) -> Vec<Option<u32>> {
    host.host()
        .feed_inline("x", &x.to_le_bytes())
        .expect("a feed");
    host.step().expect("a tick");
    let faults: Vec<String> = host
        .fault_reports()
        .map(|report| report.reason().to_owned())
        .collect();
    assert!(faults.is_empty(), "{faults:?}");
    outputs
        .iter()
        .map(|name| host.host().take_copy::<u32>(name).expect("a take"))
        .collect()
}

fn unpadded(x: u32) -> u32 {
    x + x * 1000 + x + 1
}

#[test]
fn a_padded_struct_crosses_to_a_worker_and_back_as_its_wire_form() {
    let mut host = loaded(&through_worker("edge.pad", "edge.unpad")).expect("the plan loads");
    for x in [1u32, 7, 200] {
        assert_eq!(tick(&mut host, x, &["y"]), [Some(unpadded(x))], "x = {x}");
    }
}

#[test]
fn the_plan_shows_where_the_value_is_written_and_decoded() {
    let planned = planned(&through_worker("edge.pad", "edge.unpad")).expect("the graph plans");
    let explain = explain_plan(&planned);
    let to_worker = explain
        .edges
        .iter()
        .find(|edge| edge.from_node == 1)
        .expect("the edge to the worker");
    if cfg!(target_os = "linux") {
        // Written where the worker reads it, in its lent memory: no move on the way.
        assert_eq!(to_worker.written_in.as_deref(), Some("process.lent"));
        assert!(to_worker.transfer.is_empty(), "{:?}", to_worker.transfer);
    } else {
        // Lent memory is Linux's: elsewhere the wire form is written in `cpu.wire` and moved.
        assert_eq!(to_worker.written_in.as_deref(), Some("cpu.wire"));
        assert!(!to_worker.transfer.is_empty(), "a move to the worker");
    }
    let back = explain
        .edges
        .iter()
        .find(|edge| edge.to_node == 3)
        .expect("the edge back");
    assert_eq!(back.read_in.as_deref(), Some("cpu.wire"));
    let [decode] = &explain.decodes[..] else {
        panic!(
            "one decode, for the reader after the worker: {:?}",
            explain.decodes
        );
    };
    assert_eq!((decode.node, decode.port.as_str()), (3, "in"));
    assert!(decode.ty.ends_with("Padded"), "{decode:?}");
}

#[test]
fn a_list_of_numbers_from_a_worker_is_read_where_it_is() {
    let graph = graph(
        &["edge.floats", "fixture.floats_echo", "edge.total"],
        &[
            (0, "x", 1, "x"),
            (1, "out", 2, "in"),
            (2, "out", 3, "items"),
            (3, "y", 0, "y"),
        ],
    );
    let explain = explain_plan(&planned(&graph).expect("the graph plans"));
    assert!(explain.decodes.is_empty(), "{:?}", explain.decodes);
    let mut host = loaded(&graph).expect("the plan loads");
    for x in [0u32, 3, 2000] {
        // Twice the sum of 0, 0.5, 1, ...: the sum of 0..x.
        let sum = (0..x).sum::<u32>();
        assert_eq!(tick(&mut host, x, &["y"]), [Some(sum)], "x = {x}");
    }
}

#[test]
fn a_cpu_reader_beside_a_worker_reads_the_wire_form_too() {
    // pad's output goes to the worker and to a second unpad on the CPU.
    let graph = graph(
        &[
            "edge.pad",
            "fixture.padded_echo",
            "edge.unpad",
            "edge.unpad",
        ],
        &[
            (0, "x", 1, "x"),
            (1, "out", 2, "in"),
            (2, "out", 3, "in"),
            (3, "y", 0, "y"),
            (1, "out", 4, "in"),
            (4, "y", 0, "z"),
        ],
    );
    let explain = explain_plan(&planned(&graph).expect("the graph plans"));
    let mut decoded: Vec<u32> = explain.decodes.iter().map(|decode| decode.node).collect();
    decoded.sort_unstable();
    assert_eq!(decoded, [3, 4], "{:?}", explain.decodes);
    let mut host = loaded(&graph).expect("the plan loads");
    for x in [2u32, 90] {
        let both = Some(unpadded(x));
        assert_eq!(tick(&mut host, x, &["y", "z"]), [both, both], "x = {x}");
    }
}

#[test]
fn a_type_with_no_wire_form_is_refused_with_the_fix() {
    let reason = loaded(&through_worker("edge.pad_unwired", "edge.unpad"))
        .map(|_| ())
        .expect_err("a value with no wire form can't go to a worker");
    assert!(reason.contains("edge.pad_unwired"), "{reason}");
    assert!(reason.contains("Unwired"), "{reason}");
    assert!(reason.contains("add #[derive(WireForm)] to"), "{reason}");
}

#[test]
fn a_reader_of_a_type_with_no_wire_form_is_refused_with_the_fix() {
    let reason = loaded(&through_worker("edge.pad", "edge.unpad_unwired"))
        .map(|_| ())
        .expect_err("a value from a worker can't be read with no wire form");
    assert!(
        reason.contains("input in of edge.unpad_unwired"),
        "{reason}"
    );
    assert!(reason.contains("add #[derive(WireForm)] to"), "{reason}");
    assert!(reason.contains("Unwired"), "{reason}");
}
