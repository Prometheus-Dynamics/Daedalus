//! Ports whose types are made of others: lists, tuples, maps and optional values, through the
//! planner and a loaded graph, with the host feeding and taking them.

use std::collections::BTreeMap;
use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_graph::GraphDoc;
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::{Config, plan};
use daedalus_registry::{NodeRecord, Registry};

/// A list in, a list out.
#[node(id = "c.doubled", inputs(items), outputs(out))]
fn doubled(items: &[f32]) -> Vec<f32> {
    items.iter().map(|item| item * 2.0).collect()
}

/// A list in, a tuple out.
#[node(id = "c.span", inputs(items), outputs(span))]
fn span(items: &[f32]) -> (f32, f32) {
    let low = items.iter().copied().fold(f32::INFINITY, f32::min);
    let high = items.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    (low, high)
}

/// A tuple in, a number out.
#[node(id = "c.width", inputs(span), outputs(width))]
fn width(span: (f32, f32)) -> f32 {
    span.1 - span.0
}

/// A map in, a list of what may be missing out.
#[node(id = "c.lookup", inputs(table, names), outputs(found))]
fn lookup(table: &BTreeMap<String, u32>, names: &[String]) -> Vec<Option<u32>> {
    names.iter().map(|name| table.get(name).copied()).collect()
}

/// Text in, as a `&str`, and its length out.
#[node(id = "c.shout", inputs(text), outputs(out))]
fn shout(text: &str) -> String {
    text.to_uppercase()
}

/// A list of numbers that isn't a list of floats.
#[node(id = "c.count", inputs(items), outputs(count))]
fn count(items: &[u32]) -> u32 {
    items.len() as u32
}

const NODES: [&NodeRecord; 6] = [
    &DOUBLED_NODE,
    &SPAN_NODE,
    &WIDTH_NODE,
    &LOOKUP_NODE,
    &COUNT_NODE,
    &SHOUT_NODE,
];

fn loaded(graph: &str) -> Result<LoadedHost, String> {
    let mut registry = Registry::new();
    registry.register_all(&NODES).expect("the nodes register");
    let graph = GraphDoc::parse(graph).expect("the graph parses").graph;
    let planned = plan(&graph, &registry, &[&CpuBackend], &Config::default())
        .map_err(|error| error.to_string())?;
    Ok(LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads"))
}

fn graph(nodes: &str, ports: &str, edges: &str) -> String {
    format!(
        r#"{{"format": "daedalus.graph", "schema_version": 1, "graph": {{
            "nodes": [{{"id": "host", "kind": "host_bridge", "port_types": {{{ports}}}}}, {nodes}],
            "edges": [{edges}]}}}}"#
    )
}

fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> String {
    format!(
        r#"{{"from": {{"node": {from}, "port": "{from_port}"}}, "to": {{"node": {to}, "port": "{to_port}"}}}}"#
    )
}

#[test]
fn the_port_types_are_declared_by_what_they_are_made_of() {
    use daedalus_registry::TypeRef;
    use daedalus_types::{Scalar, TypeExpr};
    let list_of_f32 = TypeRef::Expr(TypeExpr::List(&TypeExpr::Scalar(Scalar::F32)));
    assert_eq!(DOUBLED_NODE.decl.inputs[0].ty, list_of_f32);
    assert_eq!(DOUBLED_NODE.decl.outputs[0].ty, list_of_f32);
    assert!(
        matches!(SPAN_NODE.decl.outputs[0].ty, TypeRef::Expr(TypeExpr::Tuple(items)) if items.len() == 2)
    );
    assert!(matches!(
        LOOKUP_NODE.decl.inputs[0].ty,
        TypeRef::Expr(TypeExpr::Map(..))
    ));
    // A number is still declared by its key, and so are bytes, which `Vec<u8>` is.
    assert!(matches!(WIDTH_NODE.decl.outputs[0].ty, TypeRef::Key(_)));
    assert_eq!(
        daedalus_registry::port_type::<Vec<u8>>(),
        TypeRef::Key(Scalar::Bytes.key())
    );
}

#[test]
fn lists_and_tuples_flow_between_nodes_and_the_host() {
    let text = graph(
        r#"{"id": "c.doubled"}, {"id": "c.span"}, {"id": "c.width"}"#,
        r#""items": "list<f32>", "doubled": "list<f32>", "span": "(f32, f32)", "width": "f32""#,
        &[
            edge(0, "items", 1, "items"),
            edge(1, "out", 2, "items"),
            edge(1, "out", 0, "doubled"),
            edge(2, "span", 3, "span"),
            edge(2, "span", 0, "span"),
            edge(3, "width", 0, "width"),
        ]
        .join(","),
    );
    let mut loaded = loaded(&text).expect("plans");
    let host = loaded.host();
    host.feed_typed("items", Arc::new(vec![1.5f32, -2.0, 4.0]))
        .expect("a feed");
    host.step().expect("a tick");
    assert_eq!(
        host.take::<Vec<f32>>("doubled"),
        Ok(Some(vec![3.0, -4.0, 8.0]))
    );
    // A small tuple of plain numbers travels in place, as a number does.
    assert_eq!(host.take_copy::<(f32, f32)>("span"), Ok(Some((-4.0, 8.0))));
    assert_eq!(host.take_copy::<f32>("width"), Ok(Some(12.0)));
}

#[test]
fn a_map_and_a_list_of_optional_values() {
    let text = graph(
        r#"{"id": "c.lookup"}"#,
        r#""table": "map<string, u32>", "names": "list<string>", "found": "list<optional<u32>>""#,
        &[
            edge(0, "table", 1, "table"),
            edge(0, "names", 1, "names"),
            edge(1, "found", 0, "found"),
        ]
        .join(","),
    );
    let mut loaded = loaded(&text).expect("plans");
    let host = loaded.host();
    let table: BTreeMap<String, u32> = [("a".to_string(), 1), ("c".to_string(), 3)].into();
    host.feed_typed("table", Arc::new(table)).expect("a feed");
    let names: Vec<String> = ["a", "b", "c"].map(str::to_string).to_vec();
    host.feed_typed("names", Arc::new(names)).expect("a feed");
    host.step().expect("a tick");
    assert_eq!(
        host.take::<Vec<Option<u32>>>("found"),
        Ok(Some(vec![Some(1), None, Some(3)]))
    );
}

#[test]
fn a_list_of_one_thing_isnt_a_list_of_another() {
    // A list of floats into a node that takes a list of integers: refused before anything
    // runs, with both types named.
    let text = graph(
        r#"{"id": "c.doubled"}, {"id": "c.count"}"#,
        r#""items": "list<f32>", "count": "u32""#,
        &[
            edge(0, "items", 1, "items"),
            edge(1, "out", 2, "items"),
            edge(2, "count", 0, "count"),
        ]
        .join(","),
    );
    let Err(refused) = loaded(&text) else {
        panic!("a list of floats went into a list of integers");
    };
    assert!(
        refused.contains("list<f32>") && refused.contains("list<u32>"),
        "{refused}"
    );
    // And a host port that says the wrong thing is refused the same way.
    let text = graph(
        r#"{"id": "c.count"}"#,
        r#""items": "list<f32>", "count": "u32""#,
        &[edge(0, "items", 1, "items"), edge(1, "count", 0, "count")].join(","),
    );
    assert!(loaded(&text).is_err());
}

#[test]
fn a_slice_or_a_str_parameter_reads_the_list_or_string() {
    let text = graph(
        r#"{"id": "c.shout"}"#,
        r#""text": "string", "loud": "string""#,
        &[edge(0, "text", 1, "text"), edge(1, "out", 0, "loud")].join(","),
    );
    let mut loaded = loaded(&text).expect("plans");
    let host = loaded.host();
    host.feed_typed("text", Arc::new("quiet".to_string()))
        .expect("a feed");
    host.step().expect("a tick");
    assert_eq!(host.take::<String>("loud"), Ok(Some("QUIET".to_string())));
}
