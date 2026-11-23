//! A bounded edge's overflow word: one that names no rule is refused by name, and `coalesce` is a
//! rule a document can ask for.

use std::sync::Arc;

use daedalus_graph::{EdgePolicyDoc, GraphBody, NodeKindDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_planner::diagnostic::{Code, Diagnostic};
use daedalus_planner::{Config, PlanError, plan};
use daedalus_planner_tests::{decl, edge, node, u32_ty};
use daedalus_registry::Registry;
use daedalus_value::DropReason;

use super::c21_host_policy::Test;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.add")
        .input("in", u32_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    registry
}

/// The host's `x` into `t.add` over a bounded edge of one with `overflow`, and out to `y`.
fn bounded(overflow: &str) -> GraphBody {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    for port in ["x", "y"] {
        host.port_types.insert(port.to_string(), "u32".to_string());
    }
    let mut into = edge(0, "x", 1, "in");
    into.policy = EdgePolicyDoc::Bounded {
        capacity: 1,
        overflow: overflow.to_string(),
    };
    GraphBody {
        nodes: vec![host, node("t.add")],
        edges: vec![into, edge(1, "out", 0, "y")],
        groups: Vec::new(),
    }
}

#[test]
fn an_overflow_word_that_names_no_rule_is_refused_by_name() {
    let refused = plan(
        &bounded("drop_olddest"),
        &registry(),
        &[&Test],
        &Config::default(),
    );
    let Err(PlanError::Solve(errors)) = refused else {
        panic!("a misspelt overflow word planned");
    };
    let found: Vec<Diagnostic> = errors.iter().map(Diagnostic::from).collect();
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].code, Code::EdgePolicyInvalid);
    assert_eq!(found[0].node.as_deref(), Some("t.add"));
    assert_eq!(found[0].port.as_deref(), Some("in"));
    assert_eq!(
        found[0].message,
        "the edge into `t.add.in` is bounded with overflow `drop_olddest`, which is not one of \
         drop_oldest, drop_newest, error_on_full, block, coalesce"
    );
}

#[test]
fn a_document_can_ask_a_bounded_edge_to_coalesce() {
    let planned = plan(
        &bounded("coalesce"),
        &registry(),
        &[&Test],
        &Config::default(),
    )
    .unwrap();
    let mut host = LoadedHost::load(planned, LoadOptions::default()).unwrap();
    let first = host.host().feed_typed("x", Arc::new(1_u32)).unwrap();
    assert_eq!(first.dropped, None);
    let second = host.host().feed_typed("x", Arc::new(2_u32)).unwrap();
    assert_eq!(second.dropped, Some(DropReason::Coalesced));
    host.step().unwrap();
    let y = host
        .host()
        .take_value("y")
        .unwrap()
        .and_then(|value| value.typed::<u32>().copied());
    assert_eq!(y, Some(2));
}
