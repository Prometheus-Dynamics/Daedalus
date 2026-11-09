use std::sync::Arc;

use daedalus_graph::GraphBody;
use daedalus_planner::solve::Graph;
use daedalus_planner::solve::hydrate::HydrateError;
use daedalus_planner::solve::rewrite::{Rewrite, RewriteCx};
use daedalus_planner::{Config, SolveError, solve};
use daedalus_planner_tests::{decl, node, u8_ty};
use daedalus_registry::Registry;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.a")
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.b")
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    registry
}

struct Add(&'static str, u32);

impl Rewrite for Add {
    fn id(&self) -> &str {
        self.0
    }

    fn version(&self) -> u32 {
        self.1
    }

    fn apply(&self, graph: &mut Graph, cx: &RewriteCx<'_>) -> Result<(String, bool), String> {
        graph.nodes.push(cx.node(self.0));
        Ok((format!("added {}", self.0), true))
    }
}

struct Nothing;

impl Rewrite for Nothing {
    fn id(&self) -> &str {
        "nothing"
    }

    fn version(&self) -> u32 {
        7
    }

    fn apply(&self, _: &mut Graph, _: &RewriteCx<'_>) -> Result<(String, bool), String> {
        Ok(("looked".to_string(), false))
    }
}

fn config(rewrites: Vec<Arc<dyn Rewrite>>) -> Config {
    Config {
        rewrites,
        ..Config::default()
    }
}

fn body() -> GraphBody {
    GraphBody {
        nodes: vec![node("t.a")],
        ..GraphBody::default()
    }
}

#[test]
fn rewrite_visible_in_solved_graph() {
    let graph = solve(&body(), &registry(), &config(vec![Arc::new(Add("t.b", 1))])).unwrap();
    assert_eq!(graph.nodes[1].doc.id, "t.b");
    assert_eq!(
        graph.nodes[1].outputs[0].name, "out",
        "later stages see it as any other node"
    );
}

#[test]
fn two_configs_do_not_leak_into_each_other() {
    let registry = registry();
    let with = config(vec![Arc::new(Add("t.b", 1))]);
    let without = Config::default();
    assert_eq!(solve(&body(), &registry, &with).unwrap().nodes.len(), 2);
    assert_eq!(solve(&body(), &registry, &without).unwrap().nodes.len(), 1);
    assert_eq!(solve(&body(), &registry, &with).unwrap().nodes.len(), 2);
}

#[test]
fn rewrite_that_adds_an_unknown_node_fails_hydrate() {
    let errors = solve(
        &body(),
        &registry(),
        &config(vec![Arc::new(Add("t.ghost", 1))]),
    )
    .unwrap_err();
    assert!(matches!(
        &errors[..],
        [SolveError::Hydrate(HydrateError::NodeMissing { id, .. })] if id == "t.ghost"
    ));
}

#[test]
fn applied_list_records_id_version_and_changed_in_order() {
    let graph = solve(
        &body(),
        &registry(),
        &config(vec![Arc::new(Nothing), Arc::new(Add("t.b", 3))]),
    )
    .unwrap();
    let applied: Vec<_> = graph
        .applied
        .iter()
        .map(|a| (a.id.as_str(), a.version, a.changed))
        .collect();
    assert_eq!(applied, [("nothing", 7, false), ("t.b", 3, true)]);
}
