use std::sync::Arc;

use daedalus_backend::Backend;
use daedalus_graph::{GraphBody, Lit};
use daedalus_planner::cache::{PlanCache, PlanKey};
use daedalus_planner::hash::plan_hash;
use daedalus_planner::solve::Graph;
use daedalus_planner::solve::rewrite::{Rewrite, RewriteCx};
use daedalus_planner::{Config, Form, plan};
use daedalus_planner_tests::{Named, decl, edge, node, u8_ty};
use daedalus_registry::Registry;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.src")
        .output("out", u8_ty())
        .body("a")
        .body("b")
        .add(&mut registry);
    decl("t.map")
        .input("in", u8_ty())
        .config("gain", u8_ty())
        .output("out", u8_ty())
        .body("a")
        .body("b")
        .add(&mut registry);
    decl("t.sink")
        .input("in", u8_ty())
        .body("a")
        .body("b")
        .add(&mut registry);
    registry
}

fn three() -> GraphBody {
    GraphBody {
        nodes: vec![node("t.src"), node("t.map"), node("t.sink")],
        edges: vec![edge(0, "out", 1, "in"), edge(1, "out", 2, "in")],
        groups: Vec::new(),
    }
}

fn key(graph: &GraphBody, backends: &[&dyn Backend], config: &Config) -> PlanKey {
    PlanKey::new(graph, &registry(), backends, config)
}

#[test]
fn hash_golden() {
    let (graph, backend) = (three(), Named("a"));
    let config = Config {
        form: Form::Blob,
        ..Config::default()
    };
    let planned = plan(&graph, &registry(), &[&backend], &config).unwrap();
    let hash = plan_hash(&planned, &config, &[&backend]).unwrap();
    assert_eq!(hash, 3668697832233095048u64);
    assert_eq!(
        key(&graph, &[&backend], &config).graph_hash(),
        9274988837969866115u64
    );
}

#[test]
fn reordering_lists_that_decide_nothing_leaves_the_key_unchanged() {
    let (a, b) = (Named("a"), Named("b"));
    let graph = three();
    let mut config = Config {
        priority: vec!["a".to_string(), "gone".to_string(), "b".to_string()],
        ..Config::default()
    };
    let one = key(&graph, &[&a, &b], &config);
    config.priority = vec!["gone".to_string(), "a".to_string(), "b".to_string()];
    assert_eq!(one, key(&graph, &[&b, &a], &config));
}

#[test]
fn changing_which_backend_comes_first_changes_the_key() {
    let (a, b) = (Named("a"), Named("b"));
    let graph = three();
    let mut config = Config {
        priority: vec!["a".to_string(), "b".to_string()],
        ..Config::default()
    };
    let one = key(&graph, &[&a, &b], &config);
    config.priority = vec!["b".to_string(), "a".to_string()];
    assert_ne!(one, key(&graph, &[&a, &b], &config));
}

#[test]
fn one_const_changes_the_key() {
    let backend = Named("a");
    let config = Config::default();
    let mut graph = three();
    let before = key(&graph, &[&backend], &config);
    graph.nodes[1]
        .consts
        .insert("gain".to_string(), Lit::Int(2));
    assert_ne!(before, key(&graph, &[&backend], &config));
}

struct Versioned(u32);

impl Rewrite for Versioned {
    fn id(&self) -> &str {
        "t.rewrite"
    }

    fn version(&self) -> u32 {
        self.0
    }

    fn apply(&self, _: &mut Graph, _: &RewriteCx<'_>) -> Result<(String, bool), String> {
        Ok((String::new(), false))
    }
}

fn with_rewrite(version: u32) -> Config {
    let mut config = Config::default();
    config.rewrites.push(Arc::new(Versioned(version)));
    config
}

#[test]
fn a_rewrite_version_changes_the_key() {
    let (graph, backend) = (three(), Named("a"));
    assert_ne!(
        key(&graph, &[&backend], &with_rewrite(1)),
        key(&graph, &[&backend], &with_rewrite(2))
    );
}

#[test]
fn turning_lints_on_leaves_the_key_unchanged() {
    let (graph, backend) = (three(), Named("a"));
    let mut config = Config::default();
    let before = key(&graph, &[&backend], &config);
    config.lints = true;
    assert_eq!(before, key(&graph, &[&backend], &config));
}

#[test]
fn a_new_declaration_changes_the_key() {
    let (graph, backend) = (three(), Named("a"));
    let config = Config::default();
    let before = PlanKey::new(&graph, &registry(), &[&backend], &config);
    let mut bigger = registry();
    decl("t.extra").body("a").add(&mut bigger);
    assert_ne!(before, PlanKey::new(&graph, &bigger, &[&backend], &config));
}

#[test]
fn forced_collision_does_not_hit() {
    let mut cache = PlanCache::new(4);
    let stored = PlanKey::from_parts(7, 9, vec![1], vec![2]);
    let other = PlanKey::from_parts(7, 9, vec![3], vec![2]);
    cache.insert(stored.clone(), "plan");
    assert_eq!(cache.get(&other), None);
    assert_eq!(cache.get(&stored), Some(&"plan"));
}

fn numbered(n: u8) -> PlanKey {
    PlanKey::from_parts(u64::from(n), 0, vec![n], Vec::new())
}

#[test]
fn hits_and_misses_are_counted() {
    let mut cache = PlanCache::new(2);
    assert!(cache.get(&numbered(1)).is_none());
    cache.insert(numbered(1), 10);
    assert_eq!(cache.get(&numbered(1)), Some(&10));
    assert_eq!((cache.hits(), cache.misses()), (1, 1));
}

#[test]
fn the_entry_used_longest_ago_is_dropped() {
    let mut cache = PlanCache::new(2);
    cache.insert(numbered(1), 1);
    cache.insert(numbered(2), 2);
    cache.get(&numbered(1));
    cache.insert(numbered(3), 3);
    assert_eq!(cache.len(), 2);
    assert!(cache.get(&numbered(2)).is_none());
    assert_eq!(cache.get(&numbered(1)), Some(&1));
    assert_eq!(cache.get(&numbered(3)), Some(&3));
}

#[test]
fn a_cache_of_nothing_stores_nothing() {
    let mut cache = PlanCache::new(0);
    cache.insert(numbered(1), 1);
    assert!(cache.is_empty());
}
