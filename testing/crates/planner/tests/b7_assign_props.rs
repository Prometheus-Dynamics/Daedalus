use std::collections::BTreeSet;

use daedalus_backend::Backend;
use daedalus_graph::{EdgeDoc, GraphBody, NodeDoc};
use daedalus_planner::assign::error::AssignError;
use daedalus_planner::assign::{ASSIGN_BUDGET, Assignment, Input, Placed, Removed, assign};
use daedalus_planner::solve::type_table::TypeTable;
use daedalus_planner::{Config, solve};
use daedalus_planner_tests::{Named, decl, edge, node, u8_ty};
use daedalus_registry::Registry;

const BACKENDS: [Named; 4] = [Named("b0"), Named("b1"), Named("b2"), Named("b3")];

struct Rng(u64);

impl Rng {
    fn next(&mut self, below: u32) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % u64::from(below)) as u32
    }
}

fn plan(
    registry: &Registry,
    body: &GraphBody,
    count: usize,
    budget: u64,
) -> Result<Assignment, Vec<AssignError>> {
    let graph = solve(body, registry, &Config::default()).unwrap();
    let mut types = TypeTable::default();
    for node in &graph.nodes {
        for port in node.inputs.iter().chain(&node.outputs) {
            types.add(&port.ty);
        }
    }
    let backends: Vec<&dyn Backend> = BACKENDS[..count]
        .iter()
        .map(|b| b as &dyn Backend)
        .collect();
    let removed = Removed::new();
    assign(&Input {
        graph: &graph,
        registry,
        backends: &backends,
        types: &types,
        budget,
        removed: &removed,
    })
}

/// The objective of one assignment: crossings counted as (port, other backend) pairs.
fn crossings(edges: &[EdgeDoc], choice: &[u32]) -> usize {
    let mut pairs = BTreeSet::new();
    for edge in edges {
        let (from, to) = (
            choice[edge.from.node as usize],
            choice[edge.to.node as usize],
        );
        if from != to {
            pairs.insert((edge.from.node, edge.from.port.clone(), to));
        }
    }
    pairs.len()
}

type Objective = (usize, u32, Vec<u32>);

fn brute_force(candidates: &[Vec<u32>], edges: &[EdgeDoc]) -> Vec<u32> {
    let mut cursor = vec![0usize; candidates.len()];
    let mut best: Option<(Objective, Vec<u32>)> = None;
    loop {
        let choice: Vec<u32> = cursor
            .iter()
            .zip(candidates)
            .map(|(&c, list)| list[c])
            .collect();
        let key = (
            crossings(edges, &choice),
            choice.iter().sum(),
            choice.clone(),
        );
        if best.as_ref().is_none_or(|(known, _)| key < *known) {
            best = Some((key, choice));
        }
        let mut at = 0;
        loop {
            if at == cursor.len() {
                return best.unwrap().1;
            }
            cursor[at] += 1;
            if cursor[at] < candidates[at].len() {
                break;
            }
            cursor[at] = 0;
            at += 1;
        }
    }
}

#[test]
fn search_matches_enumeration_on_2000_random_graphs() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for _ in 0..2000 {
        let count = 1 + rng.next(8) as usize;
        let backends = 1 + rng.next(4) as usize;
        let mut registry = Registry::new();
        let mut nodes = Vec::new();
        let mut candidates = Vec::new();
        for index in 0..count {
            let mask = 1 + rng.next((1 << backends) - 1);
            let set: Vec<u32> = (0..backends as u32)
                .filter(|b| mask >> b & 1 == 1)
                .collect();
            let id = format!("n.{index}");
            let mut d = decl(&id);
            for input in 0..4 {
                d = d.optional(&format!("i{input}"), u8_ty());
            }
            d = d.output("o0", u8_ty()).output("o1", u8_ty());
            for backend in &set {
                d = d.body(&format!("b{backend}"));
            }
            d.add(&mut registry);
            let mut doc: NodeDoc = node(&id);
            let forced = rng.next(4) == 0;
            let list = if forced {
                let pick = set[rng.next(set.len() as u32) as usize];
                doc.backend = Some(format!("b{pick}"));
                vec![pick]
            } else {
                set
            };
            nodes.push(doc);
            candidates.push(list);
        }
        let mut edges = Vec::new();
        for to in 1..count {
            for input in 0..4 {
                if rng.next(3) == 0 {
                    let from = rng.next(to as u32);
                    let port = format!("o{}", rng.next(2));
                    edges.push(edge(from, &port, to as u32, &format!("i{input}")));
                }
            }
        }
        let expected = brute_force(&candidates, &edges);
        let body = GraphBody {
            nodes,
            edges,
            groups: Vec::new(),
        };
        let result = plan(&registry, &body, backends, ASSIGN_BUDGET).unwrap();
        let found: Vec<u32> = result
            .placed
            .iter()
            .map(|placed| match placed {
                Placed::Backend(index) => u32::from(index.0),
                Placed::Host => panic!("no host here"),
            })
            .collect();
        assert_eq!(found, expected, "{body:?}");
    }
}

fn free_pair() -> Registry {
    let mut registry = Registry::new();
    decl("t.free")
        .optional("a", u8_ty())
        .optional("b", u8_ty())
        .output("out", u8_ty())
        .body("b0")
        .body("b1")
        .add(&mut registry);
    registry
}

#[test]
fn chain_of_200_free_nodes_uses_at_most_4n_plus_8_expansions() {
    let body = GraphBody {
        nodes: (0..200).map(|_| node("t.free")).collect(),
        edges: (0..199).map(|i| edge(i, "out", i + 1, "a")).collect(),
        groups: Vec::new(),
    };
    let result = plan(&free_pair(), &body, 2, ASSIGN_BUDGET).unwrap();
    assert!(result.expansions <= 4 * 200 + 8, "{}", result.expansions);
}

#[test]
fn dense_component_over_budget_is_assignment_budget() {
    let mut edges = Vec::new();
    for to in 1..12u32 {
        edges.push(edge(to - 1, "out", to, "a"));
        edges.push(edge(0, "out", to, "b"));
    }
    let body = GraphBody {
        nodes: (0..12).map(|_| node("t.free")).collect(),
        edges,
        groups: Vec::new(),
    };
    let errors = plan(&free_pair(), &body, 2, 10).unwrap_err();
    let [AssignError::AssignmentBudget { nodes, budget }] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!((nodes.len(), *budget), (12, 10));
}
