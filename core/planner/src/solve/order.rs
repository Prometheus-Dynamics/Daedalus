//! A stable topological order: of the ready nodes, the lowest index goes first.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;

use super::Work;
use crate::pipeline::SolveError;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OrderError {
    /// Every node on a cycle, sorted by id.
    ScheduleConflict { cycle: Vec<String> },
}

impl fmt::Display for OrderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderError::ScheduleConflict { cycle } => {
                write!(f, "the graph has a cycle through: {}", cycle.join(", "))
            }
        }
    }
}

impl std::error::Error for OrderError {}

/// Drops the nodes that only hang off a cycle, so what's left is on one.
fn on_cycle(left: &mut [bool], edges: &[(usize, usize)]) {
    loop {
        let mut changed = false;
        for node in 0..left.len() {
            if left[node] && !edges.iter().any(|&(from, to)| from == node && left[to]) {
                left[node] = false;
                changed = true;
            }
        }
        if !changed {
            return;
        }
    }
}

pub fn run(work: &mut Work<'_>) {
    let count = work.graph.nodes.len();
    let edges: Vec<(usize, usize)> = work
        .graph
        .edges
        .iter()
        .map(|edge| (edge.from.node as usize, edge.to.node as usize))
        .filter(|&(from, to)| from < count && to < count)
        .collect();
    let mut waiting = vec![0u32; count];
    let mut leaving = vec![Vec::new(); count];
    for &(from, to) in &edges {
        waiting[to] += 1;
        leaving[from].push(to);
    }
    let mut ready: BinaryHeap<Reverse<usize>> = (0..count)
        .filter(|&node| waiting[node] == 0)
        .map(Reverse)
        .collect();
    let mut order = Vec::with_capacity(count);
    while let Some(Reverse(node)) = ready.pop() {
        order.push(node as u32);
        for &to in &leaving[node] {
            waiting[to] -= 1;
            if waiting[to] == 0 {
                ready.push(Reverse(to));
            }
        }
    }
    if order.len() < count {
        let mut left = waiting.iter().map(|&count| count > 0).collect::<Vec<_>>();
        on_cycle(&mut left, &edges);
        let mut cycle: Vec<String> = (0..count)
            .filter(|&node| left[node])
            .map(|node| work.graph.nodes[node].doc.id.clone())
            .collect();
        cycle.sort();
        work.errors
            .push(SolveError::Order(OrderError::ScheduleConflict { cycle }));
        return;
    }
    work.graph.order = order;
}
