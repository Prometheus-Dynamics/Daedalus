//! The exact search over one coupled component.

use super::components::Wiring;

/// Everything assigned so far. A node's place is a backend rank; the host has its own place.
pub(super) struct State<'a> {
    pub wiring: &'a Wiring,
    pub places: u8,
    pub placed: Vec<Option<u8>>,
    /// How many assigned consumers of each port sit in each place.
    pub readers: Vec<Vec<u32>>,
}

impl<'a> State<'a> {
    pub fn new(wiring: &'a Wiring, places: u8, nodes: usize) -> State<'a> {
        State {
            wiring,
            places,
            placed: vec![None; nodes],
            readers: vec![vec![0; places as usize]; wiring.producer.len()],
        }
    }

    /// Commits a node and returns the crossings that adds.
    pub fn place(&mut self, node: u32, place: u8) -> u32 {
        let mut added = 0;
        for &port in &self.wiring.produces[node as usize] {
            added += (0..self.places)
                .filter(|&other| other != place && self.readers[port as usize][other as usize] > 0)
                .count() as u32;
        }
        for &port in &self.wiring.consumes[node as usize] {
            let producer = self.wiring.producer[port as usize];
            let count = &mut self.readers[port as usize][place as usize];
            if *count == 0 && self.placed[producer as usize].is_some_and(|at| at != place) {
                added += 1;
            }
            *count += 1;
        }
        self.placed[node as usize] = Some(place);
        added
    }

    pub(crate) fn unplace(&mut self, node: u32) {
        let place = self.placed[node as usize].take();
        if let Some(place) = place {
            for &port in &self.wiring.consumes[node as usize] {
                self.readers[port as usize][place as usize] -= 1;
            }
        }
    }
}

pub(super) struct Best {
    pub crossings: u32,
    pub rank_sum: u64,
    pub choice: Vec<u8>,
}

/// The search ran out of budget.
pub(super) struct OverBudget;

/// Depth-first in node order, candidates in rank order. The first leaf to reach the minimum is the
/// lexicographically smallest optimum, because a tie never replaces the best.
pub(super) fn search(
    state: &mut State<'_>,
    nodes: &[u32],
    candidates: &[Vec<u8>],
    budget: u64,
    expansions: &mut u64,
) -> Result<Best, OverBudget> {
    let lowest: Vec<u64> = nodes
        .iter()
        .map(|&node| u64::from(candidates[node as usize][0]))
        .collect();
    let mut remaining: Vec<u64> = vec![0; nodes.len() + 1];
    for at in (0..nodes.len()).rev() {
        remaining[at] = remaining[at + 1] + lowest[at];
    }
    let mut best: Option<Best> = None;
    let mut cursor = vec![0usize; nodes.len()];
    let mut added = vec![0u32; nodes.len()];
    let (mut crossings, mut rank_sum, mut depth, mut spent) = (0u32, 0u64, 0usize, 0u64);
    loop {
        let node = nodes[depth];
        let options = &candidates[node as usize];
        if cursor[depth] == options.len() {
            cursor[depth] = 0;
            if depth == 0 {
                break;
            }
            depth -= 1;
            let parent = nodes[depth];
            rank_sum -= state.placed[parent as usize].map_or(0, u64::from);
            crossings -= added[depth];
            state.unplace(parent);
            continue;
        }
        let place = options[cursor[depth]];
        cursor[depth] += 1;
        spent += 1;
        *expansions += 1;
        if spent > budget {
            for &assigned in &nodes[..depth] {
                state.unplace(assigned);
            }
            return Err(OverBudget);
        }
        let delta = state.place(node, place);
        let (next_crossings, next_rank) = (crossings + delta, rank_sum + u64::from(place));
        let bound = (next_crossings, next_rank + remaining[depth + 1]);
        let promising = best
            .as_ref()
            .is_none_or(|known| bound < (known.crossings, known.rank_sum));
        if promising && depth + 1 == nodes.len() {
            let choice = nodes
                .iter()
                .map(|&n| state.placed[n as usize].unwrap_or(0))
                .collect();
            best = Some(Best {
                crossings: next_crossings,
                rank_sum: next_rank,
                choice,
            });
            state.unplace(node);
        } else if promising {
            added[depth] = delta;
            crossings = next_crossings;
            rank_sum = next_rank;
            depth += 1;
        } else {
            state.unplace(node);
        }
    }
    best.ok_or(OverBudget)
}
