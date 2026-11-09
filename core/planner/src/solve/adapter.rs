//! Finds the conversion path between two types: cheapest, then fewest steps, then lowest ids.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

use daedalus_registry::ConversionEntry;
use daedalus_types::TypeKey;

/// Cost, steps, then the conversions taken. A conversion's number is its place in id order.
type Route = (u64, u32, Vec<u32>);

/// The usable conversions, with routes cached per source type.
pub(super) struct Routes {
    conversions: Vec<ConversionEntry>,
    types: BTreeMap<TypeKey, usize>,
    leaving: Vec<Vec<u32>>,
    solved: BTreeMap<usize, Vec<Option<Route>>>,
}

impl Routes {
    /// A lossy conversion is never implicit, and one from a type to itself isn't a step.
    pub(super) fn new(conversions: &[ConversionEntry]) -> Routes {
        let mut usable: Vec<ConversionEntry> = conversions
            .iter()
            .filter(|conversion| !conversion.lossy && conversion.from != conversion.to)
            .cloned()
            .collect();
        usable.sort_by_key(|conversion| conversion.id);
        let mut types = BTreeMap::new();
        for conversion in &usable {
            for key in [&conversion.from, &conversion.to] {
                let next = types.len();
                types.entry(key.clone()).or_insert(next);
            }
        }
        let mut leaving = vec![Vec::new(); types.len()];
        for (number, conversion) in usable.iter().enumerate() {
            leaving[types[&conversion.from]].push(number as u32);
        }
        Routes {
            conversions: usable,
            types,
            leaving,
            solved: BTreeMap::new(),
        }
    }

    fn solve(&self, source: usize) -> Vec<Option<Route>> {
        let mut best: Vec<Option<Route>> = vec![None; self.types.len()];
        let mut done = vec![false; self.types.len()];
        let mut heap = BinaryHeap::new();
        best[source] = Some((0, 0, Vec::new()));
        heap.push(Reverse(((0u64, 0u32, Vec::new()), source)));
        while let Some(Reverse((route, at))) = heap.pop() {
            if std::mem::replace(&mut done[at], true) {
                continue;
            }
            for &number in &self.leaving[at] {
                let conversion = &self.conversions[number as usize];
                let to = self.types[&conversion.to];
                let mut path = route.2.clone();
                path.push(number);
                let next = (route.0 + u64::from(conversion.cost), route.1 + 1, path);
                if best[to].as_ref().is_none_or(|known| next < *known) {
                    best[to] = Some(next.clone());
                    heap.push(Reverse((next, to)));
                }
            }
        }
        best
    }

    /// The ids of the conversions from `from` to `to`, in order.
    pub(super) fn path(&mut self, from: &TypeKey, to: &TypeKey) -> Option<Vec<&'static str>> {
        let (&source, &target) = (self.types.get(from)?, self.types.get(to)?);
        if !self.solved.contains_key(&source) {
            let routes = self.solve(source);
            self.solved.insert(source, routes);
        }
        let route = self.solved[&source][target].as_ref()?;
        Some(
            route
                .2
                .iter()
                .map(|&number| self.conversions[number as usize].id)
                .collect(),
        )
    }
}
