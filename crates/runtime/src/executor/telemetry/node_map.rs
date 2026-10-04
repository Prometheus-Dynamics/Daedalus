//! Per-node metrics stored densely by node index.

use crate::prelude::*;
use alloc::collections::BTreeMap;
use core::fmt;

use super::NodeMetrics;

/// Metrics per planned node index (`NodeRef.0`), iterated in index order.
///
/// One allocation for the whole graph (sized by the executor before a run) instead of a tree of
/// nodes; serializes, prints and compares like a `BTreeMap<usize, NodeMetrics>`.
#[derive(Clone, Default)]
pub struct NodeMetricsMap {
    slots: Vec<Option<NodeMetrics>>,
}

impl NodeMetricsMap {
    pub fn get(&self, node_idx: usize) -> Option<&NodeMetrics> {
        self.slots.get(node_idx)?.as_ref()
    }

    pub fn get_mut(&mut self, node_idx: usize) -> Option<&mut NodeMetrics> {
        self.slots.get_mut(node_idx)?.as_mut()
    }

    /// The metrics of `node_idx`, inserted empty if missing.
    pub fn entry(&mut self, node_idx: usize) -> &mut NodeMetrics {
        if node_idx >= self.slots.len() {
            self.slots.resize_with(node_idx + 1, || None);
        }
        self.slots[node_idx].get_or_insert_with(NodeMetrics::default)
    }

    pub fn insert(&mut self, node_idx: usize, metrics: NodeMetrics) -> Option<NodeMetrics> {
        let previous = self.slots.get_mut(node_idx).and_then(Option::take);
        *self.entry(node_idx) = metrics;
        previous
    }

    /// Make room for `nodes` indices so recording allocates at most once.
    pub fn reserve_nodes(&mut self, nodes: usize) {
        if self.slots.len() < nodes {
            self.slots.resize_with(nodes, || None);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, &NodeMetrics)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| Some((idx, slot.as_ref()?)))
    }

    pub fn keys(&self) -> impl Iterator<Item = usize> + '_ {
        self.iter().map(|(idx, _)| idx)
    }

    pub fn values(&self) -> impl Iterator<Item = &NodeMetrics> {
        self.slots.iter().flatten()
    }

    pub fn contains_key(&self, node_idx: usize) -> bool {
        self.get(node_idx).is_some()
    }

    pub fn len(&self) -> usize {
        self.values().count()
    }

    pub fn is_empty(&self) -> bool {
        self.values().next().is_none()
    }

    /// Remove every entry, keeping the allocation.
    pub fn clear(&mut self) {
        self.slots.fill_with(|| None);
    }

    pub fn to_btree_map(&self) -> BTreeMap<usize, NodeMetrics> {
        self.iter().map(|(idx, m)| (idx, m.clone())).collect()
    }
}

type IntoIterFn = fn((usize, Option<NodeMetrics>)) -> Option<(usize, NodeMetrics)>;

impl IntoIterator for NodeMetricsMap {
    type Item = (usize, NodeMetrics);
    type IntoIter = core::iter::FilterMap<
        core::iter::Enumerate<alloc::vec::IntoIter<Option<NodeMetrics>>>,
        IntoIterFn,
    >;

    fn into_iter(self) -> Self::IntoIter {
        let present: IntoIterFn = |(idx, slot)| Some((idx, slot?));
        self.slots.into_iter().enumerate().filter_map(present)
    }
}

impl FromIterator<(usize, NodeMetrics)> for NodeMetricsMap {
    fn from_iter<I: IntoIterator<Item = (usize, NodeMetrics)>>(iter: I) -> Self {
        let mut map = Self::default();
        for (idx, metrics) in iter {
            map.insert(idx, metrics);
        }
        map
    }
}

impl PartialEq for NodeMetricsMap {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for NodeMetricsMap {}

impl fmt::Debug for NodeMetricsMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl serde::Serialize for NodeMetricsMap {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.iter())
    }
}

impl<'de> serde::Deserialize<'de> for NodeMetricsMap {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let map = BTreeMap::<usize, NodeMetrics>::deserialize(deserializer)?;
        Ok(map.into_iter().collect())
    }
}
