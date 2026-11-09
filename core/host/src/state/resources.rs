//! Managed resources: values a node instance holds for as long as its graph lives. They are
//! never written to snapshots. A reload moves them to the new store with [`StateStore::carry_resources`],
//! so each one is dropped exactly once.

use std::any::Any;

use daedalus_types::NodeId;

use super::StateStore;

impl StateStore {
    /// Stores a managed resource of `node` instance `instance`, replacing one of the same type.
    pub fn set_resource<T: Send + 'static>(&mut self, node: NodeId, instance: u32, value: T) {
        self.put(node, instance, true, Box::new(value), None);
    }

    /// The managed resource of type `T` for this node instance.
    pub fn resource<T: 'static>(&self, node: NodeId, instance: u32) -> Option<&T> {
        self.iter_resources(node, instance)
            .find_map(|value| (*value).downcast_ref::<T>())
    }

    fn iter_resources(
        &self,
        node: NodeId,
        instance: u32,
    ) -> impl Iterator<Item = &(dyn Any + Send)> + '_ {
        self.entries
            .iter()
            .filter(move |entry| entry.resource && entry.node == node && entry.instance == instance)
            .map(|entry| &*entry.value)
    }

    /// Moves every resource of the listed nodes out of `from` and into this store. A resource
    /// that replaces one here drops the old value once. Returns how many moved.
    pub fn carry_resources(&mut self, from: &mut StateStore, nodes: &[NodeId]) -> usize {
        let mut moved = 0;
        let mut kept = Vec::new();
        for entry in std::mem::take(&mut from.entries) {
            if entry.resource && nodes.contains(&entry.node) {
                self.put(entry.node, entry.instance, true, entry.value, entry.codec);
                moved += 1;
            } else {
                kept.push(entry);
            }
        }
        from.entries = kept;
        moved
    }
}
