//! A bounded cache of plans. The engine owns the instance.

use daedalus_backend::Backend;
use daedalus_graph::GraphBody;
use daedalus_registry::Registry;
use daedalus_types::fnv1a64;

use crate::api::Planned;
use crate::config::Config;
use crate::hash::{graph_bytes, settings_bytes};

/// What a plan was made from. The full bytes are kept, so two keys whose hashes collide are still
/// told apart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanKey {
    graph_hash: u64,
    settings_hash: u64,
    graph: Vec<u8>,
    settings: Vec<u8>,
}

impl PlanKey {
    pub fn new(
        graph: &GraphBody,
        registry: &Registry,
        backends: &[&dyn Backend],
        config: &Config,
    ) -> PlanKey {
        let (graph, settings) = (
            graph_bytes(graph),
            settings_bytes(config, registry, backends),
        );
        PlanKey {
            graph_hash: fnv1a64(&graph),
            settings_hash: fnv1a64(&settings),
            graph,
            settings,
        }
    }

    /// A key with exactly these hashes and bytes.
    pub fn from_parts(
        graph_hash: u64,
        settings_hash: u64,
        graph: Vec<u8>,
        settings: Vec<u8>,
    ) -> PlanKey {
        PlanKey {
            graph_hash,
            settings_hash,
            graph,
            settings,
        }
    }

    pub fn graph_hash(&self) -> u64 {
        self.graph_hash
    }

    pub fn settings_hash(&self) -> u64 {
        self.settings_hash
    }

    fn matches(&self, other: &PlanKey) -> bool {
        self.graph_hash == other.graph_hash
            && self.settings_hash == other.settings_hash
            && self.graph == other.graph
            && self.settings == other.settings
    }
}

struct Entry<P> {
    key: PlanKey,
    plan: P,
    used: u64,
}

/// Holds at most `capacity` plans and drops the one used longest ago.
pub struct PlanCache<P = Planned> {
    capacity: usize,
    entries: Vec<Entry<P>>,
    clock: u64,
    hits: u64,
    misses: u64,
}

impl<P> PlanCache<P> {
    pub fn new(capacity: usize) -> PlanCache<P> {
        PlanCache {
            capacity,
            entries: Vec::new(),
            clock: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    pub fn get(&mut self, key: &PlanKey) -> Option<&P> {
        let used = self.tick();
        match self.entries.iter_mut().find(|entry| entry.key.matches(key)) {
            Some(entry) => {
                entry.used = used;
                self.hits += 1;
                Some(&entry.plan)
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    /// Stores a plan, replacing any under the same key.
    pub fn insert(&mut self, key: PlanKey, plan: P) {
        if self.capacity == 0 {
            return;
        }
        let used = self.tick();
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.key.matches(&key))
        {
            entry.plan = plan;
            entry.used = used;
            return;
        }
        if self.entries.len() == self.capacity
            && let Some(oldest) = (0..self.entries.len()).min_by_key(|&at| self.entries[at].used)
        {
            self.entries.swap_remove(oldest);
        }
        self.entries.push(Entry { key, plan, used });
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn hits(&self) -> u64 {
        self.hits
    }

    pub fn misses(&self) -> u64 {
        self.misses
    }
}
