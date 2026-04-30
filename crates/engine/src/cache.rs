use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use daedalus_planner::{ExecutionPlan, Graph, PlannerConfig, PlannerOutput};
use daedalus_runtime::{RuntimePlan, SchedulerConfig};

use crate::config::DEFAULT_CACHE_ENTRIES;

#[cfg(feature = "config-env")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "config-env", serde(rename_all = "snake_case"))]
pub enum CacheStatus {
    Hit,
    Miss,
}

impl CacheStatus {
    pub fn is_cached(self) -> bool {
        matches!(self, CacheStatus::Hit)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "config-env", serde(rename_all = "snake_case"))]
pub struct CacheCounters {
    pub hits: u64,
    pub misses: u64,
    pub invalidations: u64,
    pub evictions: u64,
    pub entries: usize,
    pub max_entries: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "config-env", serde(rename_all = "snake_case"))]
pub struct EngineCacheMetrics {
    pub planner: CacheCounters,
    pub runtime_plan: CacheCounters,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PlannerCacheKey {
    graph_fingerprint: u64,
    planner_fingerprint: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RuntimePlanCacheKey {
    plan_hash: u64,
    scheduler_fingerprint: u64,
}

struct CacheShard<K, V> {
    entries: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K, V> Default for CacheShard<K, V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }
}

pub(crate) struct EngineCaches {
    planner: Mutex<CacheShard<PlannerCacheKey, PlannerOutput>>,
    runtime: Mutex<CacheShard<RuntimePlanCacheKey, RuntimePlan>>,
    planner_max_entries: usize,
    runtime_max_entries: usize,
    planner_hits: AtomicU64,
    planner_misses: AtomicU64,
    planner_invalidations: AtomicU64,
    planner_evictions: AtomicU64,
    runtime_hits: AtomicU64,
    runtime_misses: AtomicU64,
    runtime_invalidations: AtomicU64,
    runtime_evictions: AtomicU64,
}

impl EngineCaches {
    pub(crate) fn with_limits(planner_max_entries: usize, runtime_max_entries: usize) -> Self {
        Self {
            planner: Mutex::new(CacheShard::default()),
            runtime: Mutex::new(CacheShard::default()),
            planner_max_entries: planner_max_entries.max(1),
            runtime_max_entries: runtime_max_entries.max(1),
            planner_hits: AtomicU64::new(0),
            planner_misses: AtomicU64::new(0),
            planner_invalidations: AtomicU64::new(0),
            planner_evictions: AtomicU64::new(0),
            runtime_hits: AtomicU64::new(0),
            runtime_misses: AtomicU64::new(0),
            runtime_invalidations: AtomicU64::new(0),
            runtime_evictions: AtomicU64::new(0),
        }
    }

    pub(crate) fn planner_get(&self, key: &PlannerCacheKey) -> Option<PlannerOutput> {
        let value = lock_cache_shard("planner", &self.planner)
            .entries
            .get(key)
            .cloned();
        match value {
            Some(_) => {
                self.planner_hits.fetch_add(1, Ordering::Relaxed);
                value
            }
            None => {
                self.planner_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub(crate) fn planner_insert(&self, key: PlannerCacheKey, value: PlannerOutput) {
        let mut shard = lock_cache_shard("planner", &self.planner);
        let is_new = !shard.entries.contains_key(&key);
        shard.entries.insert(key.clone(), value);
        if is_new {
            shard.order.push_back(key);
        }
        evict_fifo(
            &mut shard,
            self.planner_max_entries,
            &self.planner_evictions,
        );
    }

    pub(crate) fn runtime_get(&self, key: &RuntimePlanCacheKey) -> Option<RuntimePlan> {
        let value = lock_cache_shard("runtime_plan", &self.runtime)
            .entries
            .get(key)
            .cloned();
        match value {
            Some(_) => {
                self.runtime_hits.fetch_add(1, Ordering::Relaxed);
                value
            }
            None => {
                self.runtime_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub(crate) fn runtime_insert(&self, key: RuntimePlanCacheKey, value: RuntimePlan) {
        let mut shard = lock_cache_shard("runtime_plan", &self.runtime);
        let is_new = !shard.entries.contains_key(&key);
        shard.entries.insert(key.clone(), value);
        if is_new {
            shard.order.push_back(key);
        }
        evict_fifo(
            &mut shard,
            self.runtime_max_entries,
            &self.runtime_evictions,
        );
    }

    pub(crate) fn metrics(&self) -> EngineCacheMetrics {
        let planner_entries = lock_cache_shard("planner", &self.planner).entries.len();
        let runtime_entries = lock_cache_shard("runtime_plan", &self.runtime)
            .entries
            .len();
        EngineCacheMetrics {
            planner: CacheCounters {
                hits: self.planner_hits.load(Ordering::Relaxed),
                misses: self.planner_misses.load(Ordering::Relaxed),
                invalidations: self.planner_invalidations.load(Ordering::Relaxed),
                evictions: self.planner_evictions.load(Ordering::Relaxed),
                entries: planner_entries,
                max_entries: self.planner_max_entries,
            },
            runtime_plan: CacheCounters {
                hits: self.runtime_hits.load(Ordering::Relaxed),
                misses: self.runtime_misses.load(Ordering::Relaxed),
                invalidations: self.runtime_invalidations.load(Ordering::Relaxed),
                evictions: self.runtime_evictions.load(Ordering::Relaxed),
                entries: runtime_entries,
                max_entries: self.runtime_max_entries,
            },
        }
    }

    pub(crate) fn clear(&self) -> EngineCacheMetrics {
        let planner_invalidated = {
            let mut shard = lock_cache_shard("planner", &self.planner);
            let len = shard.entries.len() as u64;
            shard.entries.clear();
            shard.order.clear();
            len
        };
        let runtime_invalidated = {
            let mut shard = lock_cache_shard("runtime_plan", &self.runtime);
            let len = shard.entries.len() as u64;
            shard.entries.clear();
            shard.order.clear();
            len
        };
        self.planner_invalidations
            .fetch_add(planner_invalidated, Ordering::Relaxed);
        self.runtime_invalidations
            .fetch_add(runtime_invalidated, Ordering::Relaxed);
        self.metrics()
    }
}

impl Default for EngineCaches {
    fn default() -> Self {
        Self::with_limits(DEFAULT_CACHE_ENTRIES, DEFAULT_CACHE_ENTRIES)
    }
}

fn lock_cache_shard<'a, K, V>(
    name: &'static str,
    shard: &'a Mutex<CacheShard<K, V>>,
) -> MutexGuard<'a, CacheShard<K, V>> {
    shard.lock().unwrap_or_else(|poisoned| {
        tracing::warn!(
            target: "daedalus_engine::cache",
            cache = name,
            "cache lock poisoned; recovering cached state"
        );
        poisoned.into_inner()
    })
}

fn evict_fifo<K, V>(shard: &mut CacheShard<K, V>, max_entries: usize, evictions: &AtomicU64)
where
    K: Clone + Eq + std::hash::Hash,
{
    while shard.entries.len() > max_entries {
        let Some(oldest) = shard.order.pop_front() else {
            break;
        };
        if shard.entries.remove(&oldest).is_some() {
            evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) fn new_caches(
    planner_max_entries: usize,
    runtime_max_entries: usize,
) -> Arc<EngineCaches> {
    Arc::new(EngineCaches::with_limits(
        planner_max_entries,
        runtime_max_entries,
    ))
}

pub(crate) fn planner_cache_key(graph: &Graph, planner_config: &PlannerConfig) -> PlannerCacheKey {
    PlannerCacheKey {
        graph_fingerprint: graph.stable_hash().0,
        planner_fingerprint: planner_config.stable_hash().0,
    }
}

pub(crate) fn runtime_plan_cache_key(
    plan: &ExecutionPlan,
    scheduler_config: &SchedulerConfig,
) -> RuntimePlanCacheKey {
    RuntimePlanCacheKey {
        plan_hash: plan.hash.0,
        scheduler_fingerprint: scheduler_config.stable_hash().0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_planner::ExecutionPlan;

    fn planner_key(value: u64) -> PlannerCacheKey {
        PlannerCacheKey {
            graph_fingerprint: value,
            planner_fingerprint: 0,
        }
    }

    fn runtime_key(value: u64) -> RuntimePlanCacheKey {
        RuntimePlanCacheKey {
            plan_hash: value,
            scheduler_fingerprint: 0,
        }
    }

    fn planner_output() -> PlannerOutput {
        PlannerOutput {
            plan: ExecutionPlan::new(Graph::default(), vec![]),
            diagnostics: vec![],
        }
    }

    fn runtime_plan() -> RuntimePlan {
        let plan = ExecutionPlan::new(Graph::default(), vec![]);
        daedalus_runtime::build_runtime(&plan, &SchedulerConfig::default())
    }

    #[test]
    fn bounded_caches_evict_and_report_metrics() {
        let caches = EngineCaches::with_limits(1, 1);

        caches.planner_insert(planner_key(1), planner_output());
        caches.runtime_insert(runtime_key(1), runtime_plan());
        assert!(caches.planner_get(&planner_key(1)).is_some());
        assert!(caches.runtime_get(&runtime_key(1)).is_some());

        caches.planner_insert(planner_key(2), planner_output());
        caches.runtime_insert(runtime_key(2), runtime_plan());

        assert!(caches.planner_get(&planner_key(1)).is_none());
        assert!(caches.runtime_get(&runtime_key(1)).is_none());
        assert!(caches.planner_get(&planner_key(2)).is_some());
        assert!(caches.runtime_get(&runtime_key(2)).is_some());

        let metrics = caches.metrics();
        assert_eq!(metrics.planner.entries, 1);
        assert_eq!(metrics.planner.max_entries, 1);
        assert_eq!(metrics.planner.evictions, 1);
        assert_eq!(metrics.runtime_plan.entries, 1);
        assert_eq!(metrics.runtime_plan.max_entries, 1);
        assert_eq!(metrics.runtime_plan.evictions, 1);

        let metrics = caches.clear();
        assert_eq!(metrics.planner.entries, 0);
        assert_eq!(metrics.runtime_plan.entries, 0);
        assert_eq!(metrics.planner.invalidations, 1);
        assert_eq!(metrics.runtime_plan.invalidations, 1);
    }
}
