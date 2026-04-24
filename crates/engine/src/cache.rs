use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use daedalus_planner::{ExecutionPlan, Graph, PlannerConfig, PlannerOutput, StableHash};
use daedalus_registry::store::Registry;
use daedalus_runtime::{RuntimePlan, SchedulerConfig};

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
    registry_fingerprint: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RuntimePlanCacheKey {
    plan_hash: u64,
    scheduler_fingerprint: u64,
}

#[derive(Default)]
pub(crate) struct EngineCaches {
    planner: Mutex<HashMap<PlannerCacheKey, PlannerOutput>>,
    runtime: Mutex<HashMap<RuntimePlanCacheKey, RuntimePlan>>,
    planner_hits: AtomicU64,
    planner_misses: AtomicU64,
    planner_invalidations: AtomicU64,
    runtime_hits: AtomicU64,
    runtime_misses: AtomicU64,
    runtime_invalidations: AtomicU64,
}

impl EngineCaches {
    pub(crate) fn planner_get(&self, key: &PlannerCacheKey) -> Option<PlannerOutput> {
        let value = self.planner.lock().ok()?.get(key).cloned();
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
        if let Ok(mut guard) = self.planner.lock() {
            guard.insert(key, value);
        }
    }

    pub(crate) fn runtime_get(&self, key: &RuntimePlanCacheKey) -> Option<RuntimePlan> {
        let value = self.runtime.lock().ok()?.get(key).cloned();
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
        if let Ok(mut guard) = self.runtime.lock() {
            guard.insert(key, value);
        }
    }

    pub(crate) fn metrics(&self) -> EngineCacheMetrics {
        EngineCacheMetrics {
            planner: CacheCounters {
                hits: self.planner_hits.load(Ordering::Relaxed),
                misses: self.planner_misses.load(Ordering::Relaxed),
                invalidations: self.planner_invalidations.load(Ordering::Relaxed),
            },
            runtime_plan: CacheCounters {
                hits: self.runtime_hits.load(Ordering::Relaxed),
                misses: self.runtime_misses.load(Ordering::Relaxed),
                invalidations: self.runtime_invalidations.load(Ordering::Relaxed),
            },
        }
    }

    pub(crate) fn clear(&self) -> EngineCacheMetrics {
        let planner_invalidated = if let Ok(mut guard) = self.planner.lock() {
            let len = guard.len() as u64;
            guard.clear();
            len
        } else {
            0
        };
        let runtime_invalidated = if let Ok(mut guard) = self.runtime.lock() {
            let len = guard.len() as u64;
            guard.clear();
            len
        } else {
            0
        };
        self.planner_invalidations
            .fetch_add(planner_invalidated, Ordering::Relaxed);
        self.runtime_invalidations
            .fetch_add(runtime_invalidated, Ordering::Relaxed);
        self.metrics()
    }
}

pub(crate) fn new_caches() -> Arc<EngineCaches> {
    Arc::new(EngineCaches::default())
}

pub(crate) fn planner_cache_key(
    graph: &Graph,
    planner_config: &PlannerConfig,
    registry: &Registry,
) -> PlannerCacheKey {
    PlannerCacheKey {
        graph_fingerprint: stable_hash_debug(graph).0,
        planner_fingerprint: stable_hash_debug(planner_config).0,
        registry_fingerprint: stable_hash_debug(&registry.snapshot()).0,
    }
}

pub(crate) fn runtime_plan_cache_key(
    plan: &ExecutionPlan,
    scheduler_config: &SchedulerConfig,
) -> RuntimePlanCacheKey {
    RuntimePlanCacheKey {
        plan_hash: plan.hash.0,
        scheduler_fingerprint: stable_hash_debug(scheduler_config).0,
    }
}

fn stable_hash_debug(value: &impl std::fmt::Debug) -> StableHash {
    StableHash::from_bytes(format!("{value:?}").as_bytes())
}
