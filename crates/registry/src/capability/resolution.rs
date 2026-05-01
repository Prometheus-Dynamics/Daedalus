use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

use daedalus_transport::{AccessMode, AdaptRequest, AdapterId, Residency, TypeKey};

use crate::diagnostics::{RegistryError, RegistryErrorCode, RegistryResult};

use super::declarations::{AdapterPathResolution, AdapterPathStep};
use super::registry::CapabilityRegistry;
use super::support::{ActiveFeatureSet, active_feature_set, features_enabled};

impl CapabilityRegistry {
    /// Resolve the cheapest adapter path between two transport type keys.
    ///
    /// This is metadata-only path planning. Runtime executable adapter functions are expected to
    /// live in host-owned tables keyed by the returned adapter ids.
    pub fn resolve_adapter_path(
        &self,
        from: &TypeKey,
        to: &TypeKey,
    ) -> RegistryResult<AdapterPathResolution> {
        self.resolve_adapter_path_for(from, &AdaptRequest::new(to.clone()))
    }

    /// Resolve the cheapest adapter path satisfying a full adaptation request.
    pub fn resolve_adapter_path_for(
        &self,
        from: &TypeKey,
        request: &AdaptRequest,
    ) -> RegistryResult<AdapterPathResolution> {
        self.resolve_adapter_path_for_with_context(from, request, &[], true)
    }

    /// Resolve the cheapest adapter path satisfying a request and feature/GPU context.
    pub fn resolve_adapter_path_for_with_context(
        &self,
        from: &TypeKey,
        request: &AdaptRequest,
        active_features: &[String],
        allow_gpu: bool,
    ) -> RegistryResult<AdapterPathResolution> {
        let active_features = active_feature_set(active_features);
        self.resolve_adapter_path_for_with_feature_set(from, request, &active_features, allow_gpu)
    }

    fn resolve_adapter_path_for_with_feature_set(
        &self,
        from: &TypeKey,
        request: &AdaptRequest,
        active_features: &ActiveFeatureSet<'_>,
        allow_gpu: bool,
    ) -> RegistryResult<AdapterPathResolution> {
        let to = &request.target;
        let targets = self.adapter_target_candidates(request, active_features, allow_gpu);
        if from == to
            && !request.exclusive
            && request.residency.is_none()
            && request.layout.is_none()
        {
            return Ok(AdapterPathResolution {
                steps: Vec::new(),
                step_details: Vec::new(),
                total_cost: 0,
                resolved_target: Some(to.clone()),
            });
        }

        if from == to
            && (request.exclusive || request.residency.is_some() || request.layout.is_some())
            && let Some(adapter) = self
                .adapters()
                .values()
                .filter(|adapter| adapter.from == *from && adapter.to == *to)
                .filter(|adapter| adapter.enabled_in_context(active_features, allow_gpu))
                .filter(|adapter| adapter.matches_request(request))
                .min_by(|a, b| {
                    a.cost
                        .weight()
                        .cmp(&b.cost.weight())
                        .then_with(|| a.id.cmp(&b.id))
                })
        {
            return Ok(AdapterPathResolution {
                steps: vec![adapter.id.clone()],
                step_details: vec![AdapterPathStep::from_decl(adapter)],
                total_cost: adapter.cost.weight(),
                resolved_target: Some(to.clone()),
            });
        }

        // Dijkstra with deterministic tie-breaking through ordered maps and path/id ordering.
        type HeapEntry = (Reverse<u64>, TypeKey, Vec<AdapterId>);
        let mut dist: BTreeMap<TypeKey, (u64, Vec<AdapterId>)> = BTreeMap::new();
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

        dist.insert(from.clone(), (0, Vec::new()));
        heap.push((Reverse(0), from.clone(), Vec::new()));

        while let Some((Reverse(cost), cur, path)) = heap.pop() {
            if targets.iter().any(|target| target == &cur)
                && (!request.exclusive || !path.is_empty())
                && self.path_satisfies_request(&path, request)
            {
                return Ok(AdapterPathResolution {
                    step_details: self.adapter_path_details(&path),
                    steps: path,
                    total_cost: cost,
                    resolved_target: Some(cur),
                });
            }

            if let Some((known, known_path)) = dist.get(&cur)
                && (cost > *known || (cost == *known && &path > known_path))
            {
                continue;
            }

            for adapter in self
                .adapters()
                .values()
                .filter(|adapter| adapter.from == cur)
                .filter(|adapter| adapter.enabled_in_context(active_features, allow_gpu))
            {
                let next_cost = cost.saturating_add(adapter.cost.weight());
                let mut next_path = path.clone();
                next_path.push(adapter.id.clone());

                let replace = match dist.get(&adapter.to) {
                    None => true,
                    Some((known_cost, known_path)) => {
                        let target_candidate = targets.iter().any(|target| target == &adapter.to);
                        let known_satisfies =
                            target_candidate && self.path_satisfies_request(known_path, request);
                        let next_satisfies =
                            target_candidate && self.path_satisfies_request(&next_path, request);
                        (next_satisfies && !known_satisfies)
                            || (known_satisfies == next_satisfies
                                && (next_cost < *known_cost
                                    || (next_cost == *known_cost && next_path < *known_path)))
                    }
                };

                if replace {
                    dist.insert(adapter.to.clone(), (next_cost, next_path.clone()));
                    heap.push((Reverse(next_cost), adapter.to.clone(), next_path));
                }
            }
        }

        Err(RegistryError::new(
            RegistryErrorCode::AdapterError,
            format!("no adapter path from {from} to {}", request.target),
        ))
    }

    fn adapter_target_candidates(
        &self,
        request: &AdaptRequest,
        active_features: &ActiveFeatureSet<'_>,
        allow_gpu: bool,
    ) -> Vec<TypeKey> {
        let mut targets = vec![request.target.clone()];
        if request.residency == Some(Residency::Gpu) && allow_gpu {
            targets.extend(
                self.devices()
                    .values()
                    .filter(|device| device.cpu == request.target)
                    .filter(|device| features_enabled(&device.feature_flags, active_features))
                    .map(|device| device.device.clone()),
            );
            targets.sort();
            targets.dedup();
        }
        targets
    }

    fn path_satisfies_request(&self, path: &[AdapterId], request: &AdaptRequest) -> bool {
        if !request.exclusive
            && request.access == AccessMode::Read
            && request.residency.is_none()
            && request.layout.is_none()
        {
            return true;
        }
        path.last()
            .and_then(|id| self.adapters().get(id))
            .is_some_and(|adapter| adapter.matches_request(request))
    }

    fn adapter_path_details(&self, path: &[AdapterId]) -> Vec<AdapterPathStep> {
        path.iter()
            .filter_map(|id| self.adapters().get(id).map(AdapterPathStep::from_decl))
            .collect()
    }
}
