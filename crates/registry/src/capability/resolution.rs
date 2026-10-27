use alloc::collections::{BTreeMap, BinaryHeap};
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cmp::{Ordering, Reverse};

use daedalus_transport::{AccessMode, AdaptRequest, Residency, TypeKey};

use crate::diagnostics::{RegistryError, RegistryErrorCode, RegistryResult};

use super::declarations::{AdapterDecl, AdapterPathResolution, AdapterPathStep};
use super::registry::CapabilityRegistry;
use super::support::{ActiveFeatureSet, active_feature_set, features_enabled};

/// A candidate path, as a link to its last adapter. Paths share their prefixes through `parent`,
/// so extending a path is one allocation; the adapters are only listed for a tie-break or the
/// returned result.
struct PathNode<'a> {
    adapter: &'a AdapterDecl,
    parent: Path<'a>,
    /// Number of adapters on the path ending at this node.
    depth: usize,
}

type Path<'a> = Option<Rc<PathNode<'a>>>;

/// The adapters of a path, first step first.
fn path_adapters<'a>(path: &Path<'a>) -> Vec<&'a AdapterDecl> {
    let mut adapters = Vec::new();
    let mut cur = path.as_deref();
    while let Some(node) = cur {
        adapters.push(node.adapter);
        cur = node.parent.as_deref();
    }
    adapters.reverse();
    adapters
}

/// Number of adapter steps on a path.
fn path_len(path: &Path<'_>) -> usize {
    path.as_deref().map_or(0, |node| node.depth)
}

/// Preference order of two paths to the same cost: `Less` is the preferred path. The path with
/// fewer adapter steps wins, then the one whose adapter ids sort first, lexicographically. The
/// order is total and deterministic, so equal-cost routes never depend on registration order.
fn path_cmp(a: &Path<'_>, b: &Path<'_>) -> Ordering {
    path_len(a).cmp(&path_len(b)).then_with(|| {
        let a = path_adapters(a);
        let b = path_adapters(b);
        a.iter()
            .map(|adapter| &adapter.id)
            .cmp(b.iter().map(|adapter| &adapter.id))
    })
}

/// A search frontier entry. The search pops from a max-heap, so the entry that pops first is the
/// greatest under this order: the cheapest, then the preferred path (see [`path_cmp`]).
struct Frontier<'a> {
    cost: u64,
    key: &'a TypeKey,
    path: Path<'a>,
}

impl Ord for Frontier<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        Reverse(self.cost)
            .cmp(&Reverse(other.cost))
            .then_with(|| path_cmp(&other.path, &self.path))
            .then_with(|| self.key.cmp(other.key))
    }
}

impl PartialOrd for Frontier<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Frontier<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Frontier<'_> {}

/// The registry's adapters grouped by source type, each group in adapter id order: the search
/// expands a type by visiting only the adapters leaving it.
///
/// Build it once with [`CapabilityRegistry::adapter_index`] and reuse it for every search of one
/// planning run. It is a snapshot: mutating the registry needs a new index.
#[derive(Clone, Debug, Default)]
pub struct AdapterIndex {
    outgoing: BTreeMap<TypeKey, Vec<AdapterDecl>>,
}

impl AdapterIndex {
    fn outgoing_from(&self, from: &TypeKey) -> &[AdapterDecl] {
        self.outgoing.get(from).map_or(&[], Vec::as_slice)
    }
}

impl CapabilityRegistry {
    /// Index this registry's adapters by source type for repeated path searches.
    pub fn adapter_index(&self) -> AdapterIndex {
        let mut outgoing: BTreeMap<TypeKey, Vec<AdapterDecl>> = BTreeMap::new();
        for adapter in self.adapters().values() {
            outgoing
                .entry(adapter.from.clone())
                .or_default()
                .push(adapter.clone());
        }
        AdapterIndex { outgoing }
    }

    /// [`Self::resolve_adapter_path_for_with_context`] over a prebuilt [`AdapterIndex`] of this
    /// registry (see [`Self::adapter_index`]), without rebuilding the index per search.
    pub fn resolve_adapter_path_indexed(
        &self,
        index: &AdapterIndex,
        from: &TypeKey,
        request: &AdaptRequest,
        active_features: &[String],
        allow_gpu: bool,
    ) -> RegistryResult<AdapterPathResolution> {
        let active_features = active_feature_set(active_features);
        self.search_adapter_path(index, from, request, &active_features, allow_gpu)
    }

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
        let index = self.adapter_index();
        self.resolve_adapter_path_indexed(&index, from, request, active_features, allow_gpu)
    }

    fn search_adapter_path(
        &self,
        index: &AdapterIndex,
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
        let mut dist: BTreeMap<&TypeKey, (u64, Path<'_>)> = BTreeMap::new();
        let mut heap: BinaryHeap<Frontier<'_>> = BinaryHeap::new();

        dist.insert(from, (0, None));
        heap.push(Frontier {
            cost: 0,
            key: from,
            path: None,
        });

        while let Some(Frontier {
            cost,
            key: cur,
            path,
        }) = heap.pop()
        {
            if targets.iter().any(|target| target == cur)
                && (!request.exclusive || path.is_some())
                && path_satisfies_request(path.as_deref().map(|node| node.adapter), request)
            {
                let adapters = path_adapters(&path);
                return Ok(AdapterPathResolution {
                    step_details: adapters
                        .iter()
                        .map(|adapter| AdapterPathStep::from_decl(adapter))
                        .collect(),
                    steps: adapters.iter().map(|adapter| adapter.id.clone()).collect(),
                    total_cost: cost,
                    resolved_target: Some(cur.clone()),
                });
            }

            if let Some((known, known_path)) = dist.get(cur)
                && (cost > *known || (cost == *known && path_cmp(&path, known_path).is_gt()))
            {
                continue;
            }

            for adapter in index
                .outgoing_from(cur)
                .iter()
                .filter(|adapter| adapter.enabled_in_context(active_features, allow_gpu))
            {
                let next_cost = cost.saturating_add(adapter.cost.weight());
                let next_path: Path<'_> = Some(Rc::new(PathNode {
                    adapter,
                    parent: path.clone(),
                    depth: path_len(&path) + 1,
                }));

                let replace = match dist.get(&adapter.to) {
                    None => true,
                    Some((known_cost, known_path)) => {
                        let target_candidate = targets.iter().any(|target| target == &adapter.to);
                        let known_satisfies = target_candidate
                            && path_satisfies_request(
                                known_path.as_deref().map(|node| node.adapter),
                                request,
                            );
                        let next_satisfies =
                            target_candidate && path_satisfies_request(Some(adapter), request);
                        (next_satisfies && !known_satisfies)
                            || (known_satisfies == next_satisfies
                                && (next_cost < *known_cost
                                    || (next_cost == *known_cost
                                        && path_cmp(&next_path, known_path).is_lt())))
                    }
                };

                if replace {
                    dist.insert(&adapter.to, (next_cost, next_path.clone()));
                    heap.push(Frontier {
                        cost: next_cost,
                        key: &adapter.to,
                        path: next_path,
                    });
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
}

/// Whether a path whose last adapter is `last` (none for the empty path) satisfies `request`.
fn path_satisfies_request(last: Option<&AdapterDecl>, request: &AdaptRequest) -> bool {
    if !request.exclusive
        && request.access == AccessMode::Read
        && request.residency.is_none()
        && request.layout.is_none()
    {
        return true;
    }
    last.is_some_and(|adapter| adapter.matches_request(request))
}
