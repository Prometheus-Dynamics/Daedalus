use daedalus_data::model::TypeExpr;
use daedalus_registry::capability::{NodeDecl, TypeDecl};
use daedalus_registry::ids::NodeId;
use std::collections::BTreeMap;

use super::PlannerConfig;

#[derive(Clone, Debug, Default)]
pub struct PlannerCatalog {
    nodes: BTreeMap<NodeId, NodeDecl>,
    types: BTreeMap<daedalus_transport::TypeKey, TypeDecl>,
}

impl PlannerCatalog {
    pub(super) fn from_config(config: &PlannerConfig) -> Self {
        let Some(capabilities) = config.transport_capabilities.as_ref() else {
            return Self::default();
        };
        let nodes = capabilities
            .nodes()
            .snapshot_filtered(&config.active_features)
            .into_iter()
            .map(|decl| (decl.id.clone(), decl))
            .collect();
        let types = capabilities
            .types()
            .snapshot_filtered(&config.active_features)
            .into_iter()
            .map(|decl| (decl.key.clone(), decl))
            .collect();
        Self { nodes, types }
    }

    pub(super) fn node(&self, id: &NodeId) -> Option<&NodeDecl> {
        self.nodes.get(id)
    }

    pub(super) fn node_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.keys()
    }

    pub(super) fn type_label_lookup(&self) -> BTreeMap<TypeExpr, String> {
        let mut out = BTreeMap::new();
        for decl in self.types.values() {
            let Some(ty) = decl.schema.clone() else {
                continue;
            };
            let label = decl
                .rust
                .as_ref()
                .map(|value| simplify_rust_name(value))
                .unwrap_or_else(|| decl.key.to_string());
            out.entry(ty).or_insert(label);
        }
        out
    }
}

pub(super) fn simplify_rust_name(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return raw.to_string();
    }
    let outer = trimmed.split('<').next().unwrap_or(trimmed);
    let simple = outer.rsplit("::").next().unwrap_or(outer);
    if simple.is_empty() {
        trimmed.to_string()
    } else {
        simple.to_string()
    }
}
