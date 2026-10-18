use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use core::cell::RefCell;
use daedalus_data::model::TypeExpr;
use daedalus_registry::capability::{AdapterIndex, NodeDecl, TypeDecl};
use daedalus_registry::ids::NodeId;
use daedalus_registry::typeexpr_transport_key;
use daedalus_transport::TypeKey;

use super::PlannerConfig;

#[derive(Clone, Debug, Default)]
pub struct PlannerCatalog {
    nodes: BTreeMap<NodeId, NodeDecl>,
    types: BTreeMap<daedalus_transport::TypeKey, TypeDecl>,
    /// The adapter index of the run's registry, built once so every edge's path search reuses it.
    adapters: Option<AdapterIndex>,
    /// Transport keys of structured types, computed once per run (see [`Self::transport_key`]).
    keys: RefCell<BTreeMap<TypeExpr, TypeKey>>,
}

impl PlannerCatalog {
    pub(super) fn from_config(config: &PlannerConfig) -> Self {
        let Some(capabilities) = config.transport_capabilities.as_ref() else {
            return Self::default();
        };
        let adapters = Some(capabilities.adapter_index());
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
        Self {
            nodes,
            types,
            adapters,
            keys: RefCell::new(BTreeMap::new()),
        }
    }

    pub(super) fn adapter_index(&self) -> Option<&AdapterIndex> {
        self.adapters.as_ref()
    }

    /// The transport key of `ty`. Opaque names are a copy of the name; a structured type is
    /// normalized and encoded once per run, however many edges it types.
    pub(super) fn transport_key(&self, ty: &TypeExpr) -> TypeKey {
        if let TypeExpr::Opaque(_) = ty {
            return typeexpr_transport_key(ty);
        }
        let mut keys = self.keys.borrow_mut();
        if let Some(key) = keys.get(ty) {
            return key.clone();
        }
        let key = typeexpr_transport_key(ty);
        keys.insert(ty.clone(), key.clone());
        key
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

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{StructField, ValueType};

    #[test]
    fn transport_key_memo_matches_the_direct_key() {
        let catalog = PlannerCatalog::default();
        let structured = TypeExpr::Struct(vec![
            StructField {
                name: "width".to_string(),
                ty: TypeExpr::Scalar(ValueType::Int),
            },
            StructField {
                name: "pixels".to_string(),
                ty: TypeExpr::List(Box::new(TypeExpr::Opaque("pixel".to_string()))),
            },
        ]);
        let opaque = TypeExpr::Opaque("frame".to_string());
        for ty in [&structured, &opaque, &structured] {
            assert_eq!(catalog.transport_key(ty), typeexpr_transport_key(ty));
        }
        assert_eq!(
            catalog.keys.borrow().len(),
            1,
            "only structured types are memoized"
        );
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
