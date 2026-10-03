//! Port type checking and diagnostic generation for planner graphs.

use daedalus_data::model::{TypeExpr, Value};
use std::collections::{BTreeMap, BTreeSet};

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::Graph;
use crate::metadata::{DynamicPortMetadata, is_generic_marker};

use super::{
    PlannerCatalog, diagnostic_node_id, latest_node, port_type, simplify_rust_name, suggest_nodes,
};

pub(super) fn typecheck(graph: &mut Graph, catalog: &PlannerCatalog, diags: &mut Vec<Diagnostic>) {
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct TypeVarKey {
        node: usize,
        is_input: bool,
        port: String,
    }

    #[derive(Clone, Debug)]
    struct Dsu {
        parent: Vec<usize>,
        rank: Vec<u8>,
        binding: Vec<Option<TypeExpr>>,
    }

    impl Dsu {
        fn new() -> Self {
            Self {
                parent: Vec::new(),
                rank: Vec::new(),
                binding: Vec::new(),
            }
        }

        fn make_set(&mut self) -> usize {
            let id = self.parent.len();
            self.parent.push(id);
            self.rank.push(0);
            self.binding.push(None);
            id
        }

        fn find(&mut self, x: usize) -> usize {
            if self.parent[x] != x {
                let p = self.parent[x];
                self.parent[x] = self.find(p);
            }
            self.parent[x]
        }

        fn union(&mut self, a: usize, b: usize) -> Result<usize, (TypeExpr, TypeExpr)> {
            let mut ra = self.find(a);
            let mut rb = self.find(b);
            if ra == rb {
                return Ok(ra);
            }
            if self.rank[ra] < self.rank[rb] {
                std::mem::swap(&mut ra, &mut rb);
            }
            self.parent[rb] = ra;
            if self.rank[ra] == self.rank[rb] {
                self.rank[ra] = self.rank[ra].saturating_add(1);
            }

            match (&self.binding[ra], &self.binding[rb]) {
                (Some(a), Some(b)) if a != b => return Err((a.clone(), b.clone())),
                (None, Some(b)) => self.binding[ra] = Some(b.clone()),
                _ => {}
            }
            Ok(ra)
        }

        fn bind(&mut self, var: usize, ty: TypeExpr) -> Result<(), (TypeExpr, TypeExpr)> {
            let r = self.find(var);
            if let Some(existing) = &self.binding[r] {
                if existing != &ty {
                    return Err((existing.clone(), ty));
                }
                return Ok(());
            }
            self.binding[r] = Some(ty);
            Ok(())
        }

        fn bound_type(&mut self, var: usize) -> Option<TypeExpr> {
            let r = self.find(var);
            self.binding[r].clone()
        }
    }

    fn display_label_for_type(ty: &TypeExpr, lookup: &BTreeMap<TypeExpr, String>) -> String {
        if let Some(found) = lookup.get(ty) {
            return found.clone();
        }

        match ty {
            TypeExpr::Opaque(name) => {
                if name == "image" {
                    return "Image".to_string();
                }
                if let Some(flavor) = name.strip_prefix("image:") {
                    return if flavor.is_empty() {
                        "Image".to_string()
                    } else {
                        format!("Image ({flavor})")
                    };
                }
                if name == "cv:binary_image" {
                    return "Image (binary)".to_string();
                }
                if let Some(raw) = name.strip_prefix("rust:") {
                    return simplify_rust_name(raw);
                }
                name.clone()
            }
            TypeExpr::Scalar(value) => format!("{value:?}"),
            TypeExpr::Optional(inner) => {
                let inner_label = display_label_for_type(inner, lookup);
                format!("{inner_label}?")
            }
            TypeExpr::List(inner) => {
                let inner_label = display_label_for_type(inner, lookup);
                format!("{inner_label}[]")
            }
            TypeExpr::Map(key, value) => {
                let key_label = display_label_for_type(key, lookup);
                let value_label = display_label_for_type(value, lookup);
                format!("map<{key_label}, {value_label}>")
            }
            TypeExpr::Tuple(items) => {
                let parts = items
                    .iter()
                    .map(|item| display_label_for_type(item, lookup))
                    .collect::<Vec<_>>();
                format!("({})", parts.join(", "))
            }
            TypeExpr::Struct(fields) => {
                let mut names = BTreeSet::new();
                for field in fields {
                    names.insert(field.name.trim().to_ascii_lowercase());
                }
                if names.len() == 2 && names.contains("x") && names.contains("y") {
                    return "Point".to_string();
                }
                if names.len() == 4
                    && names.contains("r")
                    && names.contains("g")
                    && names.contains("b")
                    && names.contains("a")
                {
                    return "Pixel".to_string();
                }
                if names.contains("data_b64") && names.contains("width") && names.contains("height")
                {
                    return "Image".to_string();
                }
                "Struct".to_string()
            }
            TypeExpr::Enum(_) => "Enum".to_string(),
        }
    }

    fn label_for_type(ty: &TypeExpr, lookup: &BTreeMap<TypeExpr, String>) -> String {
        display_label_for_type(ty, lookup)
    }

    let mut vars: BTreeMap<TypeVarKey, usize> = BTreeMap::new();
    let mut dsu = Dsu::new();
    let type_label_lookup = catalog.type_label_lookup();

    for edge in &graph.edges {
        let from_node = match graph.nodes.get(edge.from.node.0) {
            Some(n) => n,
            None => continue,
        };
        let to_node = match graph.nodes.get(edge.to.node.0) {
            Some(n) => n,
            None => continue,
        };
        let from_desc = latest_node(catalog, &from_node.id);
        let to_desc = latest_node(catalog, &to_node.id);

        let from_ty = from_desc.and_then(|d| port_type(from_node, d, &edge.from.port, false));
        let to_ty = to_desc.and_then(|d| port_type(to_node, d, &edge.to.port, true));

        if from_desc.is_none() {
            let suggestions = suggest_nodes(catalog, &from_node.id.0);
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::NodeMissing,
                    format!("node {} not found in registry", from_node.id.0),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(from_node))
                .with_meta(
                    "missing_node_id",
                    Value::String(std::borrow::Cow::Owned(from_node.id.0.clone())),
                )
                .with_meta(
                    "suggestions",
                    Value::List(
                        suggestions
                            .into_iter()
                            .map(|s| Value::String(std::borrow::Cow::Owned(s)))
                            .collect(),
                    ),
                ),
            );
            continue;
        }
        if to_desc.is_none() {
            let suggestions = suggest_nodes(catalog, &to_node.id.0);
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::NodeMissing,
                    format!("node {} not found in registry", to_node.id.0),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(to_node))
                .with_meta(
                    "missing_node_id",
                    Value::String(std::borrow::Cow::Owned(to_node.id.0.clone())),
                )
                .with_meta(
                    "suggestions",
                    Value::List(
                        suggestions
                            .into_iter()
                            .map(|s| Value::String(std::borrow::Cow::Owned(s)))
                            .collect(),
                    ),
                ),
            );
            continue;
        }

        if from_ty.is_none() {
            let available: Vec<Value> = from_desc
                .map(|d| {
                    d.outputs
                        .iter()
                        .map(|p| Value::String(std::borrow::Cow::Owned(p.name.clone())))
                        .collect()
                })
                .unwrap_or_default();
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::PortMissing,
                    format!(
                        "output port `{}` not found on node {}",
                        edge.from.port, from_node.id.0
                    ),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(from_node))
                .at_port(edge.from.port.clone())
                .with_meta(
                    "missing_port",
                    Value::String(std::borrow::Cow::Owned(edge.from.port.clone())),
                )
                .with_meta(
                    "missing_port_direction",
                    Value::String(std::borrow::Cow::Borrowed("output")),
                )
                .with_meta("available_ports", Value::List(available)),
            );
        }
        if to_ty.is_none() {
            let available: Vec<Value> = to_desc
                .map(|d| {
                    d.inputs
                        .iter()
                        .map(|p| Value::String(std::borrow::Cow::Owned(p.name.clone())))
                        .collect()
                })
                .unwrap_or_default();
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::PortMissing,
                    format!(
                        "input port `{}` not found on node {}",
                        edge.to.port, to_node.id.0
                    ),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(to_node))
                .at_port(edge.to.port.clone())
                .with_meta(
                    "missing_port",
                    Value::String(std::borrow::Cow::Owned(edge.to.port.clone())),
                )
                .with_meta(
                    "missing_port_direction",
                    Value::String(std::borrow::Cow::Borrowed("input")),
                )
                .with_meta("available_ports", Value::List(available)),
            );
        }

        let (Some(from_ty), Some(to_ty)) = (from_ty, to_ty) else {
            continue;
        };

        // Resolve `Opaque("generic")` as a proper type variable: graph edges constrain it.
        let from_term = if is_generic_marker(&from_ty) {
            let key = TypeVarKey {
                node: edge.from.node.0,
                is_input: false,
                port: edge.from.port.clone(),
            };
            let id = *vars.entry(key).or_insert_with(|| dsu.make_set());
            Some(id)
        } else {
            None
        };
        let to_term = if is_generic_marker(&to_ty) {
            let key = TypeVarKey {
                node: edge.to.node.0,
                is_input: true,
                port: edge.to.port.clone(),
            };
            let id = *vars.entry(key).or_insert_with(|| dsu.make_set());
            Some(id)
        } else {
            None
        };

        let conflict = match (from_term, to_term) {
            (Some(var), None) => dsu.bind(var, to_ty.clone()).err(),
            (None, Some(var)) => dsu.bind(var, from_ty.clone()).err(),
            (Some(a), Some(b)) => dsu.union(a, b).err(),
            (None, None) => None,
        };

        if let Some((a, b)) = conflict {
            let host = if is_generic_marker(&from_ty) {
                from_node
            } else {
                to_node
            };
            let port = if is_generic_marker(&from_ty) {
                edge.from.port.clone()
            } else {
                edge.to.port.clone()
            };
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::TypeMismatch,
                    format!(
                        "generic port `{}` inferred conflicting types: {:?} vs {:?} (edge {}.{} -> {}.{})",
                        port,
                        a,
                        b,
                        from_node.id.0,
                        edge.from.port,
                        to_node.id.0,
                        edge.to.port
                    ),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(host))
                .at_port(port)
                .with_meta(
                    "type_a",
                    Value::String(std::borrow::Cow::Owned(
                        serde_json::to_string(&a).unwrap_or_default(),
                    )),
                )
                .with_meta(
                    "type_b",
                    Value::String(std::borrow::Cow::Owned(
                        serde_json::to_string(&b).unwrap_or_default(),
                    )),
                ),
            );
        }
    }

    let mut dynamic_metadata_by_node: BTreeMap<usize, DynamicPortMetadata> = BTreeMap::new();

    // Apply solved generic types back onto node metadata so later passes/runtime can consume them.
    for (key, var) in vars {
        let Some(ty) = dsu.bound_type(var) else {
            continue;
        };
        let Some(node) = graph.nodes.get(key.node) else {
            continue;
        };
        let dynamic_metadata = dynamic_metadata_by_node
            .entry(key.node)
            .or_insert_with(|| DynamicPortMetadata::from_node_metadata(&node.metadata));
        dynamic_metadata.set_resolved_type(key.is_input, &key.port, ty.clone());
        let label = label_for_type(&ty, &type_label_lookup);
        dynamic_metadata.set_label(key.is_input, &key.port, label);
    }

    for (node_idx, dynamic_metadata) in dynamic_metadata_by_node {
        if let Some(node) = graph.nodes.get_mut(node_idx) {
            dynamic_metadata.write_to_node_metadata(&mut node.metadata);
        }
    }
}
