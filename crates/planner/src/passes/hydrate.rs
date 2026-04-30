use daedalus_data::model::Value;

use crate::diagnostics::{Diagnostic, DiagnosticCode};

use super::{PlannerCatalog, PlannerInput, diagnostic_node_id, suggest_nodes};

pub(super) fn hydrate_registry(
    input: &PlannerInput,
    catalog: &PlannerCatalog,
    diags: &mut Vec<Diagnostic>,
) {
    for node in &input.graph.nodes {
        if catalog.node(&node.id).is_some() {
            continue;
        }

        let suggestions = suggest_nodes(catalog, &node.id.0);
        diags.push(
            Diagnostic::new(
                DiagnosticCode::NodeMissing,
                format!("node {} not found in registry", node.id.0),
            )
            .in_pass("hydrate_registry")
            .at_node(diagnostic_node_id(node))
            .with_meta(
                "missing_node_id",
                Value::String(std::borrow::Cow::Owned(node.id.0.clone())),
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
    }
}
