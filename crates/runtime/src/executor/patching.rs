use crate::plan::RuntimeNode;
use daedalus_planner::{GraphNodeSelector, GraphPatch, GraphPatchOp, PatchReport};

use super::NodeConstInputs;

pub(crate) fn apply_patch_to_const_inputs(
    patch: &GraphPatch,
    nodes: &[RuntimeNode],
    const_inputs: &mut [NodeConstInputs],
) -> PatchReport {
    let mut report = PatchReport::default();
    for op in &patch.ops {
        match op {
            GraphPatchOp::SetNodeConst { node, port, value } => {
                let indices = resolve_runtime_indices(nodes, node);
                if indices.is_empty() {
                    report.skipped_ops += 1;
                    continue;
                }
                let normalized_port = normalize_port(port);
                for idx in indices {
                    if let Some(entry) = const_inputs.get_mut(idx) {
                        apply_const_override(entry, &normalized_port, port, value);
                        report.matched_nodes += 1;
                    }
                }
                report.applied_ops += 1;
            }
            GraphPatchOp::ReplaceNodeId { .. } => {
                report.skipped_ops += 1;
            }
            GraphPatchOp::DeleteNodes { .. } => {
                report.skipped_ops += 1;
            }
        }
    }
    report
}

fn resolve_runtime_indices(nodes: &[RuntimeNode], selector: &GraphNodeSelector) -> Vec<usize> {
    if let Some(index) = selector.index {
        if index < nodes.len() {
            return vec![index];
        }
        return Vec::new();
    }

    if let Some(meta) = selector.metadata.as_ref() {
        let key = meta.key.trim();
        if !key.is_empty() {
            return nodes
                .iter()
                .enumerate()
                .filter_map(|(idx, node)| {
                    node.metadata
                        .get(key)
                        .filter(|value| *value == &meta.value)
                        .map(|_| idx)
                })
                .collect();
        }
    }

    if let Some(id) = selector.id.as_ref() {
        let trimmed = id.trim();
        if !trimmed.is_empty() {
            return nodes
                .iter()
                .enumerate()
                .filter_map(|(idx, node)| (node.id == trimmed).then_some(idx))
                .collect();
        }
    }

    Vec::new()
}

fn normalize_port(port: &str) -> String {
    port.trim().to_ascii_lowercase()
}

fn apply_const_override(
    const_inputs: &mut Vec<(String, daedalus_data::model::Value)>,
    normalized_port: &str,
    port: &str,
    value: &Option<daedalus_data::model::Value>,
) {
    let mut matched = None;
    for (idx, (name, _)) in const_inputs.iter().enumerate() {
        if normalize_port(name) == normalized_port {
            matched = Some(idx);
            break;
        }
    }

    match (matched, value) {
        (Some(idx), Some(next)) => {
            const_inputs[idx] = (const_inputs[idx].0.clone(), next.clone());
        }
        (Some(idx), None) => {
            const_inputs.remove(idx);
        }
        (None, Some(next)) => {
            let key = if port.trim().is_empty() {
                normalized_port.to_string()
            } else {
                port.trim().to_string()
            };
            const_inputs.push((key, next.clone()));
        }
        (None, None) => {}
    }
}
