use daedalus_data::convert::{ConversionProvenance, ConversionResolution, ConverterId};
use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_registry::ids::{GroupId, NodeId};
use daedalus_registry::store::{GroupDescriptor, NodeDescriptor, PortAccessMode};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::NodeInstance;
use crate::graph::{ComputeAffinity, Edge, ExecutionPlan, Graph, NodeRef, PortRef};

const DYNAMIC_INPUT_TYPES_KEY: &str = "dynamic_input_types";
const DYNAMIC_OUTPUT_TYPES_KEY: &str = "dynamic_output_types";
const DYNAMIC_INPUT_LABELS_KEY: &str = "dynamic_input_labels";
const DYNAMIC_OUTPUT_LABELS_KEY: &str = "dynamic_output_labels";
const EMBEDDED_GRAPH_KEY: &str = "daedalus.embedded_graph";
const EMBEDDED_HOST_KEY: &str = "daedalus.embedded_host";
const EMBEDDED_GROUP_KEY: &str = "daedalus.embedded_group";

/// Static planner config controlling optional passes.
///
/// ```
/// use daedalus_planner::PlannerConfig;
/// let cfg = PlannerConfig::default();
/// assert!(!cfg.enable_gpu);
/// ```
#[derive(Clone, Debug, Default)]
pub struct PlannerConfig {
    pub enable_gpu: bool,
    pub enable_lints: bool,
    pub active_features: Vec<String>,
    /// When true, validate `GraphNode.inputs/outputs` strictly against the registry.
    ///
    /// This is intended for UI-persisted graphs where the node port lists are part of the
    /// persisted contract. It is deliberately off by default so "minimal" graphs (that omit
    /// declared ports and rely only on edges) remain valid.
    pub strict_port_declarations: bool,
    #[cfg(feature = "gpu")]
    pub gpu_caps: Option<daedalus_gpu::GpuCapabilities>,
}

/// Input to the planner: a graph plus registry reference.
///
/// ```
/// use daedalus_planner::{PlannerInput, Graph};
/// use daedalus_registry::store::Registry;
/// let registry = Registry::new();
/// let input = PlannerInput { graph: Graph::default(), registry: &registry };
/// assert_eq!(input.graph.nodes.len(), 0);
/// ```
#[derive(Clone, Debug)]
pub struct PlannerInput<'a> {
    pub graph: Graph,
    pub registry: &'a daedalus_registry::store::Registry,
}

/// Planner output: final plan and any diagnostics.
///
/// ```
/// use daedalus_planner::{PlannerOutput, ExecutionPlan, Graph};
/// let out = PlannerOutput { plan: ExecutionPlan::new(Graph::default(), vec![]), diagnostics: vec![] };
/// assert!(out.diagnostics.is_empty());
/// ```
#[derive(Clone, Debug)]
pub struct PlannerOutput {
    pub plan: ExecutionPlan,
    pub diagnostics: Vec<Diagnostic>,
}

fn is_host_bridge(node: &NodeInstance) -> bool {
    matches!(
        node.metadata.get("host_bridge"),
        Some(daedalus_data::model::Value::Bool(true))
    )
}

fn diagnostic_node_id(node: &NodeInstance) -> String {
    const UI_NODE_ID_KEY: &str = "helios.ui.node_id";
    if let Some(daedalus_data::model::Value::String(value)) = node.metadata.get(UI_NODE_ID_KEY) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Some(label) = node.label.as_deref() {
        let trimmed = label.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    node.id.0.clone()
}

fn expand_embedded_graphs(
    input: &mut PlannerInput<'_>,
    view: &daedalus_registry::store::RegistryView,
    diags: &mut Vec<Diagnostic>,
) {
    #[derive(Clone)]
    struct EmbeddedSpec {
        graph: Graph,
        group_id: Option<String>,
        group_label: Option<String>,
        host_label: Option<String>,
    }

    fn parse_embedded(
        raw: &str,
        node_id: &str,
        diags: &mut Vec<Diagnostic>,
    ) -> Option<Graph> {
        match serde_json::from_str::<Graph>(raw) {
            Ok(graph) => Some(graph),
            Err(err) => {
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::NodeMissing,
                        format!("embedded graph parse failed: {err}"),
                    )
                    .in_pass("expand_embedded")
                    .at_node(node_id.to_string()),
                );
                None
            }
        }
    }

    fn suggest_groups(view: &daedalus_registry::store::RegistryView, missing: &str) -> Vec<String> {
        fn edit_distance(a: &str, b: &str) -> usize {
            let mut prev: Vec<usize> = (0..=b.len()).collect();
            let mut curr = vec![0; b.len() + 1];
            for (i, ca) in a.bytes().enumerate() {
                curr[0] = i + 1;
                for (j, cb) in b.bytes().enumerate() {
                    let cost = if ca == cb { 0 } else { 1 };
                    curr[j + 1] = (prev[j + 1] + 1)
                        .min(curr[j] + 1)
                        .min(prev[j] + cost);
                }
                prev.clone_from_slice(&curr);
            }
            prev[b.len()]
        }

        let needle = missing.trim().to_ascii_lowercase();
        if needle.is_empty() {
            return Vec::new();
        }
        let mut scored: Vec<(usize, String)> = view
            .groups
            .keys()
            .map(|id| {
                let id_str = id.0.clone();
                let score = edit_distance(&needle, &id_str.to_ascii_lowercase());
                (score, id_str)
            })
            .collect();
        scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        scored.into_iter().take(5).map(|(_, id)| id).collect()
    }

    let mut embedded_graphs: HashMap<usize, EmbeddedSpec> = HashMap::new();
    for (idx, node) in input.graph.nodes.iter().enumerate() {
        let Some(desc) = latest_node(view, &node.id) else {
            continue;
        };
        if let Some(group_id) = desc.group.as_ref() {
            let Some(group) = latest_group(view, group_id) else {
                let suggestions = suggest_groups(view, &group_id.0);
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::NodeMissing,
                        format!("embedded group {} not found in registry", group_id.0),
                    )
                    .in_pass("expand_embedded")
                    .at_node(diagnostic_node_id(node))
                    .with_meta(
                        "missing_group_id",
                        Value::String(std::borrow::Cow::Owned(group_id.0.clone())),
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
            };
            let Some(graph) = parse_embedded(&group.graph, &diagnostic_node_id(node), diags) else {
                continue;
            };
            let host_label = group
                .metadata
                .get(EMBEDDED_HOST_KEY)
                .and_then(|val| match val {
                    Value::String(s) => {
                        let trimmed = s.trim();
                        (!trimmed.is_empty()).then_some(trimmed.to_string())
                    }
                    _ => None,
                });
            embedded_graphs.insert(
                idx,
                EmbeddedSpec {
                    graph,
                    group_id: Some(group_id.0.clone()),
                    group_label: group.label.clone(),
                    host_label,
                },
            );
            continue;
        }

        if let Some(Value::String(raw)) = desc.metadata.get(EMBEDDED_GRAPH_KEY) {
            if let Some(graph) = parse_embedded(raw.as_ref(), &diagnostic_node_id(node), diags) {
                embedded_graphs.insert(
                    idx,
                    EmbeddedSpec {
                        graph,
                        group_id: None,
                        group_label: None,
                        host_label: None,
                    },
                );
            }
            continue;
        }

    }

    if embedded_graphs.is_empty() {
        return;
    }

    let mut connected_inputs: HashMap<usize, HashSet<String>> = HashMap::new();
    for edge in &input.graph.edges {
        if embedded_graphs.contains_key(&edge.to.node.0) {
            connected_inputs
                .entry(edge.to.node.0)
                .or_default()
                .insert(edge.to.port.clone());
        }
    }

    #[derive(Clone, Debug)]
    struct EmbeddedMap {
        inputs: BTreeMap<String, Vec<PortRef>>,
        outputs: BTreeMap<String, Vec<PortRef>>,
    }

    let mut new_nodes: Vec<NodeInstance> = Vec::new();
    let mut embedded_internal_edges: Vec<Edge> = Vec::new();
    let mut remap: Vec<Option<usize>> = vec![None; input.graph.nodes.len()];
    let mut embedded_maps: HashMap<usize, EmbeddedMap> = HashMap::new();

    for (idx, node) in input.graph.nodes.iter().enumerate() {
        let Some(spec) = embedded_graphs.get(&idx) else {
            let new_idx = new_nodes.len();
            new_nodes.push(node.clone());
            remap[idx] = Some(new_idx);
            continue;
        };
        let graph = &spec.graph;

        let host_index = graph.nodes.iter().position(is_host_bridge).or_else(|| {
            let host_label = latest_node(view, &node.id)
                .and_then(|desc| desc.metadata.get(EMBEDDED_HOST_KEY))
                .and_then(|val| match val {
                    Value::String(s) => {
                        let trimmed = s.trim();
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        }
                    }
                    _ => None,
                });
            let host_label = host_label.or_else(|| spec.host_label.clone());
            host_label.and_then(|label| {
                graph
                    .nodes
                    .iter()
                    .position(|n| n.label.as_deref() == Some(label.as_str()))
            })
        });

        let Some(host_index) = host_index else {
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::NodeMissing,
                    "embedded graph missing host bridge".to_string(),
                )
                .in_pass("expand_embedded")
                .at_node(diagnostic_node_id(node)),
            );
            let new_idx = new_nodes.len();
            new_nodes.push(node.clone());
            remap[idx] = Some(new_idx);
            continue;
        };

        let group_label = node
            .label
            .clone()
            .or_else(|| latest_node(view, &node.id).and_then(|desc| desc.label.clone()))
            .or_else(|| spec.group_label.clone())
            .unwrap_or_else(|| node.id.0.clone());
        let group_id = spec.group_id.clone().unwrap_or_else(|| group_label.clone());
        let prefix = format!("{group_label}::");
        let mut index_map: Vec<Option<usize>> = vec![None; graph.nodes.len()];

        for (g_idx, g_node) in graph.nodes.iter().enumerate() {
            if g_idx == host_index {
                continue;
            }
            let mut cloned = g_node.clone();
            let base_label = cloned
                .label
                .clone()
                .unwrap_or_else(|| cloned.id.0.clone());
            cloned.label = Some(format!("{prefix}{base_label}"));
            cloned.metadata.insert(
                EMBEDDED_GROUP_KEY.to_string(),
                Value::String(std::borrow::Cow::from(group_label.clone())),
            );
            cloned.metadata.insert(
                "daedalus.group_id".to_string(),
                Value::String(std::borrow::Cow::from(group_id.clone())),
            );
            cloned.metadata.insert(
                "daedalus.group_label".to_string(),
                Value::String(std::borrow::Cow::from(group_label.clone())),
            );
            let new_idx = new_nodes.len();
            new_nodes.push(cloned);
            index_map[g_idx] = Some(new_idx);
        }

        let mut inputs: BTreeMap<String, Vec<PortRef>> = BTreeMap::new();
        let mut outputs: BTreeMap<String, Vec<PortRef>> = BTreeMap::new();

        for edge in &graph.edges {
            let from_is_host = edge.from.node.0 == host_index;
            let to_is_host = edge.to.node.0 == host_index;

            match (from_is_host, to_is_host) {
                (true, false) => {
                    if let Some(target_idx) = index_map[edge.to.node.0] {
                        inputs
                            .entry(edge.from.port.clone())
                            .or_default()
                            .push(PortRef {
                                node: NodeRef(target_idx),
                                port: edge.to.port.clone(),
                            });
                    }
                }
                (false, true) => {
                    if let Some(source_idx) = index_map[edge.from.node.0] {
                        outputs
                            .entry(edge.to.port.clone())
                            .or_default()
                            .push(PortRef {
                                node: NodeRef(source_idx),
                                port: edge.from.port.clone(),
                            });
                    }
                }
                (false, false) => {
                    let Some(from_idx) = index_map[edge.from.node.0] else {
                        continue;
                    };
                    let Some(to_idx) = index_map[edge.to.node.0] else {
                        continue;
                    };
                    embedded_internal_edges.push(Edge {
                        from: PortRef {
                            node: NodeRef(from_idx),
                            port: edge.from.port.clone(),
                        },
                        to: PortRef {
                            node: NodeRef(to_idx),
                            port: edge.to.port.clone(),
                        },
                        metadata: edge.metadata.clone(),
                    });
                }
                (true, true) => {}
            }
        }

        embedded_maps.insert(
            idx,
            EmbeddedMap {
                inputs,
                outputs,
            },
        );
    }

    let mut new_edges: Vec<Edge> = Vec::new();
    for edge in &input.graph.edges {
        let from_map = embedded_maps.get(&edge.from.node.0);
        let to_map = embedded_maps.get(&edge.to.node.0);

        match (from_map, to_map) {
            (None, None) => {
                let Some(from_idx) = remap[edge.from.node.0] else {
                    continue;
                };
                let Some(to_idx) = remap[edge.to.node.0] else {
                    continue;
                };
                new_edges.push(Edge {
                    from: PortRef {
                        node: NodeRef(from_idx),
                        port: edge.from.port.clone(),
                    },
                    to: PortRef {
                        node: NodeRef(to_idx),
                        port: edge.to.port.clone(),
                    },
                    metadata: edge.metadata.clone(),
                });
            }
            (None, Some(to)) => {
                let Some(from_idx) = remap[edge.from.node.0] else {
                    continue;
                };
                if let Some(targets) = to.inputs.get(&edge.to.port) {
                    for target in targets {
                        new_edges.push(Edge {
                            from: PortRef {
                                node: NodeRef(from_idx),
                                port: edge.from.port.clone(),
                            },
                            to: target.clone(),
                            metadata: edge.metadata.clone(),
                        });
                    }
                }
            }
            (Some(from), None) => {
                let Some(to_idx) = remap[edge.to.node.0] else {
                    continue;
                };
                if let Some(sources) = from.outputs.get(&edge.from.port) {
                    for source in sources {
                        new_edges.push(Edge {
                            from: source.clone(),
                            to: PortRef {
                                node: NodeRef(to_idx),
                                port: edge.to.port.clone(),
                            },
                            metadata: edge.metadata.clone(),
                        });
                    }
                }
            }
            (Some(from), Some(to)) => {
                let sources = from.outputs.get(&edge.from.port);
                let targets = to.inputs.get(&edge.to.port);
                if let (Some(sources), Some(targets)) = (sources, targets) {
                    for source in sources {
                        for target in targets {
                            new_edges.push(Edge {
                                from: source.clone(),
                                to: target.clone(),
                                metadata: edge.metadata.clone(),
                            });
                        }
                    }
                }
            }
        }
    }

    // Apply const inputs from embedded nodes when there is no incoming edge.
    for (idx, node) in input.graph.nodes.iter().enumerate() {
        let Some(map) = embedded_maps.get(&idx) else {
            continue;
        };
        let connected = connected_inputs.get(&idx);
        for (port, value) in &node.const_inputs {
            if connected
                .map(|set| set.contains(port))
                .unwrap_or(false)
            {
                continue;
            }
            if let Some(targets) = map.inputs.get(port) {
                for target in targets {
                    if let Some(inner) = new_nodes.get_mut(target.node.0) {
                        inner.const_inputs.retain(|(name, _)| name != &target.port);
                        inner.const_inputs.push((target.port.clone(), value.clone()));
                    }
                }
            }
        }
    }

    new_edges.extend(embedded_internal_edges);

    input.graph.nodes = new_nodes;
    input.graph.edges = new_edges;
}

fn apply_descriptor_defaults(graph: &mut Graph, view: &daedalus_registry::store::RegistryView) {
    for node in &mut graph.nodes {
        let Some(desc) = latest_node(view, &node.id) else {
            continue;
        };
        for port in &desc.inputs {
            let Some(value) = &port.const_value else {
                continue;
            };
            if node.const_inputs.iter().any(|(name, _)| name == &port.name) {
                continue;
            }
            node.const_inputs.push((port.name.clone(), value.clone()));
        }
    }
}

/// Build an execution plan by running the ordered pass pipeline.
/// Currently stubs; contracts are enforced via deterministic diagnostics ordering.
/// Build an execution plan from a graph and registry.
///
/// ```
/// use daedalus_planner::{build_plan, PlannerConfig, PlannerInput, Graph};
/// use daedalus_registry::store::Registry;
/// let registry = Registry::new();
/// let out = build_plan(PlannerInput { graph: Graph::default(), registry: &registry }, PlannerConfig::default());
/// assert_eq!(out.plan.graph.nodes.len(), 0);
/// ```
pub fn build_plan(mut input: PlannerInput<'_>, config: PlannerConfig) -> PlannerOutput {
    let mut diags = Vec::new();
    let view = input.registry.view();

    // Security/integrity: clients can attach arbitrary node metadata in Graph JSON. These keys are
    // planner-owned and must not be accepted as inputs, otherwise a client can "force" types.
    for node in &mut input.graph.nodes {
        node.metadata.remove(DYNAMIC_INPUT_TYPES_KEY);
        node.metadata.remove(DYNAMIC_OUTPUT_TYPES_KEY);
        node.metadata.remove(DYNAMIC_INPUT_LABELS_KEY);
        node.metadata.remove(DYNAMIC_OUTPUT_LABELS_KEY);
        node.metadata.remove("dynamic_inputs");
        node.metadata.remove("dynamic_outputs");
    }

    // Placeholder passes; extend with real logic per PLAN.md.
    expand_embedded_graphs(&mut input, &view, &mut diags);
    apply_descriptor_defaults(&mut input.graph, &view);
    hydrate_registry(&input, &view, &mut diags);
    validate_port_declarations(
        &input.graph,
        &view,
        &mut diags,
        config.strict_port_declarations,
    );
    typecheck(&mut input.graph, &view, &mut diags);
    convert(&mut input.graph, input.registry, &view, &mut diags, &config);
    align(&mut input.graph, &mut diags);
    gpu(&mut input.graph, &config, &mut diags);
    schedule(&mut input.graph, &mut diags);
    if config.enable_lints {
        lint(&input, &mut diags);
    }

    let plan = ExecutionPlan::new(input.graph.clone(), diags.clone());
    PlannerOutput {
        plan,
        diagnostics: diags,
    }
}

fn validate_port_declarations(
    graph: &Graph,
    view: &daedalus_registry::store::RegistryView,
    diags: &mut Vec<Diagnostic>,
    strict_port_declarations: bool,
) {
    fn is_dynamic(desc: &NodeDescriptor, is_input: bool) -> bool {
        let key = if is_input {
            "dynamic_inputs"
        } else {
            "dynamic_outputs"
        };
        matches!(desc.metadata.get(key), Some(Value::String(s)) if !s.trim().is_empty())
    }

    fn fanin_hints(desc: &NodeDescriptor) -> Vec<String> {
        desc.fanin_inputs
            .iter()
            .map(|spec| format!("{}{}+", spec.prefix, spec.start))
            .collect()
    }

    fn available_inputs(desc: &NodeDescriptor) -> Vec<Value> {
        let mut out: Vec<Value> = desc
            .inputs
            .iter()
            .map(|p| Value::String(std::borrow::Cow::Owned(p.name.clone())))
            .collect();
        for hint in fanin_hints(desc) {
            out.push(Value::String(std::borrow::Cow::Owned(hint)));
        }
        out
    }

    fn available_outputs(desc: &NodeDescriptor) -> Vec<Value> {
        desc.outputs
            .iter()
            .map(|p| Value::String(std::borrow::Cow::Owned(p.name.clone())))
            .collect()
    }

    for node in &graph.nodes {
        let Some(desc) = latest_node(view, &node.id) else {
            continue;
        };

        let node_label = diagnostic_node_id(node);

        // Inputs: stale graph can carry extra/missing port entries even when there are no edges.
        let dynamic_inputs = is_dynamic(desc, true);
        let mut seen_inputs: HashSet<String> = HashSet::new();
        for port in &node.inputs {
            let port_lc = port.trim().to_ascii_lowercase();
            if port_lc.is_empty() {
                continue;
            }
            if !seen_inputs.insert(port_lc.clone()) {
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortMissing,
                        format!(
                            "graph declares duplicate input port `{}` on node {}",
                            port, node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(node_label.clone())
                    .at_port(port.clone())
                    .with_meta(
                        "missing_port",
                        Value::String(std::borrow::Cow::Owned(port.clone())),
                    )
                    .with_meta(
                        "missing_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("input")),
                    )
                    .with_meta("available_ports", Value::List(available_inputs(desc))),
                );
                continue;
            }

            if dynamic_inputs {
                continue;
            }
            if desc.input_ty_for(port).is_some() {
                continue;
            }

            diags.push(
                Diagnostic::new(
                    DiagnosticCode::PortMissing,
                    format!(
                        "graph declares input port `{}` on node {}, but the registry descriptor does not provide that port",
                        port, node.id.0
                    ),
                )
                .in_pass("validate_ports")
                .at_node(node_label.clone())
                .at_port(port.clone())
                .with_meta(
                    "missing_port",
                    Value::String(std::borrow::Cow::Owned(port.clone())),
                )
                .with_meta(
                    "missing_port_direction",
                    Value::String(std::borrow::Cow::Borrowed("input")),
                )
                .with_meta("available_ports", Value::List(available_inputs(desc))),
            );
        }

        // Validate missing ports when the graph declares port lists (normal UI-persisted graphs),
        // or when strict mode is enabled.
        if !dynamic_inputs && (strict_port_declarations || !node.inputs.is_empty()) {
            let node_inputs_lc: HashSet<String> = node
                .inputs
                .iter()
                .map(|p| p.trim().to_ascii_lowercase())
                .filter(|p| !p.is_empty())
                .collect();
            for port in &desc.inputs {
                let port_lc = port.name.trim().to_ascii_lowercase();
                if port_lc.is_empty() {
                    continue;
                }
                if node_inputs_lc.contains(&port_lc) {
                    continue;
                }
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortMissing,
                        format!(
                            "graph is missing input port `{}` on node {} (graph is stale; regenerate ports from registry)",
                            port.name, node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(node_label.clone())
                    .at_port(port.name.clone())
                    .with_meta(
                        "missing_port",
                        Value::String(std::borrow::Cow::Owned(port.name.clone())),
                    )
                    .with_meta(
                        "missing_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("input")),
                    )
                    .with_meta("available_ports", Value::List(available_inputs(desc))),
                );
            }
        }

        // Outputs: same story.
        let dynamic_outputs = is_dynamic(desc, false);
        let mut seen_outputs: HashSet<String> = HashSet::new();
        for port in &node.outputs {
            let port_lc = port.trim().to_ascii_lowercase();
            if port_lc.is_empty() {
                continue;
            }
            if !seen_outputs.insert(port_lc.clone()) {
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortMissing,
                        format!(
                            "graph declares duplicate output port `{}` on node {}",
                            port, node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(node_label.clone())
                    .at_port(port.clone())
                    .with_meta(
                        "missing_port",
                        Value::String(std::borrow::Cow::Owned(port.clone())),
                    )
                    .with_meta(
                        "missing_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("output")),
                    )
                    .with_meta("available_ports", Value::List(available_outputs(desc))),
                );
                continue;
            }

            if dynamic_outputs {
                continue;
            }
            if desc.outputs.iter().any(|p| p.name == *port) {
                continue;
            }

            diags.push(
                Diagnostic::new(
                    DiagnosticCode::PortMissing,
                    format!(
                        "graph declares output port `{}` on node {}, but the registry descriptor does not provide that port",
                        port, node.id.0
                    ),
                )
                .in_pass("validate_ports")
                .at_node(node_label.clone())
                .at_port(port.clone())
                .with_meta(
                    "missing_port",
                    Value::String(std::borrow::Cow::Owned(port.clone())),
                )
                .with_meta(
                    "missing_port_direction",
                    Value::String(std::borrow::Cow::Borrowed("output")),
                )
                .with_meta("available_ports", Value::List(available_outputs(desc))),
            );
        }

        if !dynamic_outputs && (strict_port_declarations || !node.outputs.is_empty()) {
            let node_outputs_lc: HashSet<String> = node
                .outputs
                .iter()
                .map(|p| p.trim().to_ascii_lowercase())
                .filter(|p| !p.is_empty())
                .collect();
            for port in &desc.outputs {
                let port_lc = port.name.trim().to_ascii_lowercase();
                if port_lc.is_empty() {
                    continue;
                }
                if node_outputs_lc.contains(&port_lc) {
                    continue;
                }
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortMissing,
                        format!(
                            "graph is missing output port `{}` on node {} (graph is stale; regenerate ports from registry)",
                            port.name, node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(node_label.clone())
                    .at_port(port.name.clone())
                    .with_meta(
                        "missing_port",
                        Value::String(std::borrow::Cow::Owned(port.name.clone())),
                    )
                    .with_meta(
                        "missing_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("output")),
                    )
                    .with_meta("available_ports", Value::List(available_outputs(desc))),
                );
            }
        }
    }
}

fn latest_node<'a>(
    view: &'a daedalus_registry::store::RegistryView,
    id: &NodeId,
) -> Option<&'a NodeDescriptor> {
    view.nodes.get(id)
}

fn latest_group<'a>(
    view: &'a daedalus_registry::store::RegistryView,
    id: &GroupId,
) -> Option<&'a GroupDescriptor> {
    view.groups.get(id)
}

fn suggest_nodes(view: &daedalus_registry::store::RegistryView, missing: &str) -> Vec<String> {
    fn edit_distance(a: &str, b: &str) -> usize {
        let (a, b) = (a.as_bytes(), b.as_bytes());
        if a.is_empty() {
            return b.len();
        }
        if b.is_empty() {
            return a.len();
        }
        let mut prev: Vec<usize> = (0..=b.len()).collect();
        let mut curr = vec![0; b.len() + 1];
        for (i, &ac) in a.iter().enumerate() {
            curr[0] = i + 1;
            for (j, &bc) in b.iter().enumerate() {
                let cost = if ac == bc { 0 } else { 1 };
                curr[j + 1] = (prev[j + 1] + 1)
                    .min(curr[j] + 1)
                    .min(prev[j] + cost);
            }
            prev.clone_from_slice(&curr);
        }
        prev[b.len()]
    }

    let needle = missing.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return Vec::new();
    }
    let mut scored: Vec<(usize, String)> = view
        .nodes
        .keys()
        .map(|id| {
            let id_str = id.0.clone();
            let score = edit_distance(&needle, &id_str.to_ascii_lowercase());
            (score, id_str)
        })
        .collect();
    scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.into_iter().take(5).map(|(_, id)| id).collect()
}

fn hydrate_registry(
    input: &PlannerInput<'_>,
    view: &daedalus_registry::store::RegistryView,
    diags: &mut Vec<Diagnostic>,
) {
    for node in &input.graph.nodes {
        if latest_node(view, &node.id).is_none() {
            let suggestions = suggest_nodes(view, &node.id.0);
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
}

fn typecheck(
    graph: &mut Graph,
    view: &daedalus_registry::store::RegistryView,
    diags: &mut Vec<Diagnostic>,
) {
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

    fn is_generic_marker(ty: &TypeExpr) -> bool {
        matches!(ty, TypeExpr::Opaque(value) if value.eq_ignore_ascii_case("generic"))
    }

    fn upsert_string_map(meta: &mut BTreeMap<String, Value>, key: &str, port: &str, value: String) {
        let entry = meta
            .entry(key.to_string())
            .or_insert_with(|| Value::Map(Vec::new()));
        if !matches!(entry, Value::Map(_)) {
            *entry = Value::Map(Vec::new());
        }
        let Value::Map(entries) = entry else { return };
        let port_lc = port.to_ascii_lowercase();
        let mut replaced = false;
        for (k, v) in entries.iter_mut() {
            if matches!(k, Value::String(s) if s.eq_ignore_ascii_case(&port_lc)) {
                *v = Value::String(std::borrow::Cow::Owned(value.clone()));
                replaced = true;
                break;
            }
        }
        if !replaced {
            entries.push((
                Value::String(std::borrow::Cow::Owned(port_lc)),
                Value::String(std::borrow::Cow::Owned(value)),
            ));
        }
        entries.sort_by(|(ak, _), (bk, _)| {
            let a = match ak {
                Value::String(s) => s.as_ref(),
                _ => "",
            };
            let b = match bk {
                Value::String(s) => s.as_ref(),
                _ => "",
            };
            a.cmp(b)
        });
    }

    fn build_type_label_lookup(
        view: &daedalus_registry::store::RegistryView,
    ) -> BTreeMap<TypeExpr, String> {
        let mut out = BTreeMap::new();
        for desc in view.values.values() {
            let Some(ty) = desc.type_expr.clone() else {
                continue;
            };
            let label = desc
                .label
                .as_ref()
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .map(|v| v.to_string())
                .unwrap_or_else(|| desc.id.0.clone());
            out.entry(ty).or_insert(label);
        }
        out
    }

    fn simplify_rust_name(raw: &str) -> String {
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

    fn display_label_for_type(
        ty: &TypeExpr,
        lookup: &BTreeMap<TypeExpr, String>,
    ) -> String {
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
                if names.len() == 4 && names.contains("r") && names.contains("g") && names.contains("b") && names.contains("a") {
                    return "Pixel".to_string();
                }
                if names.contains("data_b64") && names.contains("width") && names.contains("height") {
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
    let type_label_lookup = build_type_label_lookup(view);

    for edge in &graph.edges {
        let from_node = match graph.nodes.get(edge.from.node.0) {
            Some(n) => n,
            None => continue,
        };
        let to_node = match graph.nodes.get(edge.to.node.0) {
            Some(n) => n,
            None => continue,
        };
        let from_desc = latest_node(view, &from_node.id);
        let to_desc = latest_node(view, &to_node.id);

        let from_ty = from_desc.and_then(|d| port_type(from_node, d, &edge.from.port, false));
        let to_ty = to_desc.and_then(|d| port_type(to_node, d, &edge.to.port, true));

        if from_desc.is_none() {
            let suggestions = suggest_nodes(view, &from_node.id.0);
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
            let suggestions = suggest_nodes(view, &to_node.id.0);
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

    // Apply solved generic types back onto node metadata so later passes/runtime can consume them.
    for (key, var) in vars {
        let Some(ty) = dsu.bound_type(var) else {
            continue;
        };
        let Some(node) = graph.nodes.get_mut(key.node) else {
            continue;
        };
        let json = match serde_json::to_string(&ty) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let meta_key = if key.is_input {
            DYNAMIC_INPUT_TYPES_KEY
        } else {
            DYNAMIC_OUTPUT_TYPES_KEY
        };
        upsert_string_map(&mut node.metadata, meta_key, &key.port, json);

        let label_key = if key.is_input {
            DYNAMIC_INPUT_LABELS_KEY
        } else {
            DYNAMIC_OUTPUT_LABELS_KEY
        };
        let label = label_for_type(&ty, &type_label_lookup);
        upsert_string_map(&mut node.metadata, label_key, &key.port, label);
    }
}

fn convert(
    graph: &mut Graph,
    registry: &daedalus_registry::store::Registry,
    view: &daedalus_registry::store::RegistryView,
    diags: &mut Vec<Diagnostic>,
    config: &PlannerConfig,
) {
    fn collect_structural_steps(
        registry: &daedalus_registry::store::Registry,
        from: &TypeExpr,
        to: &TypeExpr,
        active_features: &[String],
        allow_gpu: bool,
        steps: &mut BTreeSet<ConverterId>,
        depth: usize,
    ) -> bool {
        if from == to {
            return true;
        }
        if depth > 64 {
            return false;
        }
        if let Ok(res) =
            registry.resolve_converter_with_context(from, to, active_features, allow_gpu)
        {
            for step in res.provenance.steps {
                steps.insert(step);
            }
            return true;
        }
        match (from, to) {
            (
                TypeExpr::Scalar(ValueType::I32 | ValueType::U32),
                TypeExpr::Scalar(ValueType::Int),
            )
            | (
                TypeExpr::Scalar(ValueType::Int),
                TypeExpr::Scalar(ValueType::I32 | ValueType::U32),
            )
            | (TypeExpr::Scalar(ValueType::F32), TypeExpr::Scalar(ValueType::Float))
            | (TypeExpr::Scalar(ValueType::Float), TypeExpr::Scalar(ValueType::F32)) => true,
            (TypeExpr::Optional(a), TypeExpr::Optional(b)) => collect_structural_steps(
                registry,
                a,
                b,
                active_features,
                allow_gpu,
                steps,
                depth + 1,
            ),
            (TypeExpr::List(a), TypeExpr::List(b)) => collect_structural_steps(
                registry,
                a,
                b,
                active_features,
                allow_gpu,
                steps,
                depth + 1,
            ),
            (TypeExpr::Map(ak, av), TypeExpr::Map(bk, bv)) => {
                collect_structural_steps(
                    registry,
                    ak,
                    bk,
                    active_features,
                    allow_gpu,
                    steps,
                    depth + 1,
                ) && collect_structural_steps(
                    registry,
                    av,
                    bv,
                    active_features,
                    allow_gpu,
                    steps,
                    depth + 1,
                )
            }
            (TypeExpr::Tuple(a), TypeExpr::Tuple(b)) => {
                if a.len() != b.len() {
                    return false;
                }
                a.iter().zip(b.iter()).all(|(ai, bi)| {
                    collect_structural_steps(
                        registry,
                        ai,
                        bi,
                        active_features,
                        allow_gpu,
                        steps,
                        depth + 1,
                    )
                })
            }
            (TypeExpr::Struct(a_fields), TypeExpr::Struct(b_fields)) => {
                if a_fields.len() != b_fields.len() {
                    return false;
                }
                for bf in b_fields {
                    let Some(af) = a_fields.iter().find(|af| af.name == bf.name) else {
                        return false;
                    };
                    if !collect_structural_steps(
                        registry,
                        &af.ty,
                        &bf.ty,
                        active_features,
                        allow_gpu,
                        steps,
                        depth + 1,
                    ) {
                        return false;
                    }
                }
                true
            }
            (TypeExpr::Enum(a_vars), TypeExpr::Enum(b_vars)) => {
                if a_vars.len() != b_vars.len() {
                    return false;
                }
                for bv in b_vars {
                    let Some(av) = a_vars.iter().find(|av| av.name == bv.name) else {
                        return false;
                    };
                    match (&av.ty, &bv.ty) {
                        (None, None) => {}
                        (Some(at), Some(bt)) => {
                            if !collect_structural_steps(
                                registry,
                                at,
                                bt,
                                active_features,
                                allow_gpu,
                                steps,
                                depth + 1,
                            ) {
                                return false;
                            }
                        }
                        _ => return false,
                    }
                }
                true
            }
            _ => false,
        }
    }

    fn resolve_structural(
        registry: &daedalus_registry::store::Registry,
        from: &TypeExpr,
        to: &TypeExpr,
        active_features: &[String],
        allow_gpu: bool,
    ) -> Option<Vec<ConverterId>> {
        let mut steps = BTreeSet::new();
        if !collect_structural_steps(
            registry,
            from,
            to,
            active_features,
            allow_gpu,
            &mut steps,
            0,
        ) {
            return None;
        }
        Some(steps.into_iter().collect())
    }

    let mut converter_metadata: Vec<(String, String)> = Vec::new();
    for edge in &graph.edges {
        let from_node = match graph.nodes.get(edge.from.node.0) {
            Some(n) => n,
            None => continue,
        };
        let to_node = match graph.nodes.get(edge.to.node.0) {
            Some(n) => n,
            None => continue,
        };
        let from_desc = latest_node(view, &from_node.id);
        let to_desc = latest_node(view, &to_node.id);
        let from_ty = from_desc.and_then(|d| port_type(from_node, d, &edge.from.port, false));
        let to_ty = to_desc.and_then(|d| port_type(to_node, d, &edge.to.port, true));
        let (Some(out_ty), Some(in_ty)) = (from_ty, to_ty) else {
            continue;
        };
        if out_ty == in_ty {
            continue;
        }
        let allow_gpu = config.enable_gpu;
        let features: Vec<String> = config.active_features.clone();
        let result: Result<ConversionResolution, ()> =
            resolve_structural(registry, &out_ty, &in_ty, &features, allow_gpu)
                .map(|steps| ConversionResolution {
                    provenance: ConversionProvenance {
                        steps,
                        total_cost: 0,
                        skipped_cycles: vec![],
                        skipped_gpu: vec![],
                        skipped_features: vec![],
                    },
                })
                .ok_or(());
        match result {
            Ok(res) => {
                let mut feats = features.clone();
                feats.sort();
                let feats_str = if feats.is_empty() {
                    "none".to_string()
                } else {
                    feats.join(",")
                };
                let key = format!(
                    "converter:{}->{}:{}->{}",
                    from_node.id.0, to_node.id.0, edge.from.port, edge.to.port
                );
                let mut path: Vec<String> =
                    res.provenance.steps.iter().map(|c| c.0.clone()).collect();
                if path.is_empty() {
                    path.push("identity".into());
                }
                let value = format!(
                    "cost={};path={};features={};gpu={};skipped_gpu={:?};skipped_features={:?}",
                    res.provenance.total_cost,
                    path.join(">"),
                    feats_str,
                    allow_gpu,
                    res.provenance.skipped_gpu,
                    res.provenance.skipped_features
                );
                converter_metadata.push((key, value));
            }
            Err(_) => {
                let mut feats = features.clone();
                feats.sort();
                let feats_str = if feats.is_empty() {
                    "none".to_string()
                } else {
                    feats.join(",")
                };
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::ConverterMissing,
                        format!(
                            "no converter from {:?} to {:?} for edge {}.{} -> {}.{} [features: {}; gpu: {}]",
                            out_ty,
                            in_ty,
                            from_node.id.0,
                            edge.from.port,
                            to_node.id.0,
                            edge.to.port,
                            feats_str,
                            allow_gpu
                        ),
                    )
                    .in_pass("convert")
                    .at_node(diagnostic_node_id(to_node))
                    .at_port(edge.to.port.clone()),
                );
            }
        }
    }
    converter_metadata.sort_by(|a, b| a.0.cmp(&b.0));
    for (k, v) in converter_metadata {
        graph.metadata.insert(k, Value::String(v.into()));
    }
}
fn align(graph: &mut Graph, diags: &mut Vec<Diagnostic>) {
    // Kahn topo sort to detect cycles and emit ordering metadata.
    let n = graph.nodes.len();
    let mut indegree = vec![0usize; n];
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for edge in &graph.edges {
        if edge.from.node.0 < n
            && edge.to.node.0 < n
            && !is_host_bridge(&graph.nodes[edge.from.node.0])
            && !is_host_bridge(&graph.nodes[edge.to.node.0])
        {
            adj[edge.from.node.0].push(edge.to.node.0);
            indegree[edge.to.node.0] += 1;
        }
    }
    let mut queue: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter(|(_, d)| **d == 0)
        .map(|(i, _)| i)
        .collect();
    let mut order = Vec::new();
    while let Some(v) = queue.pop() {
        order.push(v);
        for &nxt in &adj[v] {
            indegree[nxt] -= 1;
            if indegree[nxt] == 0 {
                queue.push(nxt);
            }
        }
    }
    if order.len() != n {
        // Cycle: collect nodes with indegree > 0 for deterministic message.
        let mut cyc_nodes: Vec<String> = indegree
            .iter()
            .enumerate()
            .filter(|(_, d)| **d > 0)
            .map(|(i, _)| graph.nodes[i].id.0.clone())
            .collect();
        cyc_nodes.sort();
        diags.push(
            Diagnostic::new(
                DiagnosticCode::ScheduleConflict,
                format!(
                    "graph contains a cycle involving nodes: {}",
                    cyc_nodes.join(",")
                ),
            )
            .in_pass("align"),
        );
    } else {
        let value = order
            .iter()
            .map(|&idx| graph.nodes[idx].id.0.clone())
            .collect::<Vec<_>>()
            .join(",");
        graph
            .metadata
            .insert("topo_order".into(), Value::String(value.into()));
    }
}
fn gpu(graph: &mut Graph, config: &PlannerConfig, diags: &mut Vec<Diagnostic>) {
    let mut gpu_reasons: Vec<String> = Vec::new();
    // If GPU is disabled, flag required nodes.
    if !config.enable_gpu {
        gpu_reasons.push("gpu-disabled".into());
        let mut gpu_nodes: Vec<String> = Vec::new();
        for node in &graph.nodes {
            if matches!(node.compute, ComputeAffinity::GpuRequired) {
                gpu_nodes.push(node.id.0.clone());
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::GpuUnsupported,
                        format!("node {} requires GPU but GPU is disabled", node.id.0),
                    )
                    .in_pass("gpu")
                    .at_node(diagnostic_node_id(node)),
                );
            }
        }
        if !gpu_nodes.is_empty() {
            graph.metadata.insert(
                "gpu_segments".into(),
                Value::String(gpu_nodes.join(",").into()),
            );
            graph.metadata.insert(
                "gpu_why".into(),
                Value::String(gpu_reasons.join(",").into()),
            );
        }
        return;
    }

    // If caps are provided, validate support.
    #[cfg(feature = "gpu")]
    if let Some(caps) = &config.gpu_caps {
        let require_format = daedalus_gpu::GpuFormat::Rgba8Unorm;
        let mut ok = true;
        let has_format = caps
            .format_features
            .iter()
            .find(|f| f.format == require_format && f.sampleable);
        if caps.queue_count == 0 || !caps.has_transfer_queue {
            ok = false;
        }
        if has_format.is_none() {
            ok = false;
        }
        if !ok {
            gpu_reasons.push(format!(
                "insufficient-caps:queues={} transfer={} format_sampleable={}",
                caps.queue_count,
                caps.has_transfer_queue,
                has_format.is_some()
            ));
            for node in &graph.nodes {
                if matches!(
                    node.compute,
                    ComputeAffinity::GpuRequired | ComputeAffinity::GpuPreferred
                ) {
                    diags.push(
                        Diagnostic::new(
                            DiagnosticCode::GpuUnsupported,
                            format!(
                                "node {} cannot run on GPU: insufficient caps (queues={}, transfer={}, format={:?} sampleable={})",
                                node.id.0,
                                caps.queue_count,
                                caps.has_transfer_queue,
                                require_format,
                                has_format.is_some()
                            ),
                        )
                        .in_pass("gpu")
                        .at_node(diagnostic_node_id(node)),
                    );
                }
            }
        }
    }

    // Record GPU segments metadata (simple grouping of contiguous GPU-pref/required nodes).
    let mut segments: Vec<Vec<String>> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    for node in &graph.nodes {
        match node.compute {
            ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired => {
                current.push(node.id.0.clone());
            }
            ComputeAffinity::CpuOnly => {
                if !current.is_empty() {
                    segments.push(current);
                    current = Vec::new();
                }
            }
        }
    }
    if !current.is_empty() {
        segments.push(current);
    }
    if !segments.is_empty() {
        let seg_strs: Vec<String> = segments.into_iter().map(|seg| seg.join("->")).collect();
        graph.metadata.insert(
            "gpu_segments".into(),
            Value::String(seg_strs.join("|").into()),
        );
    }
    if !gpu_reasons.is_empty() {
        gpu_reasons.sort();
        gpu_reasons.dedup();
        graph
            .metadata
            .insert("gpu_why".into(), Value::String(gpu_reasons.join(";").into()));
    }
}
fn schedule(graph: &mut Graph, _diags: &mut Vec<Diagnostic>) {
    // If topo_order exists, use it; else declared order. Attach basic priority info.
    let order = graph
        .metadata
        .get("topo_order")
        .and_then(|value| match value {
            Value::String(s) => Some(s.to_string()),
            _ => None,
        })
        .unwrap_or_else(|| {
            graph
                .nodes
                .iter()
                .map(|n| n.id.0.clone())
                .collect::<Vec<_>>()
                .join(",")
        });
    graph
        .metadata
        .insert("schedule_order".into(), Value::String(order.into()));

    // Prefer GPU-required nodes first within same topo layer (simple heuristic).
    let mut priorities: Vec<(String, u8)> = graph
        .nodes
        .iter()
        .map(|n| {
            let p = match n.compute {
                ComputeAffinity::GpuPreferred => 1,
                ComputeAffinity::GpuRequired | ComputeAffinity::CpuOnly => 2,
            };
            (n.id.0.clone(), p)
        })
        .collect();
    priorities.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    let pr_str = priorities
        .into_iter()
        .map(|(id, p)| format!("{id}:{p}"))
        .collect::<Vec<_>>()
        .join(",");
    graph
        .metadata
        .insert("schedule_priority".into(), Value::String(pr_str.into()));
}
fn lint(input: &PlannerInput<'_>, diags: &mut Vec<Diagnostic>) {
    let n = input.graph.nodes.len();
    let mut incoming: Vec<usize> = vec![0; n];
    let mut outgoing: Vec<usize> = vec![0; n];
    for e in &input.graph.edges {
        if e.from.node.0 < n {
            outgoing[e.from.node.0] += 1;
        }
        if e.to.node.0 < n {
            incoming[e.to.node.0] += 1;
        }
    }

    // Enforce exclusivity for ports that declare `Owned`/`MutBorrowed` access.
    // This is the planner-level guardrail that makes in-place / COW transforms predictable:
    // if a producer output is fanned out, a downstream node cannot claim exclusive access.
    let view = input.registry.view();
    let mut fanout: HashMap<(usize, String), usize> = HashMap::new();
    for e in &input.graph.edges {
        *fanout
            .entry((e.from.node.0, e.from.port.clone()))
            .or_insert(0) += 1;
    }
    for e in &input.graph.edges {
        let Some(to_node) = input.graph.nodes.get(e.to.node.0) else {
            continue;
        };
        let Some(desc) = latest_node(&view, &to_node.id) else {
            continue;
        };
        let access = desc.input_access_for(&e.to.port);
        if matches!(access, PortAccessMode::Owned | PortAccessMode::MutBorrowed) {
            let count = fanout
                .get(&(e.from.node.0, e.from.port.clone()))
                .copied()
                .unwrap_or(0);
            if count > 1 {
                let Some(from_node) = input.graph.nodes.get(e.from.node.0) else {
                    continue;
                };
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::AccessViolation,
                        format!(
                            "input {}:{} requires exclusive access ({access:?}), but source {}:{} is fanned out to {} consumers",
                            diagnostic_node_id(to_node),
                            e.to.port,
                            diagnostic_node_id(from_node),
                            e.from.port,
                            count
                        ),
                    )
                    .in_pass("lint")
                    .at_node(diagnostic_node_id(to_node))
                    .at_port(e.to.port.clone()),
                );
            }
        }
    }

    for (idx, node) in input.graph.nodes.iter().enumerate() {
        if incoming[idx] == 0 && !node.inputs.is_empty() {
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::LintWarning,
                    format!(
                        "node {} has unconnected inputs: {}",
                        node.id.0,
                        node.inputs.join(",")
                    ),
                )
                .in_pass("lint")
                .at_node(diagnostic_node_id(node)),
            );
        }
        if outgoing[idx] == 0 && !node.outputs.is_empty() {
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::LintWarning,
                    format!(
                        "node {} has unused outputs: {}",
                        node.id.0,
                        node.outputs.join(",")
                    ),
                )
                .in_pass("lint")
                .at_node(diagnostic_node_id(node)),
            );
        }
    }
}

fn port_type(
    node: &NodeInstance,
    desc: &NodeDescriptor,
    name: &str,
    is_input: bool,
) -> Option<TypeExpr> {
    fn is_generic_marker(ty: &TypeExpr) -> bool {
        matches!(ty, TypeExpr::Opaque(value) if value.eq_ignore_ascii_case("generic"))
    }

    fn resolve_override(
        meta: &std::collections::BTreeMap<String, Value>,
        key: &str,
        port: &str,
    ) -> Option<TypeExpr> {
        let Value::Map(entries) = meta.get(key)? else {
            return None;
        };
        let port_lc = port.to_ascii_lowercase();
        let (_, value) = entries
            .iter()
            .find(|(k, _)| matches!(k, Value::String(s) if s.eq_ignore_ascii_case(&port_lc)))?;
        let Value::String(json) = value else {
            return None;
        };
        serde_json::from_str::<TypeExpr>(json).ok()
    }

    if is_input {
        if let Some(ty) = desc.input_ty_for(name) {
            if is_generic_marker(ty)
                && let Some(solved) =
                    resolve_override(&node.metadata, DYNAMIC_INPUT_TYPES_KEY, name)
            {
                return Some(solved);
            }
            return Some(ty.clone());
        }
        if let Some(ty) = resolve_override(&node.metadata, DYNAMIC_INPUT_TYPES_KEY, name) {
            return Some(ty);
        }
    } else if let Some(port) = desc.outputs.iter().find(|p| p.name == name) {
        if is_generic_marker(&port.ty)
            && let Some(solved) = resolve_override(&node.metadata, DYNAMIC_OUTPUT_TYPES_KEY, name)
        {
            return Some(solved);
        }
        return Some(port.ty.clone());
    } else if let Some(ty) = resolve_override(&node.metadata, DYNAMIC_OUTPUT_TYPES_KEY, name) {
        return Some(ty);
    }
    let key = if is_input {
        "dynamic_inputs"
    } else {
        "dynamic_outputs"
    };
    let resolve_meta = |meta: &std::collections::BTreeMap<String, Value>| match meta.get(key) {
        Some(Value::String(value)) if !value.trim().is_empty() => {
            Some(TypeExpr::Opaque(value.trim().to_string()))
        }
        _ => None,
    };
    // Dynamic port declarations are trusted only from the registry descriptor, not per-node
    // graph metadata (which may come from untrusted UI/clients).
    resolve_meta(&desc.metadata)
}
