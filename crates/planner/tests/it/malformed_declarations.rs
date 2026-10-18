//! Declarations whose JSON cannot be read are reported as planner diagnostics, not dropped.

use daedalus_data::model::Value;
use daedalus_planner::{
    Diagnostic, DiagnosticCode, Graph, NodeInstance, PlannerConfig, PlannerInput, build_plan,
};
use daedalus_registry::capability::{CapabilityRegistry, NodeDecl, PortDecl};

fn plan_diagnostics(decl: NodeDecl) -> Vec<Diagnostic> {
    let id = decl.id.0.clone();
    let mut capabilities = CapabilityRegistry::new();
    capabilities.register_node(decl).unwrap();
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new(id));
    build_plan(
        PlannerInput { graph },
        PlannerConfig {
            transport_capabilities: Some(capabilities),
            enable_lints: false,
            ..PlannerConfig::default()
        },
    )
    .diagnostics
}

fn with_code(diags: &[Diagnostic], code: DiagnosticCode) -> Vec<String> {
    diags
        .iter()
        .filter(|diag| diag.code == code)
        .map(|diag| diag.message.clone())
        .collect()
}

#[test]
fn malformed_descriptor_metadata_is_reported() {
    let mut decl = NodeDecl::new("demo.bad");
    decl.metadata_json
        .insert("demo.key".to_string(), "{not json".to_string());
    let diags = plan_diagnostics(decl);
    let messages = with_code(&diags, DiagnosticCode::InvalidDescriptorMetadata);
    assert_eq!(messages.len(), 1, "{diags:?}");
    assert!(messages[0].contains("demo.key"), "{messages:?}");
}

#[test]
fn malformed_const_input_is_reported_and_not_applied() {
    let mut port = PortDecl::new("in", "i64");
    port.const_value_json = Some("[1,".to_string());
    let mut graph_node = NodeInstance::new("demo.bad");
    graph_node.inputs = vec!["in".to_string()];
    let mut capabilities = CapabilityRegistry::new();
    capabilities
        .register_node(NodeDecl::new("demo.bad").input(port))
        .unwrap();
    let mut graph = Graph::default();
    graph.nodes.push(graph_node);
    let out = build_plan(
        PlannerInput { graph },
        PlannerConfig {
            transport_capabilities: Some(capabilities),
            enable_lints: false,
            ..PlannerConfig::default()
        },
    );
    let messages = with_code(&out.diagnostics, DiagnosticCode::InvalidConstInput);
    assert_eq!(messages.len(), 1, "{:?}", out.diagnostics);
    assert!(messages[0].contains("`in`"), "{messages:?}");
    assert!(out.plan.graph.nodes[0].const_inputs.is_empty());
}

#[test]
fn well_formed_declarations_report_nothing() {
    let decl = NodeDecl::new("demo.ok")
        .metadata("demo.key", Value::Bool(true))
        .input(PortDecl::new("in", "i64").const_value(Value::Int(3)));
    let diags = plan_diagnostics(decl);
    assert!(with_code(&diags, DiagnosticCode::InvalidDescriptorMetadata).is_empty());
    assert!(with_code(&diags, DiagnosticCode::InvalidConstInput).is_empty());
}
