//! Optional inputs are meant to be left unconnected: the planner's lint does not report them.

use daedalus_planner::{
    DiagnosticCode, Graph, NodeInstance, PlannerConfig, PlannerInput, build_plan,
};
use daedalus_registry::capability::{CapabilityRegistry, NodeDecl, PortDecl};

fn unconnected_warnings(inputs: &[PortDecl]) -> Vec<String> {
    let mut decl = NodeDecl::new("demo.pose");
    for port in inputs {
        decl = decl.input(port.clone());
    }
    let mut capabilities = CapabilityRegistry::new();
    capabilities.register_node(decl).unwrap();
    let mut node = NodeInstance::new("demo.pose");
    node.inputs = inputs.iter().map(|port| port.name.clone()).collect();
    let mut graph = Graph::default();
    graph.nodes.push(node);
    build_plan(
        PlannerInput { graph },
        PlannerConfig {
            transport_capabilities: Some(capabilities),
            enable_lints: true,
            ..PlannerConfig::default()
        },
    )
    .diagnostics
    .into_iter()
    .filter(|diag| {
        diag.code == DiagnosticCode::LintWarning && diag.message.contains("unconnected inputs")
    })
    .map(|diag| diag.message)
    .collect()
}

#[test]
fn lint_reports_only_required_unconnected_inputs() {
    let frame = PortDecl::new("frame", "i64");
    let refined = PortDecl::new("refined", "demo:corners").optional();
    assert_eq!(
        unconnected_warnings(&[frame, refined.clone()]),
        ["node demo.pose has unconnected inputs: frame"]
    );
    assert!(unconnected_warnings(&[refined]).is_empty());
}

#[test]
fn optional_flag_round_trips_and_is_omitted_when_false() {
    let port = PortDecl::new("refined", "demo:corners").optional();
    let json = serde_json::to_string(&port).unwrap();
    assert!(json.contains("\"optional\":true"), "{json}");
    assert_eq!(serde_json::from_str::<PortDecl>(&json).unwrap(), port);
    let required = serde_json::to_string(&PortDecl::new("frame", "i64")).unwrap();
    assert!(!required.contains("optional"), "{required}");
}
