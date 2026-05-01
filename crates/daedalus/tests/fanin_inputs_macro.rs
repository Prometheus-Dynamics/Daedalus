use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::{FanIn, PluginRegistry, macros::node, runtime::NodeError};

#[node(id = "test.fanin_full", inputs("items", "scale"), outputs("out"))]
fn fanin_full(ins: FanIn<i64>, scale: i64) -> Result<i64, NodeError> {
    Ok(ins.into_iter().sum::<i64>() * scale)
}

#[node(id = "test.fanin_default", outputs("out"))]
fn fanin_default(ins: FanIn<i64>, scale: i64) -> Result<i64, NodeError> {
    Ok(ins.into_iter().sum::<i64>() * scale)
}

#[node(id = "test.fanin_multi", inputs("lhs", "rhs"), outputs("sum"))]
fn fanin_multi(lhs: FanIn<i32>, rhs: FanIn<i32>) -> Result<i32, NodeError> {
    Ok(lhs.into_iter().sum::<i32>() + rhs.into_iter().sum::<i32>())
}

#[node(
    id = "test.fanin_ty_override",
    generics(T),
    inputs(port(name = "items", ty = TypeExpr::Scalar(ValueType::Int)), "scale"),
    outputs("out")
)]
fn fanin_ty_override<T: std::any::Any + Clone + Send + Sync + 'static>(
    items: FanIn<T>,
    scale: i64,
) -> Result<i64, NodeError> {
    Ok(items.into_vec().len() as i64 * scale)
}

#[node(
    id = "test.generic_passthrough",
    generics(T),
    inputs("value"),
    outputs("out")
)]
fn generic_passthrough<T: Clone + Send + Sync + 'static>(value: T) -> Result<T, NodeError> {
    Ok(value)
}

#[test]
fn fanin_prefix_can_come_from_inputs_list() {
    let decl = FaninFullNode::node_decl().expect("node decl");
    assert_eq!(
        decl.inputs
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        vec!["scale"]
    );
    assert_eq!(decl.fanin_inputs.len(), 1);
    assert_eq!(decl.fanin_inputs[0].prefix, "items");
    assert!(matches!(
        decl.fanin_inputs[0].schema.as_ref(),
        Some(TypeExpr::Scalar(ValueType::Int))
    ));
}

#[test]
fn fanin_prefix_defaults_to_param_name_when_inputs_unspecified() {
    let decl = FaninDefaultNode::node_decl().expect("node decl");
    assert_eq!(
        decl.inputs
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        vec!["scale"]
    );
    assert_eq!(decl.fanin_inputs.len(), 1);
    assert_eq!(decl.fanin_inputs[0].prefix, "ins");
    assert!(matches!(
        decl.fanin_inputs[0].schema.as_ref(),
        Some(TypeExpr::Scalar(ValueType::Int))
    ));
}

#[test]
fn multiple_fanin_groups_are_described() {
    let decl = FaninMultiNode::node_decl().expect("node decl");
    let mut prefixes = decl
        .fanin_inputs
        .iter()
        .map(|p| p.prefix.as_str())
        .collect::<Vec<_>>();
    prefixes.sort();
    assert_eq!(prefixes, vec!["lhs", "rhs"]);
}

#[test]
fn fanin_port_can_override_type_expr_for_generics() {
    let decl =
        FaninTyOverrideNode::node_decl_for::<i64>("test.fanin_ty_override").expect("node decl");
    assert_eq!(decl.fanin_inputs.len(), 1);
    assert_eq!(decl.fanin_inputs[0].prefix, "items");
    assert!(matches!(
        decl.fanin_inputs[0].schema.as_ref(),
        Some(TypeExpr::Scalar(ValueType::Int))
    ));
}

#[test]
fn generic_node_register_for_installs_concrete_boundary_contracts() {
    let mut registry = PluginRegistry::bare();
    GenericPassthroughNode::register_for::<i64>(&mut registry, "test.generic_i64")
        .expect("generic node register");

    assert!(
        registry
            .boundary_contracts
            .contains_key(&daedalus_registry::typeexpr_transport_key(
                &TypeExpr::Scalar(ValueType::Int)
            ))
    );
}
