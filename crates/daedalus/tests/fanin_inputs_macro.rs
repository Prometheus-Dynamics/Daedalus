use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::{FanIn, macros::node, runtime::NodeError};

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

#[test]
fn fanin_prefix_can_come_from_inputs_list() {
    let desc = fanin_full::descriptor();
    assert_eq!(
        desc.inputs
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        vec!["scale"]
    );
    assert_eq!(desc.fanin_inputs.len(), 1);
    assert_eq!(desc.fanin_inputs[0].prefix, "items");
    assert!(matches!(
        desc.input_ty_for("items0"),
        Some(TypeExpr::Scalar(ValueType::Int))
    ));
}

#[test]
fn fanin_prefix_defaults_to_param_name_when_inputs_unspecified() {
    let desc = fanin_default::descriptor();
    assert_eq!(
        desc.inputs
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        vec!["scale"]
    );
    assert_eq!(desc.fanin_inputs.len(), 1);
    assert_eq!(desc.fanin_inputs[0].prefix, "ins");
    assert!(matches!(
        desc.input_ty_for("ins2"),
        Some(TypeExpr::Scalar(ValueType::Int))
    ));
}

#[test]
fn multiple_fanin_groups_are_described() {
    let desc = fanin_multi::descriptor();
    let mut prefixes = desc
        .fanin_inputs
        .iter()
        .map(|p| p.prefix.as_str())
        .collect::<Vec<_>>();
    prefixes.sort();
    assert_eq!(prefixes, vec!["lhs", "rhs"]);
}

#[test]
fn fanin_port_can_override_type_expr_for_generics() {
    let desc = fanin_ty_override::descriptor_for::<i64>("test.fanin_ty_override");
    assert_eq!(desc.fanin_inputs.len(), 1);
    assert_eq!(desc.fanin_inputs[0].prefix, "items");
    assert!(matches!(
        desc.input_ty_for("items0"),
        Some(TypeExpr::Scalar(ValueType::Int))
    ));
}
