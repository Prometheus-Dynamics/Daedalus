use daedalus_runtime::NodeError;

use crate::NodeDescriptor;
use crate::node;

#[node(
    id = "utils.identity",
    bundle = "utils",
    inputs("value"),
    outputs("value")
)]
fn identity(value: i32) -> Result<i32, NodeError> {
    Ok(value)
}

#[node(id = "utils.add", bundle = "utils", inputs("a", "b"), outputs("out"))]
fn add(a: i32, b: i32) -> Result<i32, NodeError> {
    Ok(a + b)
}

pub fn nodes() -> Vec<NodeDescriptor> {
    vec![identity::descriptor(), add::descriptor()]
}
