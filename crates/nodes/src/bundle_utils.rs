use daedalus_runtime::NodeError;

use crate::NodeDecl;
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

pub fn nodes() -> Vec<NodeDecl> {
    vec![
        IdentityNode::node_decl().expect("utils.identity node declaration"),
        AddNode::node_decl().expect("utils.add node declaration"),
    ]
}
