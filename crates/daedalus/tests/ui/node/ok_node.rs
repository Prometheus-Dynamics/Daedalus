use daedalus::macros::node;
use daedalus::runtime::NodeError;

#[node(id = "ui.ok", inputs("value"), outputs("out"))]
fn increment(value: i32) -> Result<i32, NodeError> {
    Ok(value + 1)
}

fn main() {
    let decl = IncrementNode::node_decl().expect("node declaration");
    assert_eq!(decl.id.0, "ui.ok");
}
