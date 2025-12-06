use daedalus_nodes::node;
use daedalus_runtime::NodeError;

#[node(inputs("value"), outputs("out"))]
pub fn missing_id(value: i32) -> Result<i32, NodeError> {
    Ok(value)
}

fn main() {}
