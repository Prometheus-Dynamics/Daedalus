use daedalus::macros::node;

#[node(inputs("value"), outputs("out"))]
pub fn missing_id(value: i32) -> Result<i32, daedalus::runtime::NodeError> {
    Ok(value)
}

fn main() {}
