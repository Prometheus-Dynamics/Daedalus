use daedalus::macros::node;

#[node(id = 5, inputs("value"), outputs("out"))]
fn numeric_id(value: i32) -> Result<i32, daedalus::runtime::NodeError> {
    Ok(value)
}

fn main() {}
