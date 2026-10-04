use daedalus::macros::node;

#[node(
    id = "ui.both",
    inputs(port(
        name = "value",
        ty = daedalus::data::model::TypeExpr::opaque("ui:a"),
        type_key = "ui:b"
    )),
    outputs("out")
)]
fn both(value: i32) -> Result<i32, daedalus::runtime::NodeError> {
    Ok(value)
}

fn main() {}
