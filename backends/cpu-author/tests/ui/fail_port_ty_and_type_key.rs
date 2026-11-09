use daedalus_macros::node;

#[node(
    id = "ui.both",
    inputs(port(name = "value", ty = i32, type_key = "ui:b")),
    outputs(out)
)]
fn both(value: i32) -> i32 {
    value
}

fn main() {}
