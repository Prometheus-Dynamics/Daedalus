use daedalus_macros::node;

#[node(id = 5, inputs(value), outputs(out))]
fn numeric_id(value: i32) -> i32 {
    value
}

fn main() {}
