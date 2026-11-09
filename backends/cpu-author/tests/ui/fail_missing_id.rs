use daedalus_macros::node;

#[node(inputs(value), outputs(out))]
pub fn missing_id(value: i32) -> i32 {
    value
}

fn main() {}
