use daedalus_macros::node;

#[node(id = "ui.fire", inputs(a), outputs(out), fire = "every")]
pub fn fire_mode(a: i64) -> i64 {
    a
}

fn main() {}
