use daedalus_cpu_author::Fault;
use daedalus_macros::node;

#[node(id = "ui.ok", inputs(value), outputs(out))]
fn increment(value: i32) -> Result<i32, Fault> {
    Ok(value + 1)
}

fn main() {
    assert_eq!(INCREMENT_DECL.id, "ui.ok");
}
