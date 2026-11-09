use daedalus_cpu_author::Fault;
use daedalus_macros::node;

#[derive(Default)]
struct Total(i64);

/// Three reference parameters are still a typed node.
#[node(id = "ui.three_refs", inputs(a, b), outputs(out), state(Total))]
fn accumulate(a: &i64, b: &i64, state: &mut Total) -> Result<i64, Fault> {
    state.0 += a * b;
    Ok(state.0)
}

fn main() {
    let names: Vec<_> = ACCUMULATE_DECL.inputs.iter().map(|port| port.name).collect();
    assert_eq!(names, ["a", "b"]);
}
