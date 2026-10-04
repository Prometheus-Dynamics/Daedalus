use daedalus::macros::node;
use daedalus::runtime::NodeError;

#[derive(Default)]
struct Total(i64);

/// Three reference parameters are a typed node, not the low-level `(node, ctx, io)` form.
#[node(id = "ui.three_refs", inputs("a", "b"), outputs("out"), state(Total))]
fn accumulate(a: &i64, b: &i64, state: &mut Total) -> Result<i64, NodeError> {
    state.0 += a * b;
    Ok(state.0)
}

fn main() {
    let decl = AccumulateNode::node_decl().expect("node declaration");
    let inputs: Vec<_> = decl.inputs.iter().map(|port| port.name.as_str()).collect();
    assert_eq!(inputs, ["a", "b"]);
}
