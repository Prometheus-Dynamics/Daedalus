//! The checked-in module is what the generator writes today.

#[path = "../nodes/examples/generate.rs"]
mod generate;

#[test]
fn the_checked_in_module_is_fresh() {
    assert_eq!(
        include_bytes!("../generated/graph.rs").as_slice(),
        generate::module().as_bytes(),
        "run `cargo run -p daedalus-chip-blink-nodes --example generate`"
    );
}
