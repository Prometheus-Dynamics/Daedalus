//! The files in `generated/` are the planner's output for `graph.json`, as `examples/generate.rs`
//! writes them. The tests include them, so this test fails when they are stale: run
//! `cargo run -p daedalus-mcu-shared --example generate` and commit the result.

#[path = "../tools/plan.rs"]
mod plan;

use std::{fs, path::Path};

#[test]
fn generated_files_match_the_planner() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut stale = Vec::new();
    for (name, expected) in plan::outputs(root) {
        let actual = fs::read(root.join("generated").join(&name)).unwrap_or_default();
        if actual != expected {
            stale.push(name);
        }
    }
    assert!(
        stale.is_empty(),
        "stale generated files {stale:?}: run `cargo run -p daedalus-mcu-shared --example generate`"
    );
}
