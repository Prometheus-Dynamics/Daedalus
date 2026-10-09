//! Regenerates `generated/`, the planner's output for `graph.json` and `graph_b.json`. Run it
//! after changing a graph or a node interface; `tests/generated.rs` fails until it is run.
//!
//! ```console
//! $ cargo run -p daedalus-mcu-blink --example generate
//! ```

#[path = "../tools/plan.rs"]
mod plan;

use std::{fs, path::Path};

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = root.join("generated");
    fs::create_dir_all(&dir).unwrap_or_else(|err| panic!("{}: {err}", dir.display()));
    for (name, contents) in plan::outputs(root) {
        let path = dir.join(&name);
        fs::write(&path, contents).unwrap_or_else(|err| panic!("{}: {err}", path.display()));
    }
}
