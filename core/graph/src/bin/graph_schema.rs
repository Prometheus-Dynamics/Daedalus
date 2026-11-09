//! Writes `docs/schema/daedalus.graph.v1.schema.json` from the types.

use std::path::Path;

fn main() -> std::io::Result<()> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/schema/daedalus.graph.v1.schema.json");
    let mut text = serde_json::to_string_pretty(&daedalus_graph::json_schema())
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    text.push('\n');
    std::fs::write(path, text)
}
