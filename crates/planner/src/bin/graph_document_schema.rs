//! Print the JSON Schema of the persisted graph document format.
//! Regenerate the checked-in copy with:
//! `cargo run -p daedalus-planner --features schema --bin graph_document_schema > docs/schema/daedalus.graph.v1.schema.json`

use std::io::{self, Write};

fn main() -> io::Result<()> {
    writeln!(
        io::stdout(),
        "{:#}",
        daedalus_planner::GraphDocument::json_schema()
    )
}
