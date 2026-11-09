//! Reads a graph file, plans it, and prints the explain record.
//!
//! This binary links no nodes and no backends, so it plans host bridges only and reports anything
//! else as a missing node. A host with its own registry calls `explain_plan` directly.

#![forbid(unsafe_code)]

use std::io::Write;
use std::process::ExitCode;

use daedalus_graph::GraphDoc;
use daedalus_planner::explain::{explain_plan, json::to_json};
use daedalus_planner::{Config, Form, plan};
use daedalus_registry::Registry;

fn run(path: &str) -> Result<String, String> {
    let text = std::fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))?;
    let doc = GraphDoc::parse(&text).map_err(|error| format!("{path}: {error}"))?;
    let config = Config {
        form: Form::Blob,
        ..Config::default()
    };
    let planned = plan(&doc.graph, &Registry::new(), &[], &config).map_err(|error| {
        let lines: Vec<String> = error.diagnostics().into_iter().map(|d| d.message).collect();
        lines.join("\n")
    })?;
    to_json(&explain_plan(&planned)).map_err(|error| error.to_string())
}

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        let _ = writeln!(std::io::stderr(), "usage: plan_dump <graph.json>");
        return ExitCode::FAILURE;
    };
    match run(&path) {
        Ok(text) => {
            let _ = write!(std::io::stdout(), "{text}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            let _ = writeln!(std::io::stderr(), "{message}");
            ExitCode::FAILURE
        }
    }
}
