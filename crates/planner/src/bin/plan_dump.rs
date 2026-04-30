//! Simple CLI to pretty-print an `ExecutionPlan` JSON from stdin or a file path.
//! Usage:
//! `cargo run -p daedalus-planner --bin plan_dump -- path/to/plan.json`
//! or pipe JSON: `cat plan.json | cargo run -p daedalus-planner --bin plan_dump --`

use std::env;
use std::fs;
use std::io::{self, Read, Write};

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let input = if let Some(path) = args.first() {
        if path == "-" {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf).expect("read stdin");
            buf
        } else {
            fs::read_to_string(path).expect("read file")
        }
    } else {
        let _ = writeln!(io::stderr(), "usage: plan_dump <path|->");
        std::process::exit(1);
    };

    let plan: daedalus_planner::ExecutionPlan =
        serde_json::from_str(&input).expect("parse ExecutionPlan JSON");
    let pretty = daedalus_planner::debug::to_pretty_json(&plan);
    let _ = writeln!(io::stdout(), "{pretty}");
}
