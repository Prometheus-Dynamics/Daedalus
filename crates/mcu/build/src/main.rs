//! `daedalus-mcu`: compile loaded-mode plan blobs and encode parameter updates (docs/mcu.md).

use std::io::Write as _;
use std::process::ExitCode;

use daedalus_data::model::Value;
use daedalus_mcu_build::{CompileOptions, LibraryManifest, PlanManifest, compile_loaded_document};

const USAGE: &str = "usage:
  daedalus-mcu plan <library.json> <graph.json> <plan.bin> [<plan.json>]
      compile a graph document for a firmware's node library into a plan blob (and its
      manifest of port and parameter ids)
  daedalus-mcu param <plan.json> <name> <value>
      print the parameter update message (hex) setting <name> to <value> (true, false or a
      number); a compiled firmware's manifest works too";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            let _ = writeln!(std::io::stderr(), "daedalus-mcu: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<(), String> {
    let read = |path: &str| std::fs::read_to_string(path).map_err(|err| format!("{path}: {err}"));
    let write = |path: &str, bytes: &[u8]| {
        std::fs::write(path, bytes).map_err(|err| format!("{path}: {err}"))
    };
    match args {
        [cmd, library, graph, blob, manifest @ ..] if cmd == "plan" && manifest.len() <= 1 => {
            let library = LibraryManifest::from_json(&read(library)?).map_err(|e| e.to_string())?;
            let (bytes, plan) =
                compile_loaded_document(&read(graph)?, &library, &CompileOptions::default())
                    .map_err(|e| e.to_string())?;
            write(blob, &bytes)?;
            if let [manifest] = manifest {
                write(manifest, plan.to_json().as_bytes())?;
            }
            Ok(())
        }
        [cmd, manifest, name, value] if cmd == "param" => {
            let manifest = PlanManifest::from_json(&read(manifest)?).map_err(|e| e.to_string())?;
            let message = manifest
                .param_update(name, &parse_value(value)?)
                .map_err(|e| e.to_string())?;
            let hex: String = message.iter().map(|b| format!("{b:02x}")).collect();
            writeln!(std::io::stdout(), "{hex}").map_err(|e| e.to_string())
        }
        _ => Err(USAGE.into()),
    }
}

fn parse_value(text: &str) -> Result<Value, String> {
    match text {
        "true" => Ok(Value::Bool(true)),
        "false" => Ok(Value::Bool(false)),
        _ => text
            .parse::<i64>()
            .map(Value::Int)
            .or_else(|_| text.parse::<f64>().map(Value::Float))
            .map_err(|_| format!("`{text}` is not true, false or a number")),
    }
}
