//! `daedalus-lock`: records, checks and updates the `daedalus.lock` file that a host approves its
//! plugins and workers with. The format is `sha256sum`'s, so `sha256sum -c` reads it too.
//!
//! ```text
//! daedalus-lock [--lock FILE] record [--absolute] PATH...
//! daedalus-lock [--lock FILE] check [--root DIR] [PATH...]
//! daedalus-lock [--lock FILE] update [--root DIR] [PATH...]
//! ```
//!
//! Exit codes: 0 ok; 1 a file does not match the lock, has no entry, or would change an approved
//! entry; 2 usage; 3 the lock or a file cannot be read or is malformed.

#![forbid(unsafe_code)]

mod commands;

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use commands::{Fail, Report};

const USAGE: &str = "\
usage: daedalus-lock [--lock FILE] <command> [args]

  record [--absolute] PATH...     record each file's sha256 into the lock (created if missing)
  check [--root DIR] [PATH...]    check files against the lock; with no PATH, every entry
  update [--root DIR] [PATH...]   approve the current file for each entry, showing old -> new

  --lock FILE   the lock (default ./daedalus.lock)
  --root DIR    where the lock's relative keys are found (default: the lock's directory)
  --absolute    record each key as the file's absolute path, not its file name

exit codes: 0 ok, 1 a mismatch or missing entry, 2 usage, 3 unreadable or malformed
";

struct Options {
    lock: PathBuf,
    root: Option<PathBuf>,
    absolute: bool,
    command: String,
    paths: Vec<PathBuf>,
}

/// The options from the arguments. An empty message is a request for the usage text.
fn parse(args: impl Iterator<Item = String>) -> Result<Options, String> {
    let mut lock = PathBuf::from("daedalus.lock");
    let mut root = None;
    let mut absolute = false;
    let mut words = Vec::new();
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--lock" => lock = PathBuf::from(args.next().ok_or("--lock needs a file")?),
            "--root" => {
                root = Some(PathBuf::from(
                    args.next().ok_or("--root needs a directory")?,
                ))
            }
            "--absolute" => absolute = true,
            "--help" | "-h" => return Err(String::new()),
            other if other.starts_with("--") => return Err(format!("unknown option `{other}`")),
            _ => words.push(arg),
        }
    }
    if words.is_empty() {
        return Err("no command given".into());
    }
    let command = words.remove(0);
    match command.as_str() {
        "record" if root.is_some() => return Err("--root is for check and update".into()),
        "check" | "update" if absolute => return Err("--absolute is for record".into()),
        "record" | "check" | "update" => {}
        other => {
            return Err(format!(
                "`{other}` is not a command: record, check or update"
            ));
        }
    }
    Ok(Options {
        lock,
        root,
        absolute,
        command,
        paths: words.into_iter().map(PathBuf::from).collect(),
    })
}

fn out(text: &str) {
    let _ = std::io::stdout().lock().write_all(text.as_bytes());
}

fn err(text: &str) {
    let _ = std::io::stderr().lock().write_all(text.as_bytes());
}

fn print(report: &Report) {
    for line in &report.lines {
        out(&format!("{line}\n"));
    }
    for warning in &report.warnings {
        err(&format!("daedalus-lock: warning: {warning}\n"));
    }
}

fn main() -> ExitCode {
    let options = match parse(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(message) if message.is_empty() => {
            out(USAGE);
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            err(&format!("daedalus-lock: {message}\n{USAGE}"));
            return ExitCode::from(2);
        }
    };
    let root = options.root.as_deref();
    let result = match options.command.as_str() {
        "record" => commands::record(&options.lock, &options.paths, options.absolute),
        "check" => commands::check(&options.lock, root, &options.paths),
        _ => commands::update_entries(&options.lock, root, &options.paths),
    };
    match result {
        Ok(report) => {
            print(&report);
            if report.failed {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(Fail::Usage(message)) => {
            err(&format!("daedalus-lock: {message}\n"));
            ExitCode::from(2)
        }
        Err(Fail::Mismatch(message)) => {
            err(&format!("daedalus-lock: {message}\n"));
            ExitCode::from(1)
        }
        Err(Fail::Unreadable(message)) => {
            err(&format!("daedalus-lock: {message}\n"));
            ExitCode::from(3)
        }
    }
}
