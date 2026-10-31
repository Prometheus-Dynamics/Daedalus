//! `accel-matrix`: render the backend matrix and gate on it, or record asm evidence.
//!
//! ```text
//! accel-matrix render --registry dump.json... --results DIR... [--baseline docs/backend-matrix.csv]
//!                     [--scan REPO_ROOT] [--release] [--strict-review] --out docs/
//! accel-matrix asm --registry dump.json... --binary TEST_BINARY --target TRIPLE --out DIR
//!                  [--objdump rust-objdump]
//! ```
//!
//! `render` writes `backend-matrix.md` and `.csv` to `--out`, then fails on: registry errors,
//! marker files without an entry (`--scan`), failing cells, cells that were pass in the baseline
//! and are not now, and with `--release`, any cell that is neither pass nor impossible.
//! `--strict-review` also fails on per-element ops marked impossible on the GPU (always listed).
//!
//! `asm` disassembles a test binary built for `--target` and records, for every kernel with a
//! `symbol` on that target's backends, whether the symbol's code has vector instructions.
#![allow(clippy::print_stdout, clippy::print_stderr)] // a command-line tool

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use daedalus_accel::Backend;
use daedalus_accel::matrix::{Cell, Inventory, functions, regressions, vector_instructions};

struct Args {
    command: String,
    registries: Vec<PathBuf>,
    results: Vec<PathBuf>,
    baseline: Option<PathBuf>,
    scan: Option<PathBuf>,
    out: Option<PathBuf>,
    binary: Option<PathBuf>,
    target: Option<String>,
    objdump: String,
    release: bool,
    strict_review: bool,
}

fn parse() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let command = it.next().ok_or("usage: accel-matrix render|asm ...")?;
    let mut args = Args {
        command,
        registries: Vec::new(),
        results: Vec::new(),
        baseline: None,
        scan: None,
        out: None,
        binary: None,
        target: None,
        objdump: std::env::var("OBJDUMP").unwrap_or_else(|_| "rust-objdump".into()),
        release: false,
        strict_review: false,
    };
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--registry" => args.registries.push(value()?.into()),
            "--results" => args.results.push(value()?.into()),
            "--baseline" => args.baseline = Some(value()?.into()),
            "--scan" => args.scan = Some(value()?.into()),
            "--out" => args.out = Some(value()?.into()),
            "--binary" => args.binary = Some(value()?.into()),
            "--target" => args.target = Some(value()?),
            "--objdump" => args.objdump = value()?,
            "--release" => args.release = true,
            "--strict-review" => args.strict_review = true,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(args)
}

fn load(args: &Args) -> Result<Inventory, String> {
    let mut inv = Inventory::default();
    for path in &args.registries {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{path:?}: {e}"))?;
        inv.add_registry(&text)
            .map_err(|e| format!("{path:?}: {e}"))?;
    }
    for dir in &args.results {
        if dir.exists() {
            inv.add_records_dir(dir)?;
        }
    }
    Ok(inv)
}

fn render(args: &Args) -> Result<bool, String> {
    let inv = load(args)?;
    let out = args.out.as_ref().ok_or("--out is required")?;
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let baseline = match &args.baseline {
        Some(path) if path.exists() => {
            Some(std::fs::read_to_string(path).map_err(|e| e.to_string())?)
        }
        _ => None,
    };
    // Compare before overwriting: the baseline is usually the file being written.
    let regressed = baseline
        .as_deref()
        .map(|csv| regressions(csv, &inv))
        .unwrap_or_default();
    std::fs::write(out.join("backend-matrix.md"), inv.markdown()).map_err(|e| e.to_string())?;
    std::fs::write(out.join("backend-matrix.csv"), inv.csv()).map_err(|e| e.to_string())?;

    let mut failures = Vec::new();
    failures.extend(
        inv.check_registry()
            .into_iter()
            .map(|e| format!("registry: {e}")),
    );
    if let Some(root) = &args.scan {
        failures.extend(inv.unregistered_markers(root)?);
    }
    let mut gaps = Vec::new();
    for (op, row) in inv.cells() {
        for (column, v) in row {
            let at = format!("{} on {}", op.id, column.name());
            match v.cell {
                Cell::Fail => failures.push(format!("{at}: {}", v.reason)),
                Cell::Gap | Cell::Untested => {
                    gaps.push(format!("{at}: {} ({})", v.cell.name(), v.reason))
                }
                Cell::Pass | Cell::Impossible | Cell::NotApplicable => {}
            }
        }
    }
    failures.extend(regressed.into_iter().map(|r| format!("regression: {r}")));
    let review = inv.review();
    for item in &review {
        eprintln!("review: {item}");
    }
    if args.strict_review {
        failures.extend(review.into_iter().map(|r| format!("review: {r}")));
    }
    if args.release {
        failures.extend(gaps.iter().map(|g| format!("release gate: {g}")));
    } else {
        eprintln!("{} gap or untested cells (fail with --release)", gaps.len());
    }
    for f in &failures {
        eprintln!("error: {f}");
    }
    println!(
        "{}",
        inv.markdown()
            .lines()
            .find(|l| l.starts_with("Totals:"))
            .unwrap_or("")
    );
    Ok(failures.is_empty())
}

fn target_backends(target: &str) -> &'static [Backend] {
    if target.starts_with("x86_64") || target.starts_with("i686") || target.starts_with("i586") {
        &[Backend::X86Sse2, Backend::X86Avx2]
    } else if target.starts_with("aarch64") {
        &[Backend::Neon64]
    } else if target.starts_with("armv7") || target.starts_with("thumbv7neon") {
        &[Backend::Neon32]
    } else if target.starts_with("riscv") {
        &[Backend::Rvv]
    } else {
        &[]
    }
}

fn asm(args: &Args) -> Result<bool, String> {
    let inv = load(args)?;
    let binary = args.binary.as_ref().ok_or("--binary is required")?;
    let target = args.target.as_deref().ok_or("--target is required")?;
    let out = args.out.as_ref().ok_or("--out is required")?;
    let listing = std::process::Command::new(&args.objdump)
        .args(["-d", "--demangle", "--no-show-raw-insn"])
        .arg(binary)
        .output()
        .map_err(|e| format!("{}: {e}", args.objdump))?;
    if !listing.status.success() {
        return Err(String::from_utf8_lossy(&listing.stderr).into_owned());
    }
    let funcs = functions(&String::from_utf8_lossy(&listing.stdout));
    let backends = target_backends(target);
    let mut lines = String::new();
    let mut ok = true;
    for k in inv.kernels.iter().filter(|k| k.status == "implemented") {
        let Some(symbol) = &k.symbol else { continue };
        if !backends.contains(&k.backend) {
            continue;
        }
        let bodies: Vec<&(String, String)> = funcs
            .iter()
            .filter(|(n, _)| n.contains(symbol.as_str()))
            .collect();
        let count: usize = bodies
            .iter()
            .map(|(_, body)| vector_instructions(k.backend, body))
            .sum();
        let (pass, detail) = if bodies.is_empty() {
            (
                false,
                format!("symbol {symbol} not found in {}", binary.display()),
            )
        } else {
            (
                count > 0,
                format!(
                    "{count} vector instructions in {} function(s) matching {symbol}",
                    bodies.len()
                ),
            )
        };
        ok &= pass;
        if !pass {
            eprintln!("error: {} on {}: {detail}", k.op, k.backend.name());
        }
        lines.push_str(&format!(
            "{{\"kind\":\"asm\",\"op\":{},\"backend\":\"{}\",\"extensions\":{},\"variant\":{},\"adapter\":null,\"target\":{},\"runner\":\"objdump\",\"vlen\":null,\"rev\":\"\",\"result\":\"{}\",\"detail\":{}}}\n",
            serde_json::to_string(&k.op).map_err(|e| e.to_string())?,
            k.backend.name(),
            serde_json::to_string(&k.extensions).map_err(|e| e.to_string())?,
            k.variant.as_ref().map_or("null".into(), |v| serde_json::to_string(v).unwrap_or_default()),
            serde_json::to_string(target).map_err(|e| e.to_string())?,
            if pass { "pass" } else { "fail" },
            serde_json::to_string(&detail).map_err(|e| e.to_string())?,
        ));
    }
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let name = format!("asm-{target}-{}.jsonl", file_stem(binary));
    std::fs::write(out.join(name), lines).map_err(|e| e.to_string())?;
    Ok(ok)
}

fn file_stem(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn main() -> ExitCode {
    let result = parse().and_then(|args| match args.command.as_str() {
        "render" => render(&args),
        "asm" => asm(&args),
        other => Err(format!("unknown command {other}: render or asm")),
    });
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::from(2)
        }
    }
}
