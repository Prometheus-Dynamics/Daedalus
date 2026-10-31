//! The backend matrix: registry dumps plus CI evidence, rendered to Markdown and CSV, and the
//! release gate (`accel-matrix`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::Backend;

/// An op from a registry dump.
#[derive(Clone, Debug)]
pub struct OpRow {
    pub repo: String,
    pub id: String,
    pub summary: String,
    pub per_element: bool,
}

/// A kernel entry from a registry dump.
#[derive(Clone, Debug)]
pub struct KernelRow {
    pub repo: String,
    pub op: String,
    pub backend: Backend,
    pub variant: Option<String>,
    pub source: String,
    pub test: String,
    pub symbol: Option<String>,
    /// `implemented`, `gap`, `impossible` or `not_data_parallel`.
    pub status: String,
    pub reason: String,
}

/// One evidence record (`kind` `test` from [`crate::conform`], `asm` from the asm check).
#[derive(Clone, Debug)]
pub struct Record {
    pub kind: String,
    pub op: String,
    pub backend: Backend,
    pub variant: Option<String>,
    pub adapter: Option<String>,
    pub target: String,
    pub runner: String,
    pub vlen: Option<u32>,
    pub pass: bool,
    pub detail: Option<String>,
}

/// What a matrix cell says.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Cell {
    Pass,
    Impossible,
    Untested,
    Gap,
    Fail,
}

impl Cell {
    pub fn name(self) -> &'static str {
        match self {
            Cell::Pass => "pass",
            Cell::Impossible => "impossible",
            Cell::Untested => "untested",
            Cell::Gap => "gap",
            Cell::Fail => "fail",
        }
    }
}

/// A computed cell: its state, the reason (gap, impossible, what evidence is missing) and the
/// evidence summary.
#[derive(Clone, Debug)]
pub struct CellValue {
    pub cell: Cell,
    pub reason: String,
    pub evidence: String,
}

/// The inputs: every registry dump and evidence record.
#[derive(Default)]
pub struct Inventory {
    pub ops: Vec<OpRow>,
    pub kernels: Vec<KernelRow>,
    /// (repo, path, reason)
    pub exempt: Vec<(String, String, String)>,
    pub records: Vec<Record>,
}

fn s(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn opt(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_string)
}

fn backend(v: &Value) -> Result<Backend, String> {
    let name = s(v, "backend");
    Backend::from_name(&name).ok_or_else(|| format!("unknown backend {name:?}"))
}

impl Inventory {
    /// Add a registry dump ([`crate::dump`]).
    pub fn add_registry(&mut self, json: &str) -> Result<(), String> {
        let v: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
        let list = |key: &str| {
            v.get(key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        for op in list("ops") {
            self.ops.push(OpRow {
                repo: s(&op, "repo"),
                id: s(&op, "id"),
                summary: s(&op, "summary"),
                per_element: op
                    .get("per_element")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
        }
        for k in list("kernels") {
            self.kernels.push(KernelRow {
                repo: s(&k, "repo"),
                op: s(&k, "op"),
                backend: backend(&k)?,
                variant: opt(&k, "variant"),
                source: s(&k, "source"),
                test: s(&k, "test"),
                symbol: opt(&k, "symbol"),
                status: s(&k, "status"),
                reason: s(&k, "reason"),
            });
        }
        for e in list("exempt") {
            self.exempt
                .push((s(&e, "repo"), s(&e, "path"), s(&e, "reason")));
        }
        Ok(())
    }

    /// Add evidence lines (JSON Lines).
    pub fn add_records(&mut self, jsonl: &str) -> Result<(), String> {
        for line in jsonl.lines().filter(|l| !l.trim().is_empty()) {
            let v: Value = serde_json::from_str(line).map_err(|e| format!("{e}: {line}"))?;
            self.records.push(Record {
                kind: s(&v, "kind"),
                op: s(&v, "op"),
                backend: backend(&v)?,
                variant: opt(&v, "variant"),
                adapter: opt(&v, "adapter"),
                target: s(&v, "target"),
                runner: s(&v, "runner"),
                vlen: v.get("vlen").and_then(Value::as_u64).map(|v| v as u32),
                pass: s(&v, "result") == "pass",
                detail: opt(&v, "detail"),
            });
        }
        Ok(())
    }

    /// Add every `*.jsonl` file in `dir` (recursively).
    pub fn add_records_dir(&mut self, dir: &Path) -> Result<(), String> {
        for path in files(dir, &|p| p.extension().is_some_and(|e| e == "jsonl"))? {
            let text = std::fs::read_to_string(&path).map_err(|e| format!("{path:?}: {e}"))?;
            self.add_records(&text)?;
        }
        Ok(())
    }

    /// Registry errors: duplicate op ids, kernels of undeclared ops, implemented kernels
    /// without a source or test, missing reasons.
    pub fn check_registry(&self) -> Vec<String> {
        let mut errors = Vec::new();
        let mut seen = BTreeSet::new();
        for op in &self.ops {
            if !seen.insert(op.id.as_str()) {
                errors.push(format!("op {} is declared twice", op.id));
            }
        }
        for k in &self.kernels {
            let at = format!("{} on {}", k.op, k.backend.name());
            if !seen.contains(k.op.as_str()) {
                errors.push(format!("{at}: op is not declared"));
            }
            if k.status == "implemented" && (k.source.is_empty() || k.test.is_empty()) {
                errors.push(format!(
                    "{at}: an implemented kernel needs a source and a test"
                ));
            }
            if k.status != "implemented" && k.reason.trim().is_empty() {
                errors.push(format!("{at}: {} needs a reason", k.status));
            }
        }
        errors
    }

    /// The cell for `op` on `backend`.
    pub fn cell(&self, op: &str, backend: Backend) -> CellValue {
        let entries: Vec<&KernelRow> = self
            .kernels
            .iter()
            .filter(|k| k.op == op && k.backend == backend)
            .collect();
        let implemented: Vec<&&KernelRow> = entries
            .iter()
            .filter(|k| k.status == "implemented")
            .collect();
        if implemented.is_empty() {
            return match entries.first() {
                None => CellValue {
                    cell: Cell::Gap,
                    reason: "no registry entry".into(),
                    evidence: String::new(),
                },
                Some(k) => CellValue {
                    cell: if k.status == "gap" {
                        Cell::Gap
                    } else {
                        Cell::Impossible
                    },
                    reason: if k.status == "not_data_parallel" {
                        format!("not data-parallel: {}", k.reason)
                    } else {
                        k.reason.clone()
                    },
                    evidence: String::new(),
                },
            };
        }
        // Every implemented variant must pass; the worst one decides.
        let mut worst = CellValue {
            cell: Cell::Pass,
            reason: String::new(),
            evidence: String::new(),
        };
        let mut evidence = Vec::new();
        for k in implemented {
            let value = self.variant_cell(k);
            if !value.evidence.is_empty() {
                evidence.push(value.evidence.clone());
            }
            if value.cell > worst.cell {
                worst = value;
            }
        }
        worst.evidence = evidence.join("; ");
        worst
    }

    fn variant_cell(&self, k: &KernelRow) -> CellValue {
        let matches = |r: &&Record, kind: &str| {
            r.kind == kind && r.op == k.op && r.backend == k.backend && r.variant == k.variant
        };
        let tests: Vec<&Record> = self.records.iter().filter(|r| matches(r, "test")).collect();
        let asm: Vec<&Record> = self.records.iter().filter(|r| matches(r, "asm")).collect();
        let label = k
            .variant
            .as_deref()
            .map(|v| format!("{v}: "))
            .unwrap_or_default();
        let mut runs: BTreeSet<String> = BTreeSet::new();
        for r in tests.iter().filter(|r| r.pass) {
            let mut run = format!("{} {}", r.target, r.runner);
            if let Some(vlen) = r.vlen {
                let _ = write!(run, " vlen {vlen}");
            }
            if let Some(adapter) = &r.adapter {
                let _ = write!(run, " ({adapter})");
            }
            runs.insert(run);
        }
        let mut evidence = format!("{label}{}", runs.into_iter().collect::<Vec<_>>().join(", "));
        if asm.iter().any(|r| r.pass) {
            evidence.push_str(" +asm");
        }
        if let Some(fail) = tests.iter().chain(&asm).find(|r| !r.pass) {
            return CellValue {
                cell: Cell::Fail,
                reason: format!(
                    "{label}{} failed on {} {}: {}",
                    fail.kind,
                    fail.target,
                    fail.runner,
                    fail.detail.as_deref().unwrap_or("mismatch")
                ),
                evidence,
            };
        }
        let mut missing = Vec::new();
        if !tests.iter().any(|r| r.pass) {
            missing.push(format!("no CI run of {}", k.test));
        }
        for vlen in k.backend.required_vlens() {
            if !tests.iter().any(|r| r.pass && r.vlen == Some(*vlen)) {
                missing.push(format!("no run at VLEN {vlen}"));
            }
        }
        if k.symbol.is_some() && !asm.iter().any(|r| r.pass) {
            missing.push("no asm check of the vector code".into());
        }
        if missing.is_empty() {
            CellValue {
                cell: Cell::Pass,
                reason: String::new(),
                evidence,
            }
        } else {
            missing.dedup();
            CellValue {
                cell: Cell::Untested,
                reason: format!("{label}{}", missing.join(", ")),
                evidence,
            }
        }
    }

    /// Every cell, by (repo, op) then backend.
    pub fn cells(&self) -> Vec<(&OpRow, Vec<(Backend, CellValue)>)> {
        let mut ops: Vec<&OpRow> = self.ops.iter().collect();
        ops.sort_by(|a, b| (&a.repo, &a.id).cmp(&(&b.repo, &b.id)));
        ops.into_iter()
            .map(|op| {
                let row = Backend::ALL
                    .into_iter()
                    .map(|b| (b, self.cell(&op.id, b)))
                    .collect();
                (op, row)
            })
            .collect()
    }

    /// GPU cells of per-element ops that are marked impossible: the user's rule is that
    /// per-element work is never impossible on the GPU, so these need review.
    pub fn review(&self) -> Vec<String> {
        self.cells()
            .into_iter()
            .filter(|(op, _)| op.per_element)
            .filter_map(|(op, row)| {
                let (_, gpu) = row.into_iter().find(|(b, _)| *b == Backend::Gpu)?;
                (gpu.cell == Cell::Impossible).then(|| {
                    format!(
                        "{}: GPU impossible on a per-element op ({})",
                        op.id, gpu.reason
                    )
                })
            })
            .collect()
    }

    /// Source files under `root` with SIMD or GPU markers that no kernel's `source` and no
    /// exemption names (paths relative to `root`).
    pub fn unregistered_markers(&self, root: &Path) -> Result<Vec<String>, String> {
        let known: BTreeSet<String> = self
            .kernels
            .iter()
            .filter(|k| !k.source.is_empty())
            .map(|k| source_path(&k.source).to_string())
            .chain(self.exempt.iter().map(|(_, path, _)| path.clone()))
            .collect();
        let mut missing = Vec::new();
        for path in files(root, &|p| {
            p.extension().is_some_and(|e| e == "rs" || e == "wgsl")
        })? {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if is_test_or_tooling(&rel) || known.contains(&rel) {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            if let Some(marker) = marker(&rel, &text) {
                missing.push(format!("{rel}: {marker} without a registry entry"));
            }
        }
        Ok(missing)
    }

    /// The CSV: `repo,op,backend,cell,reason,evidence`.
    pub fn csv(&self) -> String {
        let mut out = String::from("repo,op,backend,cell,reason,evidence\n");
        for (op, row) in self.cells() {
            for (backend, value) in row {
                let _ = writeln!(
                    out,
                    "{},{},{},{},{},{}",
                    csv_field(&op.repo),
                    csv_field(&op.id),
                    backend.name(),
                    value.cell.name(),
                    csv_field(&value.reason),
                    csv_field(&value.evidence)
                );
            }
        }
        out
    }

    /// The Markdown page: totals, one table per repo, then every reason.
    pub fn markdown(&self) -> String {
        let cells = self.cells();
        let mut totals: BTreeMap<Cell, usize> = BTreeMap::new();
        for (_, row) in &cells {
            for (_, v) in row {
                *totals.entry(v.cell).or_default() += 1;
            }
        }
        let mut out = String::from(
            "# Backend matrix\n\nGenerated by `accel-matrix` from the registry tables and CI evidence; \
             do not edit. A cell is **pass** only when CI ran the kernel's conformance test on that \
             backend and it matched scalar (RVV: VLEN 128, 256 and 512; compiler-vectorized kernels: \
             an asm check too).\n\n",
        );
        let total: Vec<String> = totals
            .iter()
            .map(|(cell, n)| format!("{} {n}", cell.name()))
            .collect();
        let _ = writeln!(out, "Totals: {}.\n", total.join(", "));
        let header: Vec<&str> = Backend::ALL.iter().map(|b| b.name()).collect();
        let mut repo = "";
        let mut notes = Vec::new();
        for (op, row) in &cells {
            if op.repo != repo {
                repo = &op.repo;
                let _ = writeln!(out, "\n## {repo}\n\n| op | {} |", header.join(" | "));
                let _ = writeln!(out, "|---|{}", "---|".repeat(header.len()));
            }
            let marks: Vec<String> = row
                .iter()
                .map(|(backend, v)| {
                    if v.reason.is_empty() {
                        v.cell.name().to_string()
                    } else {
                        notes.push(format!("- `{}` on {}: {}", op.id, backend.name(), v.reason));
                        format!("{}[{}]", v.cell.name(), notes.len())
                    }
                })
                .collect();
            let _ = writeln!(out, "| `{}` | {} |", op.id, marks.join(" | "));
        }
        if !notes.is_empty() {
            out.push_str("\n## Reasons\n\n");
            for (i, note) in notes.iter().enumerate() {
                let _ = writeln!(out, "{}. {}", i + 1, note.trim_start_matches("- "));
            }
        }
        out
    }
}

/// Cells that were pass in `baseline_csv` and are not pass now.
pub fn regressions(baseline_csv: &str, current: &Inventory) -> Vec<String> {
    let now: BTreeMap<(String, String), Cell> = current
        .cells()
        .into_iter()
        .flat_map(|(op, row)| {
            row.into_iter()
                .map(|(b, v)| ((op.id.clone(), b.name().to_string()), v.cell))
                .collect::<Vec<_>>()
        })
        .collect();
    baseline_csv
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields = parse_csv_line(line);
            let (op, backend, cell) = (fields.get(1)?, fields.get(2)?, fields.get(3)?);
            (cell == "pass").then_some(())?;
            let key = (op.clone(), backend.clone());
            match now.get(&key) {
                Some(Cell::Pass) => None,
                Some(other) => Some(format!("{op} on {backend}: pass -> {}", other.name())),
                None => Some(format!("{op} on {backend}: pass -> removed")),
            }
        })
        .collect()
}

fn source_path(source: &str) -> &str {
    match source.rsplit_once(':') {
        Some((path, line)) if line.chars().all(|c| c.is_ascii_digit()) => path,
        _ => source,
    }
}

fn is_test_or_tooling(rel: &str) -> bool {
    rel.contains("/tests/")
        || rel.contains("/benches/")
        || rel.contains("/examples/")
        || rel.starts_with("examples/")
        || rel.contains("/target/")
        || rel.starts_with("target/")
        || rel.ends_with("_tests.rs")
        || rel.ends_with("/tests.rs")
}

/// The first SIMD or GPU marker in a source file.
fn marker(rel: &str, text: &str) -> Option<&'static str> {
    if rel.ends_with(".wgsl") {
        return Some("a WGSL shader");
    }
    const MARKERS: [(&str, &str); 9] = [
        ("core::arch::", "core::arch"),
        ("std::arch::", "std::arch"),
        ("#[target_feature", "#[target_feature]"),
        ("asm!(", "asm!"),
        ("multiversion!", "multiversion!"),
        ("create_compute_pipeline", "a compute pipeline"),
        ("create_render_pipeline", "a render pipeline"),
        ("FragmentPass::new(", "a fragment pass"),
        ("@compute", "an inline WGSL kernel"),
    ];
    MARKERS
        .iter()
        .find(|(needle, _)| text.contains(needle))
        .map(|(_, name)| *name)
}

fn files(root: &Path, keep: &dyn Fn(&Path) -> bool) -> Result<Vec<PathBuf>, String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).map_err(|e| format!("{dir:?}: {e}"))?;
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            if path.is_dir() {
                if name != "target" && name != ".git" && name != "node_modules" {
                    stack.push(path);
                }
            } else if keep(&path) {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn csv_field(field: &str) -> String {
    if field.contains([',', '"', '\n']) {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match (c, quoted) {
            ('"', true) if chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            ('"', _) => quoted = !quoted,
            (',', false) => fields.push(std::mem::take(&mut field)),
            (c, _) => field.push(c),
        }
    }
    fields.push(field);
    fields
}

/// Vector instructions for `backend` in an `llvm-objdump -d` listing of one function.
pub fn vector_instructions(backend: Backend, asm: &str) -> usize {
    asm.lines()
        .filter(|line| {
            let line = line.trim();
            let Some((mnemonic, operands)) =
                line.split_once(char::is_whitespace).or(Some((line, "")))
            else {
                return false;
            };
            let operands = operands.trim();
            match backend {
                Backend::X86Sse2 => operands.contains("%xmm") || operands.contains("xmm"),
                Backend::X86Avx2 => operands.contains("ymm"),
                Backend::Neon64 => [".16b", ".8b", ".8h", ".4h", ".4s", ".2s", ".2d"]
                    .iter()
                    .any(|t| operands.contains(t)),
                Backend::Neon32 => {
                    mnemonic.starts_with('v')
                        && (mnemonic.contains(".i")
                            || mnemonic.contains(".u")
                            || mnemonic.contains(".s")
                            || mnemonic.contains(".f")
                            || mnemonic.starts_with("vld")
                            || mnemonic.starts_with("vst"))
                        && (operands.contains('q') || operands.contains('d'))
                }
                Backend::Rvv => {
                    mnemonic.starts_with("vset")
                        || (mnemonic.starts_with('v')
                            && operands.split(',').any(|o| {
                                let o = o.trim();
                                o.len() >= 2
                                    && o.starts_with('v')
                                    && o[1..].chars().all(|c| c.is_ascii_digit())
                            }))
                }
                _ => false,
            }
        })
        .count()
}

/// Functions (name, body) in an `llvm-objdump -d --demangle` listing.
pub fn functions(listing: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for line in listing.lines() {
        if let Some(name) = line
            .trim_end()
            .strip_suffix(">:")
            .and_then(|l| l.split_once(" <"))
            .map(|(_, name)| name.to_string())
        {
            out.push((name, String::new()));
        } else if let Some((_, body)) = out.last_mut()
            && let Some((_, insn)) = line.split_once(":\t").or_else(|| line.split_once(": "))
        {
            body.push_str(insn.trim());
            body.push('\n');
        }
    }
    out
}

#[cfg(test)]
mod tests;
