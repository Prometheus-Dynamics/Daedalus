//! The backend matrix: registry dumps plus CI evidence, rendered to Markdown and CSV, and the
//! release gate (`accel-matrix`).

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::Backend;

mod asm;
mod render;
pub use asm::{functions, vector_instructions};
pub use render::regressions;

/// An op from a registry dump.
#[derive(Clone, Debug)]
pub struct OpRow {
    pub repo: String,
    pub id: String,
    pub summary: String,
    pub per_element: bool,
    /// Extension variants the op could use.
    pub candidates: Vec<Column>,
    /// (path, the kernel call it must contain, scalar code it must not contain)
    pub call_sites: Vec<(String, String, Vec<String>)>,
}

/// A matrix column: a backend, plus the extensions of a variant (`neon64+dotprod`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Column {
    pub backend: Backend,
    /// Sorted extension names; empty for the backend's base column.
    pub extensions: Vec<String>,
    /// The `wired` column: the op's call sites run the kernel (`backend` unused).
    pub wired: bool,
}

impl Column {
    pub fn base(backend: Backend) -> Self {
        Self {
            backend,
            extensions: Vec::new(),
            wired: false,
        }
    }

    /// The `wired` column.
    pub fn wired() -> Self {
        Self {
            backend: Backend::Scalar,
            extensions: Vec::new(),
            wired: true,
        }
    }

    /// `neon64`, `neon64+dotprod`, `x86_avx2+bmi2+popcnt`, `wired`.
    pub fn name(&self) -> String {
        if self.wired {
            return "wired".into();
        }
        let mut name = self.backend.name().to_string();
        for ext in &self.extensions {
            name.push('+');
            name.push_str(ext);
        }
        name
    }
}

/// A kernel entry from a registry dump.
#[derive(Clone, Debug)]
pub struct KernelRow {
    pub repo: String,
    pub op: String,
    pub backend: Backend,
    /// Sorted extension names.
    pub extensions: Vec<String>,
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
    /// Sorted extension names the run used.
    pub extensions: Vec<String>,
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
    /// No kernel, candidate or entry: an extension column another op declared.
    NotApplicable,
    Pass,
    Impossible,
    Untested,
    Gap,
    Fail,
}

impl Cell {
    pub fn name(self) -> &'static str {
        match self {
            Cell::NotApplicable => "n/a",
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
    /// The repository root, for the `wired` column (`--scan`).
    pub root: Option<PathBuf>,
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

fn exts(v: &Value) -> Vec<String> {
    let mut out: Vec<String> = v
        .get("extensions")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out.dedup();
    out
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
                candidates: op
                    .get("candidates")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|c| {
                        Ok(Column {
                            backend: backend(c)?,
                            extensions: exts(c),
                            wired: false,
                        })
                    })
                    .collect::<Result<_, String>>()?,
                call_sites: op
                    .get("call_sites")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|c| {
                        let forbid = c
                            .get("forbid")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect();
                        (s(c, "path"), s(c, "calls"), forbid)
                    })
                    .collect(),
            });
        }
        for k in list("kernels") {
            self.kernels.push(KernelRow {
                repo: s(&k, "repo"),
                op: s(&k, "op"),
                backend: backend(&k)?,
                extensions: exts(&k),
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
                extensions: exts(&v),
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

    /// Every column: each backend's base, then the extension variants any kernel or candidate
    /// declares.
    pub fn columns(&self) -> Vec<Column> {
        let mut variants: BTreeSet<Column> = BTreeSet::new();
        for k in self.kernels.iter().filter(|k| !k.extensions.is_empty()) {
            variants.insert(Column {
                backend: k.backend,
                extensions: k.extensions.clone(),
                wired: false,
            });
        }
        for op in &self.ops {
            variants.extend(
                op.candidates
                    .iter()
                    .filter(|c| !c.extensions.is_empty())
                    .cloned(),
            );
        }
        let mut out = Vec::new();
        for backend in Backend::ALL {
            out.push(Column::base(backend));
            out.extend(variants.iter().filter(|c| c.backend == backend).cloned());
        }
        out.push(Column::wired());
        out
    }

    /// The `wired` cell: every call site contains the kernel call and none of the scalar code
    /// it replaced. Needs `root` (the repo, `--scan`) to read the files.
    pub fn wired_cell(&self, op: &str) -> CellValue {
        let value = |cell, reason: String, evidence: String| CellValue {
            cell,
            reason,
            evidence,
        };
        let Some(row) = self.ops.iter().find(|o| o.id == op) else {
            return value(Cell::NotApplicable, String::new(), String::new());
        };
        if row.call_sites.is_empty() {
            return value(
                Cell::Gap,
                "no call sites listed: the kernel may be proven but unused".into(),
                String::new(),
            );
        }
        let Some(root) = &self.root else {
            return value(
                Cell::Untested,
                "call sites not checked (run with --scan)".into(),
                String::new(),
            );
        };
        let mut gaps = Vec::new();
        for (path, calls, forbid) in &row.call_sites {
            let Ok(text) = std::fs::read_to_string(root.join(path)) else {
                gaps.push(format!("{path} not found"));
                continue;
            };
            if let Some(old) = forbid.iter().find(|f| text.contains(f.as_str())) {
                return value(
                    Cell::Fail,
                    format!("{path} still has the scalar code `{old}`"),
                    String::new(),
                );
            }
            if !text.contains(calls.as_str()) {
                gaps.push(format!("{path} does not call `{calls}`"));
            }
        }
        if gaps.is_empty() {
            value(
                Cell::Pass,
                String::new(),
                format!("{} call site(s)", row.call_sites.len()),
            )
        } else {
            value(Cell::Gap, gaps.join("; "), String::new())
        }
    }

    /// The cell for `op` on a backend's base column.
    pub fn cell(&self, op: &str, backend: Backend) -> CellValue {
        self.cell_in(op, &Column::base(backend))
    }

    /// The cell for `op` in `column`. A base kernel's evidence never counts for an extension
    /// column.
    pub fn cell_in(&self, op: &str, column: &Column) -> CellValue {
        if column.wired {
            return self.wired_cell(op);
        }
        let entries: Vec<&KernelRow> = self
            .kernels
            .iter()
            .filter(|k| {
                k.op == op && k.backend == column.backend && k.extensions == column.extensions
            })
            .collect();
        if entries.is_empty() && !column.extensions.is_empty() {
            let candidate = self
                .ops
                .iter()
                .any(|o| o.id == op && o.candidates.contains(column));
            return CellValue {
                cell: if candidate {
                    Cell::Gap
                } else {
                    Cell::NotApplicable
                },
                reason: if candidate {
                    format!("could use {}; no kernel", column.extensions.join("+"))
                } else {
                    String::new()
                },
                evidence: String::new(),
            };
        }
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
            r.kind == kind
                && r.op == k.op
                && r.backend == k.backend
                && r.variant == k.variant
                && r.extensions == k.extensions
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
    pub fn cells(&self) -> Vec<(&OpRow, Vec<(Column, CellValue)>)> {
        let columns = self.columns();
        let mut ops: Vec<&OpRow> = self.ops.iter().collect();
        ops.sort_by(|a, b| (&a.repo, &a.id).cmp(&(&b.repo, &b.id)));
        ops.into_iter()
            .map(|op| {
                let row = columns
                    .iter()
                    .map(|c| (c.clone(), self.cell_in(&op.id, c)))
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
                let (_, gpu) = row
                    .into_iter()
                    .find(|(c, _)| *c == Column::base(Backend::Gpu))?;
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
    const MARKERS: [(&str, &str); 10] = [
        ("core::arch::", "core::arch"),
        ("std::arch::", "std::arch"),
        ("#[target_feature", "#[target_feature]"),
        ("asm!(", "asm!"),
        ("multiversion! {", "multiversion!"),
        ("create_compute_pipeline", "a compute pipeline"),
        ("create_render_pipeline", "a render pipeline"),
        ("FragmentPass::new(", "a fragment pass"),
        ("FragmentPass::with_device(", "a fragment pass"),
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

#[cfg(test)]
mod tests;
