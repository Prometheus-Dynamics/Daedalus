//! The CPU rows point at files and tests that exist, and the registry dump is written when
//! `ACCEL_DUMP` names a path (the renderer reads it with the evidence).

use std::path::{Path, PathBuf};

use daedalus_accel::Status;
use daedalus_inventory::{CPU, KERNEL_ROWS, OPS};

/// The repository root, from this crate's directory (`testing/crates/inventory`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

#[test]
fn registry_rows_point_at_existing_files_and_tests() {
    let root = repo_root();
    let mut errors = Vec::new();
    for row in KERNEL_ROWS {
        if !OPS.iter().any(|op| op.id == row.op) {
            errors.push(format!(
                "{} on {}: op is not declared",
                row.op,
                row.backend.name()
            ));
        }
        if !matches!(row.status, Status::Implemented) {
            continue;
        }
        let source = row
            .source
            .rsplit_once(':')
            .map_or(row.source, |(path, line)| {
                if line.chars().all(|c| c.is_ascii_digit()) {
                    path
                } else {
                    row.source
                }
            });
        if !source.starts_with("backends/kernels-sys/") && !source.starts_with("backends/cpu/") {
            errors.push(format!("{}: {source} is outside the CPU crates", row.op));
        }
        if !root.join(source).is_file() {
            errors.push(format!("{}: missing source {source}", row.op));
        }
        let Some((file, name)) = row.test.split_once("::") else {
            errors.push(format!("{}: test {} has no file", row.op, row.test));
            continue;
        };
        match std::fs::read_to_string(root.join(file)) {
            Ok(text) if text.contains(&format!("fn {name}(")) => {}
            Ok(_) => errors.push(format!("{}: no test fn {name} in {file}", row.op)),
            Err(e) => errors.push(format!("{}: {file}: {e}", row.op)),
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn every_cpu_op_has_a_scalar_row() {
    for op in OPS {
        assert!(
            KERNEL_ROWS
                .iter()
                .any(|row| row.op == op.id && row.backend == daedalus_accel::Backend::Scalar),
            "{} has no scalar row",
            op.id
        );
    }
}

/// Writes the registry for `accel-matrix render --registry` when `ACCEL_DUMP` is set.
#[test]
fn registry_dump() {
    if let Some(path) = std::env::var_os("ACCEL_DUMP") {
        daedalus_accel::dump(path, &[&CPU]).expect("write the registry dump");
    }
    assert!(daedalus_accel::dump_json(&[&CPU]).contains("daedalus.codec.base64_decode"));
}
