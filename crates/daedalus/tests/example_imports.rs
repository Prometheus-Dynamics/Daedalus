use std::fs;
use std::path::{Path, PathBuf};

fn collect_rs_files(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("read examples dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_rs_files(&path, files);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

#[test]
fn examples_use_only_facade() {
    let root: PathBuf = [env!("CARGO_MANIFEST_DIR"), "..", "..", "examples"]
        .iter()
        .collect();
    let mut examples = Vec::new();
    collect_rs_files(&root, &mut examples);
    let mut violations = Vec::new();

    for path in examples {
        let is_ffi_example = path
            .components()
            .any(|component| component.as_os_str().to_str() == Some("08_ffi"));
        let content = fs::read_to_string(&path).expect("read example");
        for (idx, line) in content.lines().enumerate() {
            let l = line.trim();
            // Forbid importing internal crates directly; allow the facade `daedalus` only.
            //
            // FFI examples intentionally exercise the split FFI crates and package builders rather
            // than the end-user graph facade.
            if l.starts_with("use daedalus_") && !is_ffi_example {
                violations.push(format!(
                    "{}:{}: forbidden internal import: {}",
                    path.display(),
                    idx + 1,
                    l
                ));
            }
            if l.contains("crate::") && !is_ffi_example {
                violations.push(format!(
                    "{}:{}: forbidden crate-relative import: {}",
                    path.display(),
                    idx + 1,
                    l
                ));
            }
        }
    }

    if !violations.is_empty() {
        panic!("\n{}", violations.join("\n"));
    }
}
