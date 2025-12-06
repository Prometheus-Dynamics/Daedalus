use std::fs;
use std::path::PathBuf;

#[test]
fn examples_use_only_facade() {
    let root: PathBuf = [env!("CARGO_MANIFEST_DIR"), "examples"].iter().collect();
    let entries = fs::read_dir(&root).expect("read examples dir");
    let mut violations = Vec::new();

    for entry in entries {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        let content = fs::read_to_string(&path).expect("read example");
        for (idx, line) in content.lines().enumerate() {
            let l = line.trim();
            // Forbid importing internal crates directly; allow the facade `daedalus` only.
            if l.starts_with("use daedalus_") {
                violations.push(format!(
                    "{}:{}: forbidden internal import: {}",
                    path.display(),
                    idx + 1,
                    l
                ));
            }
            if l.contains("crate::") {
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
