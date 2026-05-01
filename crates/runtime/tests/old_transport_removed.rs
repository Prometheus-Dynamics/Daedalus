use std::{
    fs, io,
    path::{Path, PathBuf},
};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    if path.is_file() {
        files.push(path.to_path_buf());
        return Ok(());
    }

    for entry in fs::read_dir(path)? {
        let entry_path = entry?.path();
        if entry_path.is_dir() {
            collect_files(&entry_path, files)?;
        } else if entry_path.is_file() {
            files.push(entry_path);
        }
    }

    Ok(())
}

#[test]
fn removed_public_transport_apis_do_not_reappear() {
    let root = repo_root();
    let files = [
        "crates/runtime/src",
        "crates/macros/src",
        "crates/gpu/src/lib.rs",
        "plugins",
    ];
    let forbidden = [
        "RuntimeValue",
        "CorrelatedValue",
        "register_conversion",
        "register_output_mover",
        "OutputMover",
        "get_any_arc",
        "get_any_raw",
        "pub use convert::{Backing, Compute, DataCell",
    ];

    let mut matches = Vec::new();

    for rel in files {
        let path = root.join(rel);
        if !path.exists() {
            continue;
        }

        let mut scanned_files = Vec::new();
        collect_files(&path, &mut scanned_files)
            .unwrap_or_else(|err| panic!("scan {}: {err}", path.display()));

        for file in scanned_files {
            let Ok(contents) = fs::read_to_string(&file) else {
                continue;
            };

            for pattern in &forbidden {
                if contents.contains(pattern) {
                    matches.push(format!(
                        "{} contains `{pattern}`",
                        file.strip_prefix(&root).unwrap_or(&file).display()
                    ));
                }
            }
        }
    }

    assert!(
        matches.is_empty(),
        "removed APIs are present:\n{}",
        matches.join("\n")
    );
}
