use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
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
    for rel in files {
        let path = root.join(rel);
        if !path.exists() {
            continue;
        }
        for pattern in forbidden {
            let output = std::process::Command::new("rg")
                .arg("-F")
                .arg(pattern)
                .arg(&path)
                .output()
                .expect("run rg");
            assert!(
                !output.status.success(),
                "removed API `{pattern}` is present:\n{}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
    }
}
