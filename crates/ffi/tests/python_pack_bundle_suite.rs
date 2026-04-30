use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

fn workspace_root() -> PathBuf {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.pop(); // crates/ffi
    root.pop(); // crates
    root
}

fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "daedalus_python_pack_bundle_suite_{prefix}_{nanos}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn has_cmd(cmd: &str, arg: &str) -> bool {
    Command::new(cmd).arg(arg).output().is_ok()
}

fn note_skip(message: &str) {
    let _ = writeln!(std::io::stderr(), "{message}");
}

#[test]
fn python_pack_bundle_embeds_py_path() {
    if std::env::var_os("DAEDALUS_TEST_PY_BUNDLE").is_none() {
        note_skip("DAEDALUS_TEST_PY_BUNDLE not set; skipping");
        return;
    }

    let python = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
    if !has_cmd(&python, "--version") {
        note_skip("skipping: python interpreter not found");
        return;
    }

    let out_dir = temp_dir("bundle");
    let script = out_dir.join("demo_pack.py");
    let manifest = out_dir.join("demo.manifest.json");
    let out_name = format!("py_pack_bundle_{}", std::process::id());

    // Add repo root to sys.path so `from daedalus_py import ...` works.
    let repo = workspace_root();
    let repo_str = repo.to_string_lossy();
    fs::write(
        &script,
        format!(
            r#"
import sys
from pathlib import Path

sys.path.insert(0, {repo_str:?})

from daedalus_py import Plugin, node_rs as node

plugin = Plugin(name="demo_pack", version="1.0.0")

@node(id="demo_pack:add", py_path=__file__, inputs=("a","b"), outputs=("out",))
def add(a: int, b: int) -> int:
    return a + b

plugin.register(add)
plugin.pack(out_name={out_name:?}, manifest_path={manifest:?}, build=False, bundle=True, lock=False, vendor=False)
"#
        ),
    )
    .expect("write python pack script");

    let status = Command::new(&python)
        .arg(&script)
        .status()
        .expect("run python pack script");
    assert!(status.success());

    let examples = repo.join("crates/ffi/examples");
    let bundle_dir = examples.join(format!("{out_name}_bundle"));
    let bundled_manifest = bundle_dir.join("manifest.json");
    assert!(bundled_manifest.exists(), "missing {bundled_manifest:?}");
    let doc: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&bundled_manifest).expect("read manifest"))
            .expect("parse manifest");
    let nodes = doc
        .get("nodes")
        .and_then(|v| v.as_array())
        .expect("nodes array");
    assert!(!nodes.is_empty());
    let py_path = nodes[0]
        .get("py_path")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        py_path.ends_with("demo_pack.py"),
        "unexpected py_path={py_path}"
    );
    assert!(
        bundle_dir.join(py_path).exists(),
        "missing bundled python file"
    );

    let _ = std::fs::remove_file(examples.join(format!("{out_name}.rs")));
    let _ = std::fs::remove_dir_all(&bundle_dir);
}
