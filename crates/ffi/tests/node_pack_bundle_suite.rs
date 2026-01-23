use std::fs;
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
        "daedalus_node_pack_bundle_suite_{prefix}_{nanos}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn has_cmd(cmd: &str, arg: &str) -> bool {
    Command::new(cmd).arg(arg).output().is_ok()
}

fn ensure_node_tools() {
    let node = std::env::var("NODE").unwrap_or_else(|_| "node".to_string());
    if !has_cmd(&node, "--version") {
        eprintln!("skipping: node interpreter not found");
        return;
    }
    let npm = std::env::var("NPM").unwrap_or_else(|_| "npm".to_string());
    if !has_cmd(&npm, "--version") {
        eprintln!("skipping: npm not found");
        return;
    }

    let pkg_dir = workspace_root().join("crates/ffi/lang/node/daedalus_node");
    let node_modules = pkg_dir.join("node_modules");
    if node_modules.exists() {
        return;
    }

    let status = Command::new(&npm)
        .current_dir(&pkg_dir)
        .arg("install")
        .arg("--no-audit")
        .arg("--no-fund")
        .status()
        .expect("run npm install for daedalus_node");
    assert!(status.success());
}

#[test]
fn node_pack_bundle_produces_self_contained_entry() {
    if std::env::var_os("DAEDALUS_TEST_NODE_BUNDLE").is_none() {
        eprintln!("DAEDALUS_TEST_NODE_BUNDLE not set; skipping");
        return;
    }

    let node = std::env::var("NODE").unwrap_or_else(|_| "node".to_string());
    if !has_cmd(&node, "--version") {
        eprintln!("skipping: node interpreter not found");
        return;
    }
    ensure_node_tools();

    let out_dir = temp_dir("bundle");
    let dep = out_dir.join("dep.mjs");
    let entry = out_dir.join("entry.mjs");
    fs::write(&dep, "export function add(a,b){ return a+b; }\n").expect("write dep");
    fs::write(
        &entry,
        "import { add } from './dep.mjs';\nexport function add_defaults(a,b){ return add(a,b); }\n",
    )
    .expect("write entry");

    let workspace = workspace_root();
    let index_mjs = workspace.join("crates/ffi/lang/node/daedalus_node/index.mjs");
    let index_url = format!("file://{}", index_mjs.to_string_lossy().replace('\\', "/"));

    let out_name = format!("node_pack_bundle_{}", std::process::id());
    let manifest_path = out_dir.join("demo.manifest.json");

    let script = out_dir.join("pack.mjs");
    fs::write(
        &script,
        format!(
            r#"
import process from "node:process";
import {{ Plugin, NodeDef, t }} from "{index_url}";

process.chdir({workspace:?});

const plugin = new Plugin({{ name: "demo_pack", version: "0.1.1" }});
plugin.register(new NodeDef({{
  id: "demo_pack:add_defaults",
  js_path: {entry:?},
  js_function: "add_defaults",
  inputs: [{{ name: "a", ty: t.int() }}, {{ name: "b", ty: t.int() }}],
  outputs: [{{ name: "out", ty: t.int() }}],
}}));

plugin.pack({{
  out_name: {out_name:?},
  manifest_path: {manifest_path:?},
  build: false,
  bundle: true,
  bundle_deps: true,
}});
"#
        ),
    )
    .expect("write pack script");

    let status = Command::new(&node)
        .arg(&script)
        .status()
        .expect("run node pack script");
    assert!(status.success());

    let bundle_dir = workspace
        .join("crates/ffi/examples")
        .join(format!("{out_name}_bundle"));
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
    let js_path = nodes[0]
        .get("js_path")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        js_path.starts_with("_bundle/js/"),
        "unexpected js_path={js_path}"
    );
    assert!(
        bundle_dir.join(js_path).exists(),
        "missing bundled js entry"
    );

    // Cleanup test artifacts written into the repo.
    let examples = workspace.join("crates/ffi/examples");
    let _ = std::fs::remove_file(examples.join(format!("{out_name}.rs")));
    let _ = std::fs::remove_dir_all(&bundle_dir);
}
