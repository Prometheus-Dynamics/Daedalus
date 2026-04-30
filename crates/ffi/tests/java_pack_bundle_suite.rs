use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
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
        "daedalus_java_pack_bundle_suite_{prefix}_{nanos}_{}",
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

fn collect_java_sources(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|s| s.to_str()) == Some("java") {
                out.push(p);
            }
        }
    }
    out
}

#[test]
fn java_pack_bundle_produces_self_contained_bundle() {
    if std::env::var_os("DAEDALUS_TEST_JAVA_BUNDLE").is_none() {
        note_skip("DAEDALUS_TEST_JAVA_BUNDLE not set; skipping");
        return;
    }

    let javac = std::env::var("JAVAC").unwrap_or_else(|_| "javac".to_string());
    let java = std::env::var("JAVA").unwrap_or_else(|_| "java".to_string());
    if !has_cmd(&javac, "--version") || !has_cmd(&java, "-version") {
        note_skip("skipping: java/javac not found");
        return;
    }

    let out_dir = temp_dir("bundle");
    let manifest_path = out_dir.join("demo.manifest.json");
    let out_name = format!("java_pack_bundle_{}", std::process::id());

    // Write a tiny Java "pack script" that calls Plugin.pack(bundle=true) and a runtime node class.
    let pack_main = out_dir.join("PackMain.java");
    let pack_nodes = out_dir.join("PackNodes.java");

    fs::write(
        &pack_nodes,
        r#"
package demo;
public final class PackNodes {
  public static int add(int a, int b) { return a + b; }
}
"#,
    )
    .expect("write PackNodes.java");

    fs::write(
        &pack_main,
        r#"
import daedalus.manifest.ManifestBuilders;
import daedalus.manifest.NodeDef;
import daedalus.manifest.Plugin;
import daedalus.manifest.Types;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class PackMain {{
  public static void main(String[] args) throws Exception {{
    Path manifest = Paths.get(args[0]).toAbsolutePath();
    String outName = args[1];

    Plugin plugin = new Plugin("demo_pack_java");
    plugin.version = "1.0.0";

    NodeDef add = new NodeDef("demo_pack_java:add")
      .javaEntrypoint(".", "demo.PackNodes", "add");
    add.input(ManifestBuilders.port("a", Types.intTy()));
    add.input(ManifestBuilders.port("b", Types.intTy()));
    add.output(ManifestBuilders.port("out", Types.intTy()));
    plugin.register(add);

    plugin.pack(outName, manifest, false, true);
  }}
}}
"#,
    )
    .expect("write PackMain.java");

    let workspace = workspace_root();
    let sdk_dir = workspace.join("crates/ffi/lang/java/sdk");
    let mut sources = collect_java_sources(&sdk_dir);
    sources.push(pack_main.clone());
    sources.push(pack_nodes.clone());

    let mut cmd = Command::new(&javac);
    cmd.arg("-d").arg(&out_dir);
    for src in &sources {
        cmd.arg(src);
    }
    let status = cmd.status().expect("javac compile java pack fixture");
    assert!(status.success(), "javac failed");

    let status = Command::new(&java)
        .env("DAEDALUS_WORKSPACE_ROOT", &workspace)
        .arg("-cp")
        .arg(&out_dir)
        .arg("PackMain")
        .arg(&manifest_path)
        .arg(&out_name)
        .status()
        .expect("run java pack script");
    assert!(status.success(), "java pack script failed");

    let examples = workspace.join("crates/ffi/examples");
    let bundle_dir = examples.join(format!("{out_name}_bundle"));
    let bundled_manifest = bundle_dir.join("manifest.json");
    assert!(bundled_manifest.exists(), "missing {bundled_manifest:?}");
    assert!(
        bundle_dir.join("demo.manifest.json.lock").exists(),
        "missing bundled lockfile"
    );

    let doc: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&bundled_manifest).expect("read manifest"))
            .expect("parse manifest");
    let nodes = doc
        .get("nodes")
        .and_then(|v| v.as_array())
        .expect("nodes array");
    assert!(!nodes.is_empty());
    let cp = nodes[0]
        .get("java_classpath")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        cp.starts_with("_bundle/java/"),
        "unexpected java_classpath={cp}"
    );
    assert!(
        bundle_dir.join(cp).exists(),
        "missing bundled java classpath at {}",
        bundle_dir.join(cp).display()
    );

    // Cleanup artifacts written into the repo.
    let _ = std::fs::remove_file(examples.join(format!("{out_name}.rs")));
    let _ = std::fs::remove_dir_all(&bundle_dir);
}
