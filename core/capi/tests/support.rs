//! Shared by the surface's tests: the plugin fixture, built once, and a graph over it.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// The fixture plugin, built by cargo into the test target directory. Its node
/// `fixture.pass` forwards a `u32`, and `fixture.boom` panics.
pub fn fixture_library() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let source = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../testing/fixtures/dylib/Cargo.toml");
            let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("dylib-fixture");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let status = Command::new(cargo)
                .args(["build", "-q", "--manifest-path"])
                .arg(source)
                .arg("--target-dir")
                .arg(&target)
                .status()
                .expect("cargo runs");
            assert!(status.success(), "the dylib fixture builds");
            target.join("debug").join(format!(
                "{}daedalus_dylib_fixture{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            ))
        })
        .clone()
}

/// A host port `in` into `node`, and its output to the host port `out`, all `u32`.
pub fn graph_json(node: &str) -> String {
    format!(
        r#"{{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {{
    "nodes": [
      {{"id": "host", "kind": "host_bridge", "port_types": {{"in": "u32", "out": "u32"}}}},
      {{"id": "{node}"}}
    ],
    "edges": [
      {{"from": {{"node": 0, "port": "in"}}, "to": {{"node": 1, "port": "in"}}}},
      {{"from": {{"node": 1, "port": "out"}}, "to": {{"node": 0, "port": "out"}}}}
    ]
  }}
}}"#
    )
}

/// The surface as a shared library, which cargo builds beside the tests.
pub fn surface_library() -> PathBuf {
    let exe = std::env::current_exe().expect("the test's own path");
    let deps = exe.parent().expect("the deps directory");
    let name = format!(
        "{}daedalus_capi{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let found = deps.join(&name);
    assert!(
        found.exists(),
        "{} isn't there: the crate builds as a cdylib",
        found.display()
    );
    found
}
