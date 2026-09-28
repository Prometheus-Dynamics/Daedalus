//! Captures toolchain facts that dynamic plugin loading compares between host and plugin.

use std::io::Write;
use std::process::Command;

fn main() {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let version = Command::new(&rustc)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "cargo:rustc-env=DAEDALUS_RUSTC_VERSION={version}");
    let _ = writeln!(out, "cargo:rustc-env=DAEDALUS_BUILD_TARGET={target}");
    let _ = writeln!(out, "cargo:rerun-if-env-changed=RUSTC");
    let _ = writeln!(out, "cargo:rerun-if-changed=build.rs");
}
