//! Captures the build facts that dynamic plugin loading compares between host and plugin: the
//! rustc version, the target and the enabled Cargo features (`ENABLED_FEATURES`).

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
    let env = |name: &str| std::env::var(name).unwrap_or_default();
    println!("cargo:rustc-env=DAEDALUS_RUSTC_VERSION={version}");
    println!("cargo:rustc-env=DAEDALUS_BUILD_TARGET={}", env("TARGET"));
    println!(
        "cargo:rustc-env=DAEDALUS_ENABLED_FEATURES={}",
        env("CARGO_CFG_FEATURE")
    );
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-changed=build.rs");
}
