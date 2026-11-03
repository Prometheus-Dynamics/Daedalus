//! Exposes the enabled Cargo features as `ENABLED_FEATURES` (dynamic plugin build fingerprint;
//! see `daedalus_core::build_facts!`).
//!
//! Shared build script: each crate's `build.rs` is a copy of this file (a `build` path outside the
//! crate root cannot be packaged, and copies need no `core.symlinks` on Windows).
//! `scripts/check-shared-sources.sh` checks the copies; `--fix` rewrites them from this file.

fn main() {
    let features = std::env::var("CARGO_CFG_FEATURE").unwrap_or_default();
    println!("cargo:rustc-env=DAEDALUS_ENABLED_FEATURES={features}");
    println!("cargo:rerun-if-changed=build.rs");
}
