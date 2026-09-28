//! Exposes the enabled Cargo features as `ENABLED_FEATURES` (dynamic plugin build fingerprint).

fn main() {
    let features = std::env::var("CARGO_CFG_FEATURE").unwrap_or_default();
    println!("cargo:rustc-env=DAEDALUS_ENABLED_FEATURES={features}");
    println!("cargo:rerun-if-changed=build.rs");
}
