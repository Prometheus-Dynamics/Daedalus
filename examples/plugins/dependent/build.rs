//! Exports the enabled features for `#[plugin(.., crate_build)]` (`daedalus::crate_build_info!()`).

fn main() {
    let features = std::env::var("CARGO_CFG_FEATURE").unwrap_or_default();
    println!("cargo:rustc-env=DAEDALUS_CRATE_FEATURES={features}");
}
