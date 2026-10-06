//! Exports the enabled features for `daedalus::crate_build_info!()`: the plugin registers its
//! crate build, so a host names the exact feature difference of a separately built copy.

fn main() {
    let features = std::env::var("CARGO_CFG_FEATURE").unwrap_or_default();
    println!("cargo:rustc-env=DAEDALUS_CRATE_FEATURES={features}");
}
