//! Stable Rust does not expose unstable target features (ARMv7 `neon`, RISC-V `v`) to
//! `cfg(target_feature)`, even when `-C target-feature=+neon` / `+v` enables them for codegen.
//! Read the flags and set `daedalus_neon32` / `daedalus_rvv` instead.
fn main() {
    println!("cargo::rustc-check-cfg=cfg(daedalus_neon32)");
    println!("cargo::rustc-check-cfg=cfg(daedalus_rvv)");
    println!("cargo::rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    // Each `target-feature` list ("+neon,+vfp4" or "-C target-feature=+v"), in order: the last
    // mention of a feature wins.
    let enabled = |feature: &str| {
        let mut on = false;
        for flag in flags.split('\x1f') {
            let list = flag
                .strip_prefix("-Ctarget-feature=")
                .or_else(|| flag.strip_prefix("target-feature="))
                .unwrap_or("");
            for item in list.split(',') {
                if item == format!("+{feature}") {
                    on = true;
                } else if item == format!("-{feature}") {
                    on = false;
                }
            }
        }
        on
    };
    if arch == "arm" && enabled("neon") {
        println!("cargo::rustc-cfg=daedalus_neon32");
    }
    if (arch == "riscv64" || arch == "riscv32") && enabled("v") {
        println!("cargo::rustc-cfg=daedalus_rvv");
    }
}
