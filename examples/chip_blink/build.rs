//! The link layout, for bare metal. The graph isn't planned here: a build script that planned
//! would compile the planner for the host on every firmware build. `generated/graph.rs` is
//! checked in, `nodes/examples/generate.rs` rewrites it and `tests/generated.rs` checks it.

use std::{env, fs, path::PathBuf};

/// A generic part with 128 KiB of flash and 32 KiB of RAM: enough to link and measure.
const MEMORY: &str = "MEMORY
{
  FLASH : ORIGIN = 0x08000000, LENGTH = 128K
  RAM : ORIGIN = 0x20000000, LENGTH = 32K
}
";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("none") {
        let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
        fs::write(out.join("memory.x"), MEMORY).expect("write memory.x");
        println!("cargo:rustc-link-search={}", out.display());
        println!("cargo:rustc-link-arg-bins=-Tlink.x");
    }
}
