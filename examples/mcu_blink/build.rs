//! Bare-metal link layout only. The graph is not planned here: the planner's output is checked in
//! under `generated/` (`cargo run -p daedalus-mcu-blink --example generate` regenerates it, and
//! `tests/generated.rs` checks it). A build script that planned would compile the planner and
//! runtime a second time for the host, on every build of the firmware.

use std::{env, fs, path::PathBuf};

/// A generic 128 KiB flash / 32 KiB RAM part (an STM32F4- or G0-class layout): enough to link and
/// measure the firmware; adapt it to the board when flashing.
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
