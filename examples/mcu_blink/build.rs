//! Plans `graph.json` on the host with the Daedalus planner and writes the device module to
//! `$OUT_DIR/graph.rs`; for bare-metal targets also provides the linker memory layout.

use std::{env, fs, path::PathBuf};

use daedalus_mcu_blink_nodes::{blink, lowpass, rising, scale, threshold};

/// A generic 128 KiB flash / 32 KiB RAM part (an STM32F4- or G0-class layout): enough to link and
/// measure the firmware; adapt it to the board when flashing.
const MEMORY: &str = "MEMORY
{
  FLASH : ORIGIN = 0x08000000, LENGTH = 128K
  RAM : ORIGIN = 0x20000000, LENGTH = 32K
}
";

fn main() {
    let nodes = [
        scale::NODE,
        lowpass::NODE,
        threshold::NODE,
        rising::NODE,
        blink::NODE,
    ];
    let json = fs::read_to_string("graph.json").expect("read graph.json");
    let source = daedalus_mcu_build::compile_document(&json, &nodes, &Default::default())
        .unwrap_or_else(|err| panic!("{err}"));
    daedalus_mcu_build::write_out_dir("graph.rs", &source).expect("write graph.rs");
    println!("cargo:rerun-if-changed=graph.json");

    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("none") {
        let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
        fs::write(out.join("memory.x"), MEMORY).expect("write memory.x");
        println!("cargo:rustc-link-search={}", out.display());
        println!("cargo:rustc-link-arg-bins=-Tlink.x");
    }
}
