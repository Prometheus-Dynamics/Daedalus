//! Link layout for the Raspberry Pi Pico (`thumbv6m-none-eabi`): the RP2040's 256-byte second
//! stage bootloader at the start of flash, then the firmware; 2 MiB flash, 256 KiB RAM.

use std::{env, fs, path::PathBuf};

const MEMORY: &str = "MEMORY
{
  BOOT2 : ORIGIN = 0x10000000, LENGTH = 0x100
  FLASH : ORIGIN = 0x10000100, LENGTH = 2048K - 0x100
  RAM : ORIGIN = 0x20000000, LENGTH = 256K
}

EXTERN(BOOT2_FIRMWARE)

SECTIONS {
  .boot2 ORIGIN(BOOT2) :
  {
    KEEP(*(.boot2));
  } > BOOT2
} INSERT BEFORE .text;
";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(pico)");
    if env::var("TARGET").as_deref() == Ok("thumbv6m-none-eabi") {
        println!("cargo:rustc-cfg=pico");
        let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
        fs::write(out.join("memory.x"), MEMORY).expect("write memory.x");
        println!("cargo:rustc-link-search={}", out.display());
        println!("cargo:rustc-link-arg-bins=-Tlink.x");
        println!("cargo:rustc-link-arg-bins=-Tdefmt.x");
    }
}
